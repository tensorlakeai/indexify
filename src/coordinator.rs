use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    time::SystemTime,
    vec,
};

use anyhow::{anyhow, Result};
use futures::Stream;
use indexify_internal_api::{
    self as internal_api,
    ContentMetadata,
    ExtractionGraphLink,
    ExtractionPolicy,
};
use indexify_proto::indexify_coordinator::{self, CreateContentStatus, HeartbeatResponse};
use internal_api::{
    ChangeType,
    ContentMetadataId,
    ExtractionGraph,
    GarbageCollectionTask,
    OutputSchema,
    ServerTaskType,
    StateChange,
    StateChangeId,
    StructuredDataSchema,
};
use tokio::sync::{broadcast, watch::Receiver};
use tracing::{debug, info, warn};

use crate::{
    api::NewContentStreamStart,
    coordinator_client::CoordinatorClient,
    coordinator_service::EXECUTOR_HEARTBEAT_PERIOD,
    forwardable_coordinator::ForwardableCoordinator,
    garbage_collector::GarbageCollector,
    metrics::Timer,
    scheduler::Scheduler,
    state::{
        store::{new_content_stream, requests::StateChangeProcessed, ExecutorId, FilterResponse},
        RaftMetrics,
        SharedState,
    },
    task_allocator::TaskAllocator,
    utils,
};

pub type ContentStream = Pin<Box<dyn Stream<Item = Result<ContentMetadata>> + Send + Sync>>;

pub struct Coordinator {
    pub shared_state: SharedState,
    scheduler: Scheduler,
    garbage_collector: Arc<GarbageCollector>,
    forwardable_coordinator: ForwardableCoordinator,
    /// Executors registered on this node.
    pub my_executors: std::sync::Mutex<HashSet<ExecutorId>>,

    /// All executors registered on the cluster.
    pub all_executors: std::sync::Mutex<HashMap<ExecutorId, SystemTime>>,
}

impl Coordinator {
    pub fn new(
        shared_state: SharedState,
        coordinator_client: CoordinatorClient,
        garbage_collector: Arc<GarbageCollector>,
    ) -> Arc<Self> {
        let task_allocator = TaskAllocator::new(shared_state.clone());
        let scheduler = Scheduler::new(shared_state.clone(), task_allocator);
        let forwardable_coordinator = ForwardableCoordinator::new(coordinator_client);
        Arc::new(Self {
            shared_state,
            scheduler,
            garbage_collector,
            forwardable_coordinator,
            my_executors: std::sync::Mutex::new(HashSet::new()),
            all_executors: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub async fn subscribe_to_new_tasks(&self, executor_id: &str) -> broadcast::Receiver<()> {
        self.shared_state.subscribe_to_new_tasks(executor_id).await
    }

    pub async fn delete_extraction_graph(
        &self,
        namespace: String,
        extraction_graph: String,
    ) -> Result<()> {
        let graph = self
            .shared_state
            .get_extraction_graphs_by_name(&namespace, &[&extraction_graph])?;
        let graph = match graph.first() {
            Some(Some(graph)) => graph,
            _ => return Err(anyhow!("extraction graph not found")),
        };
        let gc_task = self
            .garbage_collector
            .create_delete_index_task(&graph)
            .await;
        self.shared_state
            .delete_extraction_graph(namespace, extraction_graph, gc_task)
            .await
    }

    pub fn new_content_stream(&self, start: NewContentStreamStart) -> ContentStream {
        new_content_stream(self.shared_state.state_machine.clone(), start)
    }

    pub async fn add_graph_to_content(
        &self,
        namespace: String,
        extraction_graph: String,
        content_ids: Vec<String>,
    ) -> Result<()> {
        self.shared_state
            .add_graph_to_content(namespace, extraction_graph, content_ids)
            .await
    }

    pub async fn get_extraction_graph_links(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Vec<indexify_coordinator::ExtractionGraphLink>> {
        self.shared_state
            .get_extraction_graph_links(namespace, graph_name)
            .await
    }

    pub async fn link_graphs(&self, link: ExtractionGraphLink) -> Result<()> {
        self.shared_state.link_graphs(link).await
    }

    pub fn get_locked_my_executors(&self) -> std::sync::MutexGuard<HashSet<String>> {
        self.my_executors.lock().unwrap()
    }

    pub fn get_locked_all_executors(&self) -> std::sync::MutexGuard<HashMap<String, SystemTime>> {
        self.all_executors.lock().unwrap()
    }

    pub async fn run_executor_heartbeat(&self, mut shutdown: Receiver<()>) {
        let mut watcher = self.get_leader_change_watcher();
        let mut interval = tokio::time::interval(EXECUTOR_HEARTBEAT_PERIOD);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    break;
                }
                _ = interval.tick() => {
                    let _ = self.executor_heartbeat().await;
                }
                _ = watcher.changed() => {
                    // If this node becomes leader, reset last heartbeat values for
                    // all executors with current time.
                    let is_leader = *watcher.borrow_and_update();
                    if !is_leader {
                        self.get_locked_all_executors().clear();
                    }
                }
            }
        }
    }

    // If this node is follower, update the leader with the state of the executors
    // registered on this node. If this node is leader, process list of
    // all executors in the cluster and remove the stale executors.
    async fn executor_heartbeat(&self) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            let leader_coord_addr = format!("http://{}", leader_coord_addr);
            let my_executors = self.get_locked_my_executors().clone();
            self.forwardable_coordinator
                .executors_heartbeat(&leader_coord_addr, my_executors)
                .await?;
            return Ok(());
        }
        let remove_executors: Vec<_> = {
            let state_executors: HashSet<ExecutorId> = self
                .shared_state
                .get_executors()
                .await?
                .into_iter()
                .map(|e| e.id)
                .collect();
            let my_executors = self.get_locked_my_executors();
            let mut executors = self.get_locked_all_executors();
            let now = SystemTime::now();
            for executor_id in state_executors.iter() {
                if !executors.contains_key(executor_id) {
                    executors.insert(executor_id.clone(), now);
                }
            }
            for executor_id in my_executors.iter() {
                executors.insert(executor_id.clone(), now);
            }
            let mut deleted_executors = Vec::new();
            for executor_id in executors.keys() {
                if !state_executors.contains(executor_id) {
                    deleted_executors.push(executor_id.clone());
                }
            }
            for executor_id in deleted_executors {
                executors.remove(&executor_id);
            }
            executors
                .iter()
                .filter_map(|(executor_id, last_heartbeat)| {
                    match now.duration_since(*last_heartbeat) {
                        Ok(d) if d > 3 * EXECUTOR_HEARTBEAT_PERIOD => Some(executor_id.clone()),
                        _ => None,
                    }
                })
                .collect()
        };
        for executor_id in remove_executors {
            warn!("removing stale executor: {}", executor_id);
            self.shared_state.remove_executor(&executor_id).await?;
        }
        Ok(())
    }

    //  START CONVERSION METHODS
    pub fn internal_content_metadata_to_external(
        &self,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let mut content_meta_list = Vec::new();
        for content in content_list {
            let content: indexify_coordinator::ContentMetadata = content.try_into()?;
            content_meta_list.push(content.clone());
        }
        Ok(content_meta_list)
    }

    pub fn external_content_metadata_to_internal(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let mut contents = vec![];
        for content in content_list {
            let content: internal_api::ContentMetadata = content.try_into()?;
            contents.push(content.clone());
        }
        Ok(contents)
    }

    pub async fn list_content<F>(
        &self,
        filter: F,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<internal_api::ContentMetadata>>
    where
        F: Fn(&internal_api::ContentMetadata) -> bool,
    {
        self.shared_state
            .list_content(filter, start_id, limit)
            .await
    }

    pub async fn update_labels(
        &self,
        namespace: &str,
        content_id: &str,
        labels: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        self.shared_state
            .update_labels(namespace, content_id, labels)
            .await
    }

    pub async fn list_active_contents(&self, namespace: &str) -> Result<Vec<String>> {
        self.shared_state
            .state_machine
            .list_active_contents(namespace)
            .await
    }

    pub async fn get_extraction_policy(
        &self,
        namespace: &str,
        extraction_graph: &str,
        extraction_policy: &str,
    ) -> Result<internal_api::ExtractionPolicy> {
        let id = ExtractionPolicy::create_id(extraction_graph, extraction_policy, namespace);
        self.shared_state.get_extraction_policy(&id).await
    }

    pub async fn update_task(
        &self,
        task_id: &str,
        executor_id: &str,
        outcome: internal_api::TaskOutcome,
    ) -> Result<()> {
        info!(
            "updating task: {}, executor_id: {}, outcome: {:?}",
            task_id, executor_id, outcome
        );
        let mut task = self.shared_state.task_with_id(task_id).await?;
        task.outcome = outcome;
        self.shared_state
            .update_task(task, Some(executor_id.to_string()))
            .await?;
        Ok(())
    }

    pub async fn update_gc_task(
        &self,
        gc_task_id: &str,
        outcome: internal_api::TaskOutcome,
    ) -> Result<()> {
        let mut gc_task = self.shared_state.gc_task_with_id(gc_task_id).await?;
        gc_task.outcome = outcome;
        self.shared_state.update_gc_task(gc_task).await?;
        Ok(())
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        if self.shared_state.namespace_exists(namespace).await? {
            return Ok(());
        }
        self.shared_state.create_namespace(namespace).await
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.shared_state.list_namespaces().await
    }

    pub async fn list_extraction_graphs(&self, namespace: &str) -> Result<Vec<ExtractionGraph>> {
        self.shared_state.list_extraction_graphs(namespace).await
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        self.shared_state.list_extractors().await
    }

    pub async fn heartbeat(
        &self,
        executor_id: &str,
        max_pending_tasks: u64,
    ) -> Result<HeartbeatResponse> {
        self.get_locked_my_executors()
            .insert(executor_id.to_string());

        let tasks = self
            .shared_state
            .tasks_for_executor(executor_id, Some(max_pending_tasks))
            .await?;
        let tasks = tasks
            .into_iter()
            .map(|task| -> Result<indexify_coordinator::Task> { task.try_into() })
            .collect::<Result<Vec<_>>>()?;
        Ok(HeartbeatResponse {
            executor_id: executor_id.to_string(),
            tasks,
        })
    }

    pub async fn all_task_assignments(&self) -> Result<HashMap<String, String>> {
        self.shared_state.task_assignments().await
    }

    pub async fn list_state_changes(&self) -> Result<Vec<internal_api::StateChange>> {
        self.shared_state.list_state_changes().await
    }

    pub async fn list_tasks<F>(
        &self,
        filter: F,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<indexify_coordinator::ListTasksResponse>
    where
        F: Fn(&internal_api::Task) -> bool,
    {
        let response = self
            .shared_state
            .list_tasks(filter, start_id, limit)
            .await?;
        response.try_into()
    }

    pub async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        info!("removing executor: {}", executor_id);
        self.get_locked_my_executors().remove(executor_id);
        self.shared_state.remove_executor(executor_id).await?;
        Ok(())
    }

    pub async fn list_indexes(&self, namespace: &str) -> Result<Vec<internal_api::Index>> {
        self.shared_state.list_indexes(namespace).await
    }

    pub async fn get_index(&self, namespace: &str, name: &str) -> Result<internal_api::Index> {
        let mut s = DefaultHasher::new();
        namespace.hash(&mut s);
        name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        self.shared_state.get_index(&id).await
    }

    pub async fn update_indexes_state(&self, indexes: Vec<internal_api::Index>) -> Result<()> {
        self.shared_state.set_indexes(indexes).await
    }

    pub async fn get_extractor_coordinates(&self, extractor_name: &str) -> Result<Vec<String>> {
        let executors = self
            .shared_state
            .get_executors_for_extractor(extractor_name)
            .await?;
        let addresses = executors
            .iter()
            .map(|e| e.addr.clone())
            .collect::<Vec<String>>();
        Ok(addresses)
    }

    pub async fn register_executor(
        &self,
        addr: &str,
        executor_id: &str,
        extractors: Vec<internal_api::ExtractorDescription>,
    ) -> Result<()> {
        let _ = self
            .shared_state
            .register_executor(addr, executor_id, extractors)
            .await;
        Ok(())
    }

    pub async fn register_ingestion_server(&self, ingestion_server_id: &str) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            let leader_coord_addr = format!("http://{}", leader_coord_addr);
            self.forwardable_coordinator
                .register_ingestion_server(&leader_coord_addr, ingestion_server_id)
                .await?;
            return Ok(());
        }
        self.garbage_collector
            .register_ingestion_server(ingestion_server_id)
            .await;
        Ok(())
    }

    pub async fn remove_ingestion_server(&self, ingestion_server_id: &str) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_node_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_node_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            self.forwardable_coordinator
                .remove_ingestion_server(&leader_coord_addr, ingestion_server_id)
                .await?;
            return Ok(());
        }
        self.garbage_collector
            .remove_ingestion_server(ingestion_server_id)
            .await;
        Ok(())
    }

    pub async fn get_content_metadata(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let content = self
            .shared_state
            .get_content_metadata_batch(content_ids)
            .await?;
        let content = self.internal_content_metadata_to_external(content)?;
        Ok(content)
    }

    pub async fn get_task(&self, task_id: &str) -> Result<indexify_coordinator::Task> {
        let task = self.shared_state.task_with_id(task_id).await?;
        task.try_into()
    }

    pub async fn get_task_and_root_content(
        &self,
        task_id: &str,
    ) -> Result<(internal_api::Task, Option<internal_api::ContentMetadata>)> {
        let task = self.shared_state.task_with_id(task_id).await?;
        let mut root_content = None;
        if let Some(root_content_id) = &task.content_metadata.root_content_id {
            let root_cm = self
                .shared_state
                .get_content_metadata_batch(vec![root_content_id.clone()])
                .await?;
            if let Some(root_cm) = root_cm.first() {
                root_content.replace(root_cm.clone());
            }
        }
        Ok((task, root_content))
    }

    pub async fn get_content_tree_metadata(
        &self,
        content_id: &str,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let content_tree = self.shared_state.get_content_tree_metadata(content_id)?;
        let content_tree = self.internal_content_metadata_to_external(content_tree)?;
        Ok(content_tree)
    }

    pub fn get_extractor(
        &self,
        extractor_name: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        self.shared_state.extractor_with_name(extractor_name)
    }

    pub async fn create_extraction_graph(
        &self,
        extraction_graph: ExtractionGraph,
    ) -> Result<Vec<internal_api::Index>> {
        let mut structured_data_schema =
            StructuredDataSchema::new(&extraction_graph.name, &extraction_graph.namespace);
        let mut indexes_to_create = Vec::new();
        for extraction_policy in &extraction_graph.extraction_policies {
            let extractor = self.get_extractor(&extraction_policy.extractor)?;
            for (output_name, output_schema) in extractor.outputs {
                match output_schema {
                    OutputSchema::Embedding(embeddings) => {
                        let mut index_to_create = internal_api::Index {
                            id: "".to_string(),
                            namespace: extraction_policy.namespace.clone(),
                            name: "".to_string(),
                            table_name: "".to_string(),
                            schema: serde_json::to_value(embeddings).unwrap().to_string(),
                            extraction_policy_name: extraction_policy.name.clone(),
                            extractor_name: extractor.name.clone(),
                            graph_name: extraction_graph.name.clone(),
                            visibility: false,
                        };
                        index_to_create.name = index_to_create.build_name(&output_name);
                        index_to_create.table_name = index_to_create.build_table_name(&output_name);
                        index_to_create.id = index_to_create.id();
                        indexes_to_create.push(index_to_create);
                    }
                    OutputSchema::Attributes(columns) => {
                        structured_data_schema.merge(columns);
                    }
                }
            }
        }
        self.shared_state
            .create_extraction_graph(
                extraction_graph,
                structured_data_schema,
                indexes_to_create.clone(),
            )
            .await?;
        Ok(indexes_to_create)
    }

    pub async fn get_graph_analytics(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Option<indexify_internal_api::ExtractionGraphAnalytics>> {
        self.shared_state
            .get_graph_analytics(namespace, graph_name)
            .await
    }

    pub async fn create_content_tree_tasks(
        &self,
        content_tree: Vec<internal_api::ContentMetadata>,
        state_change: StateChange,
    ) -> Result<()> {
        let mut output_tables = HashMap::new();

        for content_metadata in &content_tree {
            if content_metadata.extraction_policy_ids.keys().len() == 0 {
                continue;
            }
            let applied_extraction_policy_ids: HashSet<String> = content_metadata
                .extraction_policy_ids
                .clone()
                .into_iter()
                .filter(|(_, completion_time)| *completion_time > 0)
                .map(|(extraction_policy_id, _)| extraction_policy_id)
                .collect();

            if applied_extraction_policy_ids.is_empty() {
                continue;
            }
            let applied_extraction_policies = self
                .shared_state
                .get_extraction_policies_from_ids(applied_extraction_policy_ids)
                .await?;
            for applied_extraction_policy in applied_extraction_policies {
                output_tables.insert(
                    content_metadata.id.clone(),
                    applied_extraction_policy
                        .output_table_mapping
                        .values()
                        .cloned()
                        .collect::<HashSet<_>>(),
                );
            }
        }

        let task_type = match state_change.change_type {
            ChangeType::TombstoneContentTree => ServerTaskType::Delete,
            ChangeType::TombstoneContent { .. } => ServerTaskType::DeleteBlobStore,
            _ => ServerTaskType::UpdateLabels,
        };
        let tasks = self
            .garbage_collector
            .create_gc_tasks(content_tree, output_tables, task_type)
            .await?;
        self.shared_state.create_gc_tasks(tasks.clone()).await?;
        self.shared_state
            .mark_change_events_as_processed(vec![state_change], Vec::new())
            .await?;
        Ok(())
    }

    pub async fn create_content_gc_task(
        &self,
        state_change: StateChange,
        is_root: bool,
    ) -> Result<()> {
        let content_id: ContentMetadataId = state_change.object_id.clone().try_into()?;
        let content = if is_root {
            self.shared_state
                .state_machine
                .get_content_by_id_and_version(&content_id)
                .await?
        } else {
            self.shared_state
                .state_machine
                .get_latest_version_of_content(&content_id.id)?
        };
        if let Some(content) = content {
            self.create_content_tree_tasks(vec![content], state_change)
                .await
        } else {
            self.shared_state
                .mark_change_events_as_processed(vec![state_change], Vec::new())
                .await?;
            Ok(())
        }
    }

    pub async fn create_gc_tasks(&self, state_change: StateChange) -> Result<()> {
        let content_id: ContentMetadataId = state_change.object_id.clone().try_into()?;
        let content_tree_metadata = self
            .shared_state
            .get_content_tree_metadata_with_version(&content_id)?;
        self.create_content_tree_tasks(content_tree_metadata, state_change)
            .await
    }

    async fn handle_tombstone_content_state_change(
        &self,
        change: StateChange,
        is_root: bool,
    ) -> Result<()> {
        if self.shared_state.ensure_leader().await?.is_some() {
            Ok(())
        } else {
            self.create_content_gc_task(change, is_root).await
        }
    }

    async fn handle_tombstone_content_tree_state_change(&self, change: StateChange) -> Result<()> {
        if let Some(forward_to_leader) = self.shared_state.ensure_leader().await? {
            let leader_id = forward_to_leader
                .leader_id
                .ok_or_else(|| anyhow::anyhow!("could not get leader node id"))?;
            let leader_coord_addr = self
                .shared_state
                .get_coordinator_addr(leader_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("could not get leader node coordinator address"))?;
            self.forwardable_coordinator
                .create_gc_tasks(&leader_coord_addr, change)
                .await?;
            return Ok(());
        }

        //  this coordinator node is the leader
        self.create_gc_tasks(change).await
    }

    pub async fn handle_extraction_graph_deleted_state_change(
        &self,
        start_content_id: Vec<u8>,
        state_change: StateChange,
    ) -> Result<()> {
        self.shared_state
            .delete_content_by_graph(start_content_id, state_change)
            .await
    }

    async fn handle_content_updated(&self, state_change: StateChange) -> Result<()> {
        let content_tree = self
            .shared_state
            .get_content_tree_metadata(&state_change.object_id)?;
        self.create_content_tree_tasks(content_tree, state_change)
            .await
    }

    async fn handle_task_completion_state_change(
        &self,
        change: StateChange,
        root_content_id: ContentMetadataId,
    ) -> Result<()> {
        let are_content_tasks_completed = self
            .shared_state
            .are_content_tasks_completed(&root_content_id)
            .await;

        if !are_content_tasks_completed {
            return Ok(());
        }

        //  this is the first version of the content, so nothing to garbage collect
        if root_content_id.version <= 1 {
            self.shared_state
                .mark_change_events_as_processed(vec![change], Vec::new())
                .await?;
            return Ok(());
        }
        let previous_version =
            ContentMetadataId::new_with_version(&root_content_id.id, root_content_id.version - 1);
        let content_metadata = self
            .shared_state
            .state_machine
            .get_content_by_id_and_version(&previous_version)
            .await?
            .ok_or(anyhow!("content with id: {} not found", previous_version))?;
        self.shared_state
            .tombstone_content_batch_with_version(
                &[content_metadata.id.clone()],
                vec![StateChangeProcessed {
                    state_change_id: change.id,
                    processed_at: utils::timestamp_secs(),
                }],
            )
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_scheduler(&self) -> Result<()> {
        let _timer = Timer::start(&self.shared_state.metrics.scheduler_invocations);

        let state_changes = self.shared_state.unprocessed_state_change_events().await?;
        for change in state_changes {
            debug!(
                "processing change event: {}, type: {}, id: {}",
                change.id, change.change_type, change.object_id
            );

            match change.change_type {
                ChangeType::TombstoneContentTree => {
                    self.handle_tombstone_content_tree_state_change(change)
                        .await?;
                }
                ChangeType::TombstoneContent { is_root } => {
                    self.handle_tombstone_content_state_change(change, is_root)
                        .await?;
                }
                ChangeType::TaskCompleted {
                    ref root_content_id,
                } => {
                    self.handle_task_completion_state_change(
                        change.clone(),
                        root_content_id.clone(),
                    )
                    .await?;
                }
                ChangeType::ExecutorAdded => self.scheduler.redistribute_tasks(&change).await?,
                ChangeType::NewContent => self.scheduler.create_new_tasks(change).await?,
                ChangeType::AddGraphToContent { .. } => {
                    self.scheduler.create_new_tasks(change).await?
                }
                ChangeType::ExecutorRemoved => {
                    self.scheduler.handle_executor_removed(change).await?
                }
                ChangeType::ContentUpdated => self.handle_content_updated(change).await?,
                ChangeType::ExtractionGraphDeleted {
                    ref start_content_id,
                } => {
                    self.handle_extraction_graph_deleted_state_change(
                        start_content_id.clone(),
                        change,
                    )
                    .await?
                }
            }
        }
        Ok(())
    }

    pub async fn subscribe_to_gc_events(&self) -> broadcast::Receiver<GarbageCollectionTask> {
        self.shared_state.subscribe_to_gc_task_events().await
    }

    pub fn get_state_watcher(&self) -> Receiver<StateChangeId> {
        self.shared_state.get_state_change_watcher()
    }

    pub async fn create_content_metadata(
        &self,
        content_list: Vec<indexify_internal_api::ContentMetadata>,
    ) -> Result<Vec<CreateContentStatus>> {
        self.shared_state.create_content_batch(content_list).await
    }

    pub async fn tombstone_content_metadatas(&self, content_ids: &[String]) -> Result<()> {
        self.shared_state
            .tombstone_content_batch(content_ids)
            .await?;
        Ok(())
    }

    pub async fn get_schema(
        &self,
        namespace: &str,
        content_source: &str,
    ) -> Result<StructuredDataSchema> {
        self.shared_state
            .get_structured_data_schema(namespace, content_source)
            .await
    }

    pub async fn list_schemas(&self, namespace: &str) -> Result<Vec<StructuredDataSchema>> {
        self.shared_state.get_schemas_for_namespace(namespace).await
    }

    pub fn get_leader_change_watcher(&self) -> Receiver<bool> {
        self.shared_state.leader_change_rx.clone()
    }

    pub fn get_raft_metrics(&self) -> RaftMetrics {
        self.shared_state.get_raft_metrics()
    }

    pub async fn wait_content_extraction(&self, content_id: &str) {
        self.shared_state
            .state_machine
            .data
            .indexify_state
            .wait_root_task_count_zero(content_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::Arc,
        time::{Duration, Instant},
        vec,
    };

    use filter::{Expression, LabelsFilter, Operator};
    use futures::{FutureExt, StreamExt};
    use indexify_internal_api::{self as internal_api, ExtractionGraphLink, ExtractionGraphNode};
    use indexify_proto::indexify_coordinator::CreateContentStatus;
    use internal_api::{ContentMetadataId, ContentOffset, ContentSource, TaskOutcome};
    use tokio::time::timeout;

    use super::Coordinator;
    use crate::{
        coordinator::NewContentStreamStart,
        coordinator_client::CoordinatorClient,
        garbage_collector::GarbageCollector,
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{
            complete_task,
            create_content_for_task,
            create_test_extraction_graph,
            create_test_extraction_graph_with_children,
            mock_extractor,
            next_child,
            perform_all_tasks,
            perform_task,
            test_mock_content_metadata,
            Parent::{Child, Root},
            DEFAULT_TEST_NAMESPACE,
        },
        test_utils::RaftTestCluster,
    };

    async fn setup_coordinator() -> (Arc<Coordinator>, Arc<App>) {
        let config = Arc::new(ServerConfig::default());
        let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
        let garbage_collector = GarbageCollector::new();
        let coordinator_client = CoordinatorClient::new(Arc::clone(&config));
        let shared_state = App::new(
            config.clone(),
            None,
            garbage_collector.clone(),
            &config.coordinator_addr,
            Arc::new(crate::metrics::init_provider()),
        )
        .await
        .unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(
            shared_state.clone(),
            coordinator_client,
            garbage_collector,
        );
        (coordinator, shared_state)
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_and_read_extraction_graph() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Register an executor
        let extractor = mock_extractor();
        coordinator
            .register_executor("localhost:8950", "test_executor_id", vec![extractor])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let eg = create_test_extraction_graph("extraction_graph_1", vec!["extraction_policy_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Read the extraction graph back
        let ret_graph =
            shared_state.get_extraction_graphs_by_name(DEFAULT_TEST_NAMESPACE, &[eg.name])?;
        assert!(ret_graph.first().unwrap().is_some());
        assert_eq!(ret_graph.len(), 1);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_content_metadata() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Register an executor
        let extractor = mock_extractor();
        coordinator
            .register_executor("localhost:8950", "test_executor_id", vec![extractor])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let eg = create_test_extraction_graph("extraction_graph_1", vec!["extraction_policy_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        let content_metadata = test_mock_content_metadata("test", "test", &eg.name);

        //  Add content which will trigger task creation
        coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  Read the content back from shared state and ensure graph id is correct
        let retr_content = shared_state
            .get_content_metadata_batch(vec![content_metadata.id.id.clone()])
            .await?;
        assert_eq!(
            retr_content.first().unwrap().extraction_graph_names.len(),
            1
        );
        assert_eq!(
            retr_content
                .first()
                .unwrap()
                .extraction_graph_names
                .first()
                .unwrap(),
            &eg.name
        );
        Ok(())
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_create_and_complete_tasks() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Register an executor
        let executor_id = "test_executor_id";
        let extractor = mock_extractor();
        coordinator
            .register_executor("localhost:8950", executor_id, vec![extractor])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let eg = create_test_extraction_graph("extraction_graph_1", vec!["extraction_policy_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Add content which will trigger task creation because it is part of the graph
        let content_metadata = test_mock_content_metadata("test", "test", &eg.name);
        coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 1);
        coordinator.run_scheduler().await?;

        //  Check that tasks have been created and assigned
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);
        let tasks = shared_state.tasks_for_executor(executor_id, None).await?;
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            shared_state.unprocessed_state_change_events().await?.len(),
            0
        );

        //  Create a separate piece of content metadata which will not trigger task
        // creation
        let content_metadata = test_mock_content_metadata("test2", "test2", "not_present_graph");
        let result = coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await;
        assert!(result.is_err());
        coordinator.run_scheduler().await?;
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);
        let tasks = shared_state.tasks_for_executor(executor_id, None).await?;
        assert_eq!(tasks.len(), 1);

        // FIX ME Should be in state machine test
        let mut task_clone = tasks[0].clone();
        task_clone.outcome = internal_api::TaskOutcome::Success;
        shared_state
            .update_task(task_clone, Some(executor_id.to_string()))
            .await
            .unwrap();
        let tasks = shared_state
            .tasks_for_executor(executor_id, None)
            .await
            .unwrap();
        assert_eq!(0, tasks.clone().len());
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_run_content_through_multiple_extraction_graphs() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Register an executor
        let executor_id_1 = "test_executor_id_1";
        let extractor1 = mock_extractor();
        coordinator
            .register_executor("localhost:8956", executor_id_1, vec![extractor1.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let eg =
            create_test_extraction_graph("extraction_graph_id_1", vec!["extraction_policy_id_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Register another executor
        let executor_id_2 = "test_executor_id_2";
        let mut extractor2 = mock_extractor();
        extractor2.name = "MockExtractor2".to_string();
        coordinator
            .register_executor("localhost:8957", executor_id_2, vec![extractor2.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create another extraction_graph and use the new extractor
        let mut eg2 =
            create_test_extraction_graph("extraction_graph_id_2", vec!["extraction_policy_id_2"]);
        eg2.extraction_policies[0].extractor = extractor2.name.clone();

        coordinator.create_extraction_graph(eg2.clone()).await?;

        //  create some content and specify both graphs
        let mut content_metadata = test_mock_content_metadata("test", "test", &eg2.name);
        content_metadata.extraction_graph_names = vec![eg.name.clone(), eg2.name.clone()];
        coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  expect two tasks to be created and assigned - 1 for each extractor
        let tasks = shared_state.tasks_for_executor(executor_id_1, None).await?;
        assert_eq!(tasks.len(), 1);
        let tasks = shared_state.tasks_for_executor(executor_id_2, None).await?;
        assert_eq!(tasks.len(), 1);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_multiple_contents_and_extraction_policies() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor, executor and associated extraction policy
        let extractor = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        let eg = create_test_extraction_graph("eg_name", vec!["extraction_policy_name_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;

        //  Create content metadata
        let content_metadata_1 = test_mock_content_metadata("test_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![content_metadata_1])
            .await?;

        //  Assert that tasks are created
        coordinator.run_scheduler().await?;
        let tasks = shared_state
            .tasks_for_executor("test_executor_id", None)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);

        //  The second part of the test
        //  Create a different extractor, executor and associated extraction policy
        let mut extractor_2 = mock_extractor();
        extractor_2.name = "MockExtractor2".to_string();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;

        let mut eg2 = create_test_extraction_graph("eg_name_2", vec!["extraction_policy_name_2"]);
        eg2.extraction_policies[0].extractor = extractor_2.name.clone();
        coordinator.create_extraction_graph(eg2.clone()).await?;

        let content_metadata_2 = test_mock_content_metadata("test_id_2", "", &eg2.name);
        coordinator
            .create_content_metadata(vec![content_metadata_2])
            .await?;

        //  Run the scheduler
        coordinator.run_scheduler().await?;
        let tasks = shared_state
            .tasks_for_executor("test_executor_id_2", None)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_leader_redirect() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(5)).await?;

        //  assert that the seed node is the current leader
        cluster.assert_is_leader(cluster.seed_node_id).await;

        //  force leader promotion of node 2
        cluster.force_current_leader_abdication().await?;
        let new_leader_id = 2;
        cluster.promote_node_to_leader(new_leader_id).await?;
        cluster.assert_is_leader(new_leader_id).await;

        //  check leader re-direct
        let alt_node = cluster.get_raft_node(1)?;
        let response = alt_node.check_cluster_membership().await?;
        assert_eq!(response.handled_by, new_leader_id);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_and_read_content_tree() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create two extractors
        let executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor("localhost:8956", executor_id_1, vec![extractor_1.clone()])
            .await?;
        let eg = create_test_extraction_graph("eg_name_1", vec!["ep_policy_name_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;

        let executor_id_2 = "test_executor_id_2";
        let mut extractor_2 = mock_extractor();
        extractor_2.name = "MockExtractor2".to_string();
        coordinator
            .register_executor("localhost:8957", executor_id_2, vec![extractor_2.clone()])
            .await?;

        //  Create an extraction graph with two levels of policies
        // Since both the extraction policy use the same extractors
        // each content will have 2 tasks created for it and assigned to the
        // executor_id_2
        let mut eg2 = create_test_extraction_graph(
            "extraction_graph_id_1",
            vec!["extraction_policy_id_1", "extraction_policy_id_2"],
        );
        eg2.extraction_policies[0].extractor = "MockExtractor2".to_string();
        eg2.extraction_policies[1].extractor = "MockExtractor2".to_string();
        coordinator.create_extraction_graph(eg2.clone()).await?;

        //  Build a content tree where the parent_content is the root
        let content_meta_root = test_mock_content_metadata("test_parent_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![content_meta_root.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  check that tasks have been created for the first level for the first
        // extractor
        let tasks = shared_state.tasks_for_executor(executor_id_1, None).await?;
        assert_eq!(tasks.len(), 1);

        //  Create the children content
        let mut child_content_1 =
            test_mock_content_metadata("test_child_1", &content_meta_root.id.id, &eg2.name);
        child_content_1.parent_id = Some(content_meta_root.id.clone());
        let mut child_content_2 =
            test_mock_content_metadata("test_child_2", &content_meta_root.id.id, &eg2.name);
        child_content_2.parent_id = Some(content_meta_root.id.clone());
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        ////  check that tasks have been created for the second level for the second
        //// extractor
        let tasks = shared_state.tasks_for_executor(executor_id_2, None).await?;
        assert_eq!(tasks.len(), 4);

        ////  Create the final child
        let mut child_child_content_1 =
            test_mock_content_metadata("test_child_child_1", &content_meta_root.id.id, &eg.name);
        child_child_content_1.parent_id = Some(child_content_1.id.clone());
        coordinator
            .create_content_metadata(vec![child_child_content_1.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        let content_tree = coordinator
            .shared_state
            .get_content_tree_metadata(&content_meta_root.id.id)?;
        assert_eq!(content_tree.len(), 4);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_tombstone_content_tree() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor
        let executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor("localhost:8956", executor_id_1, vec![extractor_1.clone()])
            .await?;

        //  Create an extraction graph
        let eg =
            create_test_extraction_graph("extraction_graph_id_1", vec!["extraction_policy_id_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Create a separate extraction graph
        let eg2 =
            create_test_extraction_graph("extraction_graph_id_2", vec!["extraction_policy_id_2"]);
        coordinator.create_extraction_graph(eg2.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent_content is the root
        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;

        let mut child_content_1 =
            test_mock_content_metadata("test_child_id_1", &parent_content.id.id, &eg.name);
        child_content_1.parent_id = Some(parent_content.id.clone());
        let mut child_content_2 =
            test_mock_content_metadata("test_child_id_2", &parent_content.id.id, &eg.name);
        child_content_2.parent_id = Some(parent_content.id.clone());
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;

        let mut child_content_1_child =
            test_mock_content_metadata("test_child_child_id_1", &parent_content.id.id, &eg.name);
        child_content_1_child.parent_id = Some(child_content_1.id.clone());
        coordinator
            .create_content_metadata(vec![child_content_1_child.clone()])
            .await?;

        //  Build a separate content tree where parent_content_2 is the root using the
        // second extraction graph
        let parent_content_2 = test_mock_content_metadata("test_parent_id_2", "", &eg2.name);
        coordinator
            .create_content_metadata(vec![parent_content_2.clone()])
            .await?;

        let mut child_content_2_1 =
            test_mock_content_metadata("test_child_id_2_1", &parent_content_2.id.id, &eg2.name);
        child_content_2_1.parent_id = Some(parent_content_2.id.clone());
        coordinator
            .create_content_metadata(vec![child_content_2_1.clone()])
            .await?;

        coordinator
            .tombstone_content_metadatas(&[
                parent_content.id.id.clone(),
                parent_content_2.id.id.clone(),
            ])
            .await?;

        //  Check that content has been correctly tombstoned
        let content_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&parent_content.id)?;
        let content_tree_2 = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&parent_content_2.id)?;
        for content in &content_tree {
            assert!(
                content.tombstoned,
                "Content {} is not tombstoned",
                content.id.id
            );
        }
        for content in &content_tree_2 {
            assert!(
                content.tombstoned,
                "Content {} is not tombstoned",
                content.id.id
            );
        }

        coordinator.run_scheduler().await?;

        // Check that gc tasks are created.
        let gc_tasks = coordinator.garbage_collector.gc_tasks.read().await;
        assert_eq!(gc_tasks.len(), content_tree.len() + content_tree_2.len());
        let gc_task_ids: Vec<_> = gc_tasks.iter().map(|(id, _)| id.clone()).collect();
        drop(gc_tasks);

        // Check that content is deleted when tasks are completed.
        for task_id in gc_task_ids {
            coordinator
                .update_gc_task(&task_id, TaskOutcome::Success)
                .await?;
        }

        let start = Instant::now();
        let timeout = Duration::new(2, 0);
        let mut success = false;

        while Instant::now().duration_since(start) < timeout {
            let contents = coordinator
                .shared_state
                .state_machine
                .get_content_namespace_table()?;
            match contents.get(DEFAULT_TEST_NAMESPACE) {
                Some(contents) => {
                    if contents.is_empty() {
                        success = true;
                        break;
                    }
                }
                None => {
                    success = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert!(success, "content was not deleted");

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_match_tombstoned_content() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor
        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg =
            create_test_extraction_graph("extraction_graph_id_1", vec!["extraction_policy_id_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent_content is the root
        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;

        let mut child_content_1 =
            test_mock_content_metadata("test_child_id_1", &parent_content.id.id, &eg.name);
        child_content_1.parent_id = Some(parent_content.id.clone());
        let mut child_content_2 =
            test_mock_content_metadata("test_child_id_2", &parent_content.id.id, &eg.name);
        child_content_2.parent_id = Some(parent_content.id.clone());
        child_content_2.source =
            ContentSource::ExtractionPolicyName(eg.extraction_policies[0].name.clone());
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;

        //  before tombstone
        let content = coordinator
            .shared_state
            .state_machine
            .get_latest_version_of_content(&parent_content.id.id)?
            .unwrap();
        let policies_matching_content = coordinator
            .shared_state
            .match_extraction_policies_for_content(&content, &content.extraction_graph_names)
            .await?;
        assert_eq!(policies_matching_content.len(), 1);

        coordinator
            .shared_state
            .tombstone_content_batch(&[parent_content.id.id.clone()])
            .await?;

        //  after tombstone
        let content = coordinator
            .shared_state
            .state_machine
            .get_content_by_id_and_version(&parent_content.id)
            .await?
            .unwrap();
        let policies_matching_content = coordinator
            .shared_state
            .match_extraction_policies_for_content(&content, &content.extraction_graph_names)
            .await?;
        assert_eq!(policies_matching_content.len(), 0);

        Ok(())
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_gc_tasks_creation() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor
        let executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor("localhost:8956", executor_id_1, vec![extractor_1.clone()])
            .await?;

        //  Create an extraction graph
        let eg =
            create_test_extraction_graph("extraction_graph_id_1", vec!["extraction_policy_id_1"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent content id is the root
        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        let mut child_content_1 =
            test_mock_content_metadata("test_child_id_1", &parent_content.id.id, &eg.name);
        child_content_1.parent_id = Some(parent_content.id.clone());
        child_content_1.source =
            ContentSource::ExtractionPolicyName(eg.extraction_policies[0].name.clone());

        let mut child_content_2 =
            test_mock_content_metadata("test_child_id_2", &parent_content.id.id, &eg.name);
        child_content_2.parent_id = Some(parent_content.id.clone());
        child_content_2.source =
            ContentSource::ExtractionPolicyName(eg.extraction_policies[0].name.clone());
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  mark all tasks as completed so that policy mappings are updated
        let tasks = coordinator
            .shared_state
            .tasks_for_executor("test_executor_id_1", None)
            .await?;
        assert_eq!(tasks.len(), 1);
        for task in tasks {
            coordinator
                .update_task(
                    &task.id,
                    "test_executor_id",
                    internal_api::TaskOutcome::Success,
                )
                .await?;
        }

        //  create a state change for tombstoning the content tree
        coordinator
            .tombstone_content_metadatas(&[parent_content.id.id])
            .await?;
        coordinator.run_scheduler().await?;

        let tasks = coordinator.garbage_collector.gc_tasks.read().await;
        assert_eq!(tasks.len(), 3);
        for (_, task) in &*tasks {
            match task.content_id.id.to_string().as_str() {
                "test_parent_id" => {
                    assert!(!task.output_tables.is_empty())
                }
                "test_child_id_1" | "test_child_id_2" => {
                    assert!(task.output_tables.is_empty())
                }
                _ => panic!("Unexpected content_id"),
            }
        }

        Ok(())
    }

    use tokio::select;

    #[tokio::test]
    async fn test_wait_content_extraction() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let extractor = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;

        let eg =
            create_test_extraction_graph("test_extraction_graph", vec!["test_extraction_policy"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        let content = test_mock_content_metadata("test_content_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![content.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        let tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(tasks.len(), 1);
        let task = tasks.first().unwrap();
        let content_id = task.content_metadata.id.id.clone();

        let wait_future = coordinator.wait_content_extraction(&content_id).fuse();

        tokio::pin!(wait_future);

        let mut wait_completed = false;

        select! {
            _ = &mut wait_future => {
                wait_completed = true;
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
            }
        }

        assert!(
            !wait_completed,
            "wait should not complete with task pending"
        );

        perform_all_tasks(&coordinator, "test_executor_id", &mut 1).await?;

        select! {
            _ = &mut wait_future => {
                wait_completed = true;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
            }
        }

        assert!(wait_completed, "wait should complete after task completion");

        Ok(())
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_content_update() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg = create_test_extraction_graph_with_children(
            "test_extraction_graph",
            vec![
                "test_extraction_policy_1",
                "test_extraction_policy_2",
                "test_extraction_policy_3",
                "test_extraction_policy_4",
                "test_extraction_policy_5",
                "test_extraction_policy_6",
            ],
            &[Root, Child(0), Child(0), Child(1), Child(3), Child(3)],
        );
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg.name);
        let create_res = coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);

        let mut child_id = 1;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        // update root content
        let mut parent_content_1 = parent_content.clone();
        parent_content_1.hash = "test_parent_id_1".into();
        let create_res = coordinator
            .create_content_metadata(vec![parent_content_1.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);

        // previous version tree should be moved and no longer be latest
        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                1,
            ))?;
        assert_eq!(prev_tree.len(), 7); // root + 6 children
        assert!(!prev_tree[0].latest);

        // replace all elements in the tree, should have two trees with 7 elements each
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        coordinator.run_scheduler().await?;
        coordinator.run_scheduler().await?;

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                1,
            ))?;
        assert_eq!(prev_tree.len(), 7);
        assert!(!prev_tree[0].latest);

        // the previous tree should be deleted after all tasks for new root are complete
        assert!(prev_tree.iter().all(|c| c.tombstoned));
        assert!(tree.iter().all(|c| !c.tombstoned));

        let tasks = coordinator.shared_state.list_all_gc_tasks().await?;
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.outcome == TaskOutcome::Unknown)
                .count(),
            7
        );

        for task in tasks {
            if task.outcome == TaskOutcome::Unknown {
                coordinator
                    .update_gc_task(&task.id, TaskOutcome::Success)
                    .await?;
            }
        }

        // check if previous tree deleted after gc complete
        coordinator.run_scheduler().await?;
        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                1,
            ))?;
        assert_eq!(prev_tree.len(), 0);
        let tasks = coordinator.shared_state.list_all_gc_tasks().await?;
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.outcome == TaskOutcome::Unknown)
                .count(),
            0
        );

        // update root content and have the first child be identical to previous version
        let mut parent_content_2 = parent_content_1.clone();
        parent_content_2.hash = "test_parent_id_2".into();
        let create_res = coordinator
            .create_content_metadata(vec![parent_content_2.clone()])
            .await?;
        coordinator.run_scheduler().await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);

        let mut child_content =
            create_content_for_task(&coordinator, &all_tasks[0], &next_child(&mut child_id))
                .await?;
        child_content.hash = tree[1].hash.clone();
        let create_res = coordinator
            .create_content_metadata(vec![child_content])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Duplicate);
        complete_task(&coordinator, &all_tasks[0], "test_executor_id_1").await?;

        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        // no new tasks should be created
        assert_eq!(all_tasks.len(), 0);

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);
        assert_eq!(tree[0].id.version, 3);

        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                2,
            ))?;
        // all elements should be transferred to the new root
        assert_eq!(prev_tree.len(), 1);
        assert!(!prev_tree[0].latest);

        coordinator.run_scheduler().await?;
        // the previous tree should be tombstoned after all tasks for new root are
        // complete
        assert!(prev_tree.iter().all(|c| c.tombstoned));
        assert!(tree.iter().all(|c| !c.tombstoned));

        let tasks = coordinator.shared_state.list_all_gc_tasks().await?;
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.outcome == TaskOutcome::Unknown)
                .count(),
            1
        );

        for task in tasks {
            if task.outcome == TaskOutcome::Unknown {
                coordinator
                    .update_gc_task(&task.id, TaskOutcome::Success)
                    .await?;
            }
        }

        // check if previous tree deleted after gc complete
        coordinator.run_scheduler().await?;
        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                2,
            ))?;
        assert_eq!(prev_tree.len(), 0);
        let tasks = coordinator.shared_state.list_all_gc_tasks().await?;
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.outcome == TaskOutcome::Unknown)
                .count(),
            0
        );

        // Update root content and have child in the middle of tree be identical to
        // previous version
        let mut parent_content_3 = parent_content_2.clone();
        parent_content_3.hash = "test_parent_id_3".into();
        let create_res = coordinator
            .create_content_metadata(vec![parent_content_3.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);
        perform_task(
            &coordinator,
            &all_tasks[0],
            &next_child(&mut child_id),
            "test_executor_id_1",
        )
        .await?;
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 2);
        for task in all_tasks {
            perform_task(
                &coordinator,
                &task,
                &next_child(&mut child_id),
                "test_executor_id_1",
            )
            .await?;
        }
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);
        let task = all_tasks.first().unwrap();
        let mut content =
            create_content_for_task(&coordinator, task, &next_child(&mut child_id)).await?;
        let policy = coordinator
            .get_extraction_policy(
                &task.namespace,
                &task.extraction_graph_name,
                &task.extraction_policy_name,
            )
            .await?;
        let prev_content = tree
            .iter()
            .find(|c| c.source == ContentSource::ExtractionPolicyName(policy.name.clone()))
            .unwrap();
        content.hash = prev_content.hash.clone();
        let create_res = coordinator.create_content_metadata(vec![content]).await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Duplicate);
        complete_task(&coordinator, &all_tasks[0], "test_executor_id_1").await?;
        coordinator.run_scheduler().await?;
        // No new task should be created
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 0);

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                3,
            ))?;

        // elements after and including the identical child should be transferred to the
        // new root
        assert_eq!(prev_tree.len(), 4);
        assert!(!prev_tree[0].latest);

        coordinator.run_scheduler().await?;
        // the previous tree should be tombstoned after all tasks for new root are
        // complete
        assert!(prev_tree.iter().all(|c| c.tombstoned));
        assert!(tree.iter().all(|c| !c.tombstoned));

        let tasks = coordinator.shared_state.list_all_gc_tasks().await?;
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.outcome == TaskOutcome::Unknown)
                .count(),
            4
        );

        for task in tasks {
            if task.outcome == TaskOutcome::Unknown {
                coordinator
                    .update_gc_task(&task.id, TaskOutcome::Success)
                    .await?;
            }
        }
        let prev_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&ContentMetadataId::new_with_version(
                "test_parent_id",
                3,
            ))?;
        assert_eq!(prev_tree.len(), 0);

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_graph_to_content() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg_1 = create_test_extraction_graph_with_children(
            "test_extraction_graph_1",
            vec![
                "test_extraction_policy_1",
                "test_extraction_policy_2",
                "test_extraction_policy_3",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_1.clone()).await?;
        coordinator.run_scheduler().await?;

        let eg_2 = create_test_extraction_graph_with_children(
            "test_extraction_graph_2",
            vec![
                "test_extraction_policy_4",
                "test_extraction_policy_5",
                "test_extraction_policy_6",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_2.clone()).await?;
        coordinator.run_scheduler().await?;

        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg_1.name);
        let create_res = coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);

        let mut child_id = 1;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 4);

        coordinator
            .add_graph_to_content(
                DEFAULT_TEST_NAMESPACE.to_string(),
                eg_2.name.clone(),
                vec!["test_parent_id".to_string()],
            )
            .await?;

        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;
        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn test_link_graphs() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg_1 = create_test_extraction_graph_with_children(
            "test_extraction_graph_1",
            vec![
                "test_extraction_policy_1",
                "test_extraction_policy_2",
                "test_extraction_policy_3",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_1.clone()).await?;
        coordinator.run_scheduler().await?;

        let eg_2 = create_test_extraction_graph_with_children(
            "test_extraction_graph_2",
            vec![
                "test_extraction_policy_4",
                "test_extraction_policy_5",
                "test_extraction_policy_6",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_2.clone()).await?;
        coordinator.run_scheduler().await?;

        let eg_3 = create_test_extraction_graph_with_children(
            "test_extraction_graph_3",
            vec![
                "test_extraction_policy_7",
                "test_extraction_policy_8",
                "test_extraction_policy_9",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_3.clone()).await?;
        coordinator.run_scheduler().await?;

        let link = ExtractionGraphLink {
            node: ExtractionGraphNode {
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                graph_name: eg_1.name.clone(),
                source: ContentSource::ExtractionPolicyName(
                    eg_1.extraction_policies[1].name.clone(),
                ),
            },
            graph_name: eg_2.name.clone(),
        };
        coordinator.link_graphs(link).await?;

        let link = ExtractionGraphLink {
            node: ExtractionGraphNode {
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                graph_name: eg_1.name.clone(),
                source: ContentSource::ExtractionPolicyName(
                    eg_1.extraction_policies[2].name.clone(),
                ),
            },
            graph_name: eg_3.name.clone(),
        };
        coordinator.link_graphs(link).await?;

        let parent_content = test_mock_content_metadata("test_parent_id", "", &eg_1.name);
        let create_res = coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);

        let mut child_id = 1;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_link_cycle() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg_1 = create_test_extraction_graph_with_children(
            "test_extraction_graph_1",
            vec![
                "test_extraction_policy_1",
                "test_extraction_policy_2",
                "test_extraction_policy_3",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_1.clone()).await?;
        coordinator.run_scheduler().await?;

        let eg_2 = create_test_extraction_graph_with_children(
            "test_extraction_graph_2",
            vec![
                "test_extraction_policy_4",
                "test_extraction_policy_5",
                "test_extraction_policy_6",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_2.clone()).await?;
        coordinator.run_scheduler().await?;

        let eg_3 = create_test_extraction_graph_with_children(
            "test_extraction_graph_3",
            vec![
                "test_extraction_policy_7",
                "test_extraction_policy_8",
                "test_extraction_policy_9",
            ],
            &[Root, Child(0), Child(0)],
        );
        coordinator.create_extraction_graph(eg_3.clone()).await?;
        coordinator.run_scheduler().await?;

        let link = ExtractionGraphLink {
            node: ExtractionGraphNode {
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                graph_name: eg_1.name.clone(),
                source: ContentSource::ExtractionPolicyName(
                    eg_1.extraction_policies[1].name.clone(),
                ),
            },
            graph_name: eg_1.name.clone(),
        };
        let result = coordinator.link_graphs(link).await;
        assert!(result.is_err(), "Expected linking graph to itself to fail");

        let link = ExtractionGraphLink {
            node: ExtractionGraphNode {
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                graph_name: eg_1.name.clone(),
                source: ContentSource::ExtractionPolicyName(
                    eg_1.extraction_policies[2].name.clone(),
                ),
            },
            graph_name: eg_3.name.clone(),
        };
        coordinator.link_graphs(link).await?;

        let link = ExtractionGraphLink {
            node: ExtractionGraphNode {
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                graph_name: eg_3.name.clone(),
                source: ContentSource::ExtractionPolicyName(
                    eg_3.extraction_policies[2].name.clone(),
                ),
            },
            graph_name: eg_1.name.clone(),
        };

        let result = coordinator.link_graphs(link).await;
        assert!(
            result.is_err(),
            "Expected linking graph creating cycle to fail"
        );

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_policy_filters() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;
        let namespace = "namespace";
        coordinator.create_namespace(namespace).await?;

        //  Create an executor and associated extractor
        let extractor = mock_extractor();
        let executor_id = "executor_id";
        let addr = "addr";
        coordinator
            .register_executor(addr, executor_id, vec![extractor])
            .await?;

        //  Create the extraction policy under the namespace of the content
        let mut eg =
            create_test_extraction_graph("extraction_graph_1", vec!["extraction_policy_1"]);
        eg.extraction_policies[0].filter = LabelsFilter(vec![Expression {
            key: "label1".to_string(),
            value: serde_json::json!("value1"),
            operator: Operator::Eq,
        }]);
        coordinator.create_extraction_graph(eg.clone()).await?;

        //  Create some content
        let content_labels = vec![("label1".to_string(), serde_json::json!("value1"))];
        let mut content_metadata1 = test_mock_content_metadata("content_id_1", "", &eg.name);
        content_metadata1.labels = content_labels.into_iter().collect();
        let content_labels = vec![("label1".to_string(), serde_json::json!("doesn't match"))];
        let mut content_metadata2 = test_mock_content_metadata("content_id_2", "", &eg.name);
        content_metadata2.labels = content_labels.into_iter().collect();
        coordinator
            .create_content_metadata(vec![content_metadata1, content_metadata2])
            .await?;

        coordinator.run_scheduler().await?;

        let tasks = coordinator
            .shared_state
            .tasks_for_executor(executor_id, None)
            .await?;
        let unassigned_tasks = coordinator.shared_state.unassigned_tasks().await?;

        // Check if the task is created for the content that matches the policy
        // and    // non matching content does not have a task.
        assert_eq!(tasks.len() + unassigned_tasks.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_content_stream() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        let extractor = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;

        let eg =
            create_test_extraction_graph("test_extraction_graph", vec!["test_extraction_policy"]);
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        let mut stream =
            coordinator.new_content_stream(NewContentStreamStart::FromOffset(ContentOffset(0)));

        let content = test_mock_content_metadata("test_content_id", "", &eg.name);
        coordinator
            .create_content_metadata(vec![content.clone()])
            .await?;

        let timeout_duration = Duration::from_secs(1);

        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, content.id.id);
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }

        // Should not return items without new contents processed
        match stream.next().now_or_never() {
            Some(stream_content_item) => {
                panic!("Unexpected content in stream: {:?}", stream_content_item);
            }
            None => {}
        }

        let mut child_id = 1;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, "1");
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }

        // new stream should restart from the beginning
        let mut stream =
            coordinator.new_content_stream(NewContentStreamStart::FromOffset(ContentOffset(0)));

        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, content.id.id);
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }
        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, "1");
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }

        // lookup starting from offset 2 should return only the second content
        let mut stream =
            coordinator.new_content_stream(NewContentStreamStart::FromOffset(ContentOffset(1)));
        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, "1");
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }
        match stream.next().now_or_never() {
            Some(stream_content_item) => {
                panic!("Unexpected content in stream: {:?}", stream_content_item);
            }
            None => {}
        }

        // lookup from FromLast should not return any content until new added
        let mut stream = coordinator.new_content_stream(NewContentStreamStart::FromLast);
        match stream.next().now_or_never() {
            Some(stream_content_item) => {
                panic!("Unexpected content in stream: {:?}", stream_content_item);
            }
            None => {}
        }
        let content = test_mock_content_metadata("test_content_id_1", "", &eg.name);
        coordinator
            .create_content_metadata(vec![content.clone()])
            .await?;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, "test_content_id_1");
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }
        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(stream_content_item))) => {
                assert_eq!(stream_content_item.id.id, "2");
            }
            Ok(Some(Err(e))) => {
                panic!("Stream error: {:?}", e);
            }
            Ok(None) => {
                panic!("Stream ended unexpectedly");
            }
            Err(_) => {
                panic!("Stream timed out");
            }
        }
        Ok(())
    }
}
