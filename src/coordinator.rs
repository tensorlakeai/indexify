use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::Arc,
    vec,
};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use internal_api::{
    ContentMetadataId,
    ExtractionGraph,
    ExtractionGraphId,
    ExtractionGraphName,
    ExtractionPolicy,
    ExtractionPolicyId,
    GarbageCollectionTask,
    NamespaceName,
    OutputSchema,
    StateChange,
    StructuredDataSchema,
};
use itertools::Itertools;
use tokio::sync::{broadcast, watch::Receiver};
use tracing::{debug, info};

use crate::{
    coordinator_client::CoordinatorClient,
    coordinator_filters::*,
    forwardable_coordinator::ForwardableCoordinator,
    garbage_collector::GarbageCollector,
    metrics::Timer,
    scheduler::Scheduler,
    state::{store::requests::StateChangeProcessed, RaftMetrics, SharedState},
    task_allocator::TaskAllocator,
    utils,
};

pub struct Coordinator {
    pub shared_state: SharedState,
    scheduler: Scheduler,
    garbage_collector: Arc<GarbageCollector>,
    forwardable_coordinator: ForwardableCoordinator,
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
        })
    }

    //  START CONVERSION METHODS
    pub fn internal_content_metadata_to_external(
        &self,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<indexify_coordinator::ContentMetadata>> {
        let mut content_meta_list = Vec::new();
        for content in content_list {
            let extraction_graphs = self
                .shared_state
                .get_extraction_graphs(&content.extraction_graph_ids)?
                .ok_or_else(|| {
                    anyhow!("could not find extraction graph for content {}", content.id)
                })?;
            let extraction_graph_names = extraction_graphs.into_iter().map(|eg| eg.name).collect();
            let source_name = match content.source {
                internal_api::ContentSource::ExtractionGraph(extraction_graph_id) => {
                    let extraction_graph = self
                        .shared_state
                        .get_extraction_graphs(&vec![extraction_graph_id.to_string()])?
                        .and_then(|graphs| graphs.first().cloned())
                        .ok_or_else(|| {
                            anyhow!(
                                "could not find extraction graph with id: {}",
                                extraction_graph_id
                            )
                        })?;
                    Ok(extraction_graph.name.clone())
                }
                internal_api::ContentSource::ExtractionPolicyId(extraction_policy_id) => {
                    let extraction_policy = self
                        .shared_state
                        .get_extraction_policy(&extraction_policy_id)?;
                    Ok(extraction_policy.name.clone())
                }
            }?;
            let content: indexify_coordinator::ContentMetadata =
                internal_api::ContentMetadata::to_coordinator_metadata(
                    content,
                    extraction_graph_names,
                    source_name,
                );
            content_meta_list.push(content.clone());
        }
        Ok(content_meta_list)
    }

    fn get_extraction_graphs_from_names(
        &self,
        namespace: &NamespaceName,
        extraction_graph_names: &Vec<ExtractionGraphName>,
    ) -> Result<Vec<ExtractionGraph>> {
        if extraction_graph_names.is_empty() {
            return Ok(Vec::new());
        }
        let ids = extraction_graph_names.iter().map(| name | ExtractionGraph::create_id(name, namespace) ).collect_vec();
        let extraction_graphs = self
            .shared_state
            .get_extraction_graphs(&ids)?;
        Ok(extraction_graphs)
    }

    pub fn external_content_metadata_to_internal(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let content_meta_list: Result<Vec<_>> = content_list.into_iter().map(|content| {
            let extraction_graphs = self.get_extraction_graphs_from_names(
                &content.namespace,
                &content.extraction_graph_names,
            )?;
            let extraction_policies = self.shared_state.get_extraction_policies_from_names(&content.namespace, &content.source)?;
            let source_ids: Result<Vec<_>> = content.source.iter().map(|source| {
                if let Some(graphs) = extraction_graphs {
                    let source_id = graphs.first()
                        .map(|graph| internal_api::ContentSource::ExtractionGraph(graph.id.clone()))
                        .ok_or_else(|| anyhow!("could not find extraction graph for content {} and source {}", content.id, source))?;
                    Ok(source_id)
                } else {
                    let extraction_policies = self.shared_state.get_extraction_policies_from_names(&content.namespace, &content.source)?;
                    let source_id = extraction_policies
                        .and_then(|policies| policies.first().cloned())
                        .map(|policy| internal_api::ContentSource::ExtractionPolicyId(policy.id.clone()))
                        .ok_or_else(|| anyhow!("could not find extraction policy for content {} and source {}", content.id, source))?;
                    Ok(source_id)
                }
            }).collect();
            let source_ids = source_ids?;
            Ok(internal_api::ContentMetadata::from_coordinator_metadata(content, extraction_graph_ids, source_ids))
        }).collect();
        content_meta_list
    }

    pub fn internal_extraction_policy_to_external(
        &self,
        policy_list: Vec<internal_api::ExtractionPolicy>,
    ) -> Result<Vec<indexify_coordinator::ExtractionPolicy>> {
        let policies_to_return: Result<Vec<_>> = policy_list
            .into_iter()
            .map(|policy| {
                let graph_name = self
                    .shared_state
                    .get_extraction_graphs(&vec![policy.graph_id.clone()])?
                    .and_then(|graphs| graphs.first().cloned())
                    .map(|graph| graph.name)
                    .ok_or_else(|| {
                        anyhow!(
                            "could not find extraction graph with id: {}",
                            policy.graph_id
                        )
                    })?;
                match policy.content_source {
                    internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                        ref extraction_policy_id,
                    ) => {
                        let parent_policy = self
                            .shared_state
                            .get_extraction_policy(extraction_policy_id)?;
                        Ok(internal_api::ExtractionPolicy::to_coordinator_policy(
                            policy,
                            parent_policy.name,
                            graph_name,
                        ))
                    }
                    internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                        ref extraction_graph_id,
                    ) => {
                        let extraction_graph = self
                            .shared_state
                            .get_extraction_graphs(&vec![extraction_graph_id.to_string()])?
                            .and_then(|graphs| graphs.first().cloned())
                            .ok_or_else(|| {
                                anyhow!(
                                    "could not find extraction graph with id: {}",
                                    extraction_graph_id
                                )
                            })?;
                        Ok(internal_api::ExtractionPolicy::to_coordinator_policy(
                            policy,
                            extraction_graph.name,
                            graph_name,
                        ))
                    }
                }
            })
            .collect();
        policies_to_return
    }

    pub fn internal_namespace_to_external(
        &self,
        namespaces: Vec<internal_api::Namespace>,
    ) -> Result<Vec<indexify_coordinator::Namespace>> {
        let mut namespaces_to_return: Vec<indexify_coordinator::Namespace> = Vec::new();
        for namespace in namespaces {
            let extraction_policies =
                self.internal_extraction_policy_to_external(namespace.extraction_policies.clone())?;
            namespaces_to_return.push(indexify_internal_api::Namespace::to_coordinator_namespace(
                namespace,
                extraction_policies,
            ));
        }
        Ok(namespaces_to_return)
    }

    //  END CONVERSION METHODS

    pub async fn list_content(
        &self,
        namespace: &str,
        source: &str,
        parent_id: &str,
        labels_eq: &HashMap<String, String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let content = self
            .shared_state
            .list_content(namespace)
            .await?
            .into_iter()
            .filter_map(|c| c);
        list_content_filter(content, source, parent_id, labels_eq)
            .map(Ok)
            .collect::<Result<Vec<indexify_coordinator::ContentMetadata>>>()
    }

    pub fn get_extraction_policy(
        &self,
        id: ExtractionPolicyId,
    ) -> Result<internal_api::ExtractionPolicy> {
        self.shared_state.get_extraction_policy(&id)
    }

    pub async fn list_policies(
        &self,
        namespace: &str,
    ) -> Result<Vec<internal_api::ExtractionPolicy>> {
        self.shared_state.list_extraction_policy(namespace).await
    }

    pub async fn update_task(
        &self,
        task_id: &str,
        executor_id: &str,
        outcome: internal_api::TaskOutcome,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<()> {
        info!(
            "updating task: {}, executor_id: {}, outcome: {:?}",
            task_id, executor_id, outcome
        );
        let mut task = self.shared_state.task_with_id(task_id).await?;
        let content_meta_list = self.external_content_metadata_to_internal(content_list)?;
        task.outcome = outcome;
        self.shared_state
            .update_task(task, Some(executor_id.to_string()), content_meta_list)
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
        match self.shared_state.namespace(namespace).await {
            Result::Ok(Some(_)) => {
                return Ok(());
            }
            Result::Ok(None) => {}
            Result::Err(_) => {}
        }
        self.shared_state.create_namespace(namespace).await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<internal_api::Namespace>> {
        self.shared_state.list_namespaces().await
    }

    pub async fn get_namespace(&self, namespace: &str) -> Result<Option<internal_api::Namespace>> {
        self.shared_state.namespace(namespace).await
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        self.shared_state.list_extractors().await
    }

    pub async fn heartbeat(&self, executor_id: &str) -> Result<Vec<indexify_coordinator::Task>> {
        let tasks = self
            .shared_state
            .tasks_for_executor(executor_id, Some(10))
            .await?;
        let tasks = tasks
            .into_iter()
            .map(|task| -> Result<indexify_coordinator::Task> {
                let content_metadata = self
                    .internal_content_metadata_to_external(vec![task.content_metadata.clone()])?;
                let content_metadata = content_metadata.first().ok_or_else(|| {
                    anyhow!(format!(
                        "could not convert content metadata {}",
                        task.content_metadata.id.id
                    ))
                })?;
                let task = internal_api::Task::to_coordinator_task(task, content_metadata.clone());
                Ok(task)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(tasks)
    }

    pub async fn all_task_assignments(&self) -> Result<HashMap<String, String>> {
        self.shared_state.task_assignments().await
    }

    pub async fn list_state_changes(&self) -> Result<Vec<internal_api::StateChange>> {
        self.shared_state.list_state_changes().await
    }

    pub async fn list_tasks(
        &self,
        namespace: &str,
        extraction_policy: Option<String>,
    ) -> Result<Vec<indexify_coordinator::Task>> {
        let tasks = self
            .shared_state
            .list_tasks(namespace, extraction_policy)
            .await?;
        let tasks = tasks
            .into_iter()
            .map(|task| -> Result<indexify_coordinator::Task> {
                let content_metadata = self
                    .internal_content_metadata_to_external(vec![task.content_metadata.clone()])?;
                let content_metadata = content_metadata.first().ok_or_else(|| {
                    anyhow!(format!(
                        "could not convert content metadata {}",
                        task.content_metadata.id.id
                    ))
                })?;
                let task = internal_api::Task::to_coordinator_task(task, content_metadata.clone());
                Ok(task)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(tasks)
    }

    pub async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        info!("removing executor: {}", executor_id);
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

    // TODO: edwin
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
        let coordinator_metadata =
            self.internal_content_metadata_to_external(vec![task.content_metadata.clone()])?;
        let coordinator_metadata = coordinator_metadata.first().ok_or_else(|| {
            anyhow!(format!(
                "could not convert content metadata {}",
                task.content_metadata.id.id
            ))
        })?;
        Ok(internal_api::Task::to_coordinator_task(
            task,
            coordinator_metadata.clone(),
        ))
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

    pub async fn create_policy(
        &self,
        extraction_policy: internal_api::ExtractionPolicy,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        if extractor.input_params != serde_json::Value::Null {
            extractor.validate_input_params(&extraction_policy.input_params)?;
        }
        let structured_data_schema = self
            .shared_state
            .get_structured_data_schema(
                &extraction_policy.namespace,
                &String::from(&extraction_policy.content_source),
            )
            .await?;
        let mut updated_schema = None;
        for (_, output_schema) in extractor.outputs {
            if let OutputSchema::Attributes(columns) = output_schema {
                let updated_structured_data_schema = structured_data_schema.merge(columns)?;
                updated_schema.replace(updated_structured_data_schema);
            }
        }
        self.shared_state
            .create_extraction_policy(extraction_policy, updated_schema)
            .await?;
        Ok(())
    }

    pub async fn create_extraction_graph(
        &self,
        extraction_graph: ExtractionGraph,
        extraction_policies: Vec<ExtractionPolicy>,
    ) -> Result<Vec<internal_api::Index>> {
        let structured_data_schema = StructuredDataSchema::default();
        let mut indexes_to_create = Vec::new();
        for extraction_policy in &extraction_policies {
            let extractor = self.get_extractor(&extraction_policy.extractor)?;
            extractor.validate_input_params(&extraction_policy.input_params)?;
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
                        structured_data_schema.merge(columns)?;
                    }
                }
            }
        }
        self.shared_state
            .create_extraction_graph(
                extraction_graph,
                extraction_policies,
                structured_data_schema,
                indexes_to_create.clone(),
            )
            .await?;
        Ok(indexes_to_create)
    }

    pub async fn create_gc_tasks(
        &self,
        state_change: &StateChange,
    ) -> Result<Vec<GarbageCollectionTask>> {
        let content_id: ContentMetadataId = state_change.object_id.clone().try_into()?;
        let content_tree_metadata = self
            .shared_state
            .get_content_tree_metadata_with_version(&content_id)?;
        let mut output_tables = HashMap::new();

        for content_metadata in &content_tree_metadata {
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
            for applied_extraction_policy in applied_extraction_policies.clone().unwrap() {
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

        let tasks = self
            .garbage_collector
            .create_gc_tasks(content_tree_metadata, output_tables)
            .await?;
        self.shared_state.create_gc_tasks(tasks.clone()).await?;
        self.shared_state
            .mark_change_events_as_processed(vec![state_change.clone()])
            .await?;
        Ok(tasks)
    }

    async fn handle_tombstone_content_tree_state_change(
        &self,
        change: StateChange,
    ) -> Result<Vec<GarbageCollectionTask>> {
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
                .create_gc_tasks(&leader_coord_addr, &change)
                .await?;
            return Ok(Vec::new());
        }

        //  this coordinator node is the leader
        self.create_gc_tasks(&change).await
    }

    async fn handle_task_completion_state_change(
        &self,
        change: StateChange,
        content_id: ContentMetadataId,
    ) -> Result<()> {
        let are_content_tasks_completed = self
            .shared_state
            .are_content_tasks_completed(&content_id)
            .await;

        if !are_content_tasks_completed {
            return Ok(());
        }

        //  this is the first version of the content, so nothing to garbage collect
        if content_id.version <= 1 {
            self.shared_state
                .mark_change_events_as_processed(vec![change])
                .await?;
            return Ok(());
        }
        let previous_version =
            ContentMetadataId::new_with_version(&content_id.id, content_id.version - 1);
        let content_metadata = self
            .shared_state
            .get_content_metadata_with_version(&previous_version)
            .await?;
        self.shared_state
            .tombstone_content_batch_with_version(
                &[content_metadata.id.clone()],
                vec![StateChangeProcessed {
                    state_change_id: change.id.clone(),
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
                indexify_internal_api::ChangeType::TombstoneContentTree => {
                    let _ = self
                        .handle_tombstone_content_tree_state_change(change)
                        .await?;
                    continue;
                }
                indexify_internal_api::ChangeType::TaskCompleted { ref content_id } => {
                    self.handle_task_completion_state_change(change.clone(), content_id.clone())
                        .await?;
                    continue;
                }
                _ => self.scheduler.handle_change_event(change).await?,
            }
        }
        Ok(())
    }

    pub async fn subscribe_to_gc_events(&self) -> broadcast::Receiver<GarbageCollectionTask> {
        self.shared_state.subscribe_to_gc_task_events().await
    }

    pub fn get_state_watcher(&self) -> Receiver<StateChange> {
        self.shared_state.get_state_change_watcher()
    }

    pub async fn create_content_metadata(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<()> {
        let content_meta_list = self.external_content_metadata_to_internal(content_list)?;
        self.shared_state
            .create_content_batch(content_meta_list)
            .await?;
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
        sync::Arc,
        time::Duration,
    };

    use indexify_internal_api as internal_api;
    use indexify_proto::indexify_coordinator::{self};

    use super::Coordinator;
    use crate::{
        coordinator_client::CoordinatorClient,
        garbage_collector::GarbageCollector,
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{
            mock_extractor,
            mock_extractors,
            DEFAULT_TEST_EXTRACTOR,
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
            .register_executor("localhost:8950", "test_executor_id", extractor)
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let extraction_policy_id = "extraction_policy_id";
        let extraction_graph_id = "extraction_graph_id";
        let extraction_policy = internal_api::ExtractionPolicy {
            id: extraction_policy_id.into(),
            graph_id: extraction_graph_id.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id.into(),
            ),
            ..Default::default()
        };
        let extraction_graph = internal_api::ExtractionGraph {
            id: extraction_graph_id.into(),
            name: "test".into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(extraction_graph.clone(), vec![extraction_policy])
            .await?;
        coordinator.run_scheduler().await?;

        //  Read the extraction graph back
        let ret_graph = shared_state.get_extraction_graphs(&vec![extraction_graph.id])?;
        assert!(ret_graph.is_some());
        assert_eq!(ret_graph.unwrap().len(), 1);
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
            .register_executor("localhost:8950", "test_executor_id", extractor)
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let extraction_policy_id = "extraction_policy_id";
        let extraction_graph_id = "extraction_graph_id";
        let extraction_policy = internal_api::ExtractionPolicy {
            id: extraction_policy_id.into(),
            graph_id: extraction_graph_id.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id.into(),
            ),
            ..Default::default()
        };
        let extraction_graph = internal_api::ExtractionGraph {
            id: extraction_graph_id.into(),
            name: "test".into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(extraction_graph.clone(), vec![extraction_policy])
            .await?;
        coordinator.run_scheduler().await?;

        //  Add content which will trigger task creation
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "test".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            root_content_id: "test".to_string(),
            mime: "text/plain".to_string(),
            extraction_graph_names: vec![extraction_graph.name],
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  Read the content back from shared state and ensure graph id is correct
        let retr_content = shared_state
            .get_content_metadata_batch(vec![content_metadata.id.clone()])
            .await?;
        assert_eq!(retr_content.first().unwrap().extraction_graph_ids.len(), 1);
        assert_eq!(
            retr_content
                .first()
                .unwrap()
                .extraction_graph_ids
                .first()
                .unwrap(),
            &extraction_graph.id
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
            .register_executor("localhost:8950", executor_id, extractor)
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let extraction_policy_id = "extraction_policy_id";
        let extraction_graph_id = "extraction_graph_id";
        let extraction_graph_name = "extraction_graph_name";
        let extraction_policy = internal_api::ExtractionPolicy {
            id: extraction_policy_id.into(),
            graph_id: extraction_graph_id.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id.into(),
            ),
            ..Default::default()
        };
        let extraction_graph = internal_api::ExtractionGraph {
            id: extraction_graph_id.into(),
            name: extraction_graph_name.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(extraction_graph.clone(), vec![extraction_policy])
            .await?;
        coordinator.run_scheduler().await?;

        //  Add content which will trigger task creation because it is part of the graph
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "test".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            root_content_id: "test".to_string(),
            mime: "text/plain".to_string(),
            extraction_graph_names: vec![extraction_graph.name],
            source: vec![extraction_graph_name.to_string()],
            ..Default::default()
        };
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
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "test".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            root_content_id: "test".to_string(),
            mime: "text/plain".to_string(),
            extraction_graph_names: vec![],
            source: vec![],
            ..Default::default()
        };
        coordinator
            .register_executor("localhost:8956", "test_executor_id", mock_extractors())
            .await?;
        coordinator
            .create_policy(
                internal_api::ExtractionPolicy {
                    id: "test-binding-id".to_string(),
                    graph_id: "test-graph-id".to_string(),
                    name: "test".to_string(),
                    extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                    namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                    input_params: serde_json::json!({}),
                    filters: HashMap::new(),
                    output_table_mapping: HashMap::from([(
                        "test_output".to_string(),
                        "test_namespace.test.test_output".to_string(),
                    )]),
                    content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                        "ingestion".to_string(),
                    ),
                },
                mock_extractor(),
            )
            .await?;
        assert_eq!(
            2,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        coordinator.run_scheduler().await?;
        assert_eq!(
            0,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        assert_eq!(
            1,
            shared_state
                .tasks_for_executor("test_executor_id", None)
                .await?
                .len()
        );
        assert_eq!(0, shared_state.unassigned_tasks().await?.len());

        // Add a content with a different source and ensure we don't create a task
        coordinator
            .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                id: "test2".to_string(),
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                parent_id: "test".to_string(),
                root_content_id: "test".to_string(),
                file_name: "test2".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test2".to_string(),
                labels: HashMap::new(),
                source: vec!["ingestion".to_string()],
                size_bytes: 100,
                hash: "".to_string(),
                extraction_policy_ids: HashMap::new(),
                extraction_graph_names: vec![],
            }])
            .await?;
        coordinator.run_scheduler().await?;
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);
        let tasks = shared_state.tasks_for_executor(executor_id, None).await?;
        assert_eq!(tasks.len(), 1);

        let mut task_clone = tasks[0].clone();
        task_clone.outcome = internal_api::TaskOutcome::Success;
        shared_state
            .update_task(task_clone, Some(executor_id.to_string()), vec![])
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
        let extractor = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Create an extraction graph
        let extraction_policy_id_1 = "extraction_policy_id_1";
        let extraction_graph_id_1 = "extraction_graph_id_1";
        let extraction_graph_name_1 = "extraction_graph_name_1";
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_1.into(),
            name: "extraction_policy_name_1".into(),
            graph_id: extraction_graph_id_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id_1.into(),
            name: extraction_graph_name_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(extraction_graph_1.clone(), vec![extraction_policy_1])
            .await?;
        coordinator.run_scheduler().await?;

        //  Register another executor
        let extractor_name = "MockExtractor2";
        let executor_id_2 = "test_executor_id_2";
        let mut extractor = mock_extractor();
        extractor.name = extractor_name.into();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Create another extraction_graph and use the new extractor
        let extraction_policy_id_2 = "extraction_policy_id_2";
        let extraction_graph_id_2 = "extraction_graph_id_2";
        let extraction_graph_name_2 = "extraction_graph_name_2";
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_2.into(),
            name: "extraction_policy_name_2".into(),
            graph_id: extraction_graph_id_2.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: extractor_name.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_2.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_2 = internal_api::ExtractionGraph {
            id: extraction_graph_id_2.into(),
            name: extraction_graph_name_2.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_2.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(extraction_graph_2.clone(), vec![extraction_policy_2])
            .await?;
        coordinator.run_scheduler().await?;

        //  create some content and specify both graphs
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "test".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            root_content_id: "test".to_string(),
            mime: "text/plain".to_string(),
            extraction_graph_names: vec![
                extraction_graph_1.name.clone(),
                extraction_graph_2.name.clone(),
            ],
            source: vec![
                extraction_graph_1.name.clone(),
                extraction_graph_2.name.clone(),
            ],
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![content_metadata.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  expect two tasks to be created and assigned - 1 for each extractor
        let tasks = shared_state.tasks_for_executor(executor_id_1, None).await?;
        assert_eq!(tasks.len(), 1);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_multiple_contents_and_extraction_policies() -> Result<(), anyhow::Error> {
        let (coordinator, shared_state) = setup_coordinator().await;

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create content metadata
        let content_metadata_1 = indexify_coordinator::ContentMetadata {
            id: "test_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec!["ingestion".to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![content_metadata_1])
            .await?;

        //  Create an extractor, executor and associated extraction policy
        let extractor = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_1".to_string(),
            name: "extraction_policy_name_1".to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                "ingestion".to_string(),
            ),
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_1.clone(), extractor.clone())
            .await?;

        //  Assert that tasks are created
        coordinator.run_scheduler().await?;
        let tasks = shared_state
            .tasks_for_executor("test_executor_id", None)
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);

        //  The second part of the test

        //  Create content metadata
        let content_metadata_2 = indexify_coordinator::ContentMetadata {
            id: "test_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![content_metadata_2])
            .await?;
        coordinator.run_scheduler().await?;

        //  Create a different extractor, executor and associated extraction policy
        let mut extractor_2 = mock_extractor();
        let extractor_2_name = "MockExtractor2".to_string();
        extractor_2.name = extractor_2_name.clone();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_2".to_string(),
            extractor: extractor_2_name.clone(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                extraction_policy_1.name,
            ),
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_2, extractor_2)
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
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_1".to_string(),
            name: "extraction_policy_name_1".to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                "ingestion".to_string(),
            ),
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_1.clone(), extractor.clone())
            .await?;

        let executor_id_2 = "test_executor_id_2";
        let mut extractor_2 = mock_extractor();
        extractor_2.name = "MockExtractor2".to_string();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;

        //  Create an extraction graph with two levels of policies
        let extraction_policy_id_1 = "extraction_policy_id_1";
        let extraction_policy_name_1 = "extraction_policy_name_1";

        let extraction_policy_id_2 = "extraction_policy_id_2";
        let extraction_policy_name_2 = "extraction_policy_name_2";

        let extraction_graph_id = "extraction_graph_id_1";
        let extraction_graph_name = "extraction_graph_name_1";

        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_1.into(),
            name: extraction_policy_name_1.into(),
            graph_id: extraction_graph_id.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: extractor_1.name.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id.into(),
            ),
            ..Default::default()
        };
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_2.into(),
            name: extraction_policy_name_2.into(),
            graph_id: extraction_graph_id.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: extractor_2.name.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                extraction_policy_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id.into(),
            name: extraction_graph_name.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into(), extraction_policy_id_2.into()]
                .into_iter()
                .collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_1.clone(),
                vec![extraction_policy_1.clone(), extraction_policy_2.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent_content is the root
        let parent_content = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name.to_string()],
            extraction_graph_names: vec![extraction_graph_name.to_string()],
            size_bytes: 100,
            hash: "123".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  check that tasks have been created for the first level for the first
        // extractor
        let tasks = shared_state.tasks_for_executor(executor_id_1, None).await?;
        assert_eq!(tasks.len(), 1);

        //  Create the children content
        let child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name.to_string()],
            size_bytes: 100,
            hash: "456".into(),
            ..Default::default()
        };
        let child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name.to_string()],
            size_bytes: 100,
            hash: "789".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        //  check that tasks have been created for the second level for the second
        // extractor
        let tasks = shared_state.tasks_for_executor(executor_id_2, None).await?;
        assert_eq!(tasks.len(), 2);

        //  Create the final child
        let child_content_1_child = indexify_coordinator::ContentMetadata {
            id: "test_child_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_child_id_1".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_2.name],
            extraction_graph_names: vec![extraction_graph_name.to_string()],
            size_bytes: 100,
            hash: "987".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1_child.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        let content_tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id)?;
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
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_1".to_string(),
            name: "extraction_policy_name_1".to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                "ingestion".to_string(),
            ),
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_1.clone(), extractor.clone())
            .await?;

        //  Create a different extractor, executor and associated extraction policy
        let mut extractor_2 = mock_extractor();
        let extractor_2_name = "MockExtractor2".to_string();
        extractor_2.name = extractor_2_name.clone();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_2".to_string(),
            extractor: extractor_2_name.clone(),
            .await?;

        //  Create an extraction graph
        let extraction_policy_id_1 = "extraction_policy_id_1";
        let extraction_graph_id_1 = "extraction_graph_id_1";
        let extraction_graph_name_1 = "extraction_graph_name_1";
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_1.into(),
            name: "extraction_policy_name_1".into(),
            graph_id: extraction_graph_id_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id_1.into(),
            name: extraction_graph_name_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_1.clone(),
                vec![extraction_policy_1.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Create a separate extraction graph
        let extraction_policy_id_2 = "extraction_policy_id_2";
        let extraction_graph_id_2 = "extraction_graph_id_2";
        let extraction_graph_name_2 = "extraction_graph_name_2";
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_2.into(),
            name: "extraction_policy_name_2".into(),
            graph_id: extraction_graph_id_2.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_2.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_2 = internal_api::ExtractionGraph {
            id: extraction_graph_id_2.into(),
            name: extraction_graph_name_2.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_2.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_2.clone(),
                vec![extraction_policy_2.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent_content is the root
        let parent_content = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_1.to_string()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;

        let child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        let child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_2.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;

        let child_content_1_child = indexify_coordinator::ContentMetadata {
            id: "test_child_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_child_id_1".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1_child.clone()])
            .await?;

        //  Build a separate content tree where parent_content_2 is the root using the
        // second extraction graph
        let parent_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_parent_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_2.to_string()],
            extraction_graph_names: vec![extraction_graph_name_2.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content_2.clone()])
            .await?;

        let child_content_2_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id_2".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_2.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_2_1.clone()])
            .await?;

        coordinator
            .tombstone_content_metadatas(&[parent_content.id.clone(), parent_content_2.id.clone()])
            .await?;

        //  Check that content has been correctly tombstoned
        let content_tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id)?;
        let content_tree_2 = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content_2.id)?;
        for content in &content_tree {
            assert!(
                content.tombstoned,
                "Content {} is not tombstoned",
                content.id.id
            );
        }
        for content in content_tree_2 {
            assert!(
                content.tombstoned,
                "Content {} is not tombstoned",
                content.id.id
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_match_tombstoned_content() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor
        let executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;

        //  Create an extraction graph
        let extraction_policy_id_1 = "extraction_policy_id_1";
        let extraction_graph_id_1 = "extraction_graph_id_1";
        let extraction_graph_name_1 = "extraction_graph_name_1";
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_1.into(),
            name: "extraction_policy_name_1".into(),
            graph_id: extraction_graph_id_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id_1.into(),
            name: extraction_graph_name_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_1.clone(),
                vec![extraction_policy_1.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent_content is the root
        let parent_content = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_1.to_string()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;

        let child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        let child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;

        let parent_content_internal: indexify_internal_api::ContentMetadata =
            parent_content.clone().into();

        //  before tombstone
        let policies_matching_content = coordinator
            .shared_state
            .match_extraction_policies_for_content(&parent_content_internal.id)
            .await?;
        assert_eq!(policies_matching_content.len(), 1);

        coordinator
            .shared_state
            .tombstone_content_batch(&[parent_content.id])
            .await?;

        //  after tombstone
        let policies_matching_content = coordinator
            .shared_state
            .match_extraction_policies_for_content(&parent_content_internal.id)
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
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;

        //  Create a different extractor, executor and associated extraction policy
        let mut extractor_2 = mock_extractor();
        let extractor_2_name = "MockExtractor2".to_string();
        extractor_2.name = extractor_2_name.clone();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_2".to_string(),
            extractor: extractor_2_name.clone(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id_1.into(),
            name: extraction_graph_name_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into()].into_iter().collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_1.clone(),
                vec![extraction_policy_1.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent content id is the root
        let parent_content = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_1.to_string()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        coordinator.run_scheduler().await?;

        let child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
        let child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "".into(),
            ..Default::default()
        };
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
                    vec![],
                )
                .await?;
        }

        //  create a state change for tombstoning the content tree
        coordinator
            .tombstone_content_metadatas(&[parent_content.id])
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

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_content_update() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        //  Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create two extractors
        let executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor.clone()],
            )
            .await?;
        let executor_id_2 = "test_executor_id_2";
        let mut extractor_2 = mock_extractor();
        extractor_2.name = "MockExtractor2".to_string();
        coordinator
            .register_executor(
                "localhost:8957",
                "test_executor_id_2",
                vec![extractor_2.clone()],
            )
            .await?;

        //  Create an extraction graph with 2 policies
        let extraction_graph_id_1 = "extraction_graph_id_1";
        let extraction_graph_name_1 = "extraction_graph_name_1";

        let extraction_policy_id_1 = "extraction_policy_id_1";
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_1.into(),
            name: "extraction_policy_name_1".into(),
            graph_id: extraction_graph_id_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionGraphId(
                extraction_graph_id_1.into(),
            ),
            ..Default::default()
        };
        let extraction_policy_id_2 = "extraction_policy_id_2";
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: extraction_policy_id_2.into(),
            name: "extraction_policy_name_2".into(),
            graph_id: extraction_graph_id_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extractor: extractor_2.name,
            output_table_mapping: HashMap::from([(
                "test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                extraction_policy_id_1.into(),
            ),
            ..Default::default()
        };

        let extraction_graph_1 = internal_api::ExtractionGraph {
            id: extraction_graph_id_1.into(),
            name: extraction_graph_name_1.into(),
            namespace: DEFAULT_TEST_NAMESPACE.into(),
            extraction_policies: vec![extraction_policy_id_1.into(), extraction_policy_id_2.into()]
                .into_iter()
                .collect(),
        };
        coordinator
            .create_extraction_graph(
                extraction_graph_1.clone(),
                vec![extraction_policy_1.clone(), extraction_policy_2.clone()],
            )
            .await?;
        coordinator.run_scheduler().await?;

        //  Build a content tree where the parent content id is the root
        let parent_content = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_1.to_string()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_parent_id".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);
        for mut task in all_tasks {
            task.outcome = internal_api::TaskOutcome::Success;
            coordinator
                .shared_state
                .update_task(task, Some(executor_id_1.to_string()), vec![])
                .await?;
        }

        let child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_child_id_1".into(),
            ..Default::default()
        };
        let child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_child_id_2".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![child_content_1.clone(), child_content_2.clone()])
            .await?;
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 2);
        for mut task in all_tasks {
            task.outcome = internal_api::TaskOutcome::Success;
            coordinator
                .shared_state
                .update_task(task, Some(executor_id_1.to_string()), vec![])
                .await?;
        }

        //  Create a different version of the root with a new hash
        let new_parent = indexify_coordinator::ContentMetadata {
            id: "test_parent_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_graph_name_1.to_string()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_parent_id_new_hash".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![new_parent])
            .await?;
        coordinator.run_scheduler().await?;
        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 1);
        let mut update_task = all_tasks.first().unwrap().clone();

        //  Add one of the original children and 2 new children to the new version of
        // the root
        let new_child_content_1 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_1_new".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_child_id_1".into(),
            ..Default::default()
        };
        let new_child_content_2 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_2_new".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_child_id_2_new".into(),
            ..Default::default()
        };
        let new_child_content_3 = indexify_coordinator::ContentMetadata {
            id: "test_child_id_3".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "test_parent_id".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: vec![extraction_policy_1.name.clone()],
            extraction_graph_names: vec![extraction_graph_name_1.to_string()],
            size_bytes: 100,
            hash: "test_child_id_3".into(),
            ..Default::default()
        };
        coordinator
            .create_content_metadata(vec![
                new_child_content_1,
                new_child_content_2,
                new_child_content_3,
            ])
            .await?;
        update_task.outcome = internal_api::TaskOutcome::Success;
        coordinator
            .shared_state
            .update_task(update_task, Some(executor_id_1.to_string()), vec![])
            .await?;
        coordinator.run_scheduler().await?;

        let all_tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
        assert_eq!(all_tasks.len(), 2); //  only 2 tasks should be created because one of the children is identical

        //  check that the two content trees are present and their structure is correct
        let old_content_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&indexify_internal_api::ContentMetadataId {
                id: "test_parent_id".to_string(),
                version: 1,
            })?;
        let new_content_tree = coordinator
            .shared_state
            .get_content_tree_metadata_with_version(&indexify_internal_api::ContentMetadataId {
                id: "test_parent_id".to_string(),
                version: 2,
            })?;
        assert_eq!(old_content_tree.len(), 3); // old parent and old children
        assert_eq!(new_content_tree.len(), 4); // new parent and new children
        let old_tree_ids = old_content_tree
            .iter()
            .map(|c| c.id.id.clone())
            .collect::<HashSet<_>>();
        let old_tree_ids_expected: HashSet<_> = vec![
            "test_parent_id".to_string(),
            "test_child_id_1".to_string(),
            "test_child_id_2".to_string(),
        ]
        .into_iter()
        .collect();
        assert_eq!(old_tree_ids, old_tree_ids_expected);
        let new_tree_ids = new_content_tree
            .iter()
            .map(|c| c.id.id.clone())
            .collect::<HashSet<_>>();
        let new_tree_ids_expected: HashSet<_> = vec![
            "test_parent_id".to_string(),
            "test_child_id_1_new".to_string(),
            "test_child_id_2_new".to_string(),
            "test_child_id_3".to_string(),
        ]
        .into_iter()
        .collect();
        assert_eq!(new_tree_ids, new_tree_ids_expected);

        //  check the reverse index invariants
        let old_tree_children = coordinator.shared_state.state_machine.get_content_children(
            &indexify_internal_api::ContentMetadataId::new_with_version("test_parent_id", 1),
        );
        let new_tree_children = coordinator.shared_state.state_machine.get_content_children(
            &indexify_internal_api::ContentMetadataId::new_with_version("test_parent_id", 2),
        );
        assert_eq!(old_tree_children.len(), 2);
        assert_eq!(new_tree_children.len(), 3);

        //  running the scheduler should garbage collect the old tree
        coordinator.run_scheduler().await?;
        let old_root = coordinator
            .shared_state
            .get_content_metadata_with_version(&indexify_internal_api::ContentMetadataId {
                id: "test_parent_id".to_string(),
                version: 1,
            })
            .await;
        assert!(old_root.unwrap().tombstoned);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_policy_filters() -> Result<(), anyhow::Error> {
        let (coordinator, _) = setup_coordinator().await;

        let namespace = "namespace";

        coordinator.create_namespace(namespace).await?;

        let extractor_name = "extractor";
        //  Create an executor and associated extractor
        let executor_id = "executor_id";
        let extractor = indexify_internal_api::ExtractorDescription {
            name: extractor_name.into(),
            input_mime_types: vec!["*/*".into()],
            ..Default::default()
        };
        let addr = "addr";
        coordinator
            .register_executor(addr, executor_id, vec![extractor])
            .await?;

        //  Create the extraction policy under the namespace of the content
        let extraction_policy = indexify_internal_api::ExtractionPolicy {
            namespace: namespace.into(),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                "source".to_string(),
            ),
            extractor: extractor_name.into(),
            id: "1".into(),
            name: "1".into(),
            filters: vec![("label1".to_string(), "value1".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        coordinator
            .shared_state
            .create_extraction_policy(extraction_policy.clone(), None)
            .await?;

        //  Create some content
        let content_labels = vec![("label1".to_string(), "value1".to_string())];
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "content_id_1".to_string(),
            namespace: namespace.into(),
            file_name: "name".into(),
            labels: content_labels.into_iter().collect(),
            mime: "*/*".into(),
            source: vec!["source".to_string()],
            ..Default::default()
        };
        let mut content_batch = vec![content_metadata];

        let content_labels = vec![("label1".to_string(), "doesn't match".to_string())];
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: "content_id_2".to_string(),
            namespace: namespace.into(),
            file_name: "name".into(),
            labels: content_labels.into_iter().collect(),
            mime: "*/*".into(),
            source: vec!["source".to_string()],
            ..Default::default()
        };
        content_batch.push(content_metadata);
        coordinator.create_content_metadata(content_batch).await?;

        coordinator.run_scheduler().await?;

        let tasks = coordinator
            .shared_state
            .tasks_for_executor(executor_id, None)
            .await?;
        let unassigned_tasks = coordinator.shared_state.unassigned_tasks().await?;

        // Check if the task is created for the content that matches the policy and
        // non matching content does not have a task.
        assert_eq!(tasks.len() + unassigned_tasks.len(), 1);

        let extractor_name = "extractor_2";
        //  Create an executor and associated extractor
        let executor_id = "executor_id_2";
        let extractor = indexify_internal_api::ExtractorDescription {
            name: extractor_name.into(),
            input_mime_types: vec!["*/*".into()],
            ..Default::default()
        };
        let addr = "addr_2";
        coordinator
            .register_executor(addr, executor_id, vec![extractor])
            .await?;

        //  Create the extraction policy under the namespace of the content
        let extraction_policy = indexify_internal_api::ExtractionPolicy {
            id: "2".into(),
            name: "2".into(),
            namespace: namespace.into(),
            content_source: internal_api::ExtractionPolicyContentSource::ExtractionPolicyId(
                "source".to_string(),
            ),
            extractor: extractor_name.into(),
            filters: vec![("label1".to_string(), "value1".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        coordinator
            .shared_state
            .create_extraction_policy(extraction_policy.clone(), None)
            .await?;

        coordinator.run_scheduler().await?;

        let tasks = coordinator
            .shared_state
            .tasks_for_executor(executor_id, None)
            .await?;
        assert_eq!(tasks.len(), 1);

        Ok(())
    }
}
