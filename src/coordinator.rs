use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Context, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use internal_api::{GarbageCollectionTask, OutputSchema, StateChange, StructuredDataSchema};
use jsonschema::JSONSchema;
use tokio::sync::{
    mpsc::{self, Sender},
    watch::{self, Receiver},
    Mutex,
};
use tracing::info;

use crate::{
    coordinator_filters::*,
    garbage_collector::GarbageCollector,
    scheduler::Scheduler,
    state::{RaftMetrics, SharedState},
    task_allocator::TaskAllocator,
};

pub struct Coordinator {
    shared_state: SharedState,
    scheduler: Scheduler,
    garbage_collector: GarbageCollector,
    gc_tasks_tx: Sender<indexify_internal_api::GarbageCollectionTask>,
    gc_task_allocation_event_rx: watch::Receiver<GarbageCollectionTask>,
}

impl Coordinator {
    pub fn new(shared_state: SharedState) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(8);
        let task_allocator = TaskAllocator::new(shared_state.clone());
        let scheduler = Scheduler::new(shared_state.clone(), task_allocator);
        let garbage_collector = GarbageCollector::new();
        garbage_collector.watch_deletion_events(rx);
        Arc::new(Self {
            shared_state,
            scheduler,
            gc_task_allocation_event_rx: garbage_collector
                .task_deletion_allocation_events_receiver
                .clone(),
            garbage_collector,
            gc_tasks_tx: tx,
        })
    }

    pub async fn list_content(
        &self,
        namespace: &str,
        source: &str,
        parent_id: &str,
        labels_eq: &HashMap<String, String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let content = self.shared_state.list_content(namespace).await?.into_iter();
        list_content_filter(content, source, parent_id, labels_eq)
            .map(Ok)
            .collect::<Result<Vec<internal_api::ContentMetadata>>>()
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
        content_id: &str,
        extraction_policy_name: &str,
    ) -> Result<()> {
        info!(
            "updating task: {}, executor_id: {}, outcome: {:?}",
            task_id, executor_id, outcome
        );
        let mut task = self.shared_state.task_with_id(task_id).await?;
        let content_meta_list = content_request_to_content_metadata(content_list)?;
        task.outcome = outcome;
        self.shared_state
            .update_task(task, Some(executor_id.to_string()), content_meta_list)
            .await?;
        self.shared_state
            .mark_extraction_policy_applied_on_content(content_id, extraction_policy_name)
            .await?;
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

    pub async fn heartbeat(&self, executor_id: &str) -> Result<Vec<internal_api::Task>> {
        let tasks = self
            .shared_state
            .tasks_for_executor(executor_id, Some(10))
            .await?;
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
    ) -> Result<Vec<internal_api::Task>> {
        self.shared_state
            .list_tasks(namespace, extraction_policy)
            .await
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

    pub async fn create_index(&self, namespace: &str, index: internal_api::Index) -> Result<()> {
        let id = index.id();
        self.shared_state.create_index(namespace, index, id).await
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
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        let _ = self
            .shared_state
            .register_executor(addr, executor_id, extractor)
            .await;
        Ok(())
    }

    pub async fn register_ingestion_server(
        &self,
        addr: &str,
        ingestion_server_id: &str,
    ) -> Result<()> {
        self.shared_state
            .register_ingestion_server(addr, ingestion_server_id)
            .await
    }

    pub async fn get_content_metadata(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        self.shared_state
            .get_content_metadata_batch(content_ids)
            .await
    }

    pub async fn get_extractor(
        &self,
        extractor_name: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        self.shared_state.extractor_with_name(extractor_name).await
    }

    pub async fn create_policy(
        &self,
        extraction_policy: internal_api::ExtractionPolicy,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        if extractor.input_params != serde_json::Value::Null {
            let input_params_schema =
                JSONSchema::compile(&extractor.input_params).map_err(|e| {
                    anyhow!(
                        "unable to compile json schema for input params: {:?}, error: {:?}",
                        &extractor.input_params,
                        e
                    )
                })?;
            let extractor_params_schema = extraction_policy.input_params.clone();
            let validation_result = input_params_schema.validate(&extractor_params_schema);
            if let Err(errors) = validation_result {
                let errors = errors
                    .into_iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>();
                return Err(anyhow!(
                    "unable to validate input params for extractor policy: {}, errors: {}",
                    &extraction_policy.name,
                    errors.join(",")
                ));
            }
        }
        let structured_data_schema = self
            .shared_state
            .get_structured_data_schema(
                &extraction_policy.namespace,
                &extraction_policy.content_source,
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

    #[tracing::instrument(skip(self))]
    pub async fn run_scheduler(&self) -> Result<(), anyhow::Error> {
        let state_changes = self.shared_state.unprocessed_state_change_events().await?;
        println!(
            "The unprocessed state change events are {:#?}",
            state_changes
        );
        for change in state_changes {
            info!(
                "processing change event: {}, type: {}, id: {}",
                change.id, change.change_type, change.object_id
            );
            if change
                .change_type
                .eq(&indexify_internal_api::ChangeType::DeleteContent)
            {
                //  Get the extraction policy ids applied to a content id from the mappings and figure out the table name of the index where the data is written and needs to be deleted from
                //  Then create a GCTask and notify the GarbageCollector of that
                let content_extraction_policy_mappings = self
                    .shared_state
                    .get_content_extraction_policy_mappings_for_content_id(&change.object_id)
                    .await?;
                if let Some(mappings) = content_extraction_policy_mappings {
                    //  fetch all the extraction policies that have been applied
                    let applied_extraction_policy_ids: HashSet<String> =
                        mappings.time_of_policy_completion.keys().cloned().collect();
                    let applied_extraction_policies = self
                        .shared_state
                        .get_extraction_policies_from_ids(applied_extraction_policy_ids)
                        .await?;

                    //  Get the table names from the extraction_policies
                    let mut output_tables: Vec<String> = Vec::new();
                    for applied_extraction_policy in applied_extraction_policies.clone().unwrap() {
                        output_tables.extend(
                            applied_extraction_policy
                                .output_index_name_mapping
                                .values()
                                .cloned(),
                        );
                    }

                    //  Create the garbage collection task and write it to Raft and send the gc task to the garbage collector
                    let mut hasher = DefaultHasher::new();
                    let policies = applied_extraction_policies.unwrap();
                    let policy = policies.get(0).unwrap();

                    policy.name.hash(&mut hasher);
                    policy.namespace.hash(&mut hasher);
                    change.object_id.hash(&mut hasher);
                    let id = format!("{:x}", hasher.finish());

                    let gc_task = indexify_internal_api::GarbageCollectionTask {
                        id,
                        output_index_table_mapping: output_tables.iter().cloned().collect(),
                        outcome: indexify_internal_api::TaskOutcome::Unknown,
                    };
                    self.shared_state
                        .create_gc_tasks(vec![gc_task.clone()], &change.id)
                        .await?;
                    //  TODO: Should we react to a state change event before sending the gc task to the garbage collector
                    self.gc_tasks_tx.send(gc_task).await?;
                }
                return Ok(());
            } else if change
                .change_type
                .eq(&indexify_internal_api::ChangeType::IngestionServerAdded)
            {
                let _rx = self
                    .garbage_collector
                    .register_ingestion_server(change.object_id)
                    .await?;
                return Ok(());
            }
            self.scheduler.handle_change_event(change).await?;
        }
        Ok(())
    }

    pub fn get_gc_task_allocation_event_rx(&self) -> watch::Receiver<GarbageCollectionTask> {
        self.gc_task_allocation_event_rx.clone()
    }

    pub fn get_state_watcher(&self) -> Receiver<StateChange> {
        self.shared_state.get_state_change_watcher()
    }

    pub async fn create_content_metadata(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<()> {
        let content_meta_list = content_request_to_content_metadata(content_list)?;
        self.shared_state
            .create_content_batch(content_meta_list)
            .await?;
        Ok(())
    }

    pub async fn delete_content_metadatas(
        &self,
        namespace: &str,
        content_ids: &[String],
    ) -> Result<()> {
        self.shared_state
            .delete_content_batch(namespace, content_ids)
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

fn content_request_to_content_metadata(
    content_list: Vec<indexify_coordinator::ContentMetadata>,
) -> Result<Vec<internal_api::ContentMetadata>> {
    let mut content_meta_list = Vec::new();
    for content in content_list {
        let c: internal_api::ContentMetadata = content.try_into()?;
        content_meta_list.push(c.clone());
    }
    Ok(content_meta_list)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc, time::Duration};

    use indexify_internal_api as internal_api;
    use indexify_proto::indexify_coordinator;

    use crate::{
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{mock_extractor, DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_NAMESPACE},
        test_utils::RaftTestCluster,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_extraction_events() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
        let shared_state = App::new(config, None).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        // Add content and ensure that we are creating a extraction event
        coordinator
            .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                id: "test".to_string(),
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                parent_id: "".to_string(),
                file_name: "test".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test".to_string(),
                labels: HashMap::new(),
                source: "ingestion".to_string(),
                size_bytes: 100,
            }])
            .await?;

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 1);

        //  Run scheduler without any bindings to make
        // sure that the event is processed and we don't have any tasks
        coordinator.run_scheduler().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        // Add extractors and extractor bindings and ensure that we are creating tasks
        coordinator
            .register_executor("localhost:8956", "test_executor_id", mock_extractor())
            .await?;
        coordinator
            .create_policy(
                internal_api::ExtractionPolicy {
                    id: "test-binding-id".to_string(),
                    name: "test".to_string(),
                    extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                    namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                    input_params: serde_json::json!({}),
                    filters: HashMap::new(),
                    output_index_name_mapping: HashMap::from([(
                        "test_output".to_string(),
                        "test.test_output".to_string(),
                    )]),
                    index_name_table_mapping: HashMap::from([(
                        "test.test_output".to_string(),
                        "test_namespace.test.test_output".to_string(),
                    )]),
                    content_source: "ingestion".to_string(),
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
                file_name: "test2".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test2".to_string(),
                labels: HashMap::new(),
                source: "some_extractor_produced_this".to_string(),
                size_bytes: 100,
            }])
            .await?;
        coordinator.run_scheduler().await?;
        assert_eq!(
            0,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        let tasks = shared_state
            .tasks_for_executor("test_executor_id", None)
            .await
            .unwrap();
        assert_eq!(1, tasks.clone().len());
        assert_eq!(0, shared_state.unassigned_tasks().await?.len());

        let mut task_clone = tasks[0].clone();
        task_clone.outcome = internal_api::TaskOutcome::Success;
        shared_state
            .update_task(task_clone, Some("test_executor_id".to_string()), vec![])
            .await
            .unwrap();
        let tasks = shared_state
            .tasks_for_executor("test_executor_id", None)
            .await
            .unwrap();
        assert_eq!(0, tasks.clone().len());
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_multiple_extraction_policies_and_contents() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
        let shared_state = App::new(config, None).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a namespace
        coordinator.create_namespace(DEFAULT_TEST_NAMESPACE).await?;

        //  Create an extractor, executor and associated extraction policy
        let extractor = mock_extractor();
        coordinator
            .register_executor("localhost:8956", "test_executor_id", extractor.clone())
            .await?;
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_1".to_string(),
            name: "extraction_policy_name_1".to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_index_name_mapping: HashMap::from([(
                "test_output".to_string(),
                "test.test_output".to_string(),
            )]),
            index_name_table_mapping: HashMap::from([(
                "test.test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: "ingestion".to_string(),
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_1.clone(), extractor.clone())
            .await?;

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
            source: "ingestion".to_string(),
            size_bytes: 100,
        };
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

        //  Create a different extractor, executor and associated extraction policy
        let mut extractor_2 = mock_extractor();
        let extractor_2_name = "MockExtractor2".to_string();
        extractor_2.name = extractor_2_name.clone();
        coordinator
            .register_executor("localhost:8957", "test_executor_id_2", extractor_2.clone())
            .await?;
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_2".to_string(),
            extractor: extractor_2_name.clone(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_index_name_mapping: HashMap::from([(
                "test_output".to_string(),
                "test.test_output".to_string(),
            )]),
            index_name_table_mapping: HashMap::from([(
                "test.test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: extraction_policy_1.name,
            ..Default::default()
        };
        coordinator
            .create_policy(extraction_policy_2, extractor_2)
            .await?;

        //  Create content metadata
        let content_metadata_2 = indexify_coordinator::ContentMetadata {
            id: "test_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: extraction_policy_1.id,
            size_bytes: 100,
        };
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
    // #[tracing_test::traced_test]
    async fn test_create_multiple_contents_and_extraction_policies() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
        let shared_state = App::new(config, None).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

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
            source: "ingestion".to_string(),
            size_bytes: 100,
        };
        coordinator
            .create_content_metadata(vec![content_metadata_1])
            .await?;

        //  Create an extractor, executor and associated extraction policy
        let extractor = mock_extractor();
        coordinator
            .register_executor("localhost:8956", "test_executor_id", extractor.clone())
            .await?;
        let extraction_policy_1 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_1".to_string(),
            name: "extraction_policy_name_1".to_string(),
            extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_index_name_mapping: HashMap::from([(
                "test_output".to_string(),
                "test.test_output".to_string(),
            )]),
            index_name_table_mapping: HashMap::from([(
                "test.test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: "ingestion".to_string(),
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
            id: "test_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            parent_id: "".to_string(),
            file_name: "test_file".to_string(),
            mime: "text/plain".to_string(),
            created_at: 0,
            storage_url: "test_storage_url".to_string(),
            labels: HashMap::new(),
            source: extraction_policy_1.id,
            size_bytes: 100,
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
            .register_executor("localhost:8957", "test_executor_id_2", extractor_2.clone())
            .await?;
        let extraction_policy_2 = internal_api::ExtractionPolicy {
            id: "extraction_policy_id_2".to_string(),
            extractor: extractor_2_name.clone(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            output_index_name_mapping: HashMap::from([(
                "test_output".to_string(),
                "test.test_output".to_string(),
            )]),
            index_name_table_mapping: HashMap::from([(
                "test.test_output".to_string(),
                "test_namespace.test.test_output".to_string(),
            )]),
            content_source: extraction_policy_1.name,
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
    async fn test_form_raft_cluster() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(5, None).await?;
        cluster.initialize(Duration::from_secs(10)).await?;
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
        let alt_node = cluster.get_node(1)?;
        let response = alt_node.check_cluster_membership().await?;
        assert_eq!(response.handled_by, new_leader_id);
        Ok(())
    }
}
