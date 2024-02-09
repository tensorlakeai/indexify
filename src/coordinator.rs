use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use internal_api::{StateChange, Task};
use jsonschema::JSONSchema;
use tokio::sync::watch::Receiver;
use tracing::info;

use crate::{coordinator_filters::*, state::SharedState, task_allocator::TaskAllocator};

pub struct Coordinator {
    shared_state: SharedState,
    task_allocator: TaskAllocator,
}

impl Coordinator {
    pub fn new(shared_state: SharedState) -> Arc<Self> {
        let task_allocator = TaskAllocator::new(shared_state.clone());
        Arc::new(Self {
            shared_state,
            task_allocator,
        })
    }

    #[tracing::instrument(skip(self, state_changes))]
    pub async fn process_extraction_events(
        &self,
        state_changes: &Vec<StateChange>,
    ) -> Result<Vec<Task>, anyhow::Error> {
        info!("processing {} state changes", state_changes.len());
        let mut tasks = Vec::new();
        for change in state_changes {
            info!(
                "processing change event: {}, type: {}, id: {}",
                change.id, change.change_type, change.object_id
            );
            match change.change_type {
                internal_api::ChangeType::NewBinding => {
                    let content_list = self
                        .shared_state
                        .content_matching_binding(&change.object_id)
                        .await?;
                    let tasks_for_binding =
                        self.create_task(&change.object_id, content_list).await?;
                    tasks.extend(tasks_for_binding);
                }
                internal_api::ChangeType::NewContent => {
                    let bindings = self
                        .shared_state
                        .filter_extractor_binding_for_content(&change.object_id)
                        .await?;
                    let content = self
                        .shared_state
                        .get_conent_metadata(&change.object_id)
                        .await?;
                    for binding in bindings {
                        let task_for_binding =
                            self.create_task(&binding.id, vec![content.clone()]).await?;
                        tasks.extend(task_for_binding);
                    }
                }
                internal_api::ChangeType::ExecutorAdded => {
                    info!("executor {} added", change.object_id);
                }
                internal_api::ChangeType::ExecutorRemoved => {
                    info!("executor {} removed", change.object_id);
                }
            };
            info!("created {} tasks", tasks.len());
        }
        Ok(tasks)
    }

    #[tracing::instrument(skip(self))]
    pub async fn distribute_work(&self) -> Result<HashMap<String, String>, anyhow::Error> {
        let unallocated_tasks = self.shared_state.unassigned_tasks().await?;

        // work_id -> executor_id
        let mut task_assignments = HashMap::new();
        for task in unallocated_tasks {
            let executors = self
                .shared_state
                .get_executors_for_extractor(&task.extractor)
                .await?;
            if !executors.is_empty() {
                let rand_index = rand::random::<usize>() % executors.len();
                let executor_meta = executors[rand_index].clone();
                task_assignments.insert(task.id.clone(), executor_meta.id.clone());
            }
        }
        info!("finishing work assignment: {:?}", task_assignments);
        Ok(task_assignments)
    }

    pub async fn create_task(
        &self,
        extractor_binding_id: &str,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<internal_api::Task>> {
        let extractor_binding = self
            .shared_state
            .get_extractor_binding(extractor_binding_id)
            .await?;
        let extractor = self
            .shared_state
            .extractor_with_name(&extractor_binding.extractor)
            .await?;
        let mut output_mapping: HashMap<String, String> = HashMap::new();
        for name in extractor.outputs.keys() {
            let index_name = extractor_binding
                .output_index_name_mapping
                .get(name)
                .unwrap();
            let table_name = extractor_binding
                .index_name_table_mapping
                .get(index_name)
                .unwrap();
            output_mapping.insert(name.clone(), table_name.clone());
        }
        let mut tasks = Vec::new();
        for content in content_list {
            let mut hasher = DefaultHasher::new();
            extractor_binding.name.hash(&mut hasher);
            extractor_binding.namespace.hash(&mut hasher);
            content.id.hash(&mut hasher);
            let id = format!("{:x}", hasher.finish());
            let task = internal_api::Task {
                id,
                extractor: extractor_binding.extractor.clone(),
                extractor_binding: extractor_binding.name.clone(),
                output_index_table_mapping: output_mapping.clone(),
                namespace: extractor_binding.namespace.clone(),
                content_metadata: content.clone(),
                input_params: extractor_binding.input_params.clone(),
                outcome: internal_api::TaskOutcome::Unknown,
            };
            info!("created task: {:?}", task);
            tasks.push(task);
        }
        Ok(tasks)
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

    pub async fn list_bindings(
        &self,
        namespace: &str,
    ) -> Result<Vec<internal_api::ExtractorBinding>> {
        self.shared_state.list_bindings(namespace).await
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
        let content_meta_list = content_request_to_content_metadata(content_list)?;
        task.outcome = outcome;
        self.shared_state
            .update_task(task, Some(executor_id.to_string()), content_meta_list)
            .await?;
        Ok(())
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        self.shared_state.create_namespace(namespace).await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<internal_api::Namespace>> {
        self.shared_state.list_namespaces().await
    }

    pub async fn get_namespace(&self, namespace: &str) -> Result<internal_api::Namespace> {
        self.shared_state.namespace(namespace).await
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        self.shared_state.list_extractors().await
    }

    pub async fn heartbeat(&self, executor_id: &str) -> Result<Vec<internal_api::Task>> {
        let tasks = self.shared_state.tasks_for_executor(executor_id).await?;
        Ok(tasks)
    }

    pub async fn list_state_changes(&self) -> Result<Vec<internal_api::StateChange>> {
        let store = self.shared_state.indexify_state.read().await;
        let state_changes = store.state_changes.values().cloned().collect();
        Ok(state_changes)
    }

    pub async fn list_tasks(
        &self,
        namespace: &str,
        extractor_binding: &str,
    ) -> Result<Vec<internal_api::Task>> {
        let store = self.shared_state.indexify_state.read().await;
        Ok(store
            .tasks
            .values()
            .filter(|t| t.namespace == namespace)
            .filter(|t| t.extractor_binding == extractor_binding)
            .cloned()
            .collect())
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
        self.shared_state
            .register_executor(addr, executor_id, extractor)
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

    pub async fn create_binding(
        &self,
        binding: internal_api::ExtractorBinding,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        let input_params_schema = JSONSchema::compile(&extractor.input_params).map_err(|e| {
            anyhow!(
                "unable to compile json schema for input params: {:?}, error: {:?}",
                &extractor.input_params,
                e
            )
        })?;
        let extractor_params_schema = binding.input_params.clone();
        let validation_result = input_params_schema.validate(&extractor_params_schema);
        if let Err(errors) = validation_result {
            let errors = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>();
            return Err(anyhow!(
                "unable to validate input params for extractor binding: {}, errors: {}",
                &binding.name,
                errors.join(",")
            ));
        }
        self.shared_state.create_binding(binding).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_and_distribute_work(&self) -> Result<(), anyhow::Error> {
        let state_changes = self.shared_state.unprocessed_state_change_events().await?;
        let tasks = self.process_extraction_events(&state_changes).await?;
        self.shared_state.create_tasks(tasks.clone()).await?;
        self.shared_state
            .mark_change_events_as_processed(state_changes)
            .await?;

        let task_ids = tasks.into_iter().map(|t| t.id).collect();

        self.task_allocator.allocate_tasks(task_ids).await
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

    pub fn get_leader_change_watcher(&self) -> Receiver<bool> {
        self.shared_state.leader_change_rx.clone()
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
        server_config::{ServerConfig, ServerPeer, StateStoreConfig},
        state::App,
        test_util::db_utils::{mock_extractor, DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_NAMESPACE},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_extraction_events() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
        let shared_state = App::new(config).await.unwrap();
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
            }])
            .await?;

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 1);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        // Add extractors and extractor bindings and ensure that we are creating tasks
        coordinator
            .register_executor("localhost:8956", "test_executor_id", mock_extractor())
            .await?;
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
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
        coordinator.process_and_distribute_work().await?;
        assert_eq!(
            0,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        assert_eq!(
            1,
            shared_state
                .tasks_for_executor("test_executor_id")
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
            }])
            .await?;
        coordinator.process_and_distribute_work().await?;
        assert_eq!(
            0,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        assert_eq!(
            1,
            shared_state
                .tasks_for_executor("test_executor_id")
                .await?
                .len()
        );
        assert_eq!(0, shared_state.unassigned_tasks().await?.len());
        Ok(())
    }

    fn create_test_raft_configs(
        node_count: usize,
    ) -> Result<Vec<Arc<ServerConfig>>, anyhow::Error> {
        let append = nanoid::nanoid!();
        let base_port = 18950;
        let mut configs = Vec::new();
        let mut peers = Vec::new();

        // Generate configurations and peer information
        for i in 0..node_count {
            let port = (base_port + i * 2) as u64;
            peers.push(ServerPeer {
                node_id: i as u64,
                addr: format!("localhost:{}", port + 1),
            });

            let config = Arc::new(ServerConfig {
                node_id: i as u64,
                coordinator_port: port,
                coordinator_addr: format!("localhost:{}", port),
                raft_port: port + 1,
                peers: peers.clone(),
                state_store: StateStoreConfig {
                    path: Some(format!("/tmp/indexify-test/raft/{}/{}", append, i)),
                },
                ..Default::default()
            });

            configs.push(config.clone());
        }

        Ok(configs)
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_form_raft_cluster() -> Result<(), anyhow::Error> {
        let server_configs = create_test_raft_configs(10)?;

        let mut apps = Vec::new();
        for config in server_configs {
            let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
            let shared_state = App::new(config.clone()).await?;
            apps.push(shared_state);
        }

        // Store the handles of the spawned tasks
        let mut handles = Vec::new();
        for app in apps {
            let handle = tokio::spawn(async move {
                app.initialize_raft()
                    .await
                    .map_err(|e| anyhow::anyhow!("error initializing raft: {}", e))
            });
            handles.push(handle);
        }

        // pause for 2 seconds
        tokio::time::sleep(Duration::from_secs(2)).await;

        let timeout = tokio::time::sleep(Duration::from_secs(10));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => {
                    // this is a failure - the cluster should have initialized
                    return Err(anyhow::anyhow!("timeout error: raft cluster failed to initialize within 10 seconds"));
                },
                result = futures::future::select_all(handles) => {

                    let (result, _, remaining_handles) = result;
                    result??; // Handle the result of the completed future
                    handles = remaining_handles;
                    if handles.is_empty() {
                        // all raft nodes have been initialized
                        break;
                    }
                },
            }
        }

        Ok(())
    }
}
