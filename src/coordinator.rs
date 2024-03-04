use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use internal_api::StateChange;
use jsonschema::JSONSchema;
use tokio::sync::watch::Receiver;
use tracing::info;

use crate::{
    coordinator_filters::*, scheduler::Scheduler, state::SharedState, task_allocator::TaskAllocator,
};

pub struct Coordinator {
    shared_state: SharedState,
    scheduler: Scheduler,
}

impl Coordinator {
    pub fn new(shared_state: SharedState) -> Arc<Self> {
        let task_allocator = TaskAllocator::new(shared_state.clone());
        let scheduler = Scheduler::new(shared_state.clone(), task_allocator);
        Arc::new(Self {
            shared_state,
            scheduler,
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
        let tasks = self
            .shared_state
            .tasks_for_executor(executor_id, Some(10))
            .await?;
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
        extraction_policy: Option<String>,
    ) -> Result<Vec<internal_api::Task>> {
        let store = self.shared_state.indexify_state.read().await;
        Ok(store
            .tasks
            .values()
            .filter(|t| t.namespace == namespace)
            .filter(|t| {
                extraction_policy
                    .as_ref()
                    .map(|eb| eb == &t.extraction_policy)
                    .unwrap_or(true)
            })
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
        self.shared_state
            .create_extraction_policy(extraction_policy)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_scheduler(&self) -> Result<(), anyhow::Error> {
        let state_changes = self.shared_state.unprocessed_state_change_events().await?;
        for change in state_changes {
            info!(
                "processing change event: {}, type: {}, id: {}",
                change.id, change.change_type, change.object_id
            );
            self.scheduler.handle_change_event(change).await?;
        }
        Ok(())
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
        state::{App, NodeId},
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
            }])
            .await?;
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
        Ok(())
    }

    fn create_test_raft_configs(
        node_count: usize,
    ) -> Result<Vec<Arc<ServerConfig>>, anyhow::Error> {
        let append = nanoid::nanoid!();
        let base_port = 18950;
        let mut configs = Vec::new();
        let mut peers = Vec::new();
        let seed_node = format!("localhost:{}", base_port + 1); //  use the first node as the seed node

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
                state_store: StateStoreConfig {
                    path: Some(format!("/tmp/indexify-test/raft/{}/{}", append, i)),
                },
                seed_node: seed_node.clone(),
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

        //  initialize the seed node
        let seed_node = apps.remove(0);
        let seed_node_clone = Arc::clone(&seed_node);
        tokio::spawn(async move {
            seed_node
                .initialize_raft()
                .await
                .map_err(|e| anyhow::anyhow!("Error initializing raft: {}", e))
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        let timeout = Duration::from_secs(10);
        let start_time = tokio::time::Instant::now();

        loop {
            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout error: Raft cluster failed to initialize within 10 seconds"
                ));
            }
            let metrics = seed_node_clone.as_ref().raft.metrics().borrow().clone();
            let num_of_nodes_in_cluster = metrics.membership_config.nodes().count();
            if num_of_nodes_in_cluster == apps.len() + 1 {
                let num_of_nodes_according_to_learner = apps
                    .remove(0)
                    .as_ref()
                    .raft
                    .metrics()
                    .borrow()
                    .clone()
                    .membership_config
                    .nodes()
                    .count();
                assert_eq!(
                    num_of_nodes_in_cluster, num_of_nodes_according_to_learner,
                    "The number of nodes according to the seed node and a learner should be equal"
                );
                return Ok(());
            }
            // If the cluster is not yet ready, sleep a bit before retrying
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_leader_redirect() -> Result<(), anyhow::Error> {
        let server_configs = create_test_raft_configs(3)?;

        let mut apps = Vec::new();
        for config in server_configs {
            let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
            let shared_state = App::new(config.clone()).await?;
            apps.push(shared_state);
        }

        //  initialize the seed node
        let seed_node = apps.remove(0);
        let seed_node_clone = Arc::clone(&seed_node);
        tokio::spawn(async move {
            seed_node
                .initialize_raft()
                .await
                .map_err(|e| anyhow::anyhow!("Error initializing raft: {}", e))
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        //  check that seed node is current leader and force it to step down
        match seed_node_clone.raft.ensure_linearizable().await {
            Ok(_) => {}
            Err(e) => return Err(anyhow::anyhow!("The seed node is not the leader: {}", e)),
        }
        seed_node_clone.raft.runtime_config().heartbeat(false);
        tokio::time::sleep(Duration::from_secs(8)).await;

        //  force a specific node to be elected leader
        let alternate_node = apps.remove(0);
        alternate_node.raft.trigger().elect().await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let current_leader = alternate_node.raft.current_leader().await;
        assert!(current_leader.is_some());
        assert_eq!(current_leader.unwrap(), 1);

        //  make an explicit call to previous leaders get_cluster_membership to determine response
        let last_node = apps.remove(0);
        let resp = last_node.check_cluster_membership().await;
        if let Err(e) = resp {
            let int_err = e.downcast_ref::<tonic::Status>().unwrap();
            let metadata = int_err.metadata();

            let leader_id_str = metadata.get("leader-id").unwrap().to_str().unwrap();
            let leader_id = leader_id_str
                .parse::<NodeId>()
                .expect("Failed to parse leader-id");
            assert_eq!(leader_id, 1);

            let leader_addr = metadata.get("leader-address").unwrap().to_str();
            assert!(leader_addr.is_ok());
        };
        Ok(())
    }
}
