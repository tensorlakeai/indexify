#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use itertools::Itertools;
use network::Network;
use openraft::{self, storage::Adaptor, BasicNode};
use store::{Request, Response, Store};

use self::store::{ExecutorId, TaskId};
use crate::{
    indexify_coordinator,
    internal_api::{
        ContentMetadata,
        ExecutorMetadata,
        ExtractionEvent,
        ExtractorBinding,
        ExtractorDescription,
        ExtractorHeartbeat,
        Task, self,
    },
    server_config::ServerConfig,
};

pub mod client;
pub mod network;
pub mod store;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig: D = Request, R = Response, NodeId = NodeId, Node = BasicNode,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>
);

pub type LogStore = Adaptor<TypeConfig, Arc<Store>>;
pub type StateMachineStore = Adaptor<TypeConfig, Arc<Store>>;
pub type Raft = openraft::Raft<TypeConfig, Network, LogStore, StateMachineStore>;

pub type SharedState = Arc<App>;

pub mod typ {
    use openraft::BasicNode;

    use super::{NodeId, TypeConfig};

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub store: Arc<Store>,
    pub config: Arc<openraft::Config>,
}

impl App {
    pub async fn new(server_config: Arc<ServerConfig>) -> Result<Arc<Self>> {
        let raft_config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(raft_config.validate().unwrap());
        let store = Arc::new(Store::default());
        let (log_store, state_machine) = Adaptor::new(store.clone());
        let network = Network {};

        let raft = openraft::Raft::new(
            server_config.node_id,
            config.clone(),
            network,
            log_store,
            state_machine,
        )
        .await
        .unwrap();

        let mut nodes = BTreeMap::new();
        for peer in &server_config.peers {
            nodes.insert(
                peer.node_id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }
        raft.initialize(nodes)
            .await
            .map_err(|e| anyhow!("unable to initialize raft: {}", e))?;

        Ok(Arc::new(App {
            id: server_config.node_id,
            addr: server_config
                .coordinator_lis_addr_sock()
                .unwrap()
                .to_string(),
            raft,
            store,
            config,
        }))
    }

    pub async fn unprocessed_extraction_events(&self) -> Result<Vec<ExtractionEvent>> {
        let store = self.store.state_machine.read().await;
        let mut events = vec![];
        for event_id in store.unprocessed_extraction_events.iter() {
            let event = store.extraction_events.get(event_id).unwrap();
            events.push(event.clone());
        }
        Ok(events)
    }

    pub async fn mark_extraction_event_processed(&self, event_id: &str) -> Result<()> {
        let req = Request::MarkExtractionEventProcessed {
            event_id: event_id.to_string(),
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }

    pub async fn filter_content_by_metadata(&self) -> Result<Vec<ContentMetadata>> {
        Ok(vec![])
    }

    pub async fn filter_extractor_binding_for_content(
        &self,
        _content_id: &str,
    ) -> Result<Vec<ExtractorBinding>> {
        Ok(vec![])
    }

    pub async fn unassigned_tasks(&self) -> Result<Vec<Task>> {
        let store = self.store.state_machine.read().await;
        let mut tasks = vec![];
        for task_id in store.unassigned_tasks.iter() {
            let task = store.tasks.get(task_id).unwrap();
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    pub async fn get_executors_for_extractor(
        &self,
        extractor: &str,
    ) -> Result<Vec<ExecutorMetadata>> {
        let store = self.store.state_machine.read().await;
        let executor_ids = store
            .extractors_table
            .get(extractor)
            .cloned()
            .unwrap_or(vec![]);
        let mut executors = Vec::new();
        for executor_id in executor_ids {
            let executor = store.executors.get(&executor_id).unwrap();
            executors.push(executor.clone());
        }
        Ok(executors)
    }

    pub async fn heartbeat(&self, heartbeat: ExtractorHeartbeat) -> Result<()> {
        let request = store::Request::ExecutorHeartbeat {
            heartbeat: heartbeat.clone(),
        };
        let _response = self
            .raft
            .client_write(request)
            .await
            .map_err(|e| anyhow!("unable to write heartbeat to raft {}", e.to_string()))?;
        Ok(())
    }

    pub async fn list_content(&self, repository: &str) -> Result<Vec<ContentMetadata>> {
        let store = self.store.state_machine.read().await;
        let content_ids = store
            .content_repository_table
            .get(repository)
            .cloned()
            .unwrap_or_default();
        let mut content = Vec::new();
        for content_id in content_ids {
            let content_metadata = store.content_table.get(&content_id).unwrap();
            content.push(content_metadata.clone());
        }
        Ok(content)
    }

    pub async fn create_binding(&self, binding: ExtractorBinding) -> Result<()> {
        let _resp = self
            .raft
            .client_write(Request::CreateBinding { binding })
            .await?;
        Ok(())
    }

    pub async fn extractor_with_name(&self, extractor: &str) -> Result<ExtractorDescription> {
        let store = self.store.state_machine.read().await;
        let binding = store
            .extractors
            .get(extractor)
            .ok_or(anyhow!("extractor not found"))?;
        Ok(binding.clone())
    }

    pub async fn list_bindings(&self, repository: &str) -> Result<Vec<ExtractorBinding>> {
        let store = self.store.state_machine.read().await;
        let bindings = store
            .bindings_table
            .get(repository)
            .cloned()
            .unwrap_or_default().into_iter().collect_vec();
        Ok(bindings)
    }

    pub async fn create_repository(&self, repository: &str) -> Result<()> {
        let _resp = self
            .raft
            .client_write(Request::CreateRepository { name: repository.to_string() })
            .await?;
        Ok(())
    }

    pub async fn list_repositories(&self) -> Result<Vec<internal_api::Repository>> {
        let store = self.store.state_machine.read().await;
        let mut repositories = Vec::new();
        for repository_name in &store.repositories {
            let bindings = store.bindings_table.get(repository_name).cloned().unwrap_or_default();
            let repository = internal_api::Repository{
                name: repository_name.clone(),
                extractor_bindings:bindings.into_iter().collect_vec(),
            };
            repositories.push(repository);
        }
        Ok(repositories)
    }

    pub async fn get_repository(&self, repository: &str) -> Result<internal_api::Repository> {
        let store = self.store.state_machine.read().await;
        let bindings = store.bindings_table.get(repository).cloned().unwrap_or_default();
        let repository = internal_api::Repository{
            name: repository.to_string(),
            extractor_bindings:bindings.into_iter().collect_vec(),
        };
        Ok(repository)
    }

    pub async fn list_extractors(&self) -> Result<Vec<ExtractorDescription>> {
        let store = self.store.state_machine.read().await;
        let extractors = store.extractors.values().map(|e| e.clone()).collect_vec();
        Ok(extractors)
    }

    pub async fn get_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        let store = self.store.state_machine.read().await;
        let executors = store.executors.values().map(|e| e.clone()).collect_vec();
        Ok(executors)
    }

    pub async fn commit_task_assignments(
        &self,
        assignments: HashMap<TaskId, ExecutorId>,
    ) -> Result<()> {
        let _resp = self
            .raft
            .client_write(Request::AssignTask { assignments })
            .await?;
        Ok(())
    }

    pub async fn create_content(&self, id: &str, content_metadata: ContentMetadata) -> Result<()> {
        let req = Request::CreateContent {
            id: id.to_string(),
            content_metadata,
        };
        let _ = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to create content metadata: {}", e.to_string()))?;
        Ok(())
    }

    pub async fn get_conent_metadata(&self, content_id: &str) -> Result<ContentMetadata> {
        let store = self.store.state_machine.read().await;
        let content_metadata = store
            .content_table
            .get(content_id)
            .ok_or(anyhow!("content not found"))?;
        Ok(content_metadata.clone())
    }

    pub async fn create_tasks(&self, tasks: Vec<Task>) -> Result<()> {
        let _resp = self
            .raft
            .client_write(Request::CreateTasks { tasks })
            .await?;
        Ok(())
    }

    pub async fn tasks_for_executor(&self, executor_id: &str) -> Result<Vec<Task>> {
        let store = self.store.state_machine.read().await;
        let tasks = store
            .task_assignments
            .get(executor_id)
            .map(|task_ids| {
                task_ids
                    .iter()
                    .map(|task_id| store.tasks.get(task_id).unwrap().clone())
                    .collect_vec()
            })
            .unwrap_or(vec![]);
        Ok(tasks)
    }
}
