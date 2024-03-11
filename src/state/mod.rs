#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    io::Cursor,
    path::Path,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use grpc_server::RaftGrpcServer;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_raft::raft_api_server::RaftApiServer;
use internal_api::{ExtractionPolicy, StateChange};
use itertools::Itertools;
use network::Network;
use openraft::{
    self,
    error::{InitializeError, RaftError},
    BasicNode,
    TokioRuntime,
};
use store::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    state_machine_objects::IndexifyState,
    ExecutorId,
    ExecutorIdRef,
    Response,
    TaskId,
};
use tokio::{
    sync::{
        watch::{self, Receiver, Sender},
        Mutex,
        RwLock,
    },
    task::JoinHandle,
};
use tracing::{error, info, warn};

use self::forwardable_raft::ForwardableRaft;
use crate::{
    coordinator_filters::matches_mime_type,
    server_config::ServerConfig,
    state::{raft_client::RaftClient, store::new_storage},
    utils::timestamp_secs,
};

pub mod forwardable_raft;
pub mod grpc_server;
pub mod network;
pub mod raft_client;
pub mod store;

pub type NodeId = u64;

pub type SnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StateMachineUpdateRequest,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = SnapshotData,
        AsyncRuntime = TokioRuntime
);

pub type Raft = openraft::Raft<TypeConfig>;

pub type SharedState = Arc<App>;

pub mod typ {
    use openraft::BasicNode;

    use super::{NodeId, TypeConfig};
    pub type Entry = openraft::Entry<TypeConfig>;

    pub type RPCError<E> = openraft::error::RPCError<NodeId, BasicNode, E>;
    pub type RemoteError<E> = openraft::error::RemoteError<NodeId, BasicNode, E>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type NetworkError = openraft::error::NetworkError;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;
    pub type InstallSnapshotError = openraft::error::InstallSnapshotError;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

const MEMBERSHIP_CHECK_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(3);

pub struct App {
    pub id: NodeId,
    pub addr: String,
    seed_node: String,
    pub forwardable_raft: ForwardableRaft,
    nodes: BTreeMap<NodeId, BasicNode>,
    shutdown_rx: Receiver<()>,
    shutdown_tx: Sender<()>,
    pub leader_change_rx: Receiver<bool>,
    join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,
    pub indexify_state: Arc<RwLock<IndexifyState>>,
    pub config: Arc<openraft::Config>,
    state_change_rx: Receiver<StateChange>,
    pub network: Network,
    pub node_addr: String,
}

impl App {
    pub async fn new(server_config: Arc<ServerConfig>) -> Result<Arc<Self>> {
        let raft_config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            enable_heartbeat: true,
            ..Default::default()
        };

        let config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| anyhow!("invalid raft config: {}", e.to_string()))?,
        );
        let db_path = server_config
            .state_store
            .path
            .clone()
            .unwrap_or_default()
            .clone();
        let db_path: &Path = Path::new(db_path.as_str());
        let (log_store, state_machine) = new_storage(db_path).await;
        let state_change_rx = state_machine.state_change_rx.clone();

        let indexify_state = state_machine.data.indexify_state.clone();

        let raft_client = Arc::new(RaftClient::new());
        let network = Network::new(Arc::clone(&raft_client));

        let raft = openraft::Raft::new(
            server_config.node_id,
            config.clone(),
            network.clone(),
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| anyhow!("unable to create raft: {}", e.to_string()))?;

        let forwardable_raft =
            ForwardableRaft::new(server_config.node_id, raft.clone(), network.clone());

        let mut nodes = BTreeMap::new();
        nodes.insert(
            server_config.node_id,
            BasicNode {
                addr: format!("{}:{}", server_config.listen_if, server_config.raft_port),
            },
        );
        let (tx, rx) = watch::channel::<()>(());

        let addr = server_config
            .raft_addr_sock()
            .map_err(|e| anyhow!("unable to create raft address : {}", e.to_string()))?;

        info!("starting raft server at {}", addr.to_string());
        let raft_srvr = RaftApiServer::new(RaftGrpcServer::new(
            server_config.node_id,
            Arc::new(raft.clone()),
            Arc::clone(&raft_client),
        ));
        let (leader_change_tx, leader_change_rx) = tokio::sync::watch::channel::<bool>(false);

        let app = Arc::new(App {
            id: server_config.node_id,
            addr: server_config
                .coordinator_lis_addr_sock()
                .map_err(|e| anyhow!("unable to get coordinator address : {}", e.to_string()))?
                .to_string(),
            seed_node: server_config.seed_node.clone(),
            forwardable_raft,
            shutdown_rx: rx,
            shutdown_tx: tx,
            leader_change_rx,
            join_handles: Mutex::new(vec![]),
            nodes,
            indexify_state,
            config,
            state_change_rx,
            network,
            node_addr: format!("{}:{}", server_config.listen_if, server_config.raft_port),
        });

        let raft_clone = app.forwardable_raft.clone();

        let mut rx = app.shutdown_rx.clone();
        let shutdown_rx = app.shutdown_rx.clone();

        // Start task for watching leadership changes
        tokio::spawn(async move {
            let _ = watch_for_leader_change(raft_clone, leader_change_tx, shutdown_rx).await;
        });

        //  Start task for GRPC server
        let grpc_svc = tonic::transport::Server::builder().add_service(raft_srvr);
        let h = tokio::spawn(async move {
            grpc_svc
                .serve_with_shutdown(addr, async move {
                    let _ = rx.changed().await;
                    info!("shutting down grpc server");
                })
                .await
                .map_err(|e| anyhow!("grpc server error: {}", e))
        });
        app.join_handles.lock().await.push(h);

        //  Start task for cluster membership check
        let membership_shutdown_rx = app.shutdown_rx.clone();
        app.start_periodic_membership_check(membership_shutdown_rx);

        Ok(app)
    }

    /// This function checks whether this node is the seed node
    fn is_seed_node(&self) -> bool {
        let seed_node_port = self
            .seed_node
            .split(':')
            .nth(1)
            .and_then(|s| s.trim().parse::<u64>().ok());
        let node_addr_port = self
            .node_addr
            .split(':')
            .nth(1)
            .and_then(|s| s.trim().parse::<u64>().ok());
        match (seed_node_port, node_addr_port) {
            (Some(seed_port), Some(node_port)) => seed_port == node_port,
            _ => false,
        }
    }

    pub async fn initialize_raft(&self) -> Result<()> {
        if !self.is_seed_node() {
            return Ok(());
        }
        match self.forwardable_raft.initialize(self.nodes.clone()).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // match the type of the initialize error. if it's NotAllowed, ignore it.
                // this means that the node is already initialized.
                match e {
                    RaftError::APIError(InitializeError::NotAllowed(_)) => {
                        warn!("cluster is already initialized: {}", e);
                        Ok(())
                    }
                    _ => Err(anyhow!("unable to initialize raft: {}", e)),
                }
            }
        }
    }

    pub fn get_state_change_watcher(&self) -> Receiver<StateChange> {
        self.state_change_rx.clone()
    }

    pub async fn stop(&self) -> Result<()> {
        info!("stopping raft server");
        let _ = self.forwardable_raft.shutdown().await;
        self.shutdown_tx.send(()).unwrap();
        for j in self.join_handles.lock().await.iter_mut() {
            let res = j.await;
            info!("task quit res: {:?}", res);

            // The returned error does not mean this function call failed.
            // Do not need to return this error. Keep shutting down other tasks.
            if let Err(ref e) = res {
                error!("task quit with error: {:?}", e);
            }
        }
        Ok(())
    }

    pub async fn unprocessed_state_change_events(&self) -> Result<Vec<StateChange>> {
        let store = self.indexify_state.read().await;
        let mut state_changes = vec![];
        for event_id in store.unprocessed_state_changes.iter() {
            let event = store.state_changes.get(event_id).ok_or(anyhow!(
                "internal error: unprocessed event {} not found in events table",
                event_id
            ))?;
            state_changes.push(event.clone());
        }
        Ok(state_changes)
    }

    pub async fn mark_change_events_as_processed(&self, events: Vec<StateChange>) -> Result<()> {
        let mut state_changes = vec![];
        for event in events {
            state_changes.push(StateChangeProcessed {
                state_change_id: event.id,
                processed_at: timestamp_secs(),
            });
        }
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::MarkStateChangesProcessed { state_changes },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn filter_extraction_policy_for_content(
        &self,
        content_id: &str,
    ) -> Result<Vec<ExtractionPolicy>> {
        let content_metadata = self.get_conent_metadata(content_id).await?;
        let extraction_policies = {
            let store = self.indexify_state.read().await;
            store
                .extraction_policies_table
                .get(&content_metadata.namespace)
                .cloned()
                .unwrap_or_default()
        };
        let mut matched_policies = Vec::new();
        for extraction_policy in &extraction_policies {
            if extraction_policy.content_source != content_metadata.source {
                continue;
            }
            for (name, value) in &extraction_policy.filters {
                let is_mach = content_metadata
                    .labels
                    .get(name)
                    .map(|v| v == value)
                    .unwrap_or(false);
                if !is_mach {
                    continue;
                }
            }
            // check if the mimetype matches
            let extractor = self
                .extractor_with_name(&extraction_policy.extractor)
                .await?;
            if !matches_mime_type(&extractor.input_mime_types, &content_metadata.content_type) {
                info!(
                    "content {} does not match extractor {}",
                    content_metadata.id, extraction_policy.extractor
                );
                continue;
            }
            matched_policies.push(extraction_policy.clone());
        }
        Ok(matched_policies)
    }

    pub async fn get_extraction_policy(&self, id: &str) -> Result<ExtractionPolicy> {
        let store = self.indexify_state.read().await;
        let extraction_policy = store
            .extraction_policies
            .get(id)
            .ok_or(anyhow!("policy {} not found", id))?;
        Ok(extraction_policy.clone())
    }

    /// Returns the extractor bindings that match the content metadata
    /// If the content metadata does not match any extractor bindings, returns
    /// an empty list Any filtration of extractor bindings based on content
    /// metadata should be done in this function.
    pub async fn content_matching_policy(
        &self,
        policy_id: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let extraction_policy = self.get_extraction_policy(policy_id).await?;
        // get the extractor so we can check the mimetype
        let extractor = self
            .extractor_with_name(&extraction_policy.extractor)
            .await?;
        let content_list = {
            let store = self.indexify_state.read().await;
            let content_list = store
                .content_namespace_table
                .get(&extraction_policy.namespace)
                .cloned()
                .unwrap_or_default();
            let mut content_meta_list = Vec::new();
            for content_id in content_list {
                let content_metadata = store
                    .content_table
                    .get(&content_id)
                    .ok_or(anyhow!("internal error: content {} not found", content_id))?;
                // if the content metadata mimetype does not match the extractor, skip it
                if !matches_mime_type(&extractor.input_mime_types, &content_metadata.content_type) {
                    continue;
                }
                content_meta_list.push(content_metadata.clone());
            }
            content_meta_list
        };
        let mut matched_content_list = Vec::new();
        for content in content_list {
            if content.source != extraction_policy.content_source {
                continue;
            }
            let is_match = &extraction_policy.filters.iter().all(|(name, value)| {
                content
                    .labels
                    .get(name)
                    .map(|v| v == value)
                    .unwrap_or(false)
            });
            if extraction_policy.filters.is_empty() || *is_match {
                matched_content_list.push(content);
            }
        }
        Ok(matched_content_list)
    }

    pub async fn unassigned_tasks(&self) -> Result<Vec<internal_api::Task>> {
        let store = self.indexify_state.read().await;
        let mut tasks = vec![];
        for task_id in store.unassigned_tasks.iter() {
            let task = store
                .tasks
                .get(task_id)
                .ok_or(anyhow!("internal error: task {} not found", task_id))?;
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    pub async fn get_executors_for_extractor(
        &self,
        extractor: &str,
    ) -> Result<Vec<internal_api::ExecutorMetadata>> {
        let store = self.indexify_state.read().await;
        let executor_ids = store
            .extractor_executors_table
            .get(extractor)
            .cloned()
            .unwrap_or(HashSet::new());
        let mut executors = Vec::new();
        for executor_id in executor_ids {
            let executor = store.executors.get(&executor_id).ok_or(anyhow!(
                "internal error: executor id {} not found",
                executor_id
            ))?;
            executors.push(executor.clone());
        }
        Ok(executors)
    }

    pub async fn get_executor_running_task_count(&self) -> HashMap<ExecutorId, usize> {
        self.indexify_state
            .read()
            .await
            .executor_running_task_count
            .clone()
    }

    pub async fn unfinished_tasks_by_extractor(
        &self,
        extractor: &str,
    ) -> Result<HashSet<TaskId>, anyhow::Error> {
        let sm = self.indexify_state.read().await;
        let task_ids = sm
            .unfinished_tasks_by_extractor
            .get(extractor)
            .cloned()
            .unwrap_or_default();
        Ok(task_ids)
    }

    pub async fn list_content(
        &self,
        namespace: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let store = self.indexify_state.read().await;
        let content_ids = store
            .content_namespace_table
            .get(namespace)
            .cloned()
            .unwrap_or_default();
        let mut content = Vec::new();
        for content_id in content_ids {
            let content_metadata = store
                .content_table
                .get(&content_id)
                .ok_or(anyhow!("internal error: content {} not found", content_id))?;
            content.push(content_metadata.clone());
        }
        Ok(content)
    }

    pub async fn remove_executor(&self, executor_id: &str) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::RemoveExecutor {
                executor_id: executor_id.to_string(),
            },
            new_state_changes: vec![StateChange::new(
                executor_id.to_string(),
                internal_api::ChangeType::ExecutorRemoved,
                timestamp_secs(),
            )],
            state_changes_processed: vec![],
        };
        let _resp = self
            .forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to remove executor {}", e))?;
        Ok(())
    }

    pub async fn create_extraction_policy(
        &self,
        extraction_policy: ExtractionPolicy,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateExtractionPolicy {
                extraction_policy: extraction_policy.clone(),
            },
            new_state_changes: vec![StateChange::new(
                extraction_policy.id.clone(),
                internal_api::ChangeType::NewExtractionPolicy,
                timestamp_secs(),
            )],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn update_task(
        &self,
        task: internal_api::Task,
        executor_id: Option<String>,
        content_meta_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<()> {
        let mut state_changes = vec![];
        for content in &content_meta_list {
            state_changes.push(StateChange::new(
                content.id.clone(),
                internal_api::ChangeType::NewContent,
                timestamp_secs(),
            ));
        }
        let mark_finished = task.outcome != internal_api::TaskOutcome::Unknown;
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::UpdateTask {
                task: task.clone(),
                mark_finished,
                executor_id: executor_id.clone(),
                content_metadata: content_meta_list.clone(),
            },
            new_state_changes: state_changes,
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn extractor_with_name(
        &self,
        extractor: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        let store = self.indexify_state.read().await;
        let extractor = store
            .extractors
            .get(extractor)
            .ok_or(anyhow!("extractor {:?} not found", extractor))?;
        Ok(extractor.clone())
    }

    pub async fn list_extraction_policy(&self, namespace: &str) -> Result<Vec<ExtractionPolicy>> {
        let store = self.indexify_state.read().await;
        let extraction_policies = store
            .extraction_policies_table
            .get(namespace)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect_vec();
        Ok(extraction_policies)
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateNamespace {
                name: namespace.to_string(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<internal_api::Namespace>> {
        let store = self.indexify_state.read().await;
        let mut namespaces = Vec::new();
        for namespace in &store.namespaces {
            let extraction_policies = store
                .extraction_policies_table
                .get(namespace)
                .cloned()
                .unwrap_or_default();
            let namespace = internal_api::Namespace {
                name: namespace.clone(),
                extraction_policies: extraction_policies.into_iter().collect_vec(),
            };
            namespaces.push(namespace);
        }
        Ok(namespaces)
    }

    pub async fn namespace(&self, namespace: &str) -> Result<internal_api::Namespace> {
        let store = self.indexify_state.read().await;
        let extraction_policies = store
            .extraction_policies_table
            .get(namespace)
            .cloned()
            .unwrap_or_default();
        let namespace = internal_api::Namespace {
            name: namespace.to_string(),
            extraction_policies: extraction_policies.into_iter().collect_vec(),
        };
        Ok(namespace)
    }

    pub async fn register_executor(
        &self,
        addr: &str,
        executor_id: &str,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::RegisterExecutor {
                addr: addr.to_string(),
                executor_id: executor_id.to_string(),
                extractor,
                ts_secs: timestamp_secs(),
            },
            new_state_changes: vec![StateChange::new(
                executor_id.to_string(),
                internal_api::ChangeType::ExecutorAdded,
                timestamp_secs(),
            )],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        let store = self.indexify_state.read().await;
        let extractors = store.extractors.values().cloned().collect_vec();
        Ok(extractors)
    }

    pub async fn get_executors(&self) -> Result<Vec<internal_api::ExecutorMetadata>> {
        let store = self.indexify_state.read().await;
        let executors = store.executors.values().cloned().collect_vec();
        Ok(executors)
    }

    pub async fn get_executor_by_id(
        &self,
        executor_id: ExecutorIdRef<'_>,
    ) -> Result<internal_api::ExecutorMetadata> {
        let store = self.indexify_state.read().await;
        let executor = store
            .executors
            .get(executor_id)
            .ok_or(anyhow!("executor {} not found", executor_id))?;
        Ok(executor.clone())
    }

    pub async fn commit_task_assignments(
        &self,
        assignments: HashMap<TaskId, ExecutorId>,
        state_change_id: &str,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::AssignTask { assignments },
            new_state_changes: vec![],
            state_changes_processed: vec![StateChangeProcessed {
                state_change_id: state_change_id.to_string(),
                processed_at: timestamp_secs(),
            }],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn create_content_batch(
        &self,
        content_metadata: Vec<internal_api::ContentMetadata>,
    ) -> Result<()> {
        let mut state_changes = vec![];
        for content in &content_metadata {
            state_changes.push(StateChange::new(
                content.id.clone(),
                internal_api::ChangeType::NewContent,
                timestamp_secs(),
            ));
        }
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateContent { content_metadata },
            new_state_changes: state_changes,
            state_changes_processed: vec![],
        };
        let _ = self
            .forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to create content metadata: {}", e.to_string()))?;
        Ok(())
    }

    pub async fn get_conent_metadata(
        &self,
        content_id: &str,
    ) -> Result<internal_api::ContentMetadata> {
        let store = self.indexify_state.read().await;
        let content_metadata = store
            .content_table
            .get(content_id)
            .ok_or(anyhow!("content not found"))?;
        Ok(content_metadata.clone())
    }

    pub async fn get_content_metadata_batch(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let store = self.indexify_state.read().await;
        let mut content_metadata_list = Vec::new();
        for content_id in content_ids {
            let content_metadata = store.content_table.get(&content_id);
            if let Some(content_metadata) = content_metadata {
                content_metadata_list.push(content_metadata.clone());
            }
        }
        Ok(content_metadata_list)
    }

    pub async fn create_tasks(
        &self,
        tasks: Vec<internal_api::Task>,
        state_change_id: &str,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateTasks { tasks },
            new_state_changes: vec![],
            state_changes_processed: vec![StateChangeProcessed {
                state_change_id: state_change_id.to_string(),
                processed_at: timestamp_secs(),
            }],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<internal_api::Task>> {
        let store = self.indexify_state.read().await;
        let tasks = store
            .task_assignments
            .get(executor_id)
            .map(|task_ids| {
                let limit = limit.unwrap_or(task_ids.len() as u64);
                task_ids.iter().take(limit as usize).cloned().collect_vec()
            })
            .map(|task_ids| {
                task_ids
                    .iter()
                    .map(|task_id| store.tasks.get(task_id).unwrap().clone())
                    .collect_vec()
            })
            .unwrap_or(vec![]);
        Ok(tasks)
    }

    pub async fn task_with_id(&self, task_id: &str) -> Result<internal_api::Task> {
        let store = self.indexify_state.read().await;
        let task = store.tasks.get(task_id).ok_or(anyhow!("task not found"))?;
        Ok(task.clone())
    }

    pub async fn list_indexes(&self, namespace: &str) -> Result<Vec<internal_api::Index>> {
        let store = self.indexify_state.read().await;
        let indexes = store
            .namespace_index_table
            .get(namespace)
            .cloned()
            .unwrap_or_default();
        let indexes = indexes.into_iter().collect_vec();
        Ok(indexes)
    }

    pub async fn get_index(&self, id: &str) -> Result<internal_api::Index> {
        let store = self.indexify_state.read().await;
        let index = store
            .index_table
            .get(id)
            .ok_or(anyhow!("index not found"))?;
        Ok(index.clone())
    }

    pub async fn create_index(
        &self,
        namespace: &str,
        index: internal_api::Index,
        id: String,
    ) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateIndex {
                namespace: namespace.to_string(),
                index,
                id,
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub fn start_periodic_membership_check(self: &Arc<Self>, mut shutdown_rx: Receiver<()>) {
        let app_clone = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(MEMBERSHIP_CHECK_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("shutting down periodic membership check");
                        break;
                    }
                    _ = interval.tick() => {
                        if app_clone.is_seed_node() {
                            continue;
                        }
                        if let Err(e) = app_clone.check_cluster_membership().await {
                            error!("failed to check cluster membership: {}", e);
                        }
                    }
                }
            }
        });
    }

    pub async fn check_cluster_membership(
        &self,
    ) -> Result<store::requests::StateMachineUpdateResponse, anyhow::Error> {
        self.network
            .join_cluster(self.id, &self.node_addr, &self.seed_node)
            .await
    }
}

async fn watch_for_leader_change(
    forwardable_raft: ForwardableRaft,
    leader_change_tx: Sender<bool>,
    mut shutdown_rx: Receiver<()>,
) -> Result<()> {
    let mut rx = forwardable_raft.raft.metrics();
    let prev_server_state = RefCell::new(openraft::ServerState::Learner);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("shutting down leader change watcher");
                return Ok(());
            }
            _ = rx.changed() => {
                let server_state = rx.borrow_and_update().state;
                let mut prev_srvr_state = prev_server_state.borrow_mut();
                if !(prev_srvr_state).eq(&server_state) {
                    info!("raft change metrics prev {:?} current {:?}", prev_srvr_state, server_state);
                    let result = leader_change_tx.send(server_state.is_leader()).map_err(|e| anyhow!("unable to send leader change: {}", e));
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            error!("unable to send leader change: {}", e);
                        }
                    }
                    // replace the previous state with the new state
                    *prev_srvr_state = server_state;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use indexify_internal_api::Index;

    use crate::{
        state::{
            store::requests::{RequestPayload, StateMachineUpdateRequest},
            App,
        },
        test_utils::RaftTestCluster,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_read_own_write() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateIndex {
                index: Index::default(),
                namespace: "namespace".into(),
                id: "id".into(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let read_back = |node: Arc<App>| async move {
            match node.get_index("id").await {
                Ok(read_result) if read_result == Index::default() => Ok(true),
                Ok(_) => Ok(false),
                Err(_) => Ok(false), /*  NOTE: It isn't a mistake to return false here because if
                                      * the index cannot be found `get_index` throws an error */
            }
        };
        cluster.read_own_write(request, read_back, true).await?;
        Ok(())
    }
}
