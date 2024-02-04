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
use indexify_internal_api as internal_api;
use indexify_proto::indexify_raft::raft_api_server::RaftApiServer;
use internal_api::{ExtractorBinding, StateChange};
use itertools::Itertools;
use network::Network;
use openraft::{
    self,
    error::{InitializeError, RaftError},
    BasicNode,
    TokioRuntime,
};
use store::{requests::Request, Response};
use tokio::{
    sync::{
        watch::{self, Receiver, Sender},
        Mutex,
        RwLock,
    },
    task::JoinHandle,
};
use tracing::{error, info, warn};

use self::{
    grpc_server::RaftGrpcServer,
    store::{
        requests::{RequestPayload, StateChangeProcessed},
        state_machine_objects::IndexifyState,
        ExecutorId,
        TaskId,
    },
};
use crate::{
    coordinator_filters::matches_mime_type,
    server_config::ServerConfig,
    state::store::new_storage,
    utils::timestamp_secs,
};

pub mod grpc_server;
pub mod network;
pub mod raft_client;
pub mod store;

pub type NodeId = u64;

pub type SnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
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

pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    nodes: BTreeMap<NodeId, BasicNode>,
    shutdown_rx: Receiver<()>,
    shutdown_tx: Sender<()>,
    pub leader_change_rx: Receiver<bool>,
    join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,
    pub indexify_state: Arc<RwLock<IndexifyState>>,
    pub config: Arc<openraft::Config>,
    state_change_rx: Receiver<StateChange>,
}

impl App {
    pub async fn new(server_config: Arc<ServerConfig>) -> Result<Arc<Self>> {
        let raft_config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| anyhow!("invalid raft config: {}", e.to_string()))?,
        );
        let db_path = server_config.sled.path.clone().unwrap_or_default().clone();
        let db_path: &Path = Path::new(db_path.as_str());
        let (log_store, state_machine) = new_storage(db_path).await;
        let state_change_rx = state_machine.state_change_rx.clone();

        let indexify_state = state_machine.data.indexify_state.clone();

        let network = Network::new();

        let raft = openraft::Raft::new(
            server_config.node_id,
            config.clone(),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| anyhow!("unable to create raft: {}", e.to_string()))?;

        let mut nodes = BTreeMap::new();
        for peer in &server_config.peers {
            nodes.insert(
                peer.node_id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }
        let (tx, rx) = watch::channel::<()>(());

        let addr = server_config
            .raft_addr_sock()
            .map_err(|e| anyhow!("unable to create raft address : {}", e.to_string()))?;

        info!("starting raft server at {}", addr.to_string());
        let raft_srvr = RaftApiServer::new(RaftGrpcServer::new(Arc::new(raft.clone())));
        let (leader_change_tx, leader_change_rx) = tokio::sync::watch::channel::<bool>(false);

        let app = Arc::new(App {
            id: server_config.node_id,
            addr: server_config
                .coordinator_lis_addr_sock()
                .map_err(|e| anyhow!("unable to get coordinator address : {}", e.to_string()))?
                .to_string(),
            raft,
            shutdown_rx: rx,
            shutdown_tx: tx,
            leader_change_rx,
            join_handles: Mutex::new(vec![]),
            nodes,
            indexify_state,
            config,
            state_change_rx,
        });

        let raft_clone = app.raft.clone();

        let mut rx = app.shutdown_rx.clone();
        let shutdown_rx = app.shutdown_rx.clone();
        // Start for leadership changes
        tokio::spawn(async move {
            let _ = watch_for_leader_change(raft_clone, leader_change_tx, shutdown_rx).await;
        });

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

        Ok(app)
    }

    pub async fn initialize_raft(&self) -> Result<()> {
        match self.raft.initialize(self.nodes.clone()).await {
            // .map_err(|e| anyhow!("unable to initialize raft: {}", e)) {
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
        let _ = self.raft.shutdown().await;
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
        let req = Request {
            payload: RequestPayload::MarkStateChangesProcessed { state_changes },
            state_changes: vec![],
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }

    pub async fn filter_extractor_binding_for_content(
        &self,
        content_id: &str,
    ) -> Result<Vec<ExtractorBinding>> {
        let content_metadata = self.get_conent_metadata(content_id).await?;
        let bindings = {
            let store = self.indexify_state.read().await;
            store
                .bindings_table
                .get(&content_metadata.repository)
                .cloned()
                .unwrap_or_default()
        };
        let mut matched_bindings = Vec::new();
        for binding in &bindings {
            if binding.content_source != content_metadata.source {
                continue;
            }
            for (name, value) in &binding.filters {
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
            let extractor = self.extractor_with_name(&binding.extractor).await?;
            if !matches_mime_type(&extractor.input_mime_types, &content_metadata.content_type) {
                info!(
                    "content {} does not match extractor {}",
                    content_metadata.id, binding.extractor
                );
                continue;
            }
            matched_bindings.push(binding.clone());
        }
        Ok(matched_bindings)
    }

    pub async fn get_extractor_binding(&self, id: &str) -> Result<ExtractorBinding> {
        let store = self.indexify_state.read().await;
        let binding = store
            .extractor_bindings
            .get(id)
            .ok_or(anyhow!("binding {} not found", id))?;
        Ok(binding.clone())
    }

    /// Returns the extractor bindings that match the content metadata
    /// If the content metadata does not match any extractor bindings, returns
    /// an empty list Any filtration of extractor bindings based on content
    /// metadata should be done in this function.
    pub async fn content_matching_binding(
        &self,
        binding_id: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let extractor_binding = self.get_extractor_binding(binding_id).await?;
        // get the extractor so we can check the mimetype
        let extractor = self
            .extractor_with_name(&extractor_binding.extractor)
            .await?;
        let content_list = {
            let store = self.indexify_state.read().await;
            let content_list = store
                .content_repository_table
                .get(&extractor_binding.repository)
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
            if content.source != extractor_binding.content_source {
                continue;
            }
            let is_match = &extractor_binding.filters.iter().all(|(name, value)| {
                content
                    .labels
                    .get(name)
                    .map(|v| v == value)
                    .unwrap_or(false)
            });
            if extractor_binding.filters.is_empty() || *is_match {
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

    pub async fn list_content(
        &self,
        repository: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let store = self.indexify_state.read().await;
        let content_ids = store
            .content_repository_table
            .get(repository)
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
        let req = Request {
            payload: RequestPayload::RemoveExecutor {
                executor_id: executor_id.to_string(),
            },
            state_changes: vec![StateChange::new(
                executor_id.to_string(),
                internal_api::ChangeType::ExecutorRemoved,
                timestamp_secs(),
            )],
        };
        let _resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("unable to remove executor {}", e))?;
        Ok(())
    }

    pub async fn create_binding(&self, binding: ExtractorBinding) -> Result<()> {
        let req = Request {
            payload: RequestPayload::CreateBinding {
                binding: binding.clone(),
            },
            state_changes: vec![StateChange::new(
                binding.id.clone(),
                internal_api::ChangeType::NewBinding,
                timestamp_secs(),
            )],
        };
        let _resp = self.raft.client_write(req).await?;
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
        let req = Request {
            payload: RequestPayload::UpdateTask {
                task: task.clone(),
                mark_finished,
                executor_id: executor_id.clone(),
                content_metadata: content_meta_list.clone(),
            },
            state_changes,
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }

    pub async fn extractor_with_name(
        &self,
        extractor: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        let store = self.indexify_state.read().await;
        let binding = store
            .extractors
            .get(extractor)
            .ok_or(anyhow!("extractor {:?} not found", extractor))?;
        Ok(binding.clone())
    }

    pub async fn list_bindings(&self, repository: &str) -> Result<Vec<ExtractorBinding>> {
        let store = self.indexify_state.read().await;
        let bindings = store
            .bindings_table
            .get(repository)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect_vec();
        Ok(bindings)
    }

    pub async fn create_repository(&self, repository: &str) -> Result<()> {
        let req = Request {
            payload: RequestPayload::CreateRepository {
                name: repository.to_string(),
            },
            state_changes: vec![],
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_repositories(&self) -> Result<Vec<internal_api::Repository>> {
        let store = self.indexify_state.read().await;
        let mut repositories = Vec::new();
        for repository_name in &store.repositories {
            let bindings = store
                .bindings_table
                .get(repository_name)
                .cloned()
                .unwrap_or_default();
            let repository = internal_api::Repository {
                name: repository_name.clone(),
                extractor_bindings: bindings.into_iter().collect_vec(),
            };
            repositories.push(repository);
        }
        Ok(repositories)
    }

    pub async fn get_repository(&self, repository: &str) -> Result<internal_api::Repository> {
        let store = self.indexify_state.read().await;
        let bindings = store
            .bindings_table
            .get(repository)
            .cloned()
            .unwrap_or_default();
        let repository = internal_api::Repository {
            name: repository.to_string(),
            extractor_bindings: bindings.into_iter().collect_vec(),
        };
        Ok(repository)
    }

    pub async fn register_executor(
        &self,
        addr: &str,
        executor_id: &str,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<()> {
        let req = Request {
            payload: RequestPayload::RegisterExecutor {
                addr: addr.to_string(),
                executor_id: executor_id.to_string(),
                extractor,
                ts_secs: timestamp_secs(),
            },
            state_changes: vec![StateChange::new(
                executor_id.to_string(),
                internal_api::ChangeType::ExecutorAdded,
                timestamp_secs(),
            )],
        };
        let _resp = self.raft.client_write(req).await?;
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

    pub async fn commit_task_assignments(
        &self,
        assignments: HashMap<TaskId, ExecutorId>,
    ) -> Result<()> {
        let req = Request {
            payload: RequestPayload::AssignTask { assignments },
            state_changes: vec![],
        };
        let _resp = self.raft.client_write(req).await?;
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
        let req = Request {
            payload: RequestPayload::CreateContent { content_metadata },
            state_changes,
        };
        let _ = self
            .raft
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

    pub async fn create_tasks(&self, tasks: Vec<internal_api::Task>) -> Result<()> {
        let req = Request {
            payload: RequestPayload::CreateTasks { tasks },
            state_changes: vec![],
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }

    pub async fn tasks_for_executor(&self, executor_id: &str) -> Result<Vec<internal_api::Task>> {
        let store = self.indexify_state.read().await;
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

    pub async fn task_with_id(&self, task_id: &str) -> Result<internal_api::Task> {
        let store = self.indexify_state.read().await;
        let task = store.tasks.get(task_id).ok_or(anyhow!("task not found"))?;
        Ok(task.clone())
    }

    pub async fn list_indexes(&self, repository: &str) -> Result<Vec<internal_api::Index>> {
        let store = self.indexify_state.read().await;
        let indexes = store
            .repository_extractors
            .get(repository)
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
        repository: &str,
        index: internal_api::Index,
        id: String,
    ) -> Result<()> {
        let req = Request {
            payload: RequestPayload::CreateIndex {
                repository: repository.to_string(),
                index,
                id,
            },
            state_changes: vec![],
        };
        let _resp = self.raft.client_write(req).await?;
        Ok(())
    }
}

async fn watch_for_leader_change(
    raft: Raft,
    leader_change_tx: Sender<bool>,
    mut shutdown_rx: Receiver<()>,
) -> Result<()> {
    let mut rx = raft.metrics();
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
