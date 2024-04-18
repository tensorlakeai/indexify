#![allow(clippy::uninlined_format_args)]
#![deny(unused_qualifications)]

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    io::Cursor,
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use grpc_server::RaftGrpcServer;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_raft::raft_api_server::RaftApiServer;
use internal_api::{ExtractionPolicy, StateChange, StructuredDataSchema};
use itertools::Itertools;
use network::Network;
use openraft::{
    self,
    error::{InitializeError, RaftError},
    BasicNode,
    TokioRuntime,
};
use serde::Serialize;
use store::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    ExecutorId,
    ExecutorIdRef,
    Response,
    TaskId,
};
use tokio::{
    sync::{
        broadcast,
        watch::{self, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{error, info, warn};

use self::{
    forwardable_raft::ForwardableRaft,
    store::{StateMachineColumns, StateMachineStore},
};
use crate::{
    coordinator_filters::matches_mime_type,
    garbage_collector::GarbageCollector,
    metrics::{
        coordinator::Metrics,
        raft_metrics::{self, network::MetricsSnapshot},
    },
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

#[derive(Serialize)]
pub struct RaftMetrics {
    pub openraft_metrics: openraft::RaftMetrics<NodeId, BasicNode>,
    pub raft_metrics: MetricsSnapshot,
}

pub struct App {
    pub id: NodeId,
    pub addr: String,
    coordinator_addr: String,
    seed_node: String,
    pub forwardable_raft: ForwardableRaft,
    nodes: BTreeMap<NodeId, BasicNode>,
    shutdown_rx: Receiver<()>,
    shutdown_tx: Sender<()>,
    pub leader_change_rx: Receiver<bool>,
    join_handles: Mutex<Vec<JoinHandle<Result<()>>>>,
    pub config: Arc<openraft::Config>,
    state_change_rx: Receiver<StateChange>,
    pub network: Network,
    pub node_addr: String,
    pub state_machine: Arc<StateMachineStore>,
    pub garbage_collector: Arc<GarbageCollector>,
    pub metrics: Metrics,
}

#[derive(Clone)]
pub struct RaftConfigOverrides {
    snapshot_policy: Option<openraft::SnapshotPolicy>,
}

impl App {
    pub async fn new(
        server_config: Arc<ServerConfig>,
        overrides: Option<RaftConfigOverrides>,
        garbage_collector: Arc<GarbageCollector>,
        coordinator_addr: &str,
    ) -> Result<Arc<Self>> {
        let mut raft_config = openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            enable_heartbeat: true,
            ..Default::default()
        };

        // Apply any overrides provided
        if let Some(overrides) = overrides {
            if let Some(snapshot_policy) = overrides.snapshot_policy {
                raft_config.snapshot_policy = snapshot_policy;
            }
        }

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
        let db_path_str = db_path.as_str().to_owned() + "/db";
        let sm_blob_store_path_str = db_path.as_str().to_owned() + "/sm-blob";
        let db_path: &Path = Path::new(&db_path_str);
        let sm_blob_store_path: &Path = Path::new(&sm_blob_store_path_str);

        let (log_store, state_machine) = new_storage(db_path, sm_blob_store_path).await;
        let state_change_rx = state_machine.state_change_rx.clone();

        let raft_client = Arc::new(RaftClient::new());
        let network = Network::new(Arc::clone(&raft_client));

        let raft = openraft::Raft::new(
            server_config.node_id,
            config.clone(),
            network.clone(),
            log_store,
            Arc::clone(&state_machine),
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
            addr.to_string(),
            server_config.coordinator_addr.clone(),
        ));
        let (leader_change_tx, leader_change_rx) = tokio::sync::watch::channel::<bool>(false);

        let metrics = Metrics::new(state_machine.clone());

        let app = Arc::new(App {
            id: server_config.node_id,
            addr: server_config
                .coordinator_lis_addr_sock()
                .map_err(|e| anyhow!("unable to get coordinator address : {}", e.to_string()))?
                .to_string(),
            coordinator_addr: coordinator_addr.to_string(),
            seed_node: server_config.seed_node.clone(),
            forwardable_raft,
            shutdown_rx: rx,
            shutdown_tx: tx,
            leader_change_rx,
            join_handles: Mutex::new(vec![]),
            nodes,
            config,
            state_change_rx,
            network,
            node_addr: format!("{}:{}", server_config.listen_if, server_config.raft_port),
            state_machine,
            garbage_collector,
            metrics,
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
        let mut state_changes = vec![];
        for event_id in self
            .state_machine
            .get_unprocessed_state_changes()
            .await
            .iter()
        {
            let event = self
                .state_machine
                .get_from_cf::<StateChange, _>(StateMachineColumns::StateChanges, event_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Event with id {} not found", event_id))?;
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

    /// This method uses the content id to fetch the associated extraction
    /// policies based on certain filters and checks which policies can be
    /// applied to the content It's the mirror equivalent to
    /// content_matching_policy
    pub async fn filter_extraction_policy_for_content(
        &self,
        content_id: &str,
    ) -> Result<Vec<ExtractionPolicy>> {
        let content_metadata = self.get_conent_metadata(content_id).await?;
        if content_metadata.tombstoned {
            return Ok(vec![]);
        }
        let extraction_policy_ids = {
            self.state_machine
                .get_extraction_policies_table()
                .await
                .get(&content_metadata.namespace)
                .cloned()
                .unwrap_or_default()
        };
        let extraction_policies = self
            .state_machine
            .get_extraction_policies_from_ids(extraction_policy_ids)
            .await?
            .unwrap_or_else(Vec::new);

        let mut matched_policies = Vec::new();
        for extraction_policy in &extraction_policies {
            //  Check whether the sources match. Make an additional check in case the
            // content has  a source which is an extraction policy id instead of
            // a name
            if extraction_policy.content_source != content_metadata.source &&
                self.get_extraction_policy(&content_metadata.source)
                    .await
                    .map_or(true, |retrieved_extraction_policy| {
                        extraction_policy.content_source != retrieved_extraction_policy.name
                    })
            {
                continue;
            }
            // Check if all filters match the content metadata labels. If not, skip.
            if !extraction_policy.filters.iter().all(|(name, value)| {
                content_metadata
                    .labels
                    .get(name)
                    .map_or(false, |v| v == value)
            }) {
                continue;
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
        let extraction_policy = self
            .state_machine
            .get_from_cf::<ExtractionPolicy, _>(StateMachineColumns::ExtractionPolicies, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Extraction policy with id {} not found", id))?;
        Ok(extraction_policy)
    }

    pub async fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
    ) -> Result<Option<Vec<ExtractionPolicy>>> {
        self.state_machine
            .get_extraction_policies_from_ids(extraction_policy_ids)
            .await
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
            let content_list = self
                .state_machine
                .get_content_namespace_table()
                .await
                .get(&extraction_policy.namespace)
                .cloned()
                .unwrap_or_default();
            let mut content_meta_list = Vec::new();
            for content_id in content_list {
                let content_metadata = self
                    .state_machine
                    .get_from_cf::<internal_api::ContentMetadata, _>(
                        StateMachineColumns::ContentTable,
                        &content_id,
                    )
                    .await?;
                // if the content metadata mimetype does not match the extractor, skip it
                //  if the content metadata is tombstoned, skip it
                if let Some(content_metadata) = content_metadata {
                    if !matches_mime_type(
                        &extractor.input_mime_types,
                        &content_metadata.content_type,
                    ) {
                        continue;
                    }
                    if content_metadata.tombstoned {
                        continue;
                    }
                    content_meta_list.push(content_metadata);
                }
            }
            content_meta_list
        };

        let mut matched_content_list = Vec::new();
        for content in content_list {
            //  Check whether the sources match. Make an additional check in case the
            // content has a source which is an extraction policy id instead of a name
            if content.source != extraction_policy.content_source &&
                self.get_extraction_policy(&content.source).await.map_or(
                    true,
                    |retrieved_extraction_policy| {
                        extraction_policy.content_source != retrieved_extraction_policy.name
                    },
                )
            {
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
        let mut tasks = vec![];
        for task_id in self.state_machine.get_unassigned_tasks().await.iter() {
            let task = self
                .state_machine
                .get_from_cf::<internal_api::Task, _>(StateMachineColumns::Tasks, task_id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "Unable to get task with id {} from state machine store",
                        task_id
                    )
                })?;
            tasks.push(task.clone());
        }
        Ok(tasks)
    }

    pub async fn task_assignments(&self) -> Result<HashMap<ExecutorId, TaskId>> {
        self.state_machine.get_all_task_assignments().await
    }

    pub async fn get_executors_for_extractor(
        &self,
        extractor: &str,
    ) -> Result<Vec<internal_api::ExecutorMetadata>> {
        let executor_ids = self
            .state_machine
            .get_extractor_executors_table()
            .await
            .get(extractor)
            .cloned()
            .unwrap_or(HashSet::new());
        self.state_machine
            .get_executors_from_ids(executor_ids)
            .await
    }

    pub async fn get_executor_running_task_count(&self) -> HashMap<ExecutorId, usize> {
        self.state_machine.get_executor_running_task_count().await
    }

    pub async fn unfinished_tasks_by_extractor(
        &self,
        extractor: &str,
    ) -> Result<HashSet<TaskId>, anyhow::Error> {
        let task_ids = self
            .state_machine
            .get_unfinished_tasks_by_extractor()
            .await
            .get(extractor)
            .cloned()
            .unwrap_or_default();
        Ok(task_ids)
    }

    pub async fn list_content(
        &self,
        namespace: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let content_ids = self
            .state_machine
            .get_content_namespace_table()
            .await
            .get(namespace)
            .cloned()
            .unwrap_or_default();
        self.state_machine.get_content_from_ids(content_ids).await
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
        updated_structured_data_schema: Option<StructuredDataSchema>,
    ) -> Result<()> {
        // TODO: Add delete_extraction_policy. This will only
        // remove the actual object from the forward and reverse indexes. Leave
        // artifacts in place

        //  Check if the extraction policy has already been created. If so, don't create
        // it
        let existing_policies = self
            .state_machine
            .get_extraction_policies_from_ids(HashSet::from_iter(
                vec![extraction_policy.id.clone()].into_iter(),
            ))
            .await?;

        if let Some(policies) = existing_policies {
            if !policies.is_empty() {
                info!(
                    "The extraction policy with id {} already exists, ignoring this request",
                    extraction_policy.id
                );
                return Ok(()); // Return immediately if the policy already
                               // exists.
            }
        }

        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateExtractionPolicy {
                extraction_policy: extraction_policy.clone(),
                updated_structured_data_schema,
                new_structured_data_schema: StructuredDataSchema::new(
                    &extraction_policy.name,
                    &extraction_policy.namespace,
                ),
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
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::UpdateTask {
                task: task.clone(),
                executor_id: executor_id.clone(),
                content_metadata: content_meta_list.clone(),
                update_time: SystemTime::now(),
            },
            new_state_changes: state_changes,
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    // pub async fn create_gc_tasks(
    //     &self,
    //     content_id: &str,
    // ) -> Result<Vec<internal_api::GarbageCollectionTask>> {
    //     //  Get the metadata of the children of the content id
    //     let content_tree_metadata = self.get_content_tree_metadata(content_id)?;
    //     let mut output_tables = HashMap::new();
    //     let mut policy_ids = HashMap::new();

    //     for content_metadata in &content_tree_metadata {
    //         let content_extraction_policy_mappings = self
    //
    // .get_content_extraction_policy_mappings_for_content_id(&content_metadata.id)
    //             .await?;

    //         if content_extraction_policy_mappings.is_none() {
    //             continue;
    //         }

    //         let mappings = content_extraction_policy_mappings.unwrap();
    //         let applied_extraction_policy_ids: HashSet<String> =
    //             mappings.time_of_policy_completion.keys().cloned().collect();
    //         let applied_extraction_policies = self
    //             .get_extraction_policies_from_ids(applied_extraction_policy_ids)
    //             .await?;

    //         if applied_extraction_policies.is_none() {
    //             continue;
    //         }

    //         let policy_id = &applied_extraction_policies
    //             .as_ref()
    //             .unwrap()
    //             .iter()
    //             .next()
    //             .map(|policy| policy.id.clone())
    //             .unwrap_or("".to_string());
    //         policy_ids.insert(content_metadata.id.clone(),
    // policy_id.to_string());

    //         for applied_extraction_policy in
    // applied_extraction_policies.clone().unwrap() {
    // output_tables.insert(                 content_metadata.id.clone(),
    //                 applied_extraction_policy
    //                     .index_name_table_mapping
    //                     .values()
    //                     .cloned()
    //                     .collect::<HashSet<_>>(),
    //             );
    //         }
    //     }

    //     if let Some(forward_to_leader) =
    // self.forwardable_raft.ensure_leader().await? {         //  forward to the
    // leader         let leader_node = forward_to_leader
    //             .leader_node
    //             .ok_or_else(|| anyhow::anyhow!("could not get leader address"))?;
    //         self.network
    //             .create_gc_tasks(
    //                 content_tree_metadata,
    //                 output_tables,
    //                 policy_ids,
    //                 &leader_node.addr,
    //             )
    //             .await?;
    //         return Ok(Vec::new());
    //     }

    //     //  this is the leader
    //     let gc_tasks = self
    //         .garbage_collector
    //         .create_gc_tasks(content_tree_metadata, output_tables, policy_ids)
    //         .await?;

    //     let request = StateMachineUpdateRequest {
    //         payload: RequestPayload::CreateOrAssignGarbageCollectionTask {
    //             gc_tasks: gc_tasks.clone(),
    //         },
    //         new_state_changes: vec![],
    //         state_changes_processed: vec![],
    //     };
    //     self.forwardable_raft.client_write(request).await?;

    //     Ok(gc_tasks)
    // }

    pub async fn create_gc_tasks(
        &self,
        gc_tasks: Vec<indexify_internal_api::GarbageCollectionTask>,
    ) -> Result<()> {
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.forwardable_raft.client_write(request).await?;
        Ok(())
    }

    pub async fn update_gc_task(&self, gc_task: internal_api::GarbageCollectionTask) -> Result<()> {
        let mark_finished = gc_task.outcome != internal_api::TaskOutcome::Unknown;
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::UpdateGarbageCollectionTask {
                gc_task,
                mark_finished,
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn extractor_with_name(
        &self,
        extractor: &str,
    ) -> Result<internal_api::ExtractorDescription> {
        let extractor = self
            .state_machine
            .get_from_cf::<internal_api::ExtractorDescription, _>(
                StateMachineColumns::Extractors,
                extractor,
            )
            .await?
            .ok_or_else(|| anyhow!("Extractor with name {} not found", extractor))?;
        Ok(extractor)
    }

    pub async fn list_extraction_policy(&self, namespace: &str) -> Result<Vec<ExtractionPolicy>> {
        let extraction_policy_ids = {
            self.state_machine
                .get_extraction_policies_table()
                .await
                .get(namespace)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect_vec()
        };
        let extraction_policies = self
            .state_machine
            .get_extraction_policies_from_ids(extraction_policy_ids.into_iter().collect())
            .await?
            .unwrap_or_else(Vec::new);
        Ok(extraction_policies)
    }

    pub async fn create_namespace(&self, namespace: &str) -> Result<()> {
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateNamespace {
                name: namespace.to_string(),
                structured_data_schema: StructuredDataSchema::new("ingestion", namespace),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<internal_api::Namespace>> {
        //  Fetch the namespaces from the db
        let namespaces: Vec<String> = self
            .state_machine
            .get_all_rows_from_cf::<String>(StateMachineColumns::Namespaces)
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        // Fetch extraction policies for each namespace
        let mut result_namespaces = Vec::new();
        for namespace_name in namespaces {
            let extraction_policy_ids = {
                self.state_machine
                    .get_extraction_policies_table()
                    .await
                    .get(&namespace_name)
                    .cloned()
                    .unwrap_or_default()
            };
            let extraction_policies = self
                .state_machine
                .get_extraction_policies_from_ids(extraction_policy_ids)
                .await?
                .unwrap_or_else(Vec::new);

            let namespace = internal_api::Namespace {
                name: namespace_name,
                extraction_policies: extraction_policies.into_iter().collect_vec(),
            };
            result_namespaces.push(namespace);
        }

        Ok(result_namespaces)
    }

    pub async fn namespace(&self, namespace: &str) -> Result<Option<internal_api::Namespace>> {
        self.state_machine.get_namespace(namespace).await
    }

    pub async fn register_executor(
        &self,
        addr: &str,
        executor_id: &str,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<String> {
        let state_change = StateChange::new(
            executor_id.to_string(),
            internal_api::ChangeType::ExecutorAdded,
            timestamp_secs(),
        );
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::RegisterExecutor {
                addr: addr.to_string(),
                executor_id: executor_id.to_string(),
                extractor,
                ts_secs: timestamp_secs(),
            },
            new_state_changes: vec![state_change.clone()],
            state_changes_processed: vec![],
        };
        let _resp = self.forwardable_raft.client_write(req).await?;
        Ok(state_change.id)
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        let extractors: Vec<internal_api::ExtractorDescription> = self
            .state_machine
            .get_all_rows_from_cf::<internal_api::ExtractorDescription>(
                StateMachineColumns::Extractors,
            )
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        Ok(extractors)
    }

    pub async fn get_executors(&self) -> Result<Vec<internal_api::ExecutorMetadata>> {
        let executors: Vec<internal_api::ExecutorMetadata> = self
            .state_machine
            .get_all_rows_from_cf::<internal_api::ExecutorMetadata>(StateMachineColumns::Executors)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        Ok(executors)
    }

    pub async fn get_executor_by_id(
        &self,
        executor_id: ExecutorIdRef<'_>,
    ) -> Result<internal_api::ExecutorMetadata> {
        let executor = self
            .state_machine
            .get_from_cf::<internal_api::ExecutorMetadata, _>(
                StateMachineColumns::Executors,
                executor_id,
            )
            .await?
            .ok_or_else(|| anyhow!("Executor with id {} not found", executor_id))?;
        Ok(executor)
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
        self.forwardable_raft.client_write(req).await?;
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

    pub async fn tombstone_content_batch(
        &self,
        namespace: &str,
        content_ids: &[String],
    ) -> Result<()> {
        let mut state_changes = vec![];
        for content_id in content_ids {
            state_changes.push(StateChange::new(
                content_id.clone(),
                internal_api::ChangeType::TombstoneContent,
                timestamp_secs(),
            ));
        }
        let req = StateMachineUpdateRequest {
            payload: RequestPayload::TombstoneContent {
                namespace: namespace.to_string(),
                content_ids: content_ids.iter().cloned().collect(),
            },
            new_state_changes: state_changes,
            state_changes_processed: vec![],
        };
        let _ = self
            .forwardable_raft
            .client_write(req)
            .await
            .map_err(|e| anyhow!("Unable to tombstone content metadata: {}", e.to_string()))?;
        Ok(())
    }

    pub async fn get_conent_metadata(
        &self,
        content_id: &str,
    ) -> Result<internal_api::ContentMetadata> {
        let content_metadata = self
            .state_machine
            .get_from_cf::<internal_api::ContentMetadata, _>(
                StateMachineColumns::ContentTable,
                content_id,
            )
            .await?
            .ok_or_else(|| anyhow!("Content with id {} not found", content_id))?;
        Ok(content_metadata)
    }

    pub async fn get_content_metadata_batch(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        let content_ids: HashSet<String> = content_ids.into_iter().collect();
        self.state_machine.get_content_from_ids(content_ids).await
    }

    pub async fn get_content_tree_metadata(
        &self,
        content_id: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        self.state_machine
            .get_content_tree_metadata(content_id)
            .await
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

    pub async fn list_tasks(
        &self,
        namespace: &str,
        extraction_policy: Option<String>,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks: Vec<internal_api::Task> = self
            .state_machine
            .get_all_rows_from_cf::<internal_api::Task>(StateMachineColumns::Tasks)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        let filtered_tasks = tasks
            .iter()
            .filter(|task| task.namespace == namespace)
            .filter(|task| {
                extraction_policy
                    .as_ref()
                    .map(|eb| eb == &task.extraction_policy_id)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        Ok(filtered_tasks)
    }

    pub async fn tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks = self
            .state_machine
            .get_tasks_for_executor(executor_id, limit)
            .await?;
        Ok(tasks)
    }

    pub async fn task_with_id(&self, task_id: &str) -> Result<internal_api::Task> {
        let task = self
            .state_machine
            .get_from_cf::<internal_api::Task, _>(StateMachineColumns::Tasks, task_id)
            .await?
            .ok_or_else(|| anyhow!("Task with id {} not found", task_id))?;
        Ok(task)
    }

    pub async fn gc_task_with_id(
        &self,
        gc_task_id: &str,
    ) -> Result<internal_api::GarbageCollectionTask> {
        let gc_task = self
            .state_machine
            .get_from_cf(StateMachineColumns::GarbageCollectionTasks, gc_task_id)
            .await?
            .ok_or_else(|| anyhow!("Garbage collection task with id {} not found", gc_task_id))?;
        Ok(gc_task)
    }

    pub async fn list_indexes(&self, namespace: &str) -> Result<Vec<internal_api::Index>> {
        let index_ids = {
            self.state_machine
                .get_namespace_index_table()
                .await
                .get(namespace)
                .cloned()
                .unwrap_or_default()
        };
        let indexes = self.state_machine.get_indexes_from_ids(index_ids).await?;
        Ok(indexes)
    }

    pub async fn get_index(&self, id: &str) -> Result<internal_api::Index> {
        let index = self
            .state_machine
            .get_from_cf::<internal_api::Index, _>(StateMachineColumns::IndexTable, id)
            .await?
            .ok_or_else(|| anyhow!("Index with id {} not found", id))?;
        Ok(index)
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

    pub async fn list_state_changes(&self) -> Result<Vec<StateChange>> {
        let state_changes = self
            .state_machine
            .get_all_rows_from_cf::<StateChange>(StateMachineColumns::StateChanges)
            .await?
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        Ok(state_changes)
    }

    pub async fn get_structured_data_schema(
        &self,
        namespace: &str,
        content_source: &str,
    ) -> Result<StructuredDataSchema> {
        let id = StructuredDataSchema::schema_id(namespace, content_source);
        let schema = self
            .state_machine
            .get_from_cf::<StructuredDataSchema, _>(StateMachineColumns::StructuredDataSchemas, &id)
            .await?
            .ok_or_else(|| anyhow!("Schema with id {} not found", id))?;
        Ok(schema)
    }

    pub async fn get_schemas_for_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<StructuredDataSchema>> {
        let schemas_for_ns = self
            .state_machine
            .get_schemas_by_namespace()
            .await
            .get(namespace)
            .cloned()
            .unwrap_or(HashSet::new());
        let schemas = self.state_machine.get_schemas(schemas_for_ns).await?;
        Ok(schemas)
    }

    pub async fn get_unfinished_tasks_by_extractor(
        &self,
    ) -> HashMap<store::ExtractorName, HashSet<TaskId>> {
        self.state_machine.get_unfinished_tasks_by_extractor().await
    }

    pub async fn insert_executor_running_task_count(&mut self, executor_id: &str, task_count: u64) {
        self.state_machine
            .insert_executor_running_task_count(executor_id, task_count)
            .await;
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
            .join_cluster(
                self.id,
                &self.node_addr,
                &self.coordinator_addr,
                &self.seed_node,
            )
            .await
    }

    pub fn get_raft_metrics(&self) -> RaftMetrics {
        let raft_metrics = raft_metrics::network::get_metrics_snapshot();
        let rx = self.forwardable_raft.raft.metrics();
        let openraft_metrics = rx.borrow().clone();

        RaftMetrics {
            openraft_metrics,
            raft_metrics,
        }
    }

    pub async fn subscribe_to_gc_task_events(
        &self,
    ) -> broadcast::Receiver<indexify_internal_api::GarbageCollectionTask> {
        self.state_machine.subscribe_to_gc_task_events().await
    }

    pub async fn ensure_leader(&self) -> Result<Option<typ::ForwardToLeader>> {
        self.forwardable_raft.ensure_leader().await
    }

    pub async fn get_coordinator_addr(&self, node_id: NodeId) -> Result<Option<String>> {
        self.state_machine.get_coordinator_addr(node_id).await
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
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use indexify_internal_api::{ContentMetadata, Index, TaskOutcome};

    use crate::{
        state::{
            store::{
                requests::{RequestPayload, StateMachineUpdateRequest},
                ExecutorId,
                TaskId,
            },
            App,
        },
        test_utils::RaftTestCluster,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_read_own_write() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
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

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_read_own_write_forwarding() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
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
                Err(_) => Ok(false),
            }
        };
        cluster.read_own_write(request, read_back, false).await?;
        Ok(())
    }

    /// Test to determine that an index that was created can be read back
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_write_read_index() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;
        let index_to_write = Index {
            name: "name".into(),
            namespace: "test".into(),
            ..Default::default()
        };
        node.create_index("namespace", index_to_write.clone(), "id".into())
            .await?;
        let result = node.get_index("id").await?;
        assert_eq!(index_to_write, result);
        let indexes = node.list_indexes("namespace").await?;
        assert!(indexes.len() == 1);
        Ok(())
    }

    /// Test to determine that a task that was created can be read back
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_write_read_task() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        let content = ContentMetadata {
            id: "content_id".to_string(),
            ..Default::default()
        };
        node.create_content_batch(vec![content.clone()]).await?;

        let task = indexify_internal_api::Task {
            id: "id".into(),
            content_metadata: content.clone(),
            ..Default::default()
        };
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateTasks {
                tasks: vec![task.clone()],
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };

        let read_back = {
            move |node: Arc<App>| async move {
                match node.task_with_id("id").await {
                    Ok(read_result) if read_result.id == "id" => Ok(true),
                    Ok(_) => Ok(false),
                    Err(_) => Ok(false),
                }
            }
        };
        cluster.read_own_write(request, read_back, true).await?;
        Ok(())
    }

    /// Test to determine that assigning a task to an executor works correctly
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_write_read_task_assignment() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(1, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  First create a task and ensure it's written
        let content = ContentMetadata {
            id: "content_id".to_string(),
            ..Default::default()
        };
        node.create_content_batch(vec![content.clone()]).await?;
        let task = indexify_internal_api::Task {
            id: "task_id".into(),
            content_metadata: content.clone(),
            ..Default::default()
        };
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateTasks {
                tasks: vec![task.clone()],
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };

        let read_back = {
            move |node: Arc<App>| async move {
                match node.task_with_id("task_id").await {
                    Ok(read_result) if read_result.id == "task_id" => Ok(true),
                    Ok(_) => Ok(false),
                    Err(_) => Ok(false),
                }
            }
        };
        cluster.read_own_write(request, read_back, true).await?;

        //  Second, assign the task to some executor
        let assignments: HashMap<TaskId, ExecutorId> =
            vec![("task_id".into(), "executor_id".into())]
                .into_iter()
                .collect();
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::AssignTask { assignments },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };

        let read_back = |node: Arc<App>| async move {
            match node.tasks_for_executor("executor_id", None).await {
                Ok(tasks_vec)
                    if tasks_vec.len() == 1 && tasks_vec.first().unwrap().id == "task_id" =>
                {
                    Ok(true)
                }
                Ok(_) => Ok(false),
                Err(_) => Ok(false),
            }
        };
        cluster.read_own_write(request, read_back, true).await?;

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_automatic_task_creation() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(1, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create a piece of content
        let content_id = "content_id";
        let content_metadata = ContentMetadata {
            id: content_id.into(),
            content_type: "text/plain".into(),
            ..Default::default()
        };
        node.create_content_batch(vec![content_metadata]).await?;

        //  Create a default namespace
        let namespace = "namespace";
        node.create_namespace(namespace).await?;

        //  Register an executor
        let executor_id = "executor_id";
        let extractor_name = "extractor";
        let extractor = indexify_internal_api::ExtractorDescription {
            name: extractor_name.into(),
            input_mime_types: vec!["text/plain".into()],
            ..Default::default()
        };
        let addr = "addr";
        node.register_executor(addr, executor_id, extractor.clone())
            .await?;

        //  Set an extraction policy for the content that will force task creation
        let extraction_policy = indexify_internal_api::ExtractionPolicy {
            name: "extraction_policy".into(),
            namespace: namespace.into(),
            extractor: extractor_name.into(),
            ..Default::default()
        };
        node.create_extraction_policy(extraction_policy.clone(), None)
            .await?;

        let _tasks = node
            .list_tasks(namespace, Some(extraction_policy.id))
            .await?;

        Ok(())
    }

    /// Test to determine that updating a task works correctly
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_update_task_and_read() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create a task and ensure that it can be read back
        let content = ContentMetadata {
            id: "content_id".to_string(),
            ..Default::default()
        };
        node.create_content_batch(vec![content.clone()]).await?;
        let task = indexify_internal_api::Task {
            id: "task_id".into(),
            content_metadata: content.clone(),
            ..Default::default()
        };
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateTasks {
                tasks: vec![task.clone()],
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let read_back = {
            move |node: Arc<App>| async move {
                match node.task_with_id("task_id").await {
                    Ok(read_result) if read_result.id == "task_id" => Ok(true),
                    Ok(_) => Ok(false),
                    Err(_) => Ok(false),
                }
            }
        };
        cluster.read_own_write(request, read_back, true).await?;

        //  Assign the task to an executor
        let assignments: HashMap<TaskId, ExecutorId> =
            vec![("task_id".into(), "executor_id".into())]
                .into_iter()
                .collect();
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::AssignTask { assignments },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        let read_back = |node: Arc<App>| async move {
            match node.tasks_for_executor("executor_id", None).await {
                Ok(tasks_vec)
                    if tasks_vec.len() == 1 &&
                        tasks_vec.first().unwrap().id == "task_id" &&
                        tasks_vec.first().unwrap().outcome == TaskOutcome::Unknown =>
                {
                    Ok(true)
                }
                Ok(_) => Ok(false),
                Err(_) => Ok(false),
            }
        };
        cluster.read_own_write(request, read_back, true).await?;

        //  Update the task and mark it as complete by calling the update_task method
        let task = indexify_internal_api::Task {
            id: "task_id".into(),
            content_metadata: content.clone(),
            outcome: indexify_internal_api::TaskOutcome::Success,
            ..Default::default()
        };
        let executor_id = "executor_id";
        let content_meta_list: Vec<ContentMetadata> =
            std::iter::repeat(indexify_internal_api::ContentMetadata::default())
                .take(3)
                .collect();
        let node = cluster.get_raft_node(0)?;
        node.update_task(task, Some(executor_id.into()), content_meta_list)
            .await?;

        //  Read the task back and expect to find the outcome of the task set to Success
        let retrieved_task = node.task_with_id("task_id").await?;
        assert_eq!(retrieved_task.outcome, TaskOutcome::Success);

        Ok(())
    }

    /// Test to create, register, read back and remove an executor and
    /// associated extractors Executors are typically created along with
    /// extractors so both need to be asserted
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_read_remove_executors() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create an executor and extractor and ensure they can be read back
        let executor_id = "executor_id";
        let extractor = indexify_internal_api::ExtractorDescription {
            name: "extractor".into(),
            ..Default::default()
        };
        let addr = "addr";
        node.register_executor(addr, executor_id, extractor.clone())
            .await?;

        //  Read the executors from multiple functions
        let executors = node.get_executors().await?;
        assert_eq!(executors.len(), 1);

        let executor = node.get_executor_by_id(executor_id).await?;
        assert_eq!(executor.id, executor_id);

        let executors = node.get_executors_for_extractor(&extractor.name).await?;
        assert_eq!(executors.len(), 1);
        assert_eq!(executors.first().unwrap().id, executor_id);

        //  Read the extractors
        let extractors = node.list_extractors().await?;
        assert_eq!(extractors.len(), 1);

        let retrieved_extractor = node.extractor_with_name(&extractor.name).await?;
        assert_eq!(retrieved_extractor, extractor);

        //  Remove the executor that was created and assert that it was removed
        node.remove_executor(executor_id).await?;
        let executors = node.get_executors().await?;
        assert_eq!(executors.len(), 0);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_and_read_content() -> Result<(), anyhow::Error> {
        let content_size = 3;

        let cluster = RaftTestCluster::new(1, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create some content
        let mut content_metadata_vec: Vec<ContentMetadata> = Vec::new();
        for i in 0..content_size {
            let content_metadata = ContentMetadata {
                id: format!("id{}", i),
                ..Default::default()
            };
            content_metadata_vec.push(content_metadata);
        }
        node.create_content_batch(content_metadata_vec.clone())
            .await?;

        //  Read the content back
        let read_content = node
            .list_content(&content_metadata_vec.first().unwrap().namespace)
            .await?;
        assert_eq!(read_content.len(), content_size);

        //  Read back all the pieces of content
        let read_content = node
            .get_content_metadata_batch(
                content_metadata_vec
                    .iter()
                    .map(|content| content.id.clone())
                    .collect(),
            )
            .await?;
        assert_eq!(read_content.len(), content_size);

        //  Read back a specific piece of content
        let read_content = node
            .get_conent_metadata(&content_metadata_vec[0].id)
            .await?;
        assert_eq!(read_content, content_metadata_vec[0]);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_read_and_match_extraction_policies() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(1, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create some content
        let content_labels = vec![
            ("label1".to_string(), "value1".to_string()),
            ("label2".to_string(), "value2".to_string()),
            ("label3".to_string(), "value3".to_string()),
        ];
        let content_metadata = ContentMetadata {
            namespace: "namespace".into(),
            name: "name".into(),
            labels: content_labels.into_iter().collect(),
            content_type: "*/*".into(),
            source: "source".into(),
            ..Default::default()
        };
        node.create_content_batch(vec![content_metadata.clone()])
            .await?;

        //  Create an executor and associated extractor
        let executor_id = "executor_id";
        let extractor = indexify_internal_api::ExtractorDescription {
            name: "extractor".into(),
            input_mime_types: vec!["*/*".into()],
            ..Default::default()
        };
        let addr = "addr";
        node.register_executor(addr, executor_id, extractor.clone())
            .await?;

        //  Create the extraction policy under the namespace of the content
        let extraction_policy = indexify_internal_api::ExtractionPolicy {
            namespace: content_metadata.namespace.clone(),
            content_source: "source".into(),
            extractor: extractor.name,
            filters: vec![
                ("label1".to_string(), "value1".to_string()),
                ("label2".to_string(), "value2".to_string()),
                ("label3".to_string(), "value3".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        node.create_extraction_policy(extraction_policy.clone(), None)
            .await?;

        //  Read the policy back using namespace
        let read_policy = node
            .list_extraction_policy(&extraction_policy.namespace)
            .await?;
        assert_eq!(read_policy.len(), 1);

        //  Read the policy back using the id
        let read_policy = node.get_extraction_policy(&extraction_policy.id).await?;
        assert_eq!(read_policy, extraction_policy);

        //  Fetch the content based on the policy id and check that the retrieved
        // content is correct
        let matched_content = node.content_matching_policy(&extraction_policy.id).await?;
        assert_eq!(matched_content.len(), 1);
        assert_eq!(matched_content.first().unwrap(), &content_metadata);

        //  Fetch the policy based on the content id and check that the retrieved policy
        // is correct
        let matched_policies = node
            .filter_extraction_policy_for_content(&content_metadata.id)
            .await?;
        assert_eq!(matched_policies.len(), 1);
        assert_eq!(matched_policies.first().unwrap(), &extraction_policy);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_and_read_namespaces() -> Result<(), anyhow::Error> {
        let cluster = RaftTestCluster::new(1, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  Create a namespace
        let namespace = "namespace";
        node.create_namespace(namespace).await?;

        //  Create 3 extraction policies using the same namespace but all other
        // attributes as default
        let extraction_policies = vec![
            indexify_internal_api::ExtractionPolicy {
                id: "id1".into(),
                namespace: namespace.into(),
                ..Default::default()
            },
            indexify_internal_api::ExtractionPolicy {
                id: "id2".into(),
                namespace: namespace.into(),
                ..Default::default()
            },
            indexify_internal_api::ExtractionPolicy {
                id: "id3".into(),
                namespace: namespace.into(),
                ..Default::default()
            },
        ];
        for policy in &extraction_policies {
            node.create_extraction_policy(policy.clone(), None).await?;
        }

        //  Read the namespace back and expect to get the extraction policies as well
        // which will be asserted
        let retrieved_namespace = node.namespace(namespace).await?;
        assert_eq!(retrieved_namespace.clone().unwrap().name, namespace);
        assert_eq!(
            retrieved_namespace
                .clone()
                .unwrap()
                .extraction_policies
                .len(),
            3
        );

        // Read all namespaces back and assert that only the created namespace is
        // present along with the extraction policies
        let namespaces = node.list_namespaces().await?;
        assert_eq!(namespaces.len(), 1);
        assert_eq!(namespaces.first().unwrap().name, namespace);

        Ok(())
    }
}
