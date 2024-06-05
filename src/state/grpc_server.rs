use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::lock::Mutex;
use indexify_proto::indexify_raft::{
    raft_api_server::RaftApi,
    InstallSnapshotRequest,
    RaftReply,
    RaftRequest,
    SnapshotFileChunkRequest,
};
use openraft::{
    error::{CheckIsLeaderError, Fatal, ForwardToLeader, RaftError},
    BasicNode,
    Snapshot,
    SnapshotMeta,
    StorageError,
    StorageIOError,
    Vote,
};
use requests::{RequestPayload, StateMachineUpdateRequest, StateMachineUpdateResponse};
use tokio::{fs, fs::File, io::AsyncWriteExt};
use tonic::{Request, Status};
use tracing::info;

use super::{raft_client::RaftClient, NodeId};
use crate::{
    grpc_helper::GrpcHelper,
    metrics::raft_metrics,
    state::{store::requests, Raft, SnapshotData},
};

struct SnapshotState {
    snapshot_id: String,
    files: HashMap<String, File>,
    dir: Box<SnapshotData>,
}

pub struct RaftGrpcServer {
    id: NodeId,
    raft: Arc<Raft>,
    raft_client: Arc<RaftClient>,
    address: String,
    coordinator_address: String,
    receiving_snapshot: Arc<Mutex<Option<SnapshotState>>>,
}

impl RaftGrpcServer {
    pub fn new(
        id: NodeId,
        raft: Arc<Raft>,
        raft_client: Arc<RaftClient>,
        address: String,
        coordinator_addr: String,
    ) -> Self {
        Self {
            id,
            raft,
            raft_client,
            address,
            coordinator_address: coordinator_addr,
            receiving_snapshot: Arc::new(Mutex::new(None)),
        }
    }

    fn get_request_addr(&self, request: &Request<RaftRequest>) -> String {
        if let Some(addr) = request.remote_addr() {
            addr.to_string()
        } else {
            "unknown address".to_string()
        }
    }

    fn incr_recv_bytes(&self, request: &Request<RaftRequest>) {
        let addr = self.get_request_addr(request);
        let bytes_recv = request.get_ref().data.len() as u64;
        raft_metrics::network::incr_recv_bytes(&addr, bytes_recv);
    }

    /// Get nodes from the cluster
    fn get_nodes_in_cluster(&self) -> BTreeMap<u64, BasicNode> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<BTreeMap<_, _>>()
    }

    /// Helper function to add node to the cluster only if it is not present
    async fn add_node_to_cluster_if_absent(
        &self,
        node_id: NodeId,
        address: &str,
        coordinator_addr: &str,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let nodes_in_cluster = self.get_nodes_in_cluster();
        if nodes_in_cluster.contains_key(&node_id) {
            let response = StateMachineUpdateResponse {
                handled_by: self.id,
            };
            return GrpcHelper::ok_response(response);
        }

        info!(
            "Received request from new node with id {} and address {}",
            node_id, address
        );
        let node_to_add = BasicNode {
            addr: address.to_string(),
        };

        self.raft
            .add_learner(node_id, node_to_add.clone(), true)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        info!("Done adding node {} as a learner", node_id);

        let nodes_in_cluster = self.get_nodes_in_cluster(); //  re-fetch the nodes in the cluster to get the latest view (sync point)
        let node_ids: Vec<u64> = nodes_in_cluster.keys().cloned().collect();
        self.raft
            .change_membership(node_ids, true)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        //  add the coordinator address to state machine along with the leader
        // coordinator address
        let state_machine_req = StateMachineUpdateRequest {
            payload: RequestPayload::JoinCluster {
                node_id: self.id,
                address: self.address.clone(),
                coordinator_addr: self.coordinator_address.clone(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.raft
            .client_write(state_machine_req)
            .await
            .map_err(|e| {
                GrpcHelper::internal_err(format!("Error writing to state machine: {}", e))
            })?;

        let state_machine_req = StateMachineUpdateRequest {
            payload: RequestPayload::JoinCluster {
                node_id,
                address: address.to_string(),
                coordinator_addr: coordinator_addr.to_string(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.raft
            .client_write(state_machine_req)
            .await
            .map_err(|e| {
                GrpcHelper::internal_err(format!("Error writing to state machine: {}", e))
            })?;

        let response = StateMachineUpdateResponse {
            handled_by: self.id,
        };
        GrpcHelper::ok_response(response)
    }

    /// Helper function to detect whether the current node is the leader
    async fn ensure_leader(&self) -> Result<Option<ForwardToLeader<NodeId, BasicNode>>, Status> {
        let result = self.raft.ensure_linearizable().await;
        match result {
            Ok(_) => Ok(None),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(err))) => Ok(Some(err)),
            Err(e) => Err(GrpcHelper::internal_err(e.to_string())),
        }
    }

    async fn handle_client_write(
        &self,
        request: StateMachineUpdateRequest,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let response = StateMachineUpdateResponse {
            handled_by: self.id,
        };
        self.raft
            .client_write(request)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;
        GrpcHelper::ok_response(response)
    }

    async fn begin_receiving_snapshot<'a>(
        &self,
        snapshot_id: &str,
        state: &'a mut Option<SnapshotState>,
    ) -> Result<(), Fatal<NodeId>> {
        let dir = match self.raft.begin_receiving_snapshot().await {
            Ok(dir) => dir,
            Err(RaftError::APIError(_)) => {
                panic!("Failed with infallible error");
            }
            Err(RaftError::Fatal(e)) => {
                return Err(e);
            }
        };
        *state = Some(SnapshotState {
            snapshot_id: snapshot_id.to_string(),
            files: HashMap::new(),
            dir,
        });
        Ok(())
    }

    async fn trasnfer_snapshot_(
        &self,
        chunk: SnapshotFileChunkRequest,
    ) -> Result<(), Fatal<NodeId>> {
        let mut receive_state = self.receiving_snapshot.lock().await;
        match receive_state.as_ref() {
            None => {
                self.begin_receiving_snapshot(&chunk.snapshot_id, &mut receive_state)
                    .await?;
            }
            Some(state) => {
                if state.snapshot_id != chunk.snapshot_id {
                    fs::remove_dir_all(&state.dir.snapshot_dir)
                        .await
                        .map_err(|e| {
                            let storage_error: StorageError<_> =
                                StorageIOError::<u64>::write_snapshot(None, &e).into();
                            Fatal::from(storage_error)
                        })?;
                    self.begin_receiving_snapshot(&chunk.snapshot_id, &mut receive_state)
                        .await?;
                }
            }
        };
        let state = receive_state.as_mut().ok_or_else(|| {
            let storage_error: StorageError<_> = StorageIOError::<u64>::write_snapshot(
                None,
                anyhow!("Snapshot state not found after starting snapshot receive"),
            )
            .into();
            Fatal::from(storage_error)
        })?;

        let file = match state.files.entry(chunk.name.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let file_path = state.dir.snapshot_dir.join(&chunk.name);
                let file = File::create(&file_path).await.map_err(|e| {
                    let storage_error: StorageError<_> =
                        StorageIOError::<u64>::write_snapshot(None, &e).into();
                    Fatal::from(storage_error)
                })?;
                entry.insert(file)
            }
        };

        file.write_all(&chunk.data).await.map_err(|e| {
            let storage_error: StorageError<_> =
                StorageIOError::<u64>::write_snapshot(None, &e).into();
            Fatal::from(storage_error)
        })
    }
}

#[async_trait]
impl RaftApi for RaftGrpcServer {
    async fn forward(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        self.incr_recv_bytes(&request);

        if (self.ensure_leader().await?).is_some() {
            return Err(GrpcHelper::internal_err(
                "The node we thought was the leader is not the leader",
            ));
        }

        let req = GrpcHelper::parse_req::<StateMachineUpdateRequest>(request)?;

        if let RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        } = req.payload
        {
            return self
                .add_node_to_cluster_if_absent(node_id, &address, &coordinator_addr)
                .await;
        }

        self.handle_client_write(req).await
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        self.incr_recv_bytes(&request);

        let ae_req = GrpcHelper::parse_req(request)?;
        let resp = self
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        GrpcHelper::ok_response(resp)
    }

    async fn vote(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        self.incr_recv_bytes(&request);

        let v_req = GrpcHelper::parse_req(request)?;

        let resp = self
            .raft
            .vote(v_req)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        GrpcHelper::ok_response(resp)
    }

    async fn transfer_snapshot(
        &self,
        request: Request<SnapshotFileChunkRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let chunk = request.into_inner();
        let vote: Vote<u64> = serde_json::from_str(&chunk.vote)
            .map_err(|e| Status::invalid_argument(format!("failed to parse vote: {}", e)))?;
        let my_vote = match self.raft.with_raft_state(|state| *state.vote_ref()).await {
            Ok(vote) => vote,
            Err(e) => {
                return GrpcHelper::err_response(e);
            }
        };
        if vote < my_vote {
            // terminate early if vote is stale
            return GrpcHelper::ok_response(my_vote);
        }

        let ret = match self.trasnfer_snapshot_(chunk).await {
            Ok(_) => Ok(my_vote),
            Err(e) => Err(e),
        };
        GrpcHelper::result_response(ret)
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let msg = request.into_inner();

        let vote = serde_json::from_str(&msg.vote)
            .map_err(|e| Status::invalid_argument(format!("failed to parse vote: {}", e)))?;
        let meta: SnapshotMeta<NodeId, BasicNode> = serde_json::from_str(&msg.snapshot_meta)
            .map_err(|e| {
                Status::invalid_argument(format!("failed to parse snapshot meta: {}", e))
            })?;

        let snapshot_id = meta.snapshot_id.clone();
        let mut receive_state = self.receiving_snapshot.lock().await;
        let state = match receive_state.as_mut() {
            None => {
                return Err(GrpcHelper::internal_err(format!(
                    "Snapshot state not found"
                )));
            }
            Some(state) => {
                if state.snapshot_id != snapshot_id {
                    return Err(GrpcHelper::internal_err(format!("Snapshot id mismatch")));
                } else {
                    state
                }
            }
        };
        let snapshot = Snapshot {
            meta,
            snapshot: state.dir.clone(),
        };
        let resp = self.raft.install_full_snapshot(vote, snapshot).await;
        GrpcHelper::result_response(resp)
    }

    async fn join_cluster(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        self.incr_recv_bytes(&request);

        let req = GrpcHelper::parse_req::<StateMachineUpdateRequest>(request)?;

        let RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        } = req.payload
        else {
            return Err(GrpcHelper::internal_err("Invalid request"));
        };

        //  check if this node is the leader
        if let Some(forward_to_leader) = self.ensure_leader().await? {
            let leader_address = forward_to_leader
                .leader_node
                .ok_or_else(|| GrpcHelper::internal_err("Leader node not found"))?
                .addr;
            let mut client = self
                .raft_client
                .get(&leader_address)
                .await
                .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;
            let forwarding_req = GrpcHelper::encode_raft_request(&StateMachineUpdateRequest {
                payload: RequestPayload::JoinCluster {
                    node_id,
                    address,
                    coordinator_addr,
                },
                new_state_changes: vec![],
                state_changes_processed: vec![],
            })
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

            let bytes_sent = forwarding_req.data.len() as u64;
            raft_metrics::network::incr_sent_bytes(&leader_address, bytes_sent);

            return client.forward(forwarding_req).await;
        };

        //  This node is the leader - we've confirmed it
        self.add_node_to_cluster_if_absent(node_id, &address, &coordinator_addr)
            .await
    }
}
