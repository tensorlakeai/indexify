use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use indexify_proto::indexify_raft::{
    raft_api_server::RaftApi,
    RaftReply,
    RaftRequest,
    SnapshotFrame,
};
use openraft::{
    error::{CheckIsLeaderError, ForwardToLeader, RaftError},
    BasicNode,
    SnapshotMeta,
    Vote,
};
use requests::{RequestPayload, StateMachineUpdateRequest, StateMachineUpdateResponse};
use sha2::{Digest, Sha256};
use tonic::{Request, Status, Streaming};
use tracing::info;

use super::{raft_client::RaftClient, snapshot_receiver::SnapshotReceiver, NodeId};
use crate::{
    grpc_helper::GrpcHelper,
    metrics::{raft_metrics, CounterGuard},
    state::{store::requests, Raft},
};

pub struct RaftGrpcServer {
    id: NodeId,
    raft: Arc<Raft>,
    raft_client: Arc<RaftClient>,
    address: String,
    coordinator_address: String,
    snapshot_path: std::path::PathBuf,
}

impl RaftGrpcServer {
    pub fn new(
        id: NodeId,
        raft: Arc<Raft>,
        raft_client: Arc<RaftClient>,
        address: String,
        coordinator_addr: String,
        snapshot_path: std::path::PathBuf,
    ) -> Self {
        Self {
            id,
            raft,
            raft_client,
            address,
            coordinator_address: coordinator_addr,
            snapshot_path,
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

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let request_addr = self.get_request_addr(&request);
        let _guard_inflight = {
            CounterGuard::new(&request_addr, move |addr, cnt| {
                raft_metrics::network::incr_snapshot_recv_inflight(addr, cnt);
            })
        };
        self.incr_recv_bytes(&request);

        let remote_addr = self.get_request_addr(&request);
        let is_req: openraft::raft::InstallSnapshotRequest<super::TypeConfig> =
            GrpcHelper::parse_req(request)?;
        let snapshot_size = is_req.data.len() as u64;
        let resp = self.raft.install_snapshot(is_req).await.map_err(|e| {
            raft_metrics::network::incr_snapshot_recv_failure(&remote_addr);
            GrpcHelper::internal_err(e.to_string())
        });

        if resp.is_ok() {
            raft_metrics::network::incr_snapshot_recv_success(&remote_addr);
            raft_metrics::network::add_snapshot_size(snapshot_size);
            raft_metrics::network::set_last_snapshot_creation_time(std::time::Instant::now());
        } else {
            raft_metrics::network::incr_snapshot_recv_failure(&remote_addr);
        }

        match resp {
            Ok(resp) => GrpcHelper::ok_response(resp),
            Err(e) => Err(e),
        }
    }

    async fn install_snapshot_stream(
        &self,
        request: Request<Streaming<SnapshotFrame>>,
    ) -> Result<tonic::Response<RaftReply>, Status> {
        let request_addr = if let Some(addr) = request.remote_addr() {
            addr.to_string()
        } else {
            "unknown address".to_string()
        };
        let _guard_inflight = {
            CounterGuard::new(&request_addr, move |addr, cnt| {
                raft_metrics::network::incr_snapshot_recv_inflight(addr, cnt);
            })
        };
        let mut snapshot_receiver =
            SnapshotReceiver::new(&self.snapshot_path)
                .await
                .map_err(|e| {
                    GrpcHelper::internal_err(format!("Error creating snapshot receiver: {}", e))
                })?;

        //  data that should come in via stream
        let mut snapshot_meta: Option<SnapshotMeta<NodeId, BasicNode>> = None;
        let mut vote_info: Option<Vote<NodeId>> = None;
        let mut offset: Option<u64> = None;

        //  integrity checks
        let mut total_bytes_written = 0;
        let mut expected_size = 0;
        let mut hasher = Sha256::new();
        let mut final_hash = None;

        let mut stream = request.into_inner();
        while let Some(frame) = stream.message().await? {
            match frame.frame_type.unwrap() {
                indexify_proto::indexify_raft::snapshot_frame::FrameType::StartSnapshot(start) => {
                    expected_size = start.total_size;
                }
                indexify_proto::indexify_raft::snapshot_frame::FrameType::SnapshotData(data) => {
                    hasher.update(data.data.clone());
                    snapshot_receiver
                        .write_chunk(&data.data)
                        .await
                        .map_err(|e| {
                            GrpcHelper::internal_err(format!("Error writing snapshot chunk: {}", e))
                        })?;
                    total_bytes_written += data.data.len() as u64;
                }
                indexify_proto::indexify_raft::snapshot_frame::FrameType::EndSnapshot(end) => {
                    final_hash = Some(end.hash);
                    snapshot_meta =
                        Some(serde_json::from_str(&end.metadata_json).map_err(|e| {
                            GrpcHelper::internal_err(format!(
                                "Error parsing snapshot metadata: {}",
                                e
                            ))
                        })?);
                    vote_info = Some(serde_json::from_str(&end.vote_json).map_err(|e| {
                        GrpcHelper::internal_err(format!("Error parsing vote: {}", e))
                    })?);
                    offset = Some(end.offset);
                    snapshot_receiver.finish().await.map_err(|e| {
                        GrpcHelper::internal_err(format!(
                            "Error flushing snapshot stream to disk: {}",
                            e
                        ))
                    })?;
                }
            }
        }

        if let Some(expected_hash) = final_hash {
            let computed_hash = format!("{:x}", hasher.finalize());
            if computed_hash != expected_hash || total_bytes_written != expected_size {
                return Err(tonic::Status::internal("Data corruption detected"));
            }
        } else {
            return Err(tonic::Status::internal(
                "Snapshot transmission incomplete: no end frame received",
            ));
        }

        let snapshot_data = snapshot_receiver
            .read_data()
            .await
            .map_err(|e| GrpcHelper::internal_err(format!("Error reading snapshot data: {}", e)))?;
        let snapshot_size = snapshot_data.len() as u64;

        let snapshot_req: openraft::raft::InstallSnapshotRequest<super::TypeConfig> =
            openraft::raft::InstallSnapshotRequest {
                vote: vote_info.unwrap(),
                meta: snapshot_meta.unwrap(),
                offset: offset.unwrap(),
                data: snapshot_data,
                done: true,
            };

        let resp = self.raft.install_snapshot(snapshot_req).await.map_err(|e| {
            raft_metrics::network::incr_snapshot_recv_failure(&request_addr);
            GrpcHelper::internal_err(e.to_string())
        });

        match resp {
            Ok(resp) => {
                raft_metrics::network::incr_snapshot_recv_success(&request_addr);
                raft_metrics::network::add_snapshot_size(snapshot_size);
                raft_metrics::network::set_last_snapshot_creation_time(std::time::Instant::now());
                GrpcHelper::ok_response(resp)
            }
            Err(e) => {
                raft_metrics::network::incr_snapshot_recv_failure(&request_addr);
                Err(e)
            }
        }
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
