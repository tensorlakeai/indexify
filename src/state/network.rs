use std::{error::Error, fmt::Display, sync::Arc};

use anyerror::AnyError;
use openraft::{
    error::{NetworkError, RemoteError, Unreachable},
    network::{RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode,
};
use tonic::IntoRequest;

use super::store::requests::StateMachineUpdateResponse;
use crate::{
    grpc_helper::GrpcHelper,
    metrics::{
        create_timed_future,
        raft_metrics::{self, network::incr_snapshot_recv_seconds},
        CounterGuard,
    },
    state::{
        raft_client::RaftClient,
        store::requests::{RequestPayload, StateMachineUpdateRequest},
        typ::{InstallSnapshotError, RPCError, RaftError},
        NodeId, TypeConfig,
    },
};

pub struct Network {
    raft_client: Arc<RaftClient>,
}

impl Default for Network {
    fn default() -> Self {
        let raft_client = Arc::new(RaftClient::new());
        Self::new(raft_client)
    }
}

impl Clone for Network {
    fn clone(&self) -> Self {
        Network {
            raft_client: Arc::clone(&self.raft_client),
        }
    }
}

impl Network {
    pub fn new(raft_client: Arc<RaftClient>) -> Self {
        Self { raft_client }
    }

    /// This method is used when a state machine request was received by a
    /// non-leader node to forward it to a leader node
    pub async fn forward(
        &self,
        target_addr: &str,
        request: StateMachineUpdateRequest,
    ) -> Result<StateMachineUpdateResponse, anyhow::Error> {
        let mut client = self
            .raft_client
            .get(target_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get raft client: {}", e))?;

        let tonic_request = GrpcHelper::encode_raft_request(&request)?.into_request();

        let bytes_sent = tonic_request.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(target_addr.into(), bytes_sent);

        let response = client
            .forward(tonic_request)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        let result: Result<StateMachineUpdateResponse, _> =
            serde_json::from_str(&response.into_inner().data);
        let reply = result.map_err(|e| {
            raft_metrics::network::incr_sent_failures(target_addr.into());
            return anyhow::anyhow!(format!(
                "Failed to parse the response received from forwarding a state machine request: {}",
                e.to_string()
            ));
        })?;

        Ok(reply)
    }

    /// This method is used to allow a node to try to join the main cluster
    /// after it comes up. The node makes this request periodically
    pub async fn join_cluster(
        &self,
        node_id: NodeId,
        node_addr: &str,
        target_addr: &str,
    ) -> Result<StateMachineUpdateResponse, anyhow::Error> {
        let mut client = self
            .raft_client
            .get(target_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get raft client: {}", e))?;

        let request = GrpcHelper::encode_raft_request(&StateMachineUpdateRequest {
            payload: RequestPayload::JoinCluster {
                node_id,
                address: node_addr.into(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        })?
        .into_request();

        let bytes_sent = request.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(target_addr.into(), bytes_sent);

        let response = client
            .join_cluster(request)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        let reply = serde_json::from_str::<StateMachineUpdateResponse>(&response.into_inner().data)
            .map_err(|e| {
                raft_metrics::network::incr_sent_failures(target_addr.into());
                return anyhow::anyhow!(
                    "Failed to parse the response received from sending a join_cluster request: {}",
                    e.to_string()
                );
            })?;

        Ok(reply)
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            target_node: node.clone(),
            raft_client: self.raft_client.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
    raft_client: Arc<RaftClient>,
}

impl NetworkConnection {
    fn status_to_unreachable<E>(&self, status: tonic::Status) -> RPCError<RaftError<E>>
    where
        E: Error,
    {
        RPCError::Unreachable(Unreachable::new(&status))
    }

    /// Wrap a RaftError with RPCError
    pub(crate) fn to_rpc_err<E: Error>(&self, e: RaftError<E>) -> RPCError<RaftError<E>> {
        let remote_err = RemoteError::new_with_node(self.target, self.target_node.clone(), e);
        RPCError::RemoteError(remote_err)
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<RaftError>> {
        let mut client = self
            .raft_client
            .clone()
            .get(&self.target_node.addr)
            .await
            .map_err(|e| self.status_to_unreachable(tonic::Status::aborted(e.to_string())))?;

        let raft_req = GrpcHelper::encode_raft_request(&req).map_err(|e| Unreachable::new(&e))?;
        let req = GrpcHelper::into_req(raft_req);

        let bytes_sent = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(self.target_node.addr.clone(), bytes_sent);

        let grpc_res = client.append_entries(req).await;

        let resp = grpc_res.map_err(|e| {
            raft_metrics::network::incr_sent_failures(self.target_node.addr.clone());
            return self.status_to_unreachable(e);
        })?;

        let raft_res = GrpcHelper::parse_raft_reply(resp)
            .map_err(|serde_err| new_net_err(&serde_err, || "parse append_entries reply"))?;

        raft_res.map_err(|e| self.to_rpc_err(e))
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCError<RaftError<InstallSnapshotError>>> {
        let _guard_inflight = CounterGuard::new(self.target_node.addr.clone(), move |addr, cnt| {
            raft_metrics::network::incr_snapshot_send_inflight(addr, cnt);
        });

        let mut client = self
            .raft_client
            .get(&self.target_node.addr)
            .await
            .map_err(|e| self.status_to_unreachable(tonic::Status::aborted(e.to_string())))?;

        let raft_req = GrpcHelper::encode_raft_request(&req).map_err(|e| Unreachable::new(&e))?;
        let req = GrpcHelper::into_req(raft_req);

        let bytes_sent = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(self.target_node.addr.clone(), bytes_sent);

        let addr = self.target_node.addr.clone();
        let timed_future = create_timed_future(client.install_snapshot(req), move |duration| {
            incr_snapshot_recv_seconds(addr, duration);
        });

        let grpc_res = timed_future.await;

        let resp = grpc_res.map_err(|e| {
            raft_metrics::network::incr_sent_failures(self.target_node.addr.clone());
            self.status_to_unreachable(e)
        })?;

        let raft_res = GrpcHelper::parse_raft_reply(resp).map_err(|serde_err| {
            raft_metrics::network::incr_snapshot_send_failure(self.target_node.addr.clone());
            new_net_err(&serde_err, || "parse install_snapshot reply")
        })?;

        if raft_res.is_ok() {
            raft_metrics::network::incr_snapshot_send_success(self.target_node.addr.clone());
        } else {
            raft_metrics::network::incr_snapshot_send_failure(self.target_node.addr.clone());
        }

        raft_res.map_err(|e| self.to_rpc_err(e))
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RPCError<RaftError>> {
        let mut client = self
            .raft_client
            .get(&self.target_node.addr)
            .await
            .map_err(|e| self.status_to_unreachable(tonic::Status::aborted(e.to_string())))?;

        let raft_req = GrpcHelper::encode_raft_request(&req).map_err(|e| Unreachable::new(&e))?;

        let req = GrpcHelper::into_req(raft_req);

        let bytes_sent = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(self.target_node.addr.clone(), bytes_sent);

        let grpc_res = client.vote(req).await;

        let resp = grpc_res.map_err(|e| {
            raft_metrics::network::incr_sent_failures(self.target_node.addr.clone());
            return self.status_to_unreachable(e);
        })?;

        let raft_res = GrpcHelper::parse_raft_reply(resp)
            .map_err(|serde_err| new_net_err(&serde_err, || "parse vote reply"))?;

        raft_res.map_err(|e| self.to_rpc_err(e))
    }
}

fn new_net_err<D: Display>(e: &(impl Error + 'static), msg: impl FnOnce() -> D) -> NetworkError {
    NetworkError::new(&AnyError::new(e).add_context(msg))
}
