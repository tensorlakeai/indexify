use async_trait::async_trait;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::memstore::MemNodeId;
use super::memstore::Config;

#[derive(Clone)]
pub struct IndexifyRaftNetwork {}

impl IndexifyRaftNetwork {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: MemNodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<MemNodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let addr = &target_node.addr;

        let url = format!("http://{}/{}", addr, uri);

        tracing::debug!("send_rpc to url: {}", url);

        let client = reqwest::Client::new();

        tracing::debug!("client is created for: {}", url);

        let resp = client.post(url).json(&req).send().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        tracing::debug!("client.post() is sent");

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

// NOTE: This could be implemented also on `Arc<IndexifyRaftNetwork>`, but since it's empty, implemented
// directly.
#[async_trait]
impl RaftNetworkFactory<Config> for IndexifyRaftNetwork {
    type Network = IndexifyRaftNetworkConnection;

    async fn new_client(&mut self, target: MemNodeId, node: &BasicNode) -> Self::Network {
        IndexifyRaftNetworkConnection {
            owner: IndexifyRaftNetwork {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct IndexifyRaftNetworkConnection {
    owner: IndexifyRaftNetwork,
    target: MemNodeId,
    target_node: BasicNode,
}

#[async_trait]
impl RaftNetwork<Config> for IndexifyRaftNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<Config>,
    ) -> Result<AppendEntriesResponse<MemNodeId>, RPCError<MemNodeId, BasicNode, RaftError<MemNodeId>>>
    {
        self.owner.send_rpc(self.target, &self.target_node, "raft-append", req).await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<Config>,
    ) -> Result<
        InstallSnapshotResponse<MemNodeId>,
        RPCError<MemNodeId, BasicNode, RaftError<MemNodeId, InstallSnapshotError>>,
    > {
        self.owner.send_rpc(self.target, &self.target_node, "raft-snapshot", req).await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<MemNodeId>,
    ) -> Result<VoteResponse<MemNodeId>, RPCError<MemNodeId, BasicNode, RaftError<MemNodeId>>> {
        self.owner.send_rpc(self.target, &self.target_node, "raft-vote", req).await
    }
}