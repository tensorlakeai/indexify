use std::collections::BTreeMap;
use std::fmt::Debug;

use super::network::Network;
use super::typ::CheckIsLeaderError;
use super::typ::ForwardToLeader;
use super::typ::InitializeError;
use super::typ::RaftError;
use super::BasicNode;
use super::NodeId;
use super::Raft;
use super::Request;
use super::Response;
use super::SnapshotData;
use super::TokioRuntime;

use anyhow::anyhow;
use anyhow::Result as AnyhowResult;

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

pub struct ForwardableRaft {
    raft: Raft,
    network: Network,
}

impl ForwardableRaft {
    pub fn new(raft: Raft, network: Network) -> Self {
        Self { raft, network }
    }

    pub async fn client_write(&self, request: Request) -> AnyhowResult<()> {
        //  check whether this node is the leader
        if let Some(forward_to_leader) = self.ensure_leader().await? {
            //  TODO: Forward this request to the leader
            let leader_address = forward_to_leader
                .leader_node
                .ok_or_else(|| anyhow!("Could not get leader address"))?;
            return self.network.forward(&leader_address.addr, request).await;
        }
        self.raft.client_write(request).await?;
        Ok(())
    }

    pub async fn shutdown(
        &self,
    ) -> Result<(), <TokioRuntime as openraft::AsyncRuntime>::JoinError> {
        self.raft.shutdown().await
    }

    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), RaftError<InitializeError>> {
        self.raft.initialize(members).await
    }

    /// Use this to detect whether the current node is the leader
    async fn ensure_leader(&self) -> AnyhowResult<Option<ForwardToLeader>> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(None),
            Err(e) => match e {
                RaftError::APIError(CheckIsLeaderError::ForwardToLeader(err)) => Ok(Some(err)),
                _ => Err(anyhow::anyhow!("Error occurred")),
            },
        }
    }
}
