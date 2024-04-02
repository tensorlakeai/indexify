use std::{collections::BTreeMap, fmt::Debug};

use anyhow;

use super::{
    network::Network,
    typ::{CheckIsLeaderError, ForwardToLeader, InitializeError, RaftError},
    BasicNode, NodeId, Raft, Response, SnapshotData, StateMachineUpdateRequest, TokioRuntime,
};
use crate::state::store::requests::StateMachineUpdateResponse;

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

#[derive(Clone)]
pub struct ForwardableRaft {
    id: NodeId,
    pub raft: Raft, //  the OpenRaft instance
    network: Network,
}

impl ForwardableRaft {
    pub fn new(id: NodeId, raft: Raft, network: Network) -> Self {
        Self { id, raft, network }
    }

    pub async fn client_write(
        &self,
        request: StateMachineUpdateRequest,
    ) -> anyhow::Result<StateMachineUpdateResponse> {
        //  check whether this node is not the leader
        if let Some(forward_to_leader) = self.ensure_leader().await? {
            let leader_address = forward_to_leader
                .leader_node
                .ok_or_else(|| anyhow::anyhow!("Could not get leader address"))?;
            return self.network.forward(&leader_address.addr, request).await;
        }

        self.raft.client_write(request).await?;
        let response = StateMachineUpdateResponse {
            handled_by: self.id,
        };
        Ok(response)
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
    async fn ensure_leader(&self) -> anyhow::Result<Option<ForwardToLeader>> {
        let result = self.raft.ensure_linearizable().await;
        match result {
            Ok(_) => Ok(None),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(err))) => Ok(Some(err)),
            Err(e) => Err(anyhow::anyhow!("Error occurred: {}", e.to_string())),
        }
    }
}
