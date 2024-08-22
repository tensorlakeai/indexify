use std::{collections::BTreeMap, fmt::Debug};

use openraft::ServerState;

use super::{
    network::Network,
    typ::{CheckIsLeaderError, ForwardToLeader, InitializeError, RaftError},
    BasicNode,
    NodeId,
    Raft,
    Response,
    SnapshotData,
    StateMachineUpdateRequest,
    TokioRuntime,
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

#[derive(Debug, serde::Serialize)]
pub struct RaftNode {
    pub id: NodeId,
    pub address: String,
}

#[derive(Debug, serde::Serialize)]
pub struct RaftState {
    #[serde(serialize_with = "serialize_server_state")]
    pub server: ServerState,
    pub nodes: Vec<RaftNode>,
}

fn serialize_server_state<S>(state: &ServerState, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let state = match state {
        ServerState::Leader => "leader",
        ServerState::Learner => "learner",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Shutdown => "shutdown",
    };
    serializer.serialize_str(state)
}

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
                .ok_or_else(|| anyhow::anyhow!("could not get leader address"))?;
            return self.network.forward(&leader_address.addr, request).await;
        }

        self.raft.client_write(request).await?;
        let response = StateMachineUpdateResponse {
            handled_by: self.id,
        };
        Ok(response)
    }

    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), RaftError<InitializeError>> {
        self.raft.initialize(members).await
    }

    pub async fn shutdown(
        &self,
    ) -> Result<(), <TokioRuntime as openraft::AsyncRuntime>::JoinError> {
        self.raft.shutdown().await
    }

    /// Use this to detect whether the current node is the leader
    pub async fn ensure_leader(&self) -> anyhow::Result<Option<ForwardToLeader>> {
        let result = self.raft.ensure_linearizable().await;
        match result {
            Ok(_) => Ok(None),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(err))) => Ok(Some(err)),
            Err(e) => Err(anyhow::anyhow!("Error occurred: {}", e.to_string())),
        }
    }

    pub async fn state(&self) -> Result<RaftState, anyhow::Error> {
        let state = self
            .raft
            .with_raft_state(|st| RaftState {
                server: st.server_state,
                nodes: st
                    .membership_state
                    .effective()
                    .nodes()
                    .map(|(id, node)| RaftNode {
                        id: *id,
                        address: node.addr.clone(),
                    })
                    .collect(),
            })
            .await?;

        Ok(state)
    }
}
