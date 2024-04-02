use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use anyhow;

use super::{
    network::Network,
    store::requests::RequestPayload,
    typ::{CheckIsLeaderError, ForwardToLeader, InitializeError, RaftError},
    BasicNode, NodeId, Raft, Response, SnapshotData, StateMachineUpdateRequest, TokioRuntime,
};
use crate::{
    garbage_collector::GarbageCollector, state::store::requests::StateMachineUpdateResponse,
    utils::timestamp_secs,
};

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
    garbage_collector: Arc<GarbageCollector>,
}

impl ForwardableRaft {
    pub fn new(
        id: NodeId,
        raft: Raft,
        network: Network,
        garbage_collector: Arc<GarbageCollector>,
    ) -> Self {
        Self {
            id,
            raft,
            network,
            garbage_collector,
        }
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

    pub async fn register_ingestion_server(
        &self,
        ingestion_server_id: &str,
        addr: &str,
    ) -> anyhow::Result<StateMachineUpdateResponse> {
        //  check whether this node is the leader
        if let Some(forward_to_leader) = self.ensure_leader().await? {
            let leader_address = forward_to_leader
                .leader_node
                .ok_or_else(|| anyhow::anyhow!("could not get leader address"))?;
            //  forward the request to the leader here
            let req = StateMachineUpdateRequest {
                payload: RequestPayload::RegisterIngestionServer {
                    ingestion_server_metadata: indexify_internal_api::IngestionServerMetadata {
                        id: ingestion_server_id.to_string(),
                        addr: addr.to_string(),
                        last_seen: timestamp_secs(),
                    },
                },
                new_state_changes: vec![],
                state_changes_processed: vec![],
            };
            return self.network.forward(&leader_address.addr, req).await;
        }

        //  this node is the leader, perform the action
        self.garbage_collector
            .register_ingestion_server(ingestion_server_id.to_string())
            .await?;
        let response = StateMachineUpdateResponse {
            handled_by: self.id,
        };
        Ok(response)
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
