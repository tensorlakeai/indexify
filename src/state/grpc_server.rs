use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use indexify_proto::indexify_raft::{raft_api_server::RaftApi, RaftReply, RaftRequest};
use openraft::{
    error::{CheckIsLeaderError, ForwardToLeader, RaftError},
    BasicNode,
};
use tonic::{Request, Response, Status};
use tracing::info;

use super::{raft_client::RaftClient, store::requests::RequestPayload, NodeId};
use crate::{
    grpc_helper::GrpcHelper,
    state,
    state::{store::requests, Raft},
};

pub struct RaftGrpcServer {
    raft: Arc<Raft>,
    raft_client: Arc<RaftClient>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<Raft>, raft_client: Arc<RaftClient>) -> Self {
        Self { raft, raft_client }
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
    ) -> Result<Response<RaftReply>, Status> {
        let nodes_in_cluster = self.get_nodes_in_cluster();
        if nodes_in_cluster.contains_key(&node_id) {
            return GrpcHelper::ok_response("");
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

        GrpcHelper::ok_response("")
    }

    /// Helper function to detect whether the current node is the leader
    async fn ensure_leader(&self) -> Result<Option<ForwardToLeader<NodeId, BasicNode>>, Status> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(None),
            Err(e) => match e {
                RaftError::APIError(CheckIsLeaderError::ForwardToLeader(err)) => Ok(Some(err)),
                _ => Err(GrpcHelper::internal_err(e.to_string())),
            },
        }
    }
}

#[async_trait]
impl RaftApi for RaftGrpcServer {
    async fn forward(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let req = GrpcHelper::parse_req::<state::store::requests::Request>(request)?;
        let (node_id, address) = match req.payload {
            RequestPayload::JoinCluster { node_id, address } => (node_id, address),
            _ => return Err(Status::internal("Invalid request")),
        };

        //  check if this node is the leader
        if let Some(_) = self.ensure_leader().await? {
            return Err(GrpcHelper::internal_err(
                "The node we thought was the leader is not the leader",
            ));
        };

        self.add_node_to_cluster_if_absent(node_id, &address).await
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        async {
            let ae_req = GrpcHelper::parse_req(request)?;
            let resp = self
                .raft
                .append_entries(ae_req)
                .await
                .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

            GrpcHelper::ok_response(resp)
        }
        .await
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let is_req = GrpcHelper::parse_req(request)?;
        let resp = self
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()));

        match resp {
            Ok(resp) => GrpcHelper::ok_response(resp),
            Err(e) => Err(e),
        }
    }

    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        async {
            let v_req = GrpcHelper::parse_req(request)?;

            let resp = self
                .raft
                .vote(v_req)
                .await
                .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

            GrpcHelper::ok_response(resp)
        }
        .await
    }

    async fn join_cluster_membership(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let req = GrpcHelper::parse_req::<state::store::requests::Request>(request)?;

        let RequestPayload::JoinCluster { node_id, address } = req.payload else {
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
            let forwarding_req = GrpcHelper::encode_raft_request(&requests::Request {
                payload: requests::RequestPayload::JoinCluster { node_id, address },
                new_state_changes: vec![],
                state_changes_processed: vec![],
            })
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;
            return client.forward(forwarding_req).await;
        };

        //  This node is the leader - we've confirmed it
        self.add_node_to_cluster_if_absent(node_id, &address).await
    }
}
