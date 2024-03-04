use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use indexify_proto::indexify_raft::{
    raft_api_server::RaftApi,
    ClusterMembershipResponse,
    GetClusterMembershipRequest,
    RaftReply,
    RaftRequest,
};
use openraft::BasicNode;
use tonic::{Request, Response, Status};
use tracing::info;

use super::Raft;
use crate::grpc_helper::GrpcHelper;

pub struct RaftGrpcServer {
    raft: Arc<Raft>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<Raft>) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl RaftApi for RaftGrpcServer {
    async fn forward(&self, _request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        Err(Status::unimplemented("not implemented"))
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
                .map_err(GrpcHelper::internal_err)?;

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
            .map_err(GrpcHelper::internal_err);

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
                .map_err(GrpcHelper::internal_err)?;

            GrpcHelper::ok_response(resp)
        }
        .await
    }

    async fn get_cluster_membership(
        &self,
        request: Request<GetClusterMembershipRequest>,
    ) -> Result<Response<ClusterMembershipResponse>, Status> {
        let req = request.into_inner();

        let nodes_in_cluster = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut node_ids: Vec<u64> = nodes_in_cluster.keys().cloned().collect();
        if nodes_in_cluster.contains_key(&req.node_id) {
            let response = ClusterMembershipResponse {};
            return Ok(Response::new(response));
        }

        info!(
            "Received request from new node with id {} and address {}",
            req.node_id, req.address
        );
        let node_to_add = BasicNode { addr: req.address };

        self.raft
            .add_learner(req.node_id, node_to_add.clone(), true)
            .await
            .map_err(GrpcHelper::internal_err)?;

        node_ids.push(req.node_id);
        self.raft
            .change_membership(node_ids, false)
            .await
            .map_err(GrpcHelper::internal_err)?;

        info!("Added the node as a learner and returning the response");
        let response = ClusterMembershipResponse {};
        Ok(Response::new(response))
    }
}
