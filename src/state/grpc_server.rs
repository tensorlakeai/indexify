use std::sync::Arc;

use async_trait::async_trait;
use indexify_proto::indexify_raft::{
    raft_api_server::RaftApi, GetClusterMembershipRequest, GetClusterMembershipResponse, RaftReply,
    RaftRequest,
};
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
        request: Request<GetClusterMembershipRequest>, // Adjust the request type
    ) -> Result<Response<GetClusterMembershipResponse>, Status> {
        info!("Received a request to get cluster membership");
        // Adjust the response type
        // Example implementation:
        let req = request.into_inner();
        println!(
            "Received get_cluster_membership request from Node ID: {}, Address: {}",
            req.node_id, req.address
        );

        // Here, insert logic to process the request, such as updating the cluster membership
        // or retrieving the current cluster state. This is a simplified example that returns
        // a hard-coded response.

        // Construct a response with a map of node IDs to addresses
        let mut members = std::collections::HashMap::new();
        // Example: Adding some dummy node information to the response
        members.insert("1".to_string(), "10.0.0.1:8080".to_string());
        members.insert("2".to_string(), "10.0.0.2:8080".to_string());

        let response = GetClusterMembershipResponse {
            members, // Set the members map
            error: "".to_string(),
        };

        Ok(Response::new(response))
    }
}
