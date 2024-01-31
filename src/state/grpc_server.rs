use std::sync::Arc;

use async_trait::async_trait;
use indexify_proto::indexify_raft::{raft_api_server::RaftApi, RaftReply, RaftRequest};
use tonic::{Request, Response, Status};

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
    async fn forward(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let leader_node_id = self
            .raft
            .current_leader()
            .await
            .ok_or(Status::unavailable("leader not found"))?;

        let request = GrpcHelper::parse_req(request)?;
        let resp = self
            .raft
            .client_write(request) // TODO: Get the raft store and find the leader endpoint from there
            .await
            .map_err(GrpcHelper::internal_err)?;

        GrpcHelper::ok_response(resp)
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
        let v_req = GrpcHelper::parse_req(request)?;

        let resp = self
            .raft
            .vote(v_req)
            .await
            .map_err(GrpcHelper::internal_err)?;

        GrpcHelper::ok_response(resp)
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let ae_req = GrpcHelper::parse_req(request)?;
        let resp = self
            .raft
            .append_entries(ae_req)
            .await
            .map_err(GrpcHelper::internal_err)?;

        GrpcHelper::ok_response(resp)
    }
}
