use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use indexify_proto::indexify_raft::{
    raft_api_client::RaftApiClient,
    raft_api_server::RaftApi,
    RaftReply,
    RaftRequest,
};
use openraft::BasicNode;
use tonic::{Request, Response, Status};

use super::Raft;
use crate::grpc_helper::GrpcHelper;

pub struct RaftGrpcServer {
    raft: Arc<Raft>,
    nodes: Arc<BTreeMap<u64, BasicNode>>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<Raft>, nodes: Arc<BTreeMap<u64, BasicNode>>) -> Self {
        Self { raft, nodes }
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

        RaftApiClient::connect(format!(
            "http://{}",
            self.nodes
                .get(&leader_node_id)
                .ok_or(Status::unavailable("leader not found"))?
                .addr
        ))
        .await
        .map_err(|message| Status::internal(message.to_string()))?
        .forward(request)
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
