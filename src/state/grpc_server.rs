use std::{collections::BTreeMap, sync::Arc};

use crate::state::typ::CheckIsLeaderError;
use crate::state::typ::RaftError;
use crate::state::Raft;
use async_trait::async_trait;
use indexify_proto::indexify_raft;
use indexify_proto::indexify_raft::ForwardableRequest;
use indexify_proto::indexify_raft::ForwardableResponse;
use indexify_proto::indexify_raft::{
    raft_api_server::RaftApi, ClusterMembershipResponse, GetClusterMembershipRequest, RaftReply,
    RaftRequest,
};
use openraft::BasicNode;
use tonic::{metadata::MetadataMap, Code, Request, Response, Status};
use tracing::info;

use crate::grpc_helper::GrpcHelper;

pub struct RaftGrpcServer {
    raft: Arc<Raft>,
}

impl RaftGrpcServer {
    pub fn new(raft: Arc<Raft>) -> Self {
        Self { raft }
    }

    /// Helper function to get the node ids from the cluster
    fn get_nodes_in_cluster(&self) -> BTreeMap<u64, BasicNode> {
        let nodes_in_cluster = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<BTreeMap<_, _>>();
        nodes_in_cluster
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

        //  if the current node is not the leader, send back metadata about the current leader
        // if let Err(e) = self.raft.ensure_linearizable().await {
        //     return match e {
        //         RaftError::APIError(CheckIsLeaderError::ForwardToLeader(error)) => {
        //             let mut metadata = MetadataMap::new();
        //             metadata.insert(
        //                 "leader-id",
        //                 error.leader_id.unwrap().to_string().parse().unwrap(),
        //             );
        //             metadata.insert(
        //                 "leader-address",
        //                 error.leader_node.unwrap().to_string().parse().unwrap(),
        //             );
        //             Err(Status::with_metadata(
        //                 Code::FailedPrecondition,
        //                 format!(
        //                     "Node is not the leader. Leader is {}",
        //                     error.leader_id.unwrap()
        //                 ),
        //                 metadata,
        //             ))
        //         }
        //         _ => Err(GrpcHelper::internal_err(e)),
        //     };
        // }

        // let nodes_in_cluster = self
        //     .raft
        //     .metrics()
        //     .borrow()
        //     .membership_config
        //     .nodes()
        //     .map(|(node_id, node)| (*node_id, node.clone()))
        //     .collect::<BTreeMap<_, _>>();
        let nodes_in_cluster = self.get_nodes_in_cluster();
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

        info!("Done adding node {} as a learner", req.node_id);

        let nodes_in_cluster = self.get_nodes_in_cluster(); //  re-fetch the nodes in the cluster to get the latest view (sync point)
        let node_ids: Vec<u64> = nodes_in_cluster.keys().cloned().collect();
        info!("New node ids {:#?}", node_ids);
        self.raft
            .change_membership(node_ids, true)
            .await
            .map_err(GrpcHelper::internal_err)?;

        info!(
            "Metrics after adding learner {}: {:#?}",
            req.node_id,
            self.raft.metrics()
        );

        info!(
            "Added the node {} to the configuration and returning the response",
            req.node_id
        );
        let response = ClusterMembershipResponse {};
        Ok(Response::new(response))
    }

    async fn handle_forwardable_request(
        &self,
        request: Request<ForwardableRequest>,
    ) -> Result<Response<ForwardableResponse>, Status> {
        // Example request handling logic
        let response = ForwardableResponse {
            // Populate the response based on request and your logic
            response: Some(
                indexify_raft::forwardable_response::Response::ClusterMembershipResponse(
                    ClusterMembershipResponse {},
                ),
            ),
        };

        Ok(Response::new(response))
    }
}
