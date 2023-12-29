use std::collections::{BTreeMap, BTreeSet};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use openraft::{error::Infallible, BasicNode, RaftMetrics};

use super::super::{NodeId, SharedState};

pub async fn add_learner(
    State(state): State<SharedState>,
    Json((node_id, addr)): Json<(NodeId, String)>,
) -> Result<impl IntoResponse, StatusCode> {
    let node = BasicNode { addr };
    let response = state.raft.add_learner(node_id, node, true).await;
    Ok(Json(response))
}

pub async fn change_membership(
    State(state): State<SharedState>,
    Json(request): Json<BTreeSet<NodeId>>,
) -> Result<impl IntoResponse, StatusCode> {
    let response = state.raft.change_membership(request, false).await;
    Ok(Json(response))
}

pub async fn init(State(state): State<SharedState>) -> Result<impl IntoResponse, StatusCode> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        state.id,
        BasicNode {
            addr: state.addr.clone(),
        },
    );
    let response = state.raft.initialize(nodes).await;
    Ok(Json(response))
}

pub async fn metrics(State(state): State<SharedState>) -> Result<impl IntoResponse, StatusCode> {
    let metrics = state.raft.metrics().borrow().clone();
    let response: Result<RaftMetrics<NodeId, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(response))
}
