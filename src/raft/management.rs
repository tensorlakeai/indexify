use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use axum::{extract::Json, response::IntoResponse, Json as AxumJson};
use openraft::error::Infallible;
use openraft::BasicNode;
use openraft::RaftMetrics;

use super::coordinator_config::CoordinatorRaftApp as App;
use super::memstore::MemNodeId;

// --- Cluster management

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[axum_macros::debug_handler]
pub async fn add_learner(app: axum::extract::Extension<Arc<App>>, Json((node_id, addr)): Json<(MemNodeId, String)>) -> impl IntoResponse {
    let node = BasicNode { addr: addr.clone() };
    let res = app.raft.add_learner(node_id, node, true).await;
    AxumJson(res)
}

/// Changes specified learners to members, or remove members.
#[axum_macros::debug_handler]
pub async fn change_membership(app: axum::extract::Extension<Arc<App>>, Json(nodes): Json<BTreeSet<MemNodeId>>) -> impl IntoResponse {
    let res = app.raft.change_membership(nodes, false).await;
    AxumJson(res)
}


/// Initialize a single-node cluster.
pub async fn init(app: axum::extract::Extension<Arc<App>>) -> impl IntoResponse {
    let mut nodes = BTreeMap::new();
    nodes.insert(app.id, BasicNode { addr: app.addr.clone() });
    let res = app.raft.initialize(nodes).await;
    AxumJson(res)
}

/// Get the latest metrics of the cluster
pub async fn metrics(app: axum::extract::Extension<Arc<App>>) -> impl IntoResponse {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<MemNodeId, BasicNode>, Infallible> = Ok(metrics);
    AxumJson(res)
}
