use axum::{extract::{Extension, Json}, response::IntoResponse, Json as AxumJson};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use super::coordinator_config::CoordinatorRaftApp as App;
use super::memstore::MemNodeId as NodeId;
use super::memstore::Config as TypeConfig;

// --- Raft communication

#[axum_macros::debug_handler]
pub async fn vote(app: Extension<App>, Json(req): Json<VoteRequest<NodeId>>) -> impl IntoResponse {
    let res = app.raft.vote(req).await;
    AxumJson(res)
}

#[axum_macros::debug_handler]
pub async fn append(app: Extension<App>, Json(req): Json<AppendEntriesRequest<TypeConfig>>) -> impl IntoResponse {
    let res = app.raft.append_entries(req).await;
    AxumJson(res)
}

#[axum_macros::debug_handler]
pub async fn snapshot(app: Extension<App>, Json(req): Json<InstallSnapshotRequest<TypeConfig>>) -> impl IntoResponse {
    let res = app.raft.install_snapshot(req).await;
    AxumJson(res)
}
