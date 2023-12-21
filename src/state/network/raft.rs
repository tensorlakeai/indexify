use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use super::super::{NodeId, SharedState, TypeConfig};

pub async fn vote(
    State(state): State<SharedState>,
    Json(request): Json<VoteRequest<NodeId>>,
) -> impl IntoResponse {
    let response = state.raft.vote(request).await;
    Json(response)
}

pub async fn append(
    State(state): State<SharedState>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<impl IntoResponse, StatusCode> {
    let response = state.raft.append_entries(request).await;
    Ok(Json(response))
}

pub async fn snapshot(
    State(state): State<SharedState>,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<impl IntoResponse, StatusCode> {
    let response = state.raft.install_snapshot(request).await;
    Ok(Json(response))
}
