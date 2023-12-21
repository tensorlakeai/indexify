use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use openraft::{
    error::{CheckIsLeaderError, Infallible, RaftError},
    BasicNode,
};

use super::super::{store::Request, NodeId, SharedState};

pub async fn write(
    State(state): State<SharedState>,
    Json(request): Json<Request>,
) -> Result<impl IntoResponse, StatusCode> {
    let response = state.raft.client_write(request).await;
    Ok(Json(response))
}

pub async fn read(
    State(state): State<SharedState>,
    Json(key): Json<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let state_machine = state.store.state_machine.read().await;
    let value = state_machine.data.get(&key).cloned();

    let response: Result<String, Infallible> = Ok(value.unwrap_or_default());
    Ok(Json(response))
}

pub async fn consistent_read(
    State(state): State<SharedState>,
    Json(key): Json<String>,
) -> impl IntoResponse {
    let is_leader = state.raft.is_leader().await;

    match is_leader {
        Ok(_) => {
            let state_machine = state.store.state_machine.read().await;
            let value = state_machine.data.get(&key).cloned();

            let response: Result<String, RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>> =
                Ok(value.unwrap_or_default());
            Json(response)
        }
        Err(e) => Json(Err(e)),
    }
}
