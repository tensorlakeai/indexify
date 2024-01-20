use axum::{extract::State, routing::get, Json, Router};

use super::{IndexifyAPIError, ListExecutorsResponse, RepositoryEndpointState};

pub(super) fn get_router() -> Router<RepositoryEndpointState> {
    Router::new().route("/executors", get(list_executors))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/executors",
    tag = "indexify",
    responses(
        (status = 200, description = "List of currently running executors", body = ListExecutorsResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to load executors")
    ),
)]
#[axum::debug_handler]
async fn list_executors(
    State(_state): State<RepositoryEndpointState>,
) -> Result<Json<ListExecutorsResponse>, IndexifyAPIError> {
    Ok(Json(ListExecutorsResponse { executors: vec![] }))
}
