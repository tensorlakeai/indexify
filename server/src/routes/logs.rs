use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    http::Response,
};

use super::RouteState;
use crate::http_objects::IndexifyAPIError;

/// Get the logs of a function
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/logs/{file}",
    tag = "operations",
    responses(
        (status = 200, description = "Log file"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn download_logs(
    Path((namespace, compute_graph, invocation_id, fn_name, file)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let payload = state
        .indexify_state
        .reader()
        .get_diagnostic_payload(&namespace, &compute_graph, &invocation_id, &fn_name, &file)
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download diagnostic payload: {}",
                e
            ))
        })?
        .ok_or(IndexifyAPIError::internal_error(anyhow!(
            "diagnostic payload not found"
        )))?;
    let storage_reader = state.blob_storage.get(&payload.path);
    let payload_stream = storage_reader
        .get()
        .await
        .map_err(|e| IndexifyAPIError::internal_error(e))?;

    Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", payload.size.to_string())
        .body(Body::from_stream(payload_stream))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
