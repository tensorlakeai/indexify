use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    http::Response,
};

use super::RouteState;
use crate::{blob_store::BlobStorage, data_model::DataPayload, http_objects::IndexifyAPIError};

#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/allocations/{allocation_id}/logs/{file}",
    tag = "operations",
    responses(
        (status = 200, description = "Log file for a given allocation"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn download_allocation_logs(
    Path((namespace, compute_graph, invocation_id, allocation_id, file)): Path<(
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
        .get_diagnostic_payload(
            &namespace,
            &compute_graph,
            &invocation_id,
            &allocation_id,
            &file,
        )
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download diagnostic payload: {}",
                e
            ))
        })?;

    data_payload_response(payload, &state.blob_storage).await
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute_graphs/{compute_graph}/compute_functions/{compute_function}/versions/{version}/function_executors/{function_executor_id}/startup_logs/{file}",
    tag = "operations",
    responses(
        (status = 200, description = "Log file for a given function executor"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn download_function_executor_startup_logs(
    Path((namespace, graph_name, function_name, graph_version, function_executor_id, file)): Path<
        (String, String, String, String, String, String),
    >,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let payload = state
        .indexify_state
        .reader()
        .get_function_executor_startup_diagnostic_payload(
            &namespace,
            &graph_name,
            &function_name,
            &graph_version,
            &function_executor_id,
            &file,
        )
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download function executor diagnostic payload: {}",
                e
            ))
        })?;
    data_payload_response(payload, &state.blob_storage).await
}

async fn data_payload_response(
    payload: Option<DataPayload>,
    blob_storage: &BlobStorage,
) -> Result<Response<Body>, IndexifyAPIError> {
    let Some(payload) = payload else {
        return Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", 0)
            .body(Body::empty())
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    };

    let storage_reader = blob_storage
        .get(&payload.path)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", payload.size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
