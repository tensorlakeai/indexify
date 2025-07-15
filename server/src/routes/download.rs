use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Response,
};
use futures::TryStreamExt;

use super::RouteState;
use crate::{
    blob_store::BlobStorage,
    data_model::GraphInvocationError,
    http_objects::{IndexifyAPIError, InvocationError},
};

pub async fn download_invocation_payload(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let output = state
        .indexify_state
        .reader()
        .invocation_payload(&namespace, &compute_graph, &invocation_id)
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download invocation payload: {}",
                e
            ))
        })?;
    let storage_reader = state
        .blob_storage
        .get(&output.payload.path)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    if output.encoding == "application/json" {
        let json_bytes = storage_reader
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await
            .map_err(|e| IndexifyAPIError::internal_error(anyhow!("Failed to read JSON: {}", e)))?;

        return Response::builder()
            .header("Content-Type", output.encoding)
            .body(Body::from(json_bytes))
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    }

    Response::builder()
        .header("Content-Type", output.encoding)
        .header("Content-Length", output.payload.size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}

pub async fn download_invocation_error(
    invocation_error: Option<GraphInvocationError>,
    blob_storage: &BlobStorage,
) -> Result<Option<InvocationError>, IndexifyAPIError> {
    let Some(invocation_error) = invocation_error else {
        return Ok(None);
    };

    let storage_reader = blob_storage
        .get(&invocation_error.payload.path)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let bytes = storage_reader
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "Failed to read invocation error payload: {}",
                e
            ))
        })?;

    let message = String::from_utf8(bytes).map_err(|e| {
        IndexifyAPIError::internal_error(anyhow!(
            "Invocation error payload is not valid UTF-8: {}",
            e
        ))
    })?;

    return Ok(Some(InvocationError {
        function_name: invocation_error.function_name,
        message,
    }));
}

/// Get function output
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/output/{id}",
    tag = "retrieve",
    responses(
        (status = 200, description = "Function output"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn download_fn_output_payload(
    Path((namespace, compute_graph, invocation_id, fn_name, id)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    // The ID will be node_output_id|index
    let (node_output_id, index) = id
        .split_once('|')
        .map(|(id, index)| (id, index.parse::<usize>().unwrap_or(0)))
        .unwrap_or((id.as_str(), 0));

    let output = state
        .indexify_state
        .reader()
        .fn_output_payload(
            &namespace,
            &compute_graph,
            &invocation_id,
            &fn_name,
            &node_output_id,
        )
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download invocation payload: {}",
                e
            ))
        })?
        .ok_or(IndexifyAPIError::not_found(
            format!(
                "fn output not found: {}/{}/{}/{}",
                namespace, compute_graph, invocation_id, fn_name
            )
            .as_str(),
        ))?;

    let encoding = output.encoding.clone();

    let payload = &output.payloads[index];
    let storage_reader = state
        .blob_storage
        .get(&payload.path)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    // Check if the content type is JSON
    if encoding == "application/json" {
        let json_bytes = storage_reader
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await
            .map_err(|e| IndexifyAPIError::internal_error(anyhow!("Failed to read JSON: {}", e)))?;

        return Response::builder()
            .header("Content-Type", encoding)
            .header("Content-Hash", payload.sha256_hash.clone())
            .header("Content-Length", payload.size.to_string())
            .body(Body::from(json_bytes))
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    }
    Response::builder()
        .header("Content-Type", encoding)
        .header("Content-Length", payload.size.to_string())
        .header("Content-Hash", payload.sha256_hash.clone())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
