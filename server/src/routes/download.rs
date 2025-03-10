use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Response,
};
use futures::TryStreamExt;

use super::RouteState;
use crate::http_objects::IndexifyAPIError;

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

/// Get function output
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/output/{id}",
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
    let output = state
        .indexify_state
        .reader()
        .fn_output_payload(&namespace, &compute_graph, &invocation_id, &fn_name, &id)
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download invocation payload: {}",
                e
            ))
        })?
        .ok_or(IndexifyAPIError::not_found(
            format!(
                "fn output not found: {}/{}/{}/{}/{}",
                namespace, compute_graph, invocation_id, fn_name, id
            )
            .as_str(),
        ))?;
    let encoding = output.encoding.clone();

    let payload = match output.payload {
        data_model::OutputPayload::Fn(payload) => payload,
        _ => {
            return Err(IndexifyAPIError::internal_error(anyhow!(
                "expected fn output payload, got {:?}",
                output.payload
            )))
        }
    };
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
            .body(Body::from(json_bytes))
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    }
    Response::builder()
        .header("Content-Type", encoding)
        .header("Content-Length", payload.size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}

pub async fn download_fn_output_by_key(
    Path(output_key): Path<String>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let output = state
        .indexify_state
        .reader()
        .fn_output_payload_by_key(&output_key)
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "failed to download invocation payload: {}",
                e
            ))
        })?;
    let payload = match output.payload {
        data_model::OutputPayload::Fn(payload) => payload,
        _ => {
            return Err(IndexifyAPIError::internal_error(anyhow!(
                "expected fn output payload, got {:?}",
                output.payload
            )))
        }
    };

    let encoding = output.encoding.clone();

    let storage_reader = state
        .blob_storage
        .get(&payload.path)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    if encoding == "application/json" {
        let json_bytes = storage_reader
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await
            .map_err(|e| IndexifyAPIError::internal_error(anyhow!("Failed to read JSON: {}", e)))?;

        return Response::builder()
            .header("Content-Type", encoding)
            .body(Body::from(json_bytes))
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    }
    Response::builder()
        .header("Content-Type", encoding)
        .header("Content-Length", payload.size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
