use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Response,
};

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
    let storage_reader = state.blob_storage.get(&output.payload.path);
    let payload_stream = storage_reader
        .get()
        .await
        .map_err(|e| IndexifyAPIError::internal_error(e))?;

    Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", output.payload.size.to_string())
        .body(Body::from_stream(payload_stream))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}

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
