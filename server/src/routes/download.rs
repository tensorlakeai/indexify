use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    response::Response,
};
use futures::TryStreamExt;
use hyper::StatusCode;

use super::routes_state::RouteState;
use crate::{
    blob_store::BlobStorage,
    data_model::{DataPayload, FunctionCallId, GraphInvocationError},
    http_objects::{IndexifyAPIError, RequestError},
};

pub async fn download_invocation_error(
    invocation_error: Option<GraphInvocationError>,
    blob_storage: &BlobStorage,
) -> Result<Option<RequestError>, IndexifyAPIError> {
    let Some(invocation_error) = invocation_error else {
        return Ok(None);
    };

    let storage_reader = blob_storage
        .get(
            &invocation_error.payload.path,
            Some(
                invocation_error.payload.offset..
                    invocation_error.payload.offset + invocation_error.payload.size,
            ),
        )
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let bytes = storage_reader
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!(
                "Failed to read invocation error payload: {e}",
            ))
        })?;

    let message = String::from_utf8(bytes).map_err(|e| {
        IndexifyAPIError::internal_error(anyhow!(
            "Invocation error payload is not valid UTF-8: {e}",
        ))
    })?;

    Ok(Some(RequestError {
        function_name: invocation_error.function_name,
        message,
    }))
}

/// Get function output by index
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/requests/{request_id}/output/{fn_call_id}",
    tag = "retrieve",
    responses(
        (status = 200, description = "function output"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error"),
        (status = NOT_FOUND, description = "resource not found")
    ),
)]
pub async fn v1_download_fn_output_payload(
    Path((namespace, application, request_id, fn_call_id)): Path<(String, String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let ctx = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &application, &request_id)
        .map_err(|e| {
            IndexifyAPIError::internal_error(
                anyhow!("failed to get graph invocation context: {e}",),
            )
        })?
        .ok_or(IndexifyAPIError::not_found("request not found"))?;
    let fn_run = ctx
        .function_runs
        .get(&FunctionCallId::from(fn_call_id.as_str()))
        .ok_or(IndexifyAPIError::not_found("function call id not found"))?;

    let payload = fn_run
        .output
        .clone()
        .ok_or(IndexifyAPIError::not_found("function run output not found"))?;

    let blob_storage = state.blob_storage.get_blob_store(&namespace);
    stream_data_payload(&payload, &blob_storage, &payload.encoding).await
}

/// Get function output
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/requests/{request_id}/output",
    tag = "retrieve",
    responses(
        (status = 200, description = "function output"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error"),
        (status = NOT_FOUND, description = "resource not found")
    ),
)]
pub async fn v1_download_fn_output_payload_simple(
    Path((namespace, application, request_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let ctx = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &application, &request_id)
        .map_err(|e| {
            IndexifyAPIError::internal_error(
                anyhow!("failed to get graph invocation context: {e}",),
            )
        })?
        .ok_or(IndexifyAPIError::not_found("request context not found"))?;

    let api_fn_run = ctx
        .function_runs
        .get(&FunctionCallId::from(request_id.as_str()))
        .ok_or(IndexifyAPIError::not_found("function run not found"))?;
    if let Some(payload) = api_fn_run.output.clone() {
        let blob_storage = state.blob_storage.get_blob_store(&namespace);
        return stream_data_payload(&payload, &blob_storage, &payload.encoding).await;
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}

async fn stream_data_payload(
    payload: &DataPayload,
    blob_storage: &BlobStorage,
    encoding: &str,
) -> Result<Response<Body>, IndexifyAPIError> {
    let data_size = payload.size - payload.metadata_size;
    let data_offset = payload.offset + payload.metadata_size;
    let storage_reader = blob_storage
        .get(&payload.path, Some(data_offset..data_offset + data_size))
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    if encoding == "application/json" {
        let json_bytes = storage_reader
            .map_ok(|chunk| chunk.to_vec())
            .try_concat()
            .await
            .map_err(|e| IndexifyAPIError::internal_error(anyhow!("Failed to read JSON: {e}")))?;

        return Response::builder()
            .header("Content-Type", encoding)
            .header("Content-Length", data_size.to_string())
            .body(Body::from(json_bytes))
            .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()));
    }
    Response::builder()
        .header("Content-Type", encoding)
        .header("Content-Length", data_size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
