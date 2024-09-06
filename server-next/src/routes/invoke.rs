use std::collections::HashMap;

use anyhow::anyhow;
use axum::{
    extract::{Multipart, Path, State},
    Json,
};
use blob_store::PutResult;
use data_model::DataObjectBuilder;
use futures::{stream, StreamExt};
use state_store::requests::{InvokeComputeGraphRequest, RequestType};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

use super::RouteState;
use crate::http_objects::{GraphInputFile, IndexifyAPIError};

#[allow(dead_code)]
#[derive(ToSchema)]
pub struct InvokeWithFile {
    /// Extra metadata for file
    metadata: Option<HashMap<String, serde_json::Value>>,
    #[schema(format = "binary")]
    /// File to upload
    file: Option<String>,
}
/// Upload data to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_file",
    request_body(content_type = "multipart/form-data", content = inline(InvokeWithFile)),
    tag = "ingestion",
    responses(
        (status = 200, description = "upload successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn invoke_with_file(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut metadata: Option<serde_json::Value> = None;
    let mut put_result: Option<PutResult> = None;

    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "file" {
                let name = Uuid::new_v4().to_string();
                info!("writing to blob store, file name = {:?}", name);
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
                let res = state.blob_storage.put(&name, stream).await.map_err(|e| {
                    IndexifyAPIError::internal_error(anyhow!(
                        "failed to write to blob store: {}",
                        e
                    ))
                })?;
                put_result = Some(res);
            } else if name == "metadata" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let file_metadata = serde_json::from_str(&text)?;
                metadata = Some(file_metadata);
            }
        }
    }
    if put_result.is_none() {
        return Err(IndexifyAPIError::bad_request("file is required"));
    }
    let put_result = put_result.unwrap();
    let payload = GraphInputFile {
        metadata: metadata.unwrap_or_default(),
        url: put_result.url.clone(),
        sha_256: put_result.sha256_hash.clone(),
        size: put_result.size_bytes,
    };
    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = stream::once(async move {
        let payload_json = serde_json::to_string(&payload)?.as_bytes().to_vec().clone();
        Ok(payload_json.into())
    });
    let put_result = state
        .blob_storage
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    let data_object = DataObjectBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .compute_fn_name("".to_string())
        .payload(data_payload)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    state
        .indexify_state
        .write(RequestType::InvokeComputeGraph(InvokeComputeGraphRequest {
            namespace: namespace.clone(),
            compute_graph_name: compute_graph.clone(),
            data_object,
        }))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    Ok(())
}

/// Upload JSON serialized object to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_object",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "invocation successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn invoke_with_object(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
    Json(payload): Json<serde_json::Value>,
) -> Result<(), IndexifyAPIError> {
    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = stream::once(async move {
        let payload_json = serde_json::to_string(&payload)?.as_bytes().to_vec().clone();
        Ok(payload_json.into())
    });
    let put_result = state
        .blob_storage
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    let data_object = DataObjectBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .compute_fn_name("".to_string())
        .payload(data_payload)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    state
        .indexify_state
        .write(RequestType::InvokeComputeGraph(InvokeComputeGraphRequest {
            namespace: namespace.clone(),
            compute_graph_name: compute_graph.clone(),
            data_object,
        }))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    Ok(())
}
