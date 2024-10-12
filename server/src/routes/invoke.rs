use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    response::{sse::Event, IntoResponse},
    Json,
};
use blob_store::PutResult;
use data_model::InvocationPayloadBuilder;
use futures::{stream, StreamExt};
use state_store::{
    invocation_events::{InvocationFinishedEvent, InvocationStateChangeEvent},
    requests::{
        InvokeComputeGraphRequest,
        RequestPayload,
        RerunComputeGraphRequest,
        StateMachineUpdateRequest,
    },
};
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};
use utoipa::ToSchema;
use uuid::Uuid;

use super::RouteState;
use crate::http_objects::{GraphInputFile, IndexifyAPIError, InvocationId, InvocationQueryParams};

#[allow(dead_code)]
#[derive(ToSchema)]
pub struct InvokeWithFile {
    /// Extra metadata for file
    metadata: Option<HashMap<String, serde_json::Value>>,
    #[schema(format = "binary")]
    /// Mime type of file
    mime_type: Option<String>,
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
    Query(_params): Query<InvocationQueryParams>,
    mut files: Multipart,
) -> Result<Json<InvocationId>, IndexifyAPIError> {
    let mut metadata: Option<serde_json::Value> = None;
    let mut put_result: Option<PutResult> = None;

    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "file" {
                let name = Uuid::new_v4().to_string();
                info!("writing to blob store, file name = {:?}", name);
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
                let res = state.blob_storage.put(&name, stream).await.map_err(|e| {
                    error!("failed to write to blob store: {}", e);
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
            error!("failed to write to blob store: {}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    let invocation_payload = InvocationPayloadBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .payload(data_payload)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    let id = invocation_payload.id.clone();
    let request = RequestPayload::InvokeComputeGraph(InvokeComputeGraphRequest {
        namespace: namespace.clone(),
        compute_graph_name: compute_graph.clone(),
        invocation_payload,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    Ok(Json(InvocationId { id }))
}

/// Upload JSON serialized object to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_object",
    request_body(content_type = "application/cbor", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "invocation successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn invoke_with_object(
    Path((namespace, compute_graph)): Path<(String, String)>,
    Query(params): Query<InvocationQueryParams>,
    State(state): State<RouteState>,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let should_block = params.block_until_finish.unwrap_or(false);
    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| anyhow::anyhow!(err)));
    let put_result = state
        .blob_storage
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            error!("failed to write to blob store: {}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    let invocation_payload = InvocationPayloadBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .payload(data_payload)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let id = invocation_payload.id.clone();
    let mut rx: Option<Receiver<InvocationStateChangeEvent>> = None;
    if should_block {
        rx.replace(state.indexify_state.task_event_stream());
    }
    let request = RequestPayload::InvokeComputeGraph(InvokeComputeGraphRequest {
        namespace: namespace.clone(),
        compute_graph_name: compute_graph.clone(),
        invocation_payload,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    let invocation_event_stream = async_stream::stream! {
        if !should_block {
            yield Event::default().json_data(InvocationId { id: id.clone() });
            return;
        }
        if let Some(rx) = rx.as_mut() {
            loop {
                if let Ok(ev)  =  rx.recv().await {
                    if ev.invocation_id() == id  || ev.invocation_id() == "" {
                        yield Event::default().json_data(ev.clone());

                        if let InvocationStateChangeEvent::InvocationFinished(InvocationFinishedEvent{ id }) = ev {
                            yield Event::default().json_data(InvocationId { id: id.clone() });
                            return;
                        }
                    }
                }
            }
        }
    };

    Ok(
        axum::response::Sse::new(invocation_event_stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        ),
    )
}

/// Rerun compute graph with all existing payloads
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/rerun",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "invocation successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn rerun_compute_graph(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::RerunComputeGraph(RerunComputeGraphRequest {
        namespace: namespace.clone(),
        compute_graph_name: compute_graph.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to create graph rerun task: {}", e))
        })?;
    Ok(())
}
