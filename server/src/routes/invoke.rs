use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{sse::Event, IntoResponse},
};
use futures::{Stream, StreamExt};
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::routes_state::RouteState;
use crate::{
    data_model::{self, GraphInvocationCtxBuilder, InvocationPayloadBuilder},
    http_objects::{IndexifyAPIError, RequestId, RequestQueryParams},
    state_store::{
        invocation_events::{RequestFinishedEvent, InvocationStateChangeEvent},
        requests::{InvokeComputeGraphRequest, RequestPayload, StateMachineUpdateRequest},
    },
};

// New shared function for creating SSE streams
async fn create_invocation_event_stream(
    id: String,
    rx: Option<Receiver<InvocationStateChangeEvent>>,
    state: RouteState,
    namespace: String,
    compute_graph: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    async_stream::stream! {
        // For invoke endpoint without blocking
        if rx.is_none() {
            yield Event::default().json_data(RequestId { id: id.clone() });
            return;
        }

        // check completion when starting stream
        match state
            .indexify_state
            .reader()
            .invocation_ctx(namespace.as_str(), compute_graph.as_str(), &id)
        {
            Ok(Some(invocation)) => {
                if invocation.completed {
                    yield Event::default().json_data(
                        InvocationStateChangeEvent::RequestFinished(
                            RequestFinishedEvent {
                                id: id.clone()
                            }
                        )
                    );
                    return;
                }
            }
            Ok(None) => {
                info!(
                    namespace = namespace,
                    graph = compute_graph,
                    invocation_id=id,
                    "invocation not found, stopping stream");
                return;
            }
            Err(e) => {
                error!("failed to get invocation: {:?}", e);
                return;
            }
        }

        // Stream events
        if let Some(mut rx) = rx {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        if ev.invocation_id() == id {
                            yield Event::default().json_data(ev.clone());

                            if let InvocationStateChangeEvent::RequestFinished(_) = ev {
                                return;
                            }
                        }
                    }
                    Err(RecvError::Lagged(num)) => {
                        warn!(
                            namespace = namespace,
                            graph = compute_graph,
                            invocation_id=id,
                            "lagging behind task event stream by {} events", num);

                        // Check if completion happened during lag
                        match state
                            .indexify_state
                            .reader()
                            .invocation_ctx(namespace.as_str(), compute_graph.as_str(), &id)
                        {
                            Ok(Some(context)) => {
                                if context.completed {
                                    yield Event::default().json_data(
                                        InvocationStateChangeEvent::RequestFinished(
                                            RequestFinishedEvent {
                                                id: id.clone()
                                            }
                                        )
                                    );
                                    return;
                                }
                            }
                            Ok(None) => {
                                error!(
                                    namespace = namespace,
                                    graph = compute_graph,
                                    invocation_id=id,
                                    "invocation not found");
                                return;
                            }
                            Err(e) => {
                                error!(
                                    namespace = namespace,
                                    graph = compute_graph,
                                    invocation_id=id,
                                    "failed to get invocation context: {:?}", e);
                                return;
                            }
                        }
                    }
                    Err(RecvError::Closed) => return,
                }
            }
        }
    }
}

/// Make a request to a workflow
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_object",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "request successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn invoke_with_object(
    Path((namespace, compute_graph)): Path<(String, String)>,
    Query(params): Query<RequestQueryParams>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    state.metrics.invocations.add(1, &[]);
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
            error!("failed to write to blob store: {:?}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    state.metrics.invocation_bytes.add(data_payload.size, &[]);
    let invocation_payload = InvocationPayloadBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .payload(data_payload)
        .encoding(encoding)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let id = invocation_payload.id.clone();

    // subscribing to task event stream before creation to not loose events once
    // invocation is created.
    let mut rx: Option<Receiver<InvocationStateChangeEvent>> = None;
    if should_block {
        rx.replace(state.indexify_state.task_event_stream());
    }
    let compute_graph = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &compute_graph)
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to get compute graph: {}", e))
        })?
        .ok_or(IndexifyAPIError::not_found("compute graph not found"))?;
    let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
        .namespace(namespace.to_string())
        .compute_graph_name(compute_graph.name.to_string())
        .graph_version(compute_graph.version.clone())
        .invocation_id(invocation_payload.id.clone())
        .fn_task_analytics(HashMap::new())
        .created_at(invocation_payload.created_at)
        .build(compute_graph.clone())
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let request = RequestPayload::InvokeComputeGraph(InvokeComputeGraphRequest {
        namespace: namespace.clone(),
        compute_graph_name: compute_graph.name.clone(),
        invocation_payload,
        ctx: graph_invocation_ctx,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request.clone(),
            processed_state_changes: vec![],
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    let invocation_event_stream =
        create_invocation_event_stream(id, rx, state, namespace, compute_graph.name).await;
    Ok(
        axum::response::Sse::new(invocation_event_stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        ),
    )
}

/// Stream progress of a request until it is completed
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute-graphs/{compute_graph}/requests/{request_id}/progress",
    tag = "operations",
    responses(
        (status = 200, description = "SSE events of an invocation"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
pub async fn wait_until_invocation_completed(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let rx = state.indexify_state.task_event_stream();

    let invocation_event_stream =
        create_invocation_event_stream(invocation_id, Some(rx), state, namespace, compute_graph)
            .await;
    Ok(
        axum::response::Sse::new(invocation_event_stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        ),
    )
}
