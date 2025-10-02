use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::{sse::Event, IntoResponse},
    Json,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::routes_state::RouteState;
use crate::{
    data_model::{self, ComputeGraphState, FunctionCallId, GraphInvocationCtxBuilder, InputArgs},
    http_objects::IndexifyAPIError,
    metrics::Increment,
    state_store::{
        invocation_events::{InvocationStateChangeEvent, RequestFinishedEvent},
        requests::{InvokeComputeGraphRequest, RequestPayload, StateMachineUpdateRequest},
    },
    utils::get_epoch_time_in_ms,
};

// New shared function for creating SSE streams
async fn create_invocation_progress_stream(
    id: String,
    mut rx: Receiver<InvocationStateChangeEvent>,
    state: &RouteState,
    namespace: String,
    compute_graph: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    let reader = state.indexify_state.reader();

    async_stream::stream! {
        // check completion when starting stream
        match reader.invocation_ctx(namespace.as_str(), compute_graph.as_str(), &id)
        {
            Ok(Some(invocation)) => {
                if invocation.outcome.is_some() {
                    yield Event::default().json_data(
                        InvocationStateChangeEvent::RequestFinished(
                            RequestFinishedEvent {
                                request_id: id.clone()
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
                    match reader
                        .invocation_ctx(namespace.as_str(), compute_graph.as_str(), &id)
                    {
                        Ok(Some(context)) => {
                            if context.outcome.is_some() {
                                yield Event::default().json_data(
                                    InvocationStateChangeEvent::RequestFinished(
                                        RequestFinishedEvent {
                                            request_id: id.clone()
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

#[derive(Serialize)]
struct RequestIdV1 {
    // FIXME: Remove this once we migrate clients off this.
    id: String,
    request_id: String,
}

/// Make a request to default api function of a workflow
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "request successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn invoke_default_api_with_object_v1(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    do_invoke_api_with_object_v1(namespace, application, None, state, headers, body).await
}

/// Make a request to a particular api function of a workflow
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}/{api_function}",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "request successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn invoke_api_with_object_v1(
    Path((namespace, application, api_function)): Path<(String, String, String)>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    do_invoke_api_with_object_v1(
        namespace,
        application,
        Some(api_function),
        state,
        headers,
        body,
    )
    .await
}

async fn do_invoke_api_with_object_v1(
    namespace: String,
    application: String,
    api_function: Option<String>,
    state: RouteState,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let _inc = Increment::inc(&state.metrics.invocations, &[]);
    let request_id = nanoid::nanoid!();
    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| anyhow::anyhow!(err)));
    let put_result = state
        .blob_storage
        .get_blob_store(&namespace)
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            error!("failed to write to blob store: {:?}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}"))
        })?;
    let data_payload = data_model::DataPayload {
        id: nanoid::nanoid!(),
        metadata_size: 0,
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
        offset: 0, // Whole BLOB was written, so offset is 0
        encoding,
    };

    state.metrics.invocation_bytes.add(data_payload.size, &[]);

    let application = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &application)
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to get compute graph: {e}")))?
        .ok_or(IndexifyAPIError::not_found("compute graph not found"))?;

    if let ComputeGraphState::Disabled { reason } = &application.state {
        return Result::Err(IndexifyAPIError::conflict(reason));
    }

    let function_call_id = FunctionCallId(request_id.clone());

    let api_fn = if let Some(api_function) = api_function {
        application
            .nodes
            .get(&api_function)
            .ok_or(IndexifyAPIError::not_found(&format!(
                "api function {api_function} not found",
            )))?
    } else {
        &application.start_fn
    };

    let fn_call =
        api_fn.create_function_call(function_call_id, vec![data_payload.clone()], Bytes::new());
    let cg_version = state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .compute_graph_version(&namespace, &application.name, &application.version)
        .cloned()
        .ok_or(IndexifyAPIError::not_found(
            "compute graph version not found",
        ))?;
    let fn_run = cg_version
        .create_function_run(
            &fn_call,
            vec![InputArgs {
                function_call_id: None,
                data_payload: data_payload.clone(),
            }],
            &request_id,
        )
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to create function run: {e}"))
        })?;
    let fn_runs = HashMap::from([(fn_run.id.clone(), fn_run)]);
    let fn_calls = HashMap::from([(fn_call.function_call_id.clone(), fn_call)]);

    let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
        .namespace(namespace.to_string())
        .compute_graph_name(application.name.to_string())
        .graph_version(application.version.clone())
        .request_id(request_id.clone())
        .created_at(get_epoch_time_in_ms())
        .function_runs(fn_runs)
        .function_calls(fn_calls)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;
    let request = RequestPayload::InvokeComputeGraph(InvokeComputeGraphRequest {
        namespace: namespace.clone(),
        compute_graph_name: application.name.clone(),
        ctx: graph_invocation_ctx.clone(),
    });
    if accept_header.contains("application/json") {
        return return_request_id(&state, request.clone(), request_id.clone()).await;
    }
    if accept_header.contains("text/event-stream") {
        return return_sse_response(
            &state,
            request.clone(),
            request_id.clone(),
            namespace,
            application.name,
        )
        .await;
    }
    Err(IndexifyAPIError::bad_request(
        "accept header must be application/json or text/event-stream",
    ))
}

async fn return_request_id(
    state: &RouteState,
    request_payload: RequestPayload,
    request_id: String,
) -> Result<axum::response::Response, IndexifyAPIError> {
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request_payload.clone(),
        })
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;

    Ok(Json(RequestIdV1 {
        id: request_id.clone(),
        request_id: request_id.clone(),
    })
    .into_response())
}

async fn return_sse_response(
    state: &RouteState,
    request_payload: RequestPayload,
    request_id: String,
    namespace: String,
    compute_graph: String,
) -> Result<axum::response::Response, IndexifyAPIError> {
    let rx = state.indexify_state.task_event_stream();
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request_payload.clone(),
        })
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;
    let invocation_event_stream =
        create_invocation_progress_stream(request_id, rx, state, namespace, compute_graph).await;
    Ok(axum::response::Sse::new(invocation_event_stream)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        )
        .into_response())
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
pub async fn progress_stream(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let rx = state.indexify_state.task_event_stream();

    let invocation_event_stream =
        create_invocation_progress_stream(invocation_id, rx, &state, namespace, compute_graph)
            .await;
    Ok(
        axum::response::Sse::new(invocation_event_stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        ),
    )
}
