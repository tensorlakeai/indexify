use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, sse::Event},
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{error, info, warn};
use uuid::Uuid;

use super::routes_state::RouteState;
use crate::{
    data_model::{self, ApplicationState, FunctionCallId, InputArgs, RequestCtxBuilder},
    http_objects::IndexifyAPIError,
    metrics::Increment,
    state_store::{
        request_events::{RequestFinishedEvent, RequestStateChangeEvent},
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
    },
    utils::get_epoch_time_in_ms,
};

// New shared function for creating SSE streams
async fn create_request_progress_stream(
    id: String,
    mut rx: Receiver<RequestStateChangeEvent>,
    state: RouteState,
    namespace: String,
    application: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    let reader = state.indexify_state.reader();

    async_stream::stream! {
        // check completion when starting stream
        match reader.request_ctx(namespace.as_str(), application.as_str(), &id).await
        {
            Ok(Some(request_ctx)) => {
                if request_ctx.outcome.is_some() {
                    yield Event::default().json_data(
                        RequestStateChangeEvent::RequestFinished(
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
                    app = application,
                    request_id=id,
                    "request not found, stopping stream");
                return;
            }
            Err(e) => {
                error!("failed to get request: {:?}", e);
                return;
            }
        }

        // Stream events
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if ev.request_id() == id {
                        yield Event::default().json_data(ev.clone());

                        if let RequestStateChangeEvent::RequestFinished(_) = ev {
                            return;
                        }
                    }
                }
                Err(RecvError::Lagged(num)) => {
                    warn!(
                        namespace = namespace,
                        app = application,
                        request_id=id,
                        "lagging behind request event stream by {} events", num);

                    // Check if completion happened during lag
                    match reader
                        .request_ctx(namespace.as_str(), application.as_str(), &id).await
                    {
                        Ok(Some(context)) => {
                            if context.outcome.is_some() {
                                yield Event::default().json_data(
                                    RequestStateChangeEvent::RequestFinished(
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
                                app = application,
                                request_id=id,
                                "request not found");
                            return;
                        }
                        Err(e) => {
                            error!(
                                namespace = namespace,
                                app = application,
                                request_id=id,
                                "failed to get request context: {:?}", e);
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

/// Make a request to application
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
pub async fn invoke_application_with_object_v1(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let _inc = Increment::inc(&state.metrics.requests, &[]);
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

    state
        .metrics
        .request_input_bytes
        .add(data_payload.size, &[]);
    state.metrics.requests.add(1, &[]);

    let application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application)
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to get application: {e}")))?
        .ok_or(IndexifyAPIError::not_found("application not found"))?;

    if let ApplicationState::Disabled { reason } = &application.state {
        return Result::Err(IndexifyAPIError::conflict(reason));
    }

    let function_call_id = FunctionCallId(request_id.clone());

    let entrypoint_fn_name = &application.entrypoint.function_name;
    let Some(entrypoint_fn) = application.functions.get(entrypoint_fn_name) else {
        return Err(IndexifyAPIError::not_found(&format!(
            "application entrypoint function {entrypoint_fn_name} is not in the application function list",
        )));
    };

    let fn_call = entrypoint_fn.create_function_call(
        function_call_id,
        vec![data_payload.clone()],
        Bytes::new(),
        None,
    );
    let app_version = state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .application_version(&namespace, &application.name, &application.version)
        .cloned()
        .ok_or(IndexifyAPIError::not_found(
            "compute graph version not found",
        ))?;
    let fn_run = app_version
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

    let request_ctx = RequestCtxBuilder::default()
        .namespace(namespace.to_string())
        .application_name(application.name.to_string())
        .application_version(application.version.clone())
        .request_id(request_id.clone())
        .created_at(get_epoch_time_in_ms())
        .function_runs(fn_runs)
        .function_calls(fn_calls)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;
    let request = RequestPayload::InvokeApplication(InvokeApplicationRequest {
        namespace: namespace.clone(),
        application_name: application.name.clone(),
        ctx: request_ctx.clone(),
    });
    if accept_header.contains("application/json") {
        return return_request_id(
            &state,
            request.clone(),
            request_id.clone(),
            namespace,
            application.name.clone(),
        )
        .await;
    }
    if accept_header.contains("text/event-stream") {
        return return_sse_response(
            // cloning the state is cheap because all its fields are inside arcs
            state.clone(),
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
    namespace: String,
    application: String,
) -> Result<axum::response::Response, IndexifyAPIError> {
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request_payload.clone(),
        })
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;

    info!(
        request_id = request_id.clone(),
        namespace = namespace.clone(),
        app = application.clone(),
        "request created",
    );

    Ok(Json(RequestIdV1 {
        id: request_id.clone(),
        request_id: request_id.clone(),
    })
    .into_response())
}

async fn return_sse_response(
    state: RouteState,
    request_payload: RequestPayload,
    request_id: String,
    namespace: String,
    application: String,
) -> Result<axum::response::Response, IndexifyAPIError> {
    let rx = state.indexify_state.function_run_event_stream();
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request_payload.clone(),
        })
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;
    info!(
        request_id = request_id.clone(),
        namespace = namespace.clone(),
        app = application.clone(),
        "request created",
    );
    let request_event_stream =
        create_request_progress_stream(request_id, rx, state, namespace, application).await;
    Ok(axum::response::Sse::new(request_event_stream)
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
    path = "/namespaces/{namespace}/compute-graphs/{application}/requests/{request_id}/progress",
    tag = "operations",
    responses(
        (status = 200, description = "SSE events of a request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
pub async fn progress_stream(
    Path((namespace, application, request_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let rx = state.indexify_state.function_run_event_stream();

    // cloning the state is cheap because all its fields are inside arcs
    let request_event_stream =
        create_request_progress_stream(request_id, rx, state.clone(), namespace, application).await;
    Ok(axum::response::Sse::new(request_event_stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("keep-alive-text"),
    ))
}
