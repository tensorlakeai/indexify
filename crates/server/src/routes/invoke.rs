use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, sse::Event},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use pin_project::pin_project;
use serde::Serialize;
use tokio::sync::broadcast::{self, error::RecvError};
use tracing::{debug, error, info, warn};

use super::routes_state::RouteState;
use crate::{
    data_model::{
        self, ApplicationState, DataPayload, FunctionCallId, InputArgs, RequestCtx,
        RequestCtxBuilder, RequestOutcome,
    },
    http_objects::IndexifyAPIError,
    metrics::Increment,
    state_store::{
        IndexifyState, driver,
        request_events::{RequestStateChangeEvent, RequestStateFinishedOutput},
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
        scanner::StateReader,
    },
    utils::get_epoch_time_in_ms,
};

/// We allow at max the length of a UUID4 with hyphens.
const MAX_REQUEST_ID_LENGTH: usize = 36;
const MAX_INLINE_JSON_SIZE: u64 = 1024 * 1024;

fn build_output_path(namespace: &str, application: &str, request_id: &str) -> String {
    format!(
        "/v1/namespaces/{}/applications/{}/requests/{}/output",
        namespace, application, request_id
    )
}

async fn read_json_output(
    payload: &DataPayload,
    state: &RouteState,
    namespace: &str,
) -> Option<serde_json::Value> {
    if payload.encoding != "application/json" {
        return None;
    }

    if payload.data_size() > MAX_INLINE_JSON_SIZE {
        return None;
    }

    let blob_storage = state.blob_storage.get_blob_store(namespace);

    let storage_reader = match blob_storage
        .get(&payload.path, Some(payload.data_range()))
        .await
    {
        Ok(reader) => reader,
        Err(e) => {
            warn!(?e, "failed to read output blob for SSE");
            return None;
        }
    };

    let bytes = match storage_reader
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
    {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(?e, "failed to concat output blob chunks for SSE");
            return None;
        }
    };

    serde_json::from_slice(&bytes).ok()
}

async fn build_finished_event_for_outcome(
    state: &RouteState,
    ctx: &RequestCtx,
    outcome: &RequestOutcome,
) -> RequestStateChangeEvent {
    let output = match outcome {
        RequestOutcome::Success => {
            debug!("building finished event for success outcome");
            // Get the entrypoint function's output (uses request_id as function_call_id)
            let function_run = ctx
                .function_runs
                .get(&FunctionCallId::from(ctx.request_id.as_str()))
                .and_then(|fn_run| fn_run.output.clone());

            match function_run {
                Some(payload) => {
                    read_json_output(&payload, state, &ctx.namespace)
                        .await
                        .map(|body| {
                            debug!("function run output found");
                            RequestStateFinishedOutput {
                                body: Some(body),
                                content_encoding: payload.encoding,
                                path: build_output_path(
                                    &ctx.namespace,
                                    &ctx.application_name,
                                    &ctx.request_id,
                                ),
                            }
                        })
                }
                None => {
                    info!("no function run output found");
                    None
                }
            }
        }
        RequestOutcome::Failure(_) | RequestOutcome::Unknown => {
            debug!("building finished event for failure or unknown outcome");
            None
        }
    };

    RequestStateChangeEvent::finished(ctx, outcome, output)
}

enum CheckForFinishedResult {
    NoOutcome,
    Finished(RequestStateChangeEvent),
    NotFound,
    Error,
}

async fn check_for_finished(
    reader: &StateReader,
    state: &RouteState,
    namespace: &str,
    application: &str,
    request_id: &str,
) -> CheckForFinishedResult {
    match reader
        .request_ctx(&namespace, &application, &request_id)
        .await
    {
        Ok(Some(ref context)) => match &context.outcome {
            Some(outcome) => {
                let event = build_finished_event_for_outcome(&state, &context, &outcome).await;
                CheckForFinishedResult::Finished(event)
            }
            None => CheckForFinishedResult::NoOutcome,
        },
        Ok(None) => {
            info!("request not found, stopping stream");
            CheckForFinishedResult::NotFound
        }
        Err(e) => {
            error!(?e, "failed to get request");
            CheckForFinishedResult::Error
        }
    }
}

struct SubscriptionGuard {
    indexify_state: Arc<IndexifyState>,
    namespace: String,
    application: String,
    request_id: String,
}

impl SubscriptionGuard {
    fn new(
        indexify_state: Arc<IndexifyState>,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> Self {
        Self {
            indexify_state,
            namespace: namespace.to_string(),
            application: application.to_string(),
            request_id: request_id.to_string(),
        }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let indexify_state = self.indexify_state.clone();
        let namespace = self.namespace.clone();
        let application = self.application.clone();
        let request_id = self.request_id.clone();

        tokio::spawn(async move {
            indexify_state
                .unsubscribe_request_events(&namespace, &application, &request_id)
                .await;
        });
    }
}

#[pin_project]
struct StreamWithGuard {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<Event, axum::Error>> + Send>>,
    _guard: SubscriptionGuard,
}

impl Stream for StreamWithGuard {
    type Item = Result<Event, axum::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

#[tracing::instrument(skip(rx, state))]
pub(crate) async fn create_request_progress_stream(
    mut rx: broadcast::Receiver<RequestStateChangeEvent>,
    state: RouteState,
    namespace: String,
    application: String,
    request_id: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    let stream = async_stream::stream! {
        let reader = state.indexify_state.reader();

        match check_for_finished(&reader, &state, &namespace, &application, &request_id).await {
            CheckForFinishedResult::Finished(event) => {
                info!("request finished, sending event");
                yield Event::default().json_data(event);
                return;
            }
            CheckForFinishedResult::NoOutcome => {
                info!("no outcome found, continuing stream");
            }
            CheckForFinishedResult::NotFound => {
                info!("request not found, stopping stream");
                return;
            }
            CheckForFinishedResult::Error => {
                error!("failed to check for finished, stopping stream");
                return;
            }
        }

        loop {
            match rx.recv().await {
                Ok(event) if matches!(event, RequestStateChangeEvent::RequestFinished(_)) => {
                    debug!("received finished event: {:?}", event);
                    match check_for_finished(&reader, &state, &namespace, &application, &request_id).await {
                        CheckForFinishedResult::Finished(finished_event) => {
                            info!("request finished, sending event");
                            yield Event::default().json_data(finished_event);
                        }
                        CheckForFinishedResult::NoOutcome | CheckForFinishedResult::NotFound | CheckForFinishedResult::Error => {
                            warn!("no outcome or request not found or error, sending event as is");
                            yield Event::default().json_data(event);
                        }
                    }
                    info!("finished event received, stopping stream");
                    break;
                }
                Ok(event) => {
                    debug!("received event: {:?}", event);
                    yield Event::default().json_data(event);
                }
                Err(RecvError::Lagged(num)) => {
                    warn!("lagging behind request event stream by {} events", num);
                    match check_for_finished(&reader, &state, &namespace, &application, &request_id).await {
                        CheckForFinishedResult::Finished(event) => {
                            info!("request finished during lag, sending event and stopping stream");
                            yield Event::default().json_data(event);
                            break;
                        }
                        CheckForFinishedResult::NoOutcome => {
                            info!("no outcome found during lag, continuing to listen");
                        }
                        CheckForFinishedResult::NotFound => {
                            info!("request not found during lag, stopping stream");
                            break;
                        }
                        CheckForFinishedResult::Error => {
                            error!("failed to check for finished during lag, stopping stream");
                            break;
                        }
                    }
                }
                Err(RecvError::Closed) => {
                    info!("request event stream closed, checking for finished state");
                    if let CheckForFinishedResult::Finished(event) = check_for_finished(&reader, &state, &namespace, &application, &request_id).await {
                        info!("request finished, sending event");
                        yield Event::default().json_data(event);
                    }
                    break;
                }
            }
        }
    };

    stream
}

#[derive(Serialize)]
struct RequestIdV1 {
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
    Path((namespace, application_name)): Path<(String, String)>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let _inc = Increment::inc(&state.metrics.requests, &[]);

    let request_id = match headers.get("Idempotency-Key").and_then(|v| v.to_str().ok()) {
        Some(id) => {
            if id.len() > MAX_REQUEST_ID_LENGTH {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "Idempotency key for requests exceeds maximum length of {MAX_REQUEST_ID_LENGTH} characters"
                )));
            }
            if id.is_empty() {
                return Err(IndexifyAPIError::bad_request(
                    "Idempotency key for requests cannot be empty",
                ));
            }
            id.to_string()
        }
        None => nanoid::nanoid!(),
    };

    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    let payload_key = format!(
        "{}/input",
        data_model::DataPayload::request_key_prefix(&namespace, &application_name, &request_id)
    );
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
        id: request_id.clone(), // Use request_id for idempotency
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
        .get_application(&namespace, &application_name)
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to get application: {e}")))?
        .ok_or(IndexifyAPIError::not_found("application not found"))?;

    if let ApplicationState::Disabled { reason } = &application.state {
        return Result::Err(IndexifyAPIError::conflict(reason));
    }

    let function_call_id = FunctionCallId(request_id.clone()); // This clone is necessary here as we reuse request_id later

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
                data_payload,
            }],
            &request_id,
        )
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to create function run: {e}"))
        })?;
    let fn_runs = HashMap::from([(fn_run.id.clone(), fn_run)]);
    let fn_calls = HashMap::from([(fn_call.function_call_id.clone(), fn_call)]);

    let request_ctx = RequestCtxBuilder::default()
        .namespace(namespace.clone())
        .application_name(application.name.clone())
        .application_version(application.version.clone())
        .request_id(request_id.clone())
        .created_at(get_epoch_time_in_ms())
        .function_runs(fn_runs)
        .function_calls(fn_calls)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;
    let payload = RequestPayload::InvokeApplication(InvokeApplicationRequest {
        namespace: request_ctx.namespace.clone(),
        application_name: request_ctx.application_name.clone(),
        ctx: request_ctx.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest { payload })
        .await
        .map_err(|e| {
            if let Some(driver_error) = e.downcast_ref::<driver::Error>()
                && driver_error.is_request_already_exists()
            {
                IndexifyAPIError::conflict(&driver_error.to_string())
            } else {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}"))
            }
        })?;

    if accept_header.contains("application/json") {
        return Ok(Json(RequestIdV1 {
            request_id: request_id.clone(),
        })
        .into_response());
    }
    if accept_header.contains("text/event-stream") {
        return return_sse_response(
            // cloning the state is cheap because all its fields are inside arcs
            state.clone(),
            request_ctx,
        )
        .await;
    }
    Err(IndexifyAPIError::bad_request(
        "accept header must be application/json or text/event-stream",
    ))
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
    let ctx = state
        .indexify_state
        .reader()
        .request_ctx(&namespace, &application, &request_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to get request context: {e}"))
        })?
        .ok_or(IndexifyAPIError::not_found("request not found"))?;
    return_sse_response(state, ctx).await
}

async fn return_sse_response(
    state: RouteState,
    ctx: RequestCtx,
) -> Result<axum::response::Response, IndexifyAPIError> {
    let rx = state
        .indexify_state
        .subscribe_request_events(&ctx.namespace, &ctx.application_name, &ctx.request_id)
        .await;

    let guard = SubscriptionGuard::new(
        state.indexify_state.clone(),
        &ctx.namespace,
        &ctx.application_name,
        &ctx.request_id,
    );

    let inner_stream = create_request_progress_stream(
        rx,
        state.clone(),
        ctx.namespace,
        ctx.application_name,
        ctx.request_id,
    )
    .await;

    let stream_with_guard = StreamWithGuard {
        stream: Box::pin(inner_stream),
        _guard: guard,
    };

    Ok(axum::response::Sse::new(stream_with_guard)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(1)))
        .into_response())
}
