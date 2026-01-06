use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Multipart, OptionalFromRequest, Path, Request, State},
    http::HeaderMap,
    response::{IntoResponse, sse::Event},
};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use pin_project::pin_project;
use serde::Serialize;
use tokio::sync::broadcast::{self, error::RecvError};
use tracing::{error, info, warn};

use super::routes_state::RouteState;
use crate::{
    data_model::{
        self,
        ApplicationState,
        DataPayload,
        FunctionCallId,
        InputArgs,
        RequestCtx,
        RequestCtxBuilder,
        RequestOutcome,
    },
    http_objects::IndexifyAPIError,
    metrics::Increment,
    state_store::{
        IndexifyState,
        driver,
        request_events::{RequestStateChangeEvent, RequestStateFinishedOutput},
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
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

async fn build_finished_event_with_output(
    state: &RouteState,
    ctx: &RequestCtx,
    outcome: &RequestOutcome,
) -> RequestStateChangeEvent {
    // Get the entrypoint function's output (uses request_id as function_call_id)
    let output_payload = ctx
        .function_runs
        .get(&FunctionCallId::from(ctx.request_id.as_str()))
        .and_then(|fn_run| fn_run.output.clone());

    let Some(payload) = output_payload else {
        return RequestStateChangeEvent::finished(ctx, outcome, None);
    };

    let body = read_json_output(&payload, state, &ctx.namespace).await;

    let output = RequestStateFinishedOutput {
        body,
        content_encoding: payload.encoding,
        path: build_output_path(&ctx.namespace, &ctx.application_name, &ctx.request_id),
    };

    RequestStateChangeEvent::finished(ctx, outcome, Some(output))
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
async fn create_request_progress_stream(
    mut rx: broadcast::Receiver<RequestStateChangeEvent>,
    state: RouteState,
    namespace: String,
    application: String,
    request_id: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    async_stream::stream! {
        let reader = state.indexify_state.reader();

        // Check completion when starting stream
        match reader.request_ctx(&namespace, &application, &request_id).await {
            Ok(Some(context)) => {
                if let Some(outcome) = &context.outcome {
                    yield Event::default().json_data(
                        build_finished_event_with_output(&state, &context, outcome).await
                    );
                    return;
                }
            }
            Ok(None) => {
                info!("request not found, stopping stream");
                return;
            }
            Err(e) => {
                error!(?e, "failed to get request");
                return;
            }
        }

        loop {
            match rx.recv().await {
                Ok(ev) => {
                    let is_finished = matches!(ev, RequestStateChangeEvent::RequestFinished(_));

                    if is_finished && let Ok(Some(context)) = reader.request_ctx(&namespace, &application, &request_id).await
                            && let Some(outcome) = &context.outcome {
                        yield Event::default().json_data(
                            build_finished_event_with_output(&state, &context, outcome).await
                        );
                    } else {
                        yield Event::default().json_data(&ev);
                    }

                    if is_finished {
                        return;
                    }
                }
                Err(RecvError::Lagged(num)) => {
                    warn!("lagging behind request event stream by {} events", num);

                    // Check if completion happened during lag
                    match reader.request_ctx(&namespace, &application, &request_id).await {
                        Ok(Some(context)) => {
                            if let Some(outcome) = &context.outcome {
                                yield Event::default().json_data(
                                    build_finished_event_with_output(&state, &context, outcome).await
                                );
                                return;
                            }
                        }
                        Ok(None) => {
                            error!("request not found");
                            return;
                        }
                        Err(e) => {
                            error!(?e, "failed to get request context");
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
    request_id: String,
}

/// Make a request to application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    request_body(content_type = "application/json",
        content = inline(serde_json::Value),
        description = "The first positional argument for the application function",
    ),
    request_body(
        content_type = "multipart/form-data",
        description = concat!("Each multipart form field is mapped to an application function argument. ",
            "If the field name is integer N, it is mapped to the N-th positional argument. ",
            "If the field name is string NAME, it is mapped to the kword argument NAME.",
        ),
    ),
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
    request: Request<Body>,
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

    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    let content_type = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    let content_length = headers
        .get("Content-Length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    // Argument ix/name -> DataPayload
    let mut data_payloads: HashMap<String, DataPayload> = HashMap::new();
    if content_type.starts_with("multipart/form-data") {
        // Multi parameter application call. Each part is an application function
        // argument.
        let multipart = Multipart::from_request(request, &state)
            .await
            .map_err(|e| {
                IndexifyAPIError::bad_request(&format!("failed to parse multipart/form-data: {e}"))
            })?
            .ok_or(IndexifyAPIError::bad_request(
                "failed to parse multipart/form-data: no multipart data found",
            ))?;

        data_payloads = upload_multipart_application_arguments(
            &state,
            &namespace,
            &application_name,
            &request_id,
            multipart,
        )
        .await?;

        // If content_length is 0 then we're dealing with parameterless
        // application function call.
    } else if content_length > 0 {
        // The body is the first positional argument (arg 0).
        let body_stream = request
            .into_body()
            .into_data_stream()
            .map_err(|err| -> anyhow::Error { anyhow!(err) });
        let arg0_data_payload = upload_application_argument(
            &state,
            &namespace,
            &application_name,
            &request_id,
            0,
            "application/json".to_string(),
            body_stream,
        )
        .await?;
        data_payloads.insert("0".to_string(), arg0_data_payload);
    }

    state.metrics.requests.add(1, &[]);

    let function_call_id = FunctionCallId(request_id.clone()); // This clone is necessary here as we reuse request_id later

    let entrypoint_fn_name = &application.entrypoint.function_name;
    let Some(entrypoint_fn) = application.functions.get(entrypoint_fn_name) else {
        return Err(IndexifyAPIError::not_found(&format!(
            "application entrypoint function {entrypoint_fn_name} is not in the application function list",
        )));
    };

    let mut inputs: Vec<data_model::FunctionArgs> = Vec::new();
    let mut input_args: Vec<InputArgs> = Vec::new();
    let mut input_names: Vec<String> = Vec::new();
    data_payloads.iter().for_each(|(arg_name, data_payload)| {
        // Order in vectors must be the same.
        inputs.push(data_model::FunctionArgs::DataPayload(data_payload.clone()));
        input_args.push(InputArgs {
            function_call_id: None,
            data_payload: data_payload.clone(),
        });
        input_names.push(arg_name.clone());
    });

    let fn_call = data_model::FunctionCall {
        function_call_id,
        fn_name: entrypoint_fn.name.clone(),
        parent_function_call_id: None,
        inputs,
        call_metadata: Bytes::new(),
        input_names: Some(input_names),
    };

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
        .create_function_run(&fn_call, input_args, &request_id)
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
            if let Some(driver_error) = e.downcast_ref::<driver::Error>() &&
                driver_error.is_request_already_exists()
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
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text(""),
        )
        .into_response())
}

async fn upload_application_argument(
    state: &RouteState,
    namespace: &str,
    application_name: &str,
    request_id: &str,
    arg_seq_number: usize,
    arg_content_type: String,
    arg_data_stream: impl futures::Stream<Item = anyhow::Result<Bytes>> + Send + Unpin,
) -> Result<DataPayload, IndexifyAPIError> {
    let payload_key = format!(
        "{}/inputs/{arg_seq_number}",
        data_model::DataPayload::request_key_prefix(&namespace, &application_name, &request_id)
    );

    let put_result = state
        .blob_storage
        .get_blob_store(&namespace)
        .put(&payload_key, arg_data_stream)
        .await
        .map_err(|e| {
            error!("failed to write to blob store: {:?}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}"))
        })?;

    let data_payload = data_model::DataPayload {
        id: nanoid::nanoid!(), // Not really used anywhere as of now
        metadata_size: 0,
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
        offset: 0, // Whole BLOB was written, so offset is 0
        encoding: arg_content_type,
    };

    state
        .metrics
        .request_input_bytes
        .add(data_payload.size, &[]);

    Ok(data_payload)
}

async fn upload_multipart_application_arguments(
    state: &RouteState,
    namespace: &str,
    application_name: &str,
    request_id: &str,
    mut multipart: Multipart,
) -> Result<HashMap<String, DataPayload>, IndexifyAPIError> {
    let mut arg_seq_number = 0;
    let mut data_payloads: HashMap<String, DataPayload> = HashMap::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| IndexifyAPIError::internal_error(anyhow!(err)))?
    {
        let field_name = field
            .name()
            .ok_or(IndexifyAPIError::bad_request(
                "multipart field name is missing",
            ))?
            .to_string();

        let field_content_type = field
            .content_type()
            .ok_or(IndexifyAPIError::bad_request(
                "multipart field content type is missing",
            ))?
            .to_string();

        let data_stream = field
            .into_stream()
            .map_err(|err| -> anyhow::Error { anyhow!(err) });

        let data_payload = upload_application_argument(
            state,
            namespace,
            application_name,
            request_id,
            arg_seq_number,
            field_content_type,
            data_stream,
        )
        .await?;

        arg_seq_number += 1;
        data_payloads.insert(field_name, data_payload);
    }

    Ok(data_payloads)
}
