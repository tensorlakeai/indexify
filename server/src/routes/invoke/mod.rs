mod saga;

use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderName},
    response::{IntoResponse, sse::Event},
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{error, info, warn};
use uuid::{Uuid, uuid};

use super::routes_state::RouteState;
use crate::{
    data_model::{
        self,
        Application,
        ApplicationState,
        DataPayload,
        FunctionCallId,
        InputArgs,
        RequestCtxBuilder,
    },
    http_objects::IndexifyAPIError,
    metrics::Increment,
    routes::invoke::saga::{InvokeApplicationSaga, InvokeApplicationSagaBuilder},
    state_store::{EnsureIdempotencyError, request_events::RequestStateChangeEvent},
    utils::get_epoch_time_in_ms,
};

// New shared function for creating SSE streams
#[tracing::instrument(skip(rx, state))]
async fn create_request_progress_stream(
    mut rx: Receiver<RequestStateChangeEvent>,
    state: RouteState,
    namespace: String,
    application: String,
    request_id: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    let reader = state.indexify_state.reader();

    async_stream::stream! {
        // check completion when starting stream
        match reader.request_ctx(&namespace, &application, &request_id).await
        {
            Ok(Some(request_ctx)) => {
                if let Some(outcome) = &request_ctx.outcome {
                    yield Event::default().json_data(
                        RequestStateChangeEvent::finished(
                            &namespace,
                            &application,
                            &request_ctx.application_version,
                            &request_id,
                            outcome.clone(),
                        )
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

        // Stream events
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if ev.request_id() == request_id {
                        yield Event::default().json_data(ev.clone());

                        if let RequestStateChangeEvent::RequestFinished(_) = ev {
                            return;
                        }
                    }
                }
                Err(RecvError::Lagged(num)) => {
                    warn!("lagging behind request event stream by {} events", num);

                    // Check if completion happened during lag
                    match reader
                        .request_ctx(&namespace, &application, &request_id)
                        .await
                    {
                        Ok(Some(context)) => {
                            if let Some(outcome) = &context.outcome {
                                yield Event::default().json_data(
                                    RequestStateChangeEvent::finished(
                                        &namespace,
                                        &application,
                                        &context.application_version,
                                        &request_id,
                                        outcome.clone(),
                                    )
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
    // FIXME: Remove this once we migrate clients off this.
    id: String,
    request_id: String,
}

enum ResponseType {
    Json,
    Sse,
}

/// Make a request to application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    params(
        ("x-request-idempotency-key" = Option<String>, Header, description = "Treat the request as idempotent for the given application namespace, name and this key.", max_length = 255),
    ),
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = OK, description = "request successful"),
        (status = BAD_REQUEST, description = "bad request"),
        (status = CONFLICT, description = "application disabled or duplicate idempotent request"),
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

    let application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application)
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to get application: {e}")))?
        .ok_or(IndexifyAPIError::not_found("application not found"))?;

    let request_id = resolve_request_id(&headers, &application)?;

    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    // Validate request
    let response_type = if accept_header.contains("application/json") {
        ResponseType::Json
    } else if accept_header.contains("text/event-stream") {
        ResponseType::Sse
    } else {
        return Err(IndexifyAPIError::bad_request(
            "Unsupported Accept header. Supported: application/json, text/event-stream",
        ));
    };

    // Validate application
    if let ApplicationState::Disabled { reason } = &application.state {
        return Err(IndexifyAPIError::conflict(reason));
    }

    let entrypoint_fn_name = &application.entrypoint.function_name;
    let Some(entrypoint_fn) = application.functions.get(entrypoint_fn_name) else {
        return Err(IndexifyAPIError::not_found(&format!(
            "application entrypoint function {entrypoint_fn_name} is not in the application function list",
        )));
    };

    let blob_storage = state.blob_storage.get_blob_store(&application.namespace);

    let saga = InvokeApplicationSagaBuilder::default()
        .blob_storage(blob_storage)
        .state(state.indexify_state.clone())
        .request_id(request_id.clone())
        .build()
        .map_err(|err| IndexifyAPIError::internal_error(anyhow!("failed to build saga: {err}")))?;

    // Stage 1: Ensure idempotency
    saga.ensure_idempotent(&application)
        .await
        .map_err(|error| match error {
            EnsureIdempotencyError::AlreadyExists => IndexifyAPIError::conflict(
                "duplicate invoke application request detected for the given idempotency key",
            ),
            EnsureIdempotencyError::Error(e) => IndexifyAPIError::internal_error(anyhow!(e)),
        })?;

    // Stage 2: Store request payload
    let data_payload = store_request_payload(&saga, body, encoding).await?;

    state
        .metrics
        .request_input_bytes
        .add(data_payload.size, &[]);
    state.metrics.requests.add(1, &[]);

    // Stage 3: Create function call and function run
    let function_call_id = FunctionCallId(request_id.clone());

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

    saga.write_fn_call(request_ctx)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    match response_type {
        ResponseType::Json => {
            saga.commit().await;
            Ok(Json(RequestIdV1 {
                id: request_id.clone(),
                request_id: request_id.clone(),
            })
            .into_response())
        }
        ResponseType::Sse => {
            let response =
                return_sse_response(state.clone(), namespace, application.name, request_id).await?;
            saga.commit().await;
            Ok(response)
        }
    }
}

async fn store_request_payload(
    saga: &InvokeApplicationSaga,
    body: Body,
    encoding: String,
) -> Result<DataPayload, IndexifyAPIError> {
    let payload_stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| anyhow::anyhow!(err)));

    let put_result = saga
        .store_body(Box::pin(payload_stream))
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;

    let data_payload = data_model::DataPayload {
        id: nanoid::nanoid!(),
        metadata_size: 0,
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
        offset: 0, // Whole BLOB was written, so offset is 0
        encoding,
    };
    Ok(data_payload)
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
    return_sse_response(state, namespace, application, request_id).await
}

async fn return_sse_response(
    state: RouteState,
    namespace: String,
    application: String,
    request_id: String,
) -> Result<axum::response::Response, IndexifyAPIError> {
    let rx = state.indexify_state.function_run_event_stream();

    let request_event_stream =
        create_request_progress_stream(rx, state, namespace, application, request_id).await;
    Ok(axum::response::Sse::new(request_event_stream)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        )
        .into_response())
}

const REQUEST_ID_NAMESPACE: Uuid = uuid!("a7334912-af46-4619-8d7e-7afa4458f62f");
const IDEMPOTENCY_KEY_HEADER: HeaderName = HeaderName::from_static("x-request-idempotency-key");

fn resolve_request_id(
    headers: &HeaderMap,
    application: &Application,
) -> Result<String, IndexifyAPIError> {
    let Some(key) = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    else {
        return Ok(nanoid::nanoid!());
    };

    if key.len() > 255 {
        return Err(IndexifyAPIError::bad_request(
            "X-Request-Idempotency-Key header exceeds maximum length of 255 characters",
        ));
    }

    info!("Generating request ID using idempotency key: {}", key);

    let request_id_value = format!("{}:{}:{}", application.namespace, application.name, key);

    let request_id = Uuid::new_v5(&REQUEST_ID_NAMESPACE, request_id_value.as_bytes());

    Ok(request_id.to_string())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::data_model::{ApplicationBuilder, ApplicationEntryPoint, DataPayloadBuilder};

    fn create_app() -> Application {
        let code = DataPayloadBuilder::default()
            .path("a path".to_string())
            .metadata_size(0)
            .offset(0)
            .size(0)
            .sha256_hash("sha256".to_string())
            .build()
            .expect("build data payload should be successful");

        let entry_point = ApplicationEntryPoint {
            function_name: "main".to_string(),
            input_serializer: "json".to_string(),
            output_serializer: "json".to_string(),
            output_type_hints_base64: "json".to_string(),
        };

        ApplicationBuilder::default()
            .namespace("ns1".to_string())
            .name("myapp".to_string())
            .tombstoned(false)
            .description("some description".to_string())
            .version("1.0".to_string())
            .code(code)
            .functions(HashMap::new())
            .entrypoint(entry_point)
            .build()
            .expect("build application should be successful")
    }

    #[test]
    pub fn test_resolve_request_id_no_header() {
        let headers = HeaderMap::new();
        let app = create_app();

        let result =
            resolve_request_id(&headers, &app).expect("resolve request id should be successful");

        assert_eq!(result.len(), 21);
    }

    #[test]
    pub fn test_resolve_request_id_empty_header() {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, HeaderValue::from_static(""));
        let app = create_app();

        let result =
            resolve_request_id(&headers, &app).expect("resolve request id should be successful");

        assert_eq!(result.len(), 21);
    }

    #[test]
    pub fn test_resolve_request_id_header_too_long() {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(&("a".repeat(256))).expect("should be valid header"),
        );
        let app = create_app();

        let result =
            resolve_request_id(&headers, &app).expect_err("resolve request id should error");
        let response = result.into_response();
        assert_eq!(response.status(), 400);
    }

    #[test]
    pub fn test_resolve_request_id_header_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, HeaderValue::from_static("abc-1234"));
        let app = create_app();

        let result = resolve_request_id(&headers, &app).expect("should resolve request id");
        assert_eq!("1a00a958-a460-53ac-9540-e73c873c54a4", result);
    }
}
