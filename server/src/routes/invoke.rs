use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, sse::Event},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{error, info, warn};

use super::routes_state::RouteState;
use crate::{
    data_model::{self, ApplicationState, FunctionCallId, InputArgs, RequestCtxBuilder},
    http_objects::IndexifyAPIError,
    metrics::Increment,
    state_store::{
        request_events::RequestStateChangeEvent,
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
    },
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

fn validate_idempotency_key(key: &str) -> Result<(), &'static str> {
    if key.is_empty() {
        return Err("idempotency key cannot be empty");
    }
    if key.len() > 256 {
        return Err("idempotency key cannot exceed 256 characters");
    }
    Ok(())
}

/// Generates a deterministic request_id from namespace, application, and
/// idempotency key. Uses SHA256 hash encoded as base64url, truncated to 21
/// characters (matching nanoid length).
fn request_id_from_idempotency_key(
    namespace: &str,
    application: &str,
    idempotency_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace.as_bytes());
    hasher.update(b"|");
    hasher.update(application.as_bytes());
    hasher.update(b"|");
    hasher.update(idempotency_key.as_bytes());
    let hash = hasher.finalize();

    // Encode as base64url and take first 21 chars to match nanoid length
    let encoded = URL_SAFE_NO_PAD.encode(hash);
    encoded[..21].to_string()
}

/// Make a request to application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    params(
        ("X-Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key. If a request with this key already exists, returns the existing request ID (idempotent). Must be unique within the namespace/application scope.")
    ),
    responses(
        (status = 200, description = "request successful (or idempotent response for existing request)"),
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

    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    // Check for idempotency key - generates deterministic request_id from hash.
    let idempotency_key = headers
        .get("X-Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty());

    // Generate request_id: hash of idempotency key if provided, otherwise nanoid
    let request_id = if let Some(key) = idempotency_key {
        validate_idempotency_key(key)
            .map_err(|e| IndexifyAPIError::bad_request(&format!("invalid idempotency key: {e}")))?;

        // Generate deterministic request_id from namespace, app, and idempotency key
        let generated_id = request_id_from_idempotency_key(&namespace, &application, key);

        // Check if request already exists - if so, return idempotent response
        let existing = state
            .indexify_state
            .reader()
            .request_ctx(&namespace, &application, &generated_id)
            .await
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to check request existence: {e}"))
            })?;

        if existing.is_some() {
            // Idempotent response - return the existing request ID
            if accept_header.contains("application/json") {
                return Ok(Json(RequestIdV1 {
                    id: generated_id.clone(),
                    request_id: generated_id,
                })
                .into_response());
            }
            if accept_header.contains("text/event-stream") {
                return return_sse_response(state.clone(), namespace, application, generated_id)
                    .await;
            }
            return Err(IndexifyAPIError::bad_request(
                "accept header must be application/json or text/event-stream",
            ));
        }

        generated_id
    } else {
        nanoid::nanoid!()
    };

    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    let payload_stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| anyhow::anyhow!(err)));
    let put_result = state
        .blob_storage
        .get_blob_store(&namespace)
        .put(&request_id, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            error!("failed to write to blob store: {:?}", e);
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}"))
        })?;
    let data_payload = data_model::DataPayload {
        id: request_id.clone(),
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
    let payload = RequestPayload::InvokeApplication(InvokeApplicationRequest {
        namespace: namespace.clone(),
        application_name: application.name.clone(),
        ctx: request_ctx.clone(),
    });

    state
        .indexify_state
        .write(StateMachineUpdateRequest { payload })
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}")))?;

    if accept_header.contains("application/json") {
        return Ok(Json(RequestIdV1 {
            id: request_id.clone(),
            request_id: request_id.clone(),
        })
        .into_response());
    }
    if accept_header.contains("text/event-stream") {
        return return_sse_response(
            // cloning the state is cheap because all its fields are inside arcs
            state.clone(),
            namespace,
            application.name,
            request_id,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_idempotency_key_valid() {
        // Valid idempotency keys - more permissive than request IDs
        assert!(validate_idempotency_key("abc123").is_ok());
        assert!(validate_idempotency_key("my-request-id").is_ok());
        assert!(validate_idempotency_key("order/2024/001").is_ok()); // slashes allowed
        assert!(validate_idempotency_key("user@example.com:order:123").is_ok()); // special chars allowed
        assert!(validate_idempotency_key("a").is_ok()); // Single char
        assert!(validate_idempotency_key("a".repeat(256).as_str()).is_ok()); // Max length
    }

    #[test]
    fn test_validate_idempotency_key_empty() {
        assert_eq!(
            validate_idempotency_key(""),
            Err("idempotency key cannot be empty")
        );
    }

    #[test]
    fn test_validate_idempotency_key_too_long() {
        let long_key = "a".repeat(257);
        assert_eq!(
            validate_idempotency_key(&long_key),
            Err("idempotency key cannot exceed 256 characters")
        );
    }

    #[test]
    fn test_request_id_from_idempotency_key_deterministic() {
        // Same inputs should always produce same output
        let id1 = request_id_from_idempotency_key("ns1", "app1", "key1");
        let id2 = request_id_from_idempotency_key("ns1", "app1", "key1");
        assert_eq!(id1, id2);

        // Different inputs should produce different outputs
        let id3 = request_id_from_idempotency_key("ns1", "app1", "key2");
        assert_ne!(id1, id3);

        let id4 = request_id_from_idempotency_key("ns2", "app1", "key1");
        assert_ne!(id1, id4);

        let id5 = request_id_from_idempotency_key("ns1", "app2", "key1");
        assert_ne!(id1, id5);
    }

    #[test]
    fn test_request_id_from_idempotency_key_format() {
        let id = request_id_from_idempotency_key("namespace", "application", "my-order-123");

        // Should be 21 characters (matching nanoid length)
        assert_eq!(id.len(), 21);

        // Should only contain base64url characters (alphanumeric, -, _)
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }
}
