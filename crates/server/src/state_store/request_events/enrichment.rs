//! Output enrichment for `RequestFinished` events.
//!
//! `RequestStateChangeEvent::RequestFinished` is written to the outbox with
//! `output: None` by the state machine.  When the event is delivered to a
//! consumer (SSE stream or HTTP cloud-events export) we enrich it with the
//! actual function output by reading from blob storage.
//!
//! Centralising this logic here lets both the SSE route (`routes/invoke.rs`)
//! and the HTTP drain worker (`processor/request_state_change_processor.rs`)
//! call the same code path.

use anyhow::anyhow;
use futures::TryStreamExt;
use tracing::{debug, warn};

use super::{RequestStateChangeEvent, RequestStateFinishedOutput};
use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{DataPayload, FunctionCallId, RequestCtx, RequestOutcome},
    state_store::scanner::StateReader,
};

/// Maximum output size that is inlined as JSON in the event body.
/// Larger payloads only include the `path` reference.
const MAX_INLINE_JSON_SIZE: u64 = 1024 * 1024;

/// Returns the canonical API path for retrieving a request's output.
fn build_output_path(namespace: &str, application: &str, request_id: &str) -> String {
    format!(
        "/v1/namespaces/{}/applications/{}/requests/{}/output",
        namespace, application, request_id
    )
}

/// Reads the output blob and returns its parsed JSON body if the payload is
/// JSON-encoded and small enough to inline.  Returns `None` on any error or
/// when the payload exceeds `MAX_INLINE_JSON_SIZE`.
async fn read_json_output(
    payload: &DataPayload,
    blob_storage: &BlobStorageRegistry,
    namespace: &str,
) -> Option<serde_json::Value> {
    if !payload.encoding.starts_with("application/json") {
        return None;
    }

    if payload.data_size() > MAX_INLINE_JSON_SIZE {
        return None;
    }

    let blob_store = blob_storage.get_blob_store(namespace);

    let storage_reader = match blob_store
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

/// Builds a `RequestFinished` event enriched with the function output, if
/// available.  For failed requests the output is always `None`.
pub async fn build_finished_event_for_outcome(
    blob_storage: &BlobStorageRegistry,
    ctx: &RequestCtx,
    outcome: &RequestOutcome,
) -> RequestStateChangeEvent {
    if !outcome.is_success() {
        return RequestStateChangeEvent::finished(ctx, outcome, None);
    }

    let function_run_output = ctx
        .function_runs
        .get(&FunctionCallId::from(ctx.request_id.as_str()))
        .and_then(|fn_run| fn_run.output.clone());

    let Some(payload) = function_run_output else {
        return RequestStateChangeEvent::finished(ctx, outcome, None);
    };

    let output = read_json_output(&payload, blob_storage, &ctx.namespace)
        .await
        .map(|body| {
            debug!("function run output found");
            RequestStateFinishedOutput {
                body: Some(body),
                content_encoding: payload.encoding,
                path: build_output_path(&ctx.namespace, &ctx.application_name, &ctx.request_id),
            }
        });

    RequestStateChangeEvent::finished(ctx, outcome, output)
}

/// Checks whether a request has finished and, if so, returns its enriched
/// `RequestFinished` event.
///
/// Returns:
/// - `Ok(Some(event))` — request is finished.
/// - `Ok(None)` — request is still in progress.
/// - `Err(_)` — request was not found or a storage error occurred.
pub async fn check_for_finished(
    reader: &StateReader,
    blob_storage: &BlobStorageRegistry,
    namespace: &str,
    application: &str,
    request_id: &str,
) -> anyhow::Result<Option<RequestStateChangeEvent>> {
    let Some(context) = reader
        .request_ctx(namespace, application, request_id)
        .await?
    else {
        return Err(anyhow!("request not found"));
    };

    match &context.outcome {
        Some(outcome) => {
            let event = build_finished_event_for_outcome(blob_storage, &context, outcome).await;
            Ok(Some(event))
        }
        None => Ok(None),
    }
}
