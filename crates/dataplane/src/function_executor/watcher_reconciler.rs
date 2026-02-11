//! Watcher registration and result delivery for allocation execution.

use std::collections::HashSet;

use proto_api::{
    executor_api_pb::FunctionCallResult as ServerFunctionCallResult,
    function_executor_pb::{self, AllocationFunctionCallResult, AllocationState},
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{
    allocation_runner::make_allocation_update,
    blob_reconciler::presign_read_only_blob_for_data_payload,
    fe_client::FunctionExecutorGrpcClient,
    watcher_registry::{WatcherRegistry, WatcherResult},
};
use crate::blob_ops::BlobStore;

/// Register new function call watchers with the shared registry.
///
/// New watcher IDs are inserted into `active_watcher_ids` which also serves
/// as the dedup set (watchers already in the set are skipped). The caller
/// removes IDs when their results are delivered.
pub(super) async fn reconcile_watchers(
    allocation: &proto_api::executor_api_pb::Allocation,
    watcher_registry: &WatcherRegistry,
    active_watcher_ids: &mut HashSet<String>,
    result_tx: &mpsc::UnboundedSender<WatcherResult>,
    state: &AllocationState,
) {
    for watcher in &state.function_call_watchers {
        let watcher_id = watcher.id.as_deref().unwrap_or("");
        if !watcher_id.is_empty() && !active_watcher_ids.contains(watcher_id) {
            let watched_fc_id = watcher.root_function_call_id.as_deref().unwrap_or("");

            debug!(
                watcher_id = %watcher_id,
                root_function_call_id = %watched_fc_id,
                "New function call watcher, registering"
            );

            let namespace = allocation
                .function
                .as_ref()
                .and_then(|f| f.namespace.as_deref())
                .unwrap_or("");
            let application = allocation
                .function
                .as_ref()
                .and_then(|f| f.application_name.as_deref())
                .unwrap_or("");
            let request_id = allocation.request_id.as_deref().unwrap_or("");

            watcher_registry
                .register_watcher(
                    namespace,
                    application,
                    request_id,
                    watched_fc_id,
                    watcher_id,
                    result_tx.clone(),
                )
                .await;
            active_watcher_ids.insert(watcher_id.to_string());
        }
    }
}

/// Convert a server FunctionCallResult to FE AllocationFunctionCallResult
/// and send it to the FE via send_allocation_update.
pub(super) async fn deliver_function_call_result_to_fe(
    client: &mut FunctionExecutorGrpcClient,
    allocation_id: &str,
    watcher_id: &str,
    watched_function_call_id: &str,
    fc_result: ServerFunctionCallResult,
    blob_store: &BlobStore,
) {
    debug!(
        watcher_id = %watcher_id,
        function_call_id = %watched_function_call_id,
        outcome = ?fc_result.outcome_code,
        "Delivering function call result to FE"
    );

    let fe_outcome =
        super::proto_convert::convert_outcome_code_server_to_fe(fc_result.outcome_code());

    let mut fe_result = AllocationFunctionCallResult {
        function_call_id: Some(watched_function_call_id.to_string()),
        watcher_id: Some(watcher_id.to_string()),
        outcome_code: Some(fe_outcome.into()),
        value_output: None,
        value_blob: None,
        request_error_output: None,
        request_error_blob: None,
    };

    if let Some(ref return_value) = fc_result.return_value {
        fe_result.value_output =
            Some(super::proto_convert::data_payload_to_serialized_object_inside_blob(return_value));

        match presign_read_only_blob_for_data_payload(return_value, blob_store).await {
            Ok(blob) => {
                fe_result.value_blob = Some(blob);
            }
            Err(e) => {
                warn!(
                    watcher_id = %watcher_id,
                    error = %e,
                    "Failed to presign value blob for function call result"
                );
            }
        }
    }

    if let Some(ref request_error) = fc_result.request_error {
        fe_result.request_error_output = Some(
            super::proto_convert::data_payload_to_serialized_object_inside_blob(request_error),
        );

        match presign_read_only_blob_for_data_payload(request_error, blob_store).await {
            Ok(blob) => {
                fe_result.request_error_blob = Some(blob);
            }
            Err(e) => {
                warn!(
                    watcher_id = %watcher_id,
                    error = %e,
                    "Failed to presign request error blob for function call result"
                );
            }
        }
    }

    let update = make_allocation_update(
        allocation_id,
        function_executor_pb::allocation_update::Update::FunctionCallResult(fe_result),
    );

    if let Err(e) = client.send_allocation_update(update).await {
        warn!(
            watcher_id = %watcher_id,
            error = %e,
            "Failed to send function call result to FE"
        );
    }
}
