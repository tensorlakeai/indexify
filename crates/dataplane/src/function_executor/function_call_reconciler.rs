//! Function call forwarding for allocation execution.

use std::collections::HashSet;

use prost::Message as _;
use proto_api::{
    executor_api_pb::{self, Allocation as ServerAllocation, AllocationLogEntry},
    function_executor_pb::{self, AllocationFunctionCallCreationResult, AllocationState},
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use super::{
    allocation_runner::{error_status, make_allocation_update, ok_status},
    fe_client::FunctionExecutorGrpcClient,
};
use crate::blob_ops::{BlobStore, MultipartUploadHandle};

/// Maximum function call request size in bytes (1 MB).
const MAX_FUNCTION_CALL_SIZE: usize = 1024 * 1024;

/// Maximum number of items in execution plan updates.
const MAX_EXECUTION_PLAN_UPDATE_ITEMS: usize = 1000;

/// Handle new function calls from the FE (send via allocation stream channel).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(executor_id, allocation_id))]
pub(super) async fn reconcile_function_calls(
    client: &mut FunctionExecutorGrpcClient,
    allocation_id: &str,
    allocation: &ServerAllocation,
    stream_tx: &mpsc::UnboundedSender<AllocationLogEntry>,
    seen_function_call_ids: &mut HashSet<String>,
    seen_op_ids: &mut HashSet<String>,
    metrics: &crate::metrics::DataplaneMetrics,
    state: &AllocationState,
    uri_prefix: &str,
    output_blob_handles: &[MultipartUploadHandle],
    blob_store: &BlobStore,
    _executor_id: &str,
) {
    for fc in &state.function_calls {
        let fc_id = fc.id.as_deref().unwrap_or("");
        if !fc_id.is_empty() && seen_function_call_ids.insert(fc_id.to_string()) {
            debug!(
                function_call_id = %fc_id,
                "New function call from allocation"
            );

            let root_fc_id = fc
                .updates
                .as_ref()
                .and_then(|u| u.root_function_call_id.clone());

            // Reconstruct canonical blob URI from the blob ID rather than
            // using the presigned upload URL stored in the blob chunks.
            let args_blob_uri = fc
                .args_blob
                .as_ref()
                .and_then(|b| b.id.as_deref())
                .map(|blob_id| format!("{}.{}.output_{}", uri_prefix, allocation_id, blob_id));

            // Complete the multipart upload for the args blob so the data is
            // visible in S3 before the server forwards the function call.
            // S3 multipart uploads are invisible until completed.
            if let Some(ref blob_uri) = args_blob_uri &&
                let Some(handle) = output_blob_handles.iter().find(|h| h.uri == *blob_uri)
            {
                let etags: Vec<String> = fc
                    .args_blob
                    .as_ref()
                    .map(|b| b.chunks.iter().filter_map(|c| c.etag.clone()).collect())
                    .unwrap_or_default();

                if !etags.is_empty() &&
                    let Err(e) = blob_store
                        .complete_multipart_upload(blob_uri, &handle.upload_id, &etags)
                        .await
                {
                    warn!(
                        blob_uri = %blob_uri,
                        error = ?e,
                        "Failed to complete multipart upload for args blob"
                    );
                }
            }

            let server_updates = fc
                .updates
                .as_ref()
                .map(|u| convert_execution_plan_updates(u, args_blob_uri.as_deref()));

            // Deduplicate individual function call / reduce op IDs across all
            // AllocationFunctionCall entries.  The SDK may serialize the same
            // logical function call both as a blocking child call *and* inside
            // the tail-call execution plan updates.  Without dedup the server
            // creates duplicate function runs that never get consumed, causing
            // the request to hang.
            let server_updates = server_updates.map(|mut updates| {
                updates.updates.retain(|u| {
                    let op_id = u.op.as_ref().and_then(|op| match op {
                        executor_api_pb::execution_plan_update::Op::FunctionCall(fc) => {
                            fc.id.clone()
                        }
                        executor_api_pb::execution_plan_update::Op::Reduce(r) => r.id.clone(),
                    });
                    match op_id {
                        Some(id) if !id.is_empty() => seen_op_ids.insert(id),
                        _ => true, // keep ops without IDs
                    }
                });
                updates
            });

            // If all ops were duplicates, skip sending to the server entirely
            // but still report success to the FE so it doesn't retry.
            if server_updates
                .as_ref()
                .map(|u| u.updates.is_empty())
                .unwrap_or(false)
            {
                debug!(
                    function_call_id = %fc_id,
                    "Skipping function call with all duplicate ops"
                );
                let creation_result = AllocationFunctionCallCreationResult {
                    function_call_id: root_fc_id,
                    allocation_function_call_id: Some(fc_id.to_string()),
                    status: Some(ok_status()),
                };
                let update = make_allocation_update(
                    allocation_id,
                    function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
                        creation_result,
                    ),
                );
                if let Err(e) = client.send_allocation_update(update).await {
                    warn!(
                        error = %e,
                        "Failed to send function call creation result"
                    );
                }
                continue;
            }

            let fc_request = executor_api_pb::FunctionCallRequest {
                namespace: allocation
                    .function
                    .as_ref()
                    .and_then(|f| f.namespace.clone()),
                application: allocation
                    .function
                    .as_ref()
                    .and_then(|f| f.application_name.clone()),
                request_id: allocation.request_id.clone(),
                updates: server_updates,
                source_function_call_id: allocation.function_call_id.clone(),
            };

            // Validate function call size and update count limits
            let request_size = fc_request.encoded_len();
            let update_count = fc_request
                .updates
                .as_ref()
                .map(|u| u.updates.len())
                .unwrap_or(0);

            if request_size > MAX_FUNCTION_CALL_SIZE {
                warn!(
                    function_call_id = %fc_id,
                    size = request_size,
                    limit = MAX_FUNCTION_CALL_SIZE,
                    "Function call exceeds size limit"
                );
                send_fc_validation_error(
                    client,
                    allocation_id,
                    fc_id,
                    root_fc_id.clone(),
                    format!(
                        "Function call size {} exceeds limit {} bytes",
                        request_size, MAX_FUNCTION_CALL_SIZE
                    ),
                )
                .await;
                continue;
            }

            if update_count > MAX_EXECUTION_PLAN_UPDATE_ITEMS {
                warn!(
                    function_call_id = %fc_id,
                    count = update_count,
                    limit = MAX_EXECUTION_PLAN_UPDATE_ITEMS,
                    "Function call exceeds update items limit"
                );
                send_fc_validation_error(
                    client,
                    allocation_id,
                    fc_id,
                    root_fc_id.clone(),
                    format!(
                        "Execution plan update count {} exceeds limit {}",
                        update_count, MAX_EXECUTION_PLAN_UPDATE_ITEMS
                    ),
                )
                .await;
                continue;
            }

            // Send the AllocationLogEntry containing the call_function entry
            // directly via the activity channel (included in heartbeat).
            let stream_request = AllocationLogEntry {
                allocation_id: allocation.allocation_id.clone().unwrap_or_default(),
                clock: 0,
                entry: Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(
                    fc_request,
                )),
            };

            let creation_result =
                match send_execution_update_with_retry(stream_tx, stream_request, metrics).await {
                    Ok(_) => AllocationFunctionCallCreationResult {
                        function_call_id: root_fc_id,
                        allocation_function_call_id: Some(fc_id.to_string()),
                        status: Some(ok_status()),
                    },
                    Err(e) => {
                        warn!(
                            error = ?e,
                            "Failed to send function call via allocation stream"
                        );
                        AllocationFunctionCallCreationResult {
                            function_call_id: root_fc_id,
                            allocation_function_call_id: Some(fc_id.to_string()),
                            status: Some(error_status(13, e.to_string())),
                        }
                    }
                };

            let update = make_allocation_update(
                allocation_id,
                function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
                    creation_result,
                ),
            );
            if let Err(e) = client.send_allocation_update(update).await {
                warn!(
                    error = ?e,
                    "Failed to send function call creation result"
                );
            }
        }
    }
}

/// Send a validation error result for a function call back to the FE.
async fn send_fc_validation_error(
    client: &mut FunctionExecutorGrpcClient,
    allocation_id: &str,
    fc_id: &str,
    root_fc_id: Option<String>,
    message: String,
) {
    let creation_result = AllocationFunctionCallCreationResult {
        function_call_id: root_fc_id,
        allocation_function_call_id: Some(fc_id.to_string()),
        status: Some(error_status(8, message)),
    };
    let update = make_allocation_update(
        allocation_id,
        function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
            creation_result,
        ),
    );
    let _ = client.send_allocation_update(update).await;
}

/// Send an allocation log entry via the activity channel.
async fn send_execution_update_with_retry(
    stream_tx: &mpsc::UnboundedSender<AllocationLogEntry>,
    request: AllocationLogEntry,
    metrics: &crate::metrics::DataplaneMetrics,
) -> Result<(), String> {
    let msg_size = prost::Message::encoded_len(&request);
    metrics
        .histograms
        .function_call_message_size_mb
        .record(msg_size as f64 / (1024.0 * 1024.0), &[]);

    metrics.counters.call_function_rpcs.add(1, &[]);

    match stream_tx.send(request) {
        Ok(_) => Ok(()),
        Err(e) => {
            metrics.counters.call_function_rpc_errors.add(1, &[]);
            Err(format!(
                "Failed to send via allocation stream channel: {}",
                e
            ))
        }
    }
}

/// Convert FE ExecutionPlanUpdates to server ExecutionPlanUpdates.
pub(super) fn convert_execution_plan_updates(
    fe_updates: &function_executor_pb::ExecutionPlanUpdates,
    args_blob_uri: Option<&str>,
) -> executor_api_pb::ExecutionPlanUpdates {
    let updates: Vec<executor_api_pb::ExecutionPlanUpdate> = fe_updates
        .updates
        .iter()
        .map(|u| {
            let op = u.op.as_ref().map(|op| match op {
                function_executor_pb::execution_plan_update::Op::FunctionCall(fc) => {
                    executor_api_pb::execution_plan_update::Op::FunctionCall(
                        executor_api_pb::FunctionCall {
                            id: fc.id.clone(),
                            target: fc
                                .target
                                .as_ref()
                                .map(super::proto_convert::convert_function_ref),
                            args: fc
                                .args
                                .iter()
                                .map(|a| convert_function_arg(a, args_blob_uri))
                                .collect(),
                            call_metadata: fc.call_metadata.clone(),
                        },
                    )
                }
                function_executor_pb::execution_plan_update::Op::Reduce(r) => {
                    executor_api_pb::execution_plan_update::Op::Reduce(executor_api_pb::ReduceOp {
                        id: r.id.clone(),
                        collection: r
                            .collection
                            .iter()
                            .map(|a| convert_function_arg(a, args_blob_uri))
                            .collect(),
                        reducer: r
                            .reducer
                            .as_ref()
                            .map(super::proto_convert::convert_function_ref),
                        call_metadata: r.call_metadata.clone(),
                    })
                }
            });

            executor_api_pb::ExecutionPlanUpdate { op }
        })
        .collect();

    executor_api_pb::ExecutionPlanUpdates {
        updates,
        root_function_call_id: fe_updates.root_function_call_id.clone(),
        start_at: fe_updates.start_at,
    }
}

/// Convert FE FunctionArg to server FunctionArg.
fn convert_function_arg(
    fe_arg: &function_executor_pb::FunctionArg,
    args_blob_uri: Option<&str>,
) -> executor_api_pb::FunctionArg {
    let source = fe_arg.source.as_ref().map(|s| match s {
        function_executor_pb::function_arg::Source::FunctionCallId(id) => {
            executor_api_pb::function_arg::Source::FunctionCallId(id.clone())
        }
        function_executor_pb::function_arg::Source::Value(so) => {
            executor_api_pb::function_arg::Source::InlineData(
                super::proto_convert::serialized_object_to_data_payload(
                    so,
                    args_blob_uri.map(|u| u.to_string()),
                ),
            )
        }
    });

    executor_api_pb::FunctionArg { source }
}
