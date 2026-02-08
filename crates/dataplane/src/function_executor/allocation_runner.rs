//! Core allocation execution protocol.
//!
//! Executes an allocation on a function executor subprocess. This is the
//! "Running" phase of the 3-phase lifecycle (Preparing → Running → Finalizing).
//!
//! Prep and finalization are handled separately by the controller so that
//! only execution occupies a concurrency slot.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use prost::Message as _;
use proto_api::{
    executor_api_pb::{
        Allocation as ServerAllocation,
        AllocationResult as ServerAllocationResult,
        FunctionCallResult as ServerFunctionCallResult,
        executor_api_client::ExecutorApiClient,
    },
    function_executor_pb::{
        self,
        AllocationFunctionCallCreationResult,
        AllocationFunctionCallResult,
        AllocationOutputBlob,
        AllocationRequestStateCommitWriteOperationResult,
        AllocationRequestStateOperationResult,
        AllocationRequestStatePrepareReadOperationResult,
        AllocationRequestStatePrepareWriteOperationResult,
        AllocationState,
        AllocationUpdate,
        CreateAllocationRequest,
        allocation_request_state_operation,
    },
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use super::{
    events::{AllocationOutcome, PreparedAllocation},
    fe_client::FunctionExecutorGrpcClient,
    watcher_registry::WatcherRegistry,
};
use crate::blob_ops::{self, BlobStore, MultipartUploadHandle};

/// Maximum function call request size in bytes (1 MB).
/// Matches Python executor's limits.py MAX_FUNCTION_CALL_SIZE_MB.
const MAX_FUNCTION_CALL_SIZE: usize = 1024 * 1024;

/// Maximum number of items in execution plan updates.
/// Matches Python executor's limits.py
/// MAX_FUNCTION_CALL_EXECUTION_PLAN_UPDATE_ITEMS_COUNT.
const MAX_EXECUTION_PLAN_UPDATE_ITEMS: usize = 1000;

/// Maximum retries for server RPCs (e.g. call_function).
const SERVER_RPC_MAX_RETRIES: u32 = 5;
/// Initial retry delay for server RPCs.
const SERVER_RPC_INITIAL_DELAY: Duration = Duration::from_millis(100);
/// Maximum retry delay for server RPCs.
const SERVER_RPC_MAX_DELAY: Duration = Duration::from_secs(15);

/// Execute an allocation on the FE and return the outcome.
///
/// This is the "Running" phase — it occupies a concurrency slot. Preparation
/// (presigning blobs) and finalization (completing uploads) are handled by the
/// controller outside the slot.
#[allow(clippy::too_many_arguments)]
pub async fn execute_allocation(
    mut client: FunctionExecutorGrpcClient,
    allocation: ServerAllocation,
    prepared: PreparedAllocation,
    server_channel: Channel,
    blob_store: Arc<BlobStore>,
    timeout: Duration,
    cancel_token: CancellationToken,
    watcher_registry: WatcherRegistry,
) -> AllocationOutcome {
    let allocation_id = allocation.allocation_id.clone().unwrap_or_default();
    let start_time = Instant::now();

    info!(allocation_id = %allocation_id, "Starting allocation execution");

    // Create allocation on FE
    let fe_allocation = function_executor_pb::Allocation {
        request_id: allocation.request_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        allocation_id: allocation.allocation_id.clone(),
        inputs: Some(prepared.inputs),
        result: None,
    };

    let create_request = CreateAllocationRequest {
        allocation: Some(fe_allocation),
    };

    if let Err(e) = client.create_allocation(create_request).await {
        error!(allocation_id = %allocation_id, error = %e, "Failed to create allocation on FE");
        return AllocationOutcome::Failed {
            reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError,
            error_message: e.to_string(),
            output_blob_handles: Vec::new(),
            likely_fe_crash: true,
        };
    }

    // Phase 3: Watch allocation state stream
    let mut stream = match client.watch_allocation_state(&allocation_id).await {
        Ok(s) => s,
        Err(e) => {
            error!(allocation_id = %allocation_id, error = %e, "Failed to open allocation state stream");
            let _ = client.delete_allocation(&allocation_id).await;
            return AllocationOutcome::Failed {
                reason: proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                error_message: e.to_string(),
                output_blob_handles: Vec::new(),
                likely_fe_crash: true,
            };
        }
    };

    // Phase 4: Reconciliation loop
    let mut seen_blob_ids: HashSet<String> = HashSet::new();
    let mut seen_function_call_ids: HashSet<String> = HashSet::new();
    let mut seen_watcher_ids: HashSet<String> = HashSet::new();
    let mut pending_state_read_ops: HashSet<String> = HashSet::new();
    let mut pending_state_write_ops: HashMap<String, StateWriteOpInfo> = HashMap::new();
    let mut output_blob_handles: Vec<MultipartUploadHandle> = Vec::new();
    let mut deadline = Instant::now() + timeout;
    #[allow(unused_assignments)] // Set to None here, assigned in the loop, read after the loop
    let mut final_result: Option<function_executor_pb::AllocationResult> = None;
    let mut watcher_tasks = WatcherTaskGuard {
        handles: Vec::new(),
    };
    let mut has_active_watchers = false;

    let uri_prefix = allocation
        .request_data_payload_uri_prefix
        .as_deref()
        .unwrap_or("file:///tmp/indexify-blobs");

    loop {
        if cancel_token.is_cancelled() {
            info!(allocation_id = %allocation_id, "Allocation cancelled");
            let _ = client.delete_allocation(&allocation_id).await;
            return AllocationOutcome::Cancelled {
                output_blob_handles,
            };
        }

        // If we have active watchers (waiting for child functions), we extend the
        // deadline to prevent timeout while waiting. The child function has its
        // own timeout.
        if has_active_watchers {
            deadline = Instant::now() + timeout;
        }

        if Instant::now() > deadline {
            warn!(allocation_id = %allocation_id, "Allocation timed out");
            let _ = client.delete_allocation(&allocation_id).await;
            return AllocationOutcome::Failed {
                reason: proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                error_message: "Allocation execution timed out".to_string(),
                output_blob_handles,
                likely_fe_crash: false,
            };
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = client.delete_allocation(&allocation_id).await;
                return AllocationOutcome::Cancelled {
                    output_blob_handles,
                };
            }
            result = tokio::time::timeout(remaining, stream.message()) => result,
        };

        match message {
            Ok(Ok(Some(state))) => {
                // Extend deadline on progress
                if state.progress.is_some() {
                    deadline = Instant::now() + timeout;
                }

                has_active_watchers = !state.function_call_watchers.is_empty();

                // Reconcile output blob requests
                for blob_req in &state.output_blob_requests {
                    let blob_id = blob_req.id.as_deref().unwrap_or("");
                    if !blob_id.is_empty() && seen_blob_ids.insert(blob_id.to_string()) {
                        debug!(
                            allocation_id = %allocation_id,
                            blob_id = %blob_id,
                            size = ?blob_req.size,
                            "New output blob request"
                        );

                        let blob_size = blob_req.size.unwrap_or(0);

                        // Create output blob with presigned write URLs
                        match blob_ops::create_output_blob(
                            &allocation_id,
                            blob_id,
                            uri_prefix,
                            blob_size,
                            &blob_store,
                        )
                        .await
                        {
                            Ok((handle, blob)) => {
                                output_blob_handles.push(handle);

                                let status = proto_api::google_rpc::Status {
                                    code: 0, // OK
                                    message: String::new(),
                                    details: vec![],
                                };

                                let update = AllocationUpdate {
                                    allocation_id: Some(allocation_id.clone()),
                                    update: Some(
                                        function_executor_pb::allocation_update::Update::OutputBlob(
                                            AllocationOutputBlob {
                                                status: Some(status),
                                                blob: Some(blob),
                                            },
                                        ),
                                    ),
                                };
                                if let Err(e) = client.send_allocation_update(update).await {
                                    warn!(
                                        allocation_id = %allocation_id,
                                        blob_id = %blob_id,
                                        error = %e,
                                        "Failed to send output blob update"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    allocation_id = %allocation_id,
                                    blob_id = %blob_id,
                                    error = %e,
                                    "Failed to create output blob"
                                );
                                // Send error status back to FE
                                let status = proto_api::google_rpc::Status {
                                    code: 13, // INTERNAL
                                    message: e.to_string(),
                                    details: vec![],
                                };
                                let update = AllocationUpdate {
                                    allocation_id: Some(allocation_id.clone()),
                                    update: Some(
                                        function_executor_pb::allocation_update::Update::OutputBlob(
                                            AllocationOutputBlob {
                                                status: Some(status),
                                                blob: None,
                                            },
                                        ),
                                    ),
                                };
                                let _ = client.send_allocation_update(update).await;
                            }
                        }
                    }
                }

                // Reconcile function calls
                for fc in &state.function_calls {
                    let fc_id = fc.id.as_deref().unwrap_or("");
                    if !fc_id.is_empty() && seen_function_call_ids.insert(fc_id.to_string()) {
                        debug!(
                            allocation_id = %allocation_id,
                            function_call_id = %fc_id,
                            "New function call from allocation"
                        );

                        // Call the server's call_function RPC
                        let mut server_client = ExecutorApiClient::new(server_channel.clone());

                        let root_fc_id = fc
                            .updates
                            .as_ref()
                            .and_then(|u| u.root_function_call_id.clone());

                        // Get the args blob URI from the function call
                        let args_blob_uri = fc
                            .args_blob
                            .as_ref()
                            .and_then(|b| b.chunks.first())
                            .and_then(|c| c.uri.clone());

                        // Convert FE ExecutionPlanUpdates to server format
                        let server_updates = fc
                            .updates
                            .as_ref()
                            .map(|u| convert_execution_plan_updates(u, args_blob_uri.as_deref()));

                        let fc_request = proto_api::executor_api_pb::FunctionCallRequest {
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
                                allocation_id = %allocation_id,
                                function_call_id = %fc_id,
                                size = request_size,
                                limit = MAX_FUNCTION_CALL_SIZE,
                                "Function call exceeds size limit"
                            );
                            let creation_result = AllocationFunctionCallCreationResult {
                                function_call_id: root_fc_id.clone(),
                                allocation_function_call_id: Some(fc_id.to_string()),
                                status: Some(proto_api::google_rpc::Status {
                                    code: 8, // RESOURCE_EXHAUSTED
                                    message: format!(
                                        "Function call size {} exceeds limit {} bytes",
                                        request_size, MAX_FUNCTION_CALL_SIZE
                                    ),
                                    details: vec![],
                                }),
                            };
                            let update = AllocationUpdate {
                                allocation_id: Some(allocation_id.clone()),
                                update: Some(
                                    function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
                                        creation_result,
                                    ),
                                ),
                            };
                            let _ = client.send_allocation_update(update).await;
                            continue;
                        }

                        if update_count > MAX_EXECUTION_PLAN_UPDATE_ITEMS {
                            warn!(
                                allocation_id = %allocation_id,
                                function_call_id = %fc_id,
                                count = update_count,
                                limit = MAX_EXECUTION_PLAN_UPDATE_ITEMS,
                                "Function call exceeds update items limit"
                            );
                            let creation_result = AllocationFunctionCallCreationResult {
                                function_call_id: root_fc_id.clone(),
                                allocation_function_call_id: Some(fc_id.to_string()),
                                status: Some(proto_api::google_rpc::Status {
                                    code: 8, // RESOURCE_EXHAUSTED
                                    message: format!(
                                        "Execution plan update count {} exceeds limit {}",
                                        update_count, MAX_EXECUTION_PLAN_UPDATE_ITEMS
                                    ),
                                    details: vec![],
                                }),
                            };
                            let update = AllocationUpdate {
                                allocation_id: Some(allocation_id.clone()),
                                update: Some(
                                    function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
                                        creation_result,
                                    ),
                                ),
                            };
                            let _ = client.send_allocation_update(update).await;
                            continue;
                        }

                        let creation_result =
                            match call_function_with_retry(&mut server_client, fc_request).await {
                                Ok(_) => AllocationFunctionCallCreationResult {
                                    function_call_id: root_fc_id,
                                    allocation_function_call_id: Some(fc_id.to_string()),
                                    status: Some(proto_api::google_rpc::Status {
                                        code: 0,
                                        message: String::new(),
                                        details: vec![],
                                    }),
                                },
                                Err(e) => {
                                    warn!(
                                        allocation_id = %allocation_id,
                                        error = %e,
                                        "call_function RPC failed after retries"
                                    );
                                    AllocationFunctionCallCreationResult {
                                        function_call_id: root_fc_id,
                                        allocation_function_call_id: Some(fc_id.to_string()),
                                        status: Some(proto_api::google_rpc::Status {
                                            code: 13, // INTERNAL
                                            message: e.to_string(),
                                            details: vec![],
                                        }),
                                    }
                                }
                            };

                        let update = AllocationUpdate {
                            allocation_id: Some(allocation_id.clone()),
                            update: Some(
                                function_executor_pb::allocation_update::Update::FunctionCallCreationResult(
                                    creation_result,
                                ),
                            ),
                        };
                        if let Err(e) = client.send_allocation_update(update).await {
                            warn!(
                                allocation_id = %allocation_id,
                                error = %e,
                                "Failed to send function call creation result"
                            );
                        }
                    }
                }

                // Reconcile function call watchers
                for watcher in &state.function_call_watchers {
                    let watcher_id = watcher.id.as_deref().unwrap_or("");
                    if !watcher_id.is_empty() && seen_watcher_ids.insert(watcher_id.to_string()) {
                        let watched_fc_id = watcher.root_function_call_id.as_deref().unwrap_or("");

                        debug!(
                            allocation_id = %allocation_id,
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

                        // Register the watcher with the shared registry
                        let mut result_rx = watcher_registry
                            .register_watcher(namespace, application, request_id, watched_fc_id)
                            .await;

                        // Spawn a background task to wait for the result and deliver it to FE
                        let watcher_client = client.clone();
                        let watcher_alloc_id = allocation_id.clone();
                        let watcher_id_owned = watcher_id.to_string();
                        let watched_fc_id_owned = watched_fc_id.to_string();
                        let watcher_blob_store = blob_store.clone();

                        let handle = tokio::spawn(async move {
                            if let Some(fc_result) = result_rx.recv().await {
                                deliver_function_call_result_to_fe(
                                    watcher_client,
                                    &watcher_alloc_id,
                                    &watcher_id_owned,
                                    &watched_fc_id_owned,
                                    fc_result,
                                    &watcher_blob_store,
                                )
                                .await;
                            }
                        });
                        watcher_tasks.handles.push(handle);
                    }
                }

                // Reconcile request state operations
                reconcile_request_state_operations(
                    &mut client,
                    &allocation_id,
                    uri_prefix,
                    &state,
                    &mut pending_state_read_ops,
                    &mut pending_state_write_ops,
                    &blob_store,
                )
                .await;

                // Check for final result
                if let Some(result) = state.result {
                    let outcome = result.outcome_code();
                    if outcome == function_executor_pb::AllocationOutcomeCode::Failure {
                        let failure_reason = result.failure_reason();
                        let has_request_error = result.request_error_output.is_some();
                        let has_value = result.outputs.is_some();
                        error!(
                            allocation_id = %allocation_id,
                            outcome = ?result.outcome_code,
                            failure_reason = ?failure_reason,
                            has_request_error = has_request_error,
                            has_value = has_value,
                            "Allocation FAILED"
                        );
                    } else {
                        info!(
                            allocation_id = %allocation_id,
                            outcome = ?result.outcome_code,
                            "Allocation completed"
                        );
                    }
                    final_result = Some(result);
                    break;
                }
            }
            Ok(Ok(None)) => {
                // Stream closed without result — FE likely crashed
                warn!(allocation_id = %allocation_id, "Allocation state stream closed prematurely");
                let _ = client.delete_allocation(&allocation_id).await;
                return AllocationOutcome::Failed {
                    reason: proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                    error_message: "Allocation state stream closed without result".to_string(),
                    output_blob_handles,
                    likely_fe_crash: true,
                };
            }
            Ok(Err(e)) => {
                // gRPC stream error — FE likely crashed
                error!(allocation_id = %allocation_id, error = %e, "Allocation state stream error");
                let _ = client.delete_allocation(&allocation_id).await;
                return AllocationOutcome::Failed {
                    reason: proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                    error_message: e.to_string(),
                    output_blob_handles,
                    likely_fe_crash: true,
                };
            }
            Err(_) => {
                // Timeout
                if has_active_watchers {
                    // This is expected if the FE is blocked waiting for a child function.
                    // We just continue the loop (deadline was already extended).
                    continue;
                }
                warn!(allocation_id = %allocation_id, "Allocation timed out waiting for state");
                let _ = client.delete_allocation(&allocation_id).await;
                return AllocationOutcome::Failed {
                    reason: proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                    error_message: "Allocation timed out".to_string(),
                    output_blob_handles,
                    likely_fe_crash: false,
                };
            }
        }
    }

    let execution_duration_ms = start_time.elapsed().as_millis() as u64;

    // Delete allocation from FE (fast gRPC call, do before freeing the slot)
    if let Err(e) = client.delete_allocation(&allocation_id).await {
        warn!(allocation_id = %allocation_id, error = %e, "Failed to delete allocation from FE");
    }

    // Convert FE result to server result. Finalization is handled by the
    // controller.
    match &final_result {
        Some(fe_result) => {
            let server_result =
                convert_fe_result_to_server(&allocation, fe_result, execution_duration_ms);
            AllocationOutcome::Completed {
                result: server_result,
                fe_result: final_result,
                output_blob_handles,
            }
        }
        None => AllocationOutcome::Failed {
            reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError,
            error_message: "No result from allocation".to_string(),
            output_blob_handles,
            likely_fe_crash: false,
        },
    }
}

/// Call server's call_function RPC with exponential backoff retry.
///
/// Retries on transient gRPC errors (Unavailable, Internal, DeadlineExceeded).
/// Matches Python executor's retry behavior for server RPCs.
async fn call_function_with_retry(
    client: &mut ExecutorApiClient<Channel>,
    request: proto_api::executor_api_pb::FunctionCallRequest,
) -> Result<(), tonic::Status> {
    let mut delay = SERVER_RPC_INITIAL_DELAY;

    for attempt in 0..=SERVER_RPC_MAX_RETRIES {
        match client.call_function(request.clone()).await {
            Ok(_) => return Ok(()),
            Err(status) => {
                let retryable = matches!(
                    status.code(),
                    tonic::Code::Unavailable |
                        tonic::Code::Internal |
                        tonic::Code::DeadlineExceeded
                );

                if !retryable || attempt == SERVER_RPC_MAX_RETRIES {
                    return Err(status);
                }

                warn!(
                    attempt = attempt + 1,
                    max_retries = SERVER_RPC_MAX_RETRIES,
                    code = ?status.code(),
                    delay_ms = delay.as_millis() as u64,
                    "call_function RPC failed, retrying"
                );

                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, SERVER_RPC_MAX_DELAY);
            }
        }
    }

    unreachable!()
}

/// Convert FE AllocationResult to server AllocationResult.
fn convert_fe_result_to_server(
    allocation: &ServerAllocation,
    fe_result: &function_executor_pb::AllocationResult,
    execution_duration_ms: u64,
) -> ServerAllocationResult {
    use proto_api::executor_api_pb;

    let outcome_code = match fe_result.outcome_code() {
        function_executor_pb::AllocationOutcomeCode::Success => {
            executor_api_pb::AllocationOutcomeCode::Success
        }
        function_executor_pb::AllocationOutcomeCode::Failure => {
            executor_api_pb::AllocationOutcomeCode::Failure
        }
        function_executor_pb::AllocationOutcomeCode::Unknown => {
            executor_api_pb::AllocationOutcomeCode::Unknown
        }
    };

    let failure_reason = match fe_result.failure_reason() {
        function_executor_pb::AllocationFailureReason::InternalError => {
            Some(executor_api_pb::AllocationFailureReason::InternalError)
        }
        function_executor_pb::AllocationFailureReason::FunctionError => {
            Some(executor_api_pb::AllocationFailureReason::FunctionError)
        }
        function_executor_pb::AllocationFailureReason::RequestError => {
            Some(executor_api_pb::AllocationFailureReason::RequestError)
        }
        function_executor_pb::AllocationFailureReason::Unknown => None,
    };

    // Get the output blob URI from the FE result
    let output_blob_uri = fe_result
        .uploaded_function_outputs_blob
        .as_ref()
        .and_then(|b| b.chunks.first())
        .and_then(|c| c.uri.clone());

    // Get the request error blob URI from the FE result
    let request_error_blob_uri = fe_result
        .uploaded_request_error_blob
        .as_ref()
        .and_then(|b| b.chunks.first())
        .and_then(|c| c.uri.clone());

    // Convert request_error (SerializedObjectInsideBLOB → DataPayload)
    let request_error = fe_result.request_error_output.as_ref().map(|so| {
        let manifest = so.manifest.as_ref();
        executor_api_pb::DataPayload {
            uri: request_error_blob_uri,
            encoding: manifest.and_then(|m| m.encoding),
            encoding_version: manifest.and_then(|m| m.encoding_version),
            content_type: manifest.and_then(|m| m.content_type.clone()),
            metadata_size: manifest.and_then(|m| m.metadata_size),
            offset: so.offset,
            size: manifest.and_then(|m| m.size),
            sha256_hash: manifest.and_then(|m| m.sha256_hash.clone()),
            source_function_call_id: manifest.and_then(|m| m.source_function_call_id.clone()),
            id: None,
        }
    });

    // Convert return value (value or execution plan updates)
    let return_value = match &fe_result.outputs {
        Some(function_executor_pb::allocation_result::Outputs::Value(so)) => {
            // Convert SerializedObjectInsideBLOB to DataPayload
            let manifest = so.manifest.as_ref();
            Some(executor_api_pb::allocation_result::ReturnValue::Value(
                executor_api_pb::DataPayload {
                    uri: output_blob_uri.clone(),
                    encoding: manifest.and_then(|m| m.encoding),
                    encoding_version: manifest.and_then(|m| m.encoding_version),
                    content_type: manifest.and_then(|m| m.content_type.clone()),
                    metadata_size: manifest.and_then(|m| m.metadata_size),
                    offset: so.offset,
                    size: manifest.and_then(|m| m.size),
                    sha256_hash: manifest.and_then(|m| m.sha256_hash.clone()),
                    source_function_call_id: manifest
                        .and_then(|m| m.source_function_call_id.clone()),
                    id: None,
                },
            ))
        }
        Some(function_executor_pb::allocation_result::Outputs::Updates(updates)) => {
            Some(executor_api_pb::allocation_result::ReturnValue::Updates(
                convert_execution_plan_updates(updates, output_blob_uri.as_deref()),
            ))
        }
        None => None,
    };

    ServerAllocationResult {
        function: allocation.function.clone(),
        allocation_id: allocation.allocation_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        request_id: allocation.request_id.clone(),
        outcome_code: Some(outcome_code.into()),
        failure_reason: failure_reason.map(|r| r.into()),
        return_value,
        request_error,
        execution_duration_ms: Some(execution_duration_ms),
    }
}

/// Convert FE ExecutionPlanUpdates to server ExecutionPlanUpdates.
/// `args_blob_uri` is the URI of the blob containing inline arg data (from FE's
/// args_blob).
fn convert_execution_plan_updates(
    fe_updates: &function_executor_pb::ExecutionPlanUpdates,
    args_blob_uri: Option<&str>,
) -> proto_api::executor_api_pb::ExecutionPlanUpdates {
    use proto_api::executor_api_pb;

    let updates: Vec<executor_api_pb::ExecutionPlanUpdate> = fe_updates
        .updates
        .iter()
        .map(|u| {
            let op = u.op.as_ref().map(|op| match op {
                function_executor_pb::execution_plan_update::Op::FunctionCall(fc) => {
                    executor_api_pb::execution_plan_update::Op::FunctionCall(
                        executor_api_pb::FunctionCall {
                            id: fc.id.clone(),
                            target: fc.target.as_ref().map(|t| executor_api_pb::FunctionRef {
                                namespace: t.namespace.clone(),
                                application_name: t.application_name.clone(),
                                function_name: t.function_name.clone(),
                                application_version: t.application_version.clone(),
                            }),
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
                        reducer: r.reducer.as_ref().map(|t| executor_api_pb::FunctionRef {
                            namespace: t.namespace.clone(),
                            application_name: t.application_name.clone(),
                            function_name: t.function_name.clone(),
                            application_version: t.application_version.clone(),
                        }),
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

/// Convert a server FunctionCallResult to FE AllocationFunctionCallResult
/// and send it to the FE via send_allocation_update.
async fn deliver_function_call_result_to_fe(
    mut client: FunctionExecutorGrpcClient,
    allocation_id: &str,
    watcher_id: &str,
    watched_function_call_id: &str,
    fc_result: ServerFunctionCallResult,
    blob_store: &BlobStore,
) {
    debug!(
        allocation_id = %allocation_id,
        watcher_id = %watcher_id,
        function_call_id = %watched_function_call_id,
        outcome = ?fc_result.outcome_code,
        "Delivering function call result to FE"
    );

    // Convert server outcome code to FE outcome code
    let fe_outcome = match fc_result.outcome_code() {
        proto_api::executor_api_pb::AllocationOutcomeCode::Success => {
            function_executor_pb::AllocationOutcomeCode::Success
        }
        proto_api::executor_api_pb::AllocationOutcomeCode::Failure => {
            function_executor_pb::AllocationOutcomeCode::Failure
        }
        proto_api::executor_api_pb::AllocationOutcomeCode::Unknown => {
            function_executor_pb::AllocationOutcomeCode::Unknown
        }
    };

    let mut fe_result = AllocationFunctionCallResult {
        function_call_id: Some(watched_function_call_id.to_string()),
        watcher_id: Some(watcher_id.to_string()),
        outcome_code: Some(fe_outcome.into()),
        value_output: None,
        value_blob: None,
        request_error_output: None,
        request_error_blob: None,
    };

    // Convert return_value (DataPayload) to SerializedObjectInsideBLOB + presigned
    // BLOB
    if let Some(ref return_value) = fc_result.return_value {
        fe_result.value_output = Some(data_payload_to_serialized_object_inside_blob(return_value));

        match presign_read_only_blob_for_data_payload(return_value, blob_store).await {
            Ok(blob) => {
                fe_result.value_blob = Some(blob);
            }
            Err(e) => {
                warn!(
                    allocation_id = %allocation_id,
                    watcher_id = %watcher_id,
                    error = %e,
                    "Failed to presign value blob for function call result"
                );
            }
        }
    }

    // Convert request_error (DataPayload) to SerializedObjectInsideBLOB + presigned
    // BLOB
    if let Some(ref request_error) = fc_result.request_error {
        fe_result.request_error_output =
            Some(data_payload_to_serialized_object_inside_blob(request_error));

        match presign_read_only_blob_for_data_payload(request_error, blob_store).await {
            Ok(blob) => {
                fe_result.request_error_blob = Some(blob);
            }
            Err(e) => {
                warn!(
                    allocation_id = %allocation_id,
                    watcher_id = %watcher_id,
                    error = %e,
                    "Failed to presign request error blob for function call result"
                );
            }
        }
    }

    let update = AllocationUpdate {
        allocation_id: Some(allocation_id.to_string()),
        update: Some(
            function_executor_pb::allocation_update::Update::FunctionCallResult(fe_result),
        ),
    };

    if let Err(e) = client.send_allocation_update(update).await {
        warn!(
            allocation_id = %allocation_id,
            watcher_id = %watcher_id,
            error = %e,
            "Failed to send function call result to FE"
        );
    }
}

/// Convert a server DataPayload to FE SerializedObjectInsideBLOB.
fn data_payload_to_serialized_object_inside_blob(
    data_payload: &proto_api::executor_api_pb::DataPayload,
) -> function_executor_pb::SerializedObjectInsideBlob {
    let manifest = function_executor_pb::SerializedObjectManifest {
        encoding: data_payload.encoding,
        encoding_version: data_payload.encoding_version,
        size: data_payload.size,
        metadata_size: data_payload.metadata_size,
        sha256_hash: data_payload.sha256_hash.clone(),
        content_type: data_payload.content_type.clone(),
        source_function_call_id: data_payload.source_function_call_id.clone(),
    };

    function_executor_pb::SerializedObjectInsideBlob {
        manifest: Some(manifest),
        offset: data_payload.offset,
    }
}

/// Create a presigned read-only BLOB for a server DataPayload.
async fn presign_read_only_blob_for_data_payload(
    data_payload: &proto_api::executor_api_pb::DataPayload,
    blob_store: &BlobStore,
) -> Result<function_executor_pb::Blob> {
    let blob_uri = data_payload.uri.as_deref().unwrap_or("");
    let offset = data_payload.offset.unwrap_or(0);
    let size = data_payload.size.unwrap_or(0);
    let total_size = offset + size;

    blob_ops::presign_read_only_blob("fc-result", blob_uri, total_size, blob_store).await
}

/// Info about a pending request state write operation.
struct StateWriteOpInfo {
    blob_uri: String,
    upload_id: String,
}

/// Build the blob URI for a request state key.
fn request_state_key_blob_uri(uri_prefix: &str, state_key: &str) -> String {
    // URL-encode the state key to make it safe for use in URIs.
    let safe_key: String = state_key
        .bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                vec![b as char]
            } else {
                format!("%{:02X}", b).chars().collect()
            }
        })
        .collect();
    format!("{}/state/{}", uri_prefix, safe_key)
}

/// Reconcile request state operations from the FE allocation state.
async fn reconcile_request_state_operations(
    client: &mut FunctionExecutorGrpcClient,
    allocation_id: &str,
    uri_prefix: &str,
    state: &AllocationState,
    pending_read_ops: &mut HashSet<String>,
    pending_write_ops: &mut HashMap<String, StateWriteOpInfo>,
    blob_store: &BlobStore,
) {
    // Process read and write operations
    let mut fe_read_op_ids: HashSet<String> = HashSet::new();

    for op in &state.request_state_operations {
        let op_id = op.operation_id.as_deref().unwrap_or("");
        if op_id.is_empty() {
            continue;
        }

        match &op.operation {
            Some(allocation_request_state_operation::Operation::PrepareRead(_)) => {
                fe_read_op_ids.insert(op_id.to_string());
                if !pending_read_ops.contains(op_id) {
                    pending_read_ops.insert(op_id.to_string());

                    let state_key = op.state_key.as_deref().unwrap_or("");
                    let blob_uri = request_state_key_blob_uri(uri_prefix, state_key);

                    debug!(
                        allocation_id = %allocation_id,
                        operation_id = %op_id,
                        state_key = %state_key,
                        "Handling prepare_read state operation"
                    );

                    let mut alloc_update = AllocationUpdate {
                        allocation_id: Some(allocation_id.to_string()),
                        update: Some(
                            function_executor_pb::allocation_update::Update::RequestStateOperationResult(
                                AllocationRequestStateOperationResult {
                                    operation_id: Some(op_id.to_string()),
                                    status: None,
                                    result: Some(
                                        function_executor_pb::allocation_request_state_operation_result::Result::PrepareRead(
                                            AllocationRequestStatePrepareReadOperationResult {
                                                blob: None,
                                            },
                                        ),
                                    ),
                                },
                            ),
                        ),
                    };

                    match blob_store.get_metadata(&blob_uri).await {
                        Ok(metadata) => {
                            match blob_ops::presign_read_only_blob(
                                op_id,
                                &blob_uri,
                                metadata.size_bytes,
                                blob_store,
                            )
                            .await
                            {
                                Ok(blob) => {
                                    set_state_op_result_status(&mut alloc_update, 0); // OK
                                    set_state_op_read_blob(&mut alloc_update, blob);
                                }
                                Err(e) => {
                                    warn!(
                                        allocation_id = %allocation_id,
                                        operation_id = %op_id,
                                        error = %e,
                                        "Failed to presign read blob for state operation"
                                    );
                                    set_state_op_result_status(&mut alloc_update, 13); // INTERNAL
                                }
                            }
                        }
                        Err(_) => {
                            // Blob not found - state key doesn't exist yet
                            set_state_op_result_status(&mut alloc_update, 5); // NOT_FOUND
                        }
                    }

                    if let Err(e) = client.send_allocation_update(alloc_update).await {
                        warn!(
                            allocation_id = %allocation_id,
                            operation_id = %op_id,
                            error = %e,
                            "Failed to send state read operation result"
                        );
                    }
                }
            }
            Some(allocation_request_state_operation::Operation::PrepareWrite(pw)) => {
                if !pending_write_ops.contains_key(op_id) {
                    let state_key = op.state_key.as_deref().unwrap_or("");
                    let blob_uri = request_state_key_blob_uri(uri_prefix, state_key);
                    let write_size = pw.size.unwrap_or(0);

                    debug!(
                        allocation_id = %allocation_id,
                        operation_id = %op_id,
                        state_key = %state_key,
                        size = write_size,
                        "Handling prepare_write state operation"
                    );

                    let mut alloc_update = AllocationUpdate {
                        allocation_id: Some(allocation_id.to_string()),
                        update: Some(
                            function_executor_pb::allocation_update::Update::RequestStateOperationResult(
                                AllocationRequestStateOperationResult {
                                    operation_id: Some(op_id.to_string()),
                                    status: Some(proto_api::google_rpc::Status {
                                        code: 0,
                                        message: String::new(),
                                        details: vec![],
                                    }),
                                    result: Some(
                                        function_executor_pb::allocation_request_state_operation_result::Result::PrepareWrite(
                                            AllocationRequestStatePrepareWriteOperationResult {
                                                blob: None,
                                            },
                                        ),
                                    ),
                                },
                            ),
                        ),
                    };

                    match blob_store.create_multipart_upload(&blob_uri).await {
                        Ok(handle) => {
                            match blob_ops::presign_write_only_blob(
                                op_id,
                                &blob_uri,
                                &handle.upload_id,
                                write_size,
                                blob_store,
                            )
                            .await
                            {
                                Ok(blob) => {
                                    pending_write_ops.insert(
                                        op_id.to_string(),
                                        StateWriteOpInfo {
                                            blob_uri: blob_uri.clone(),
                                            upload_id: handle.upload_id.clone(),
                                        },
                                    );
                                    set_state_op_write_blob(&mut alloc_update, blob);
                                }
                                Err(e) => {
                                    warn!(
                                        allocation_id = %allocation_id,
                                        operation_id = %op_id,
                                        error = %e,
                                        "Failed to presign write blob for state operation"
                                    );
                                    let _ = blob_store
                                        .abort_multipart_upload(&blob_uri, &handle.upload_id)
                                        .await;
                                    set_state_op_result_status(&mut alloc_update, 13); // INTERNAL
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                allocation_id = %allocation_id,
                                operation_id = %op_id,
                                error = %e,
                                "Failed to create multipart upload for state operation"
                            );
                            set_state_op_result_status(&mut alloc_update, 13); // INTERNAL
                        }
                    }

                    if let Err(e) = client.send_allocation_update(alloc_update).await {
                        warn!(
                            allocation_id = %allocation_id,
                            operation_id = %op_id,
                            error = %e,
                            "Failed to send state write operation result"
                        );
                    }
                }
            }
            Some(allocation_request_state_operation::Operation::CommitWrite(cw)) => {
                debug!(
                    allocation_id = %allocation_id,
                    operation_id = %op_id,
                    "Handling commit_write state operation"
                );

                // Get the write operation ID from the blob's id field
                let write_op_id = cw
                    .blob
                    .as_ref()
                    .and_then(|b| b.id.as_deref())
                    .map(|s| s.to_string());

                let mut alloc_update = AllocationUpdate {
                    allocation_id: Some(allocation_id.to_string()),
                    update: Some(
                        function_executor_pb::allocation_update::Update::RequestStateOperationResult(
                            AllocationRequestStateOperationResult {
                                operation_id: Some(op_id.to_string()),
                                status: Some(proto_api::google_rpc::Status {
                                    code: 0,
                                    message: String::new(),
                                    details: vec![],
                                }),
                                result: Some(
                                    function_executor_pb::allocation_request_state_operation_result::Result::CommitWrite(
                                        AllocationRequestStateCommitWriteOperationResult {},
                                    ),
                                ),
                            },
                        ),
                    ),
                };

                if let Some(ref wop_id) = write_op_id {
                    if let Some(write_info) = pending_write_ops.remove(wop_id.as_str()) {
                        // Collect etags from the commit_write blob chunks
                        let etags: Vec<String> = cw
                            .blob
                            .as_ref()
                            .map(|b| b.chunks.iter().filter_map(|c| c.etag.clone()).collect())
                            .unwrap_or_default();

                        if let Err(e) = blob_store
                            .complete_multipart_upload(
                                &write_info.blob_uri,
                                &write_info.upload_id,
                                &etags,
                            )
                            .await
                        {
                            warn!(
                                allocation_id = %allocation_id,
                                operation_id = %op_id,
                                error = %e,
                                "Failed to complete multipart upload for state operation"
                            );
                            set_state_op_result_status(&mut alloc_update, 13); // INTERNAL
                        }
                    } else {
                        set_state_op_result_status(&mut alloc_update, 5); // NOT_FOUND
                    }
                } else {
                    set_state_op_result_status(&mut alloc_update, 5); // NOT_FOUND
                }

                if let Err(e) = client.send_allocation_update(alloc_update).await {
                    warn!(
                        allocation_id = %allocation_id,
                        operation_id = %op_id,
                        error = %e,
                        "Failed to send state commit operation result"
                    );
                }
            }
            None => {}
        }
    }

    // Clean up read ops that FE removed
    *pending_read_ops = fe_read_op_ids;
}

/// Helper to set the status code on a state operation result update.
fn set_state_op_result_status(update: &mut AllocationUpdate, code: i32) {
    if let Some(function_executor_pb::allocation_update::Update::RequestStateOperationResult(
        ref mut result,
    )) = update.update
    {
        result.status = Some(proto_api::google_rpc::Status {
            code,
            message: String::new(),
            details: vec![],
        });
    }
}

/// Helper to set the blob on a prepare_read result.
fn set_state_op_read_blob(update: &mut AllocationUpdate, blob: function_executor_pb::Blob) {
    if let Some(function_executor_pb::allocation_update::Update::RequestStateOperationResult(
        ref mut result,
    )) = update.update &&
        let Some(
            function_executor_pb::allocation_request_state_operation_result::Result::PrepareRead(
                ref mut read_result,
            ),
        ) = result.result
    {
        read_result.blob = Some(blob);
    }
}

/// Helper to set the blob on a prepare_write result.
fn set_state_op_write_blob(update: &mut AllocationUpdate, blob: function_executor_pb::Blob) {
    if let Some(function_executor_pb::allocation_update::Update::RequestStateOperationResult(
        ref mut result,
    )) = update.update &&
        let Some(
            function_executor_pb::allocation_request_state_operation_result::Result::PrepareWrite(
                ref mut write_result,
            ),
        ) = result.result
    {
        write_result.blob = Some(blob);
    }
}

/// Convert FE FunctionArg to server FunctionArg.
/// `args_blob_uri` is the URI of the blob containing the arg data.
fn convert_function_arg(
    fe_arg: &function_executor_pb::FunctionArg,
    args_blob_uri: Option<&str>,
) -> proto_api::executor_api_pb::FunctionArg {
    use proto_api::executor_api_pb;

    let source = fe_arg.source.as_ref().map(|s| match s {
        function_executor_pb::function_arg::Source::FunctionCallId(id) => {
            executor_api_pb::function_arg::Source::FunctionCallId(id.clone())
        }
        function_executor_pb::function_arg::Source::Value(so) => {
            // Convert SerializedObjectInsideBLOB → inline DataPayload
            let manifest = so.manifest.as_ref();
            executor_api_pb::function_arg::Source::InlineData(executor_api_pb::DataPayload {
                uri: args_blob_uri.map(|u| u.to_string()),
                encoding: manifest.and_then(|m| m.encoding),
                encoding_version: manifest.and_then(|m| m.encoding_version),
                content_type: manifest.and_then(|m| m.content_type.clone()),
                metadata_size: manifest.and_then(|m| m.metadata_size),
                offset: so.offset,
                size: manifest.and_then(|m| m.size),
                sha256_hash: manifest.and_then(|m| m.sha256_hash.clone()),
                source_function_call_id: manifest.and_then(|m| m.source_function_call_id.clone()),
                id: None,
            })
        }
    });

    executor_api_pb::FunctionArg { source }
}

struct WatcherTaskGuard {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for WatcherTaskGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}
