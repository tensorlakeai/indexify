//! Core allocation execution protocol.
//!
//! Executes an allocation on a function executor subprocess. This is the
//! "Running" phase of the 3-phase lifecycle (Preparing → Running → Finalizing).
//!
//! Prep and finalization are handled separately by the controller so that
//! only execution occupies a concurrency slot.
//!
//! Reconciliation of output blobs and function calls is delegated to the
//! `blob_reconciler` and `function_call_reconciler` modules respectively.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use proto_api::{
    executor_api_pb::{
        Allocation as ServerAllocation,
        AllocationResult as ServerAllocationResult,
        AllocationStreamRequest,
    },
    function_executor_pb::{self, AllocationState, AllocationUpdate, CreateAllocationRequest},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use super::{
    blob_reconciler,
    events::{AllocationOutcome, PreparedAllocation},
    fe_client::FunctionExecutorGrpcClient,
    function_call_reconciler,
    proto_convert,
    state_ops::RequestStateHandler,
};
use crate::{
    allocation_result_dispatcher::AllocationResultDispatcher,
    blob_ops::{BlobStore, MultipartUploadHandle},
};

/// Shared infrastructure context for allocation execution.
///
/// Bundles the dependencies that are common across the allocation runner,
/// reconcilers, and related helpers — reducing parameter counts.
pub struct AllocationContext {
    pub server_channel: Channel,
    pub blob_store: Arc<BlobStore>,
    pub metrics: Arc<crate::metrics::DataplaneMetrics>,
    pub driver: Arc<dyn crate::driver::ProcessDriver>,
    pub process_handle: crate::driver::ProcessHandle,
    pub executor_id: String,
    pub stream_tx: mpsc::UnboundedSender<AllocationStreamRequest>,
    pub allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
}

/// Build an AllocationUpdate wrapping the given update variant.
pub(super) fn make_allocation_update(
    allocation_id: &str,
    update: function_executor_pb::allocation_update::Update,
) -> AllocationUpdate {
    AllocationUpdate {
        allocation_id: Some(allocation_id.to_string()),
        update: Some(update),
    }
}

/// Build an OK gRPC status.
pub(super) fn ok_status() -> proto_api::google_rpc::Status {
    proto_api::google_rpc::Status {
        code: 0,
        message: String::new(),
        details: vec![],
    }
}

/// Build an error gRPC status.
pub(super) fn error_status(code: i32, message: impl Into<String>) -> proto_api::google_rpc::Status {
    proto_api::google_rpc::Status {
        code,
        message: message.into(),
        details: vec![],
    }
}

/// Execute an allocation on the FE and return the outcome.
///
/// This is the "Running" phase — it occupies a concurrency slot. Preparation
/// (presigning blobs) and finalization (completing uploads) are handled by the
/// controller outside the slot.
pub async fn execute_allocation(
    client: FunctionExecutorGrpcClient,
    allocation: ServerAllocation,
    prepared: PreparedAllocation,
    ctx: AllocationContext,
    timeout: Duration,
    cancel_token: CancellationToken,
) -> AllocationOutcome {
    let mut runner = AllocationRunner::new(client, allocation, ctx, timeout, cancel_token);
    runner.run(prepared).await
}

/// Execution context for a single allocation on an FE subprocess.
struct AllocationRunner {
    client: FunctionExecutorGrpcClient,
    allocation: ServerAllocation,
    allocation_id: String,
    ctx: AllocationContext,
    timeout: Duration,
    cancel_token: CancellationToken,
    uri_prefix: String,
    request_error_blob_uri: Option<String>,
    // Reconciliation state
    seen_blob_ids: HashSet<String>,
    seen_function_call_ids: HashSet<String>,
    seen_op_ids: HashSet<String>,
    state_handler: RequestStateHandler,
    output_blob_handles: Vec<MultipartUploadHandle>,
    // Maps root_function_call_id → watcher.id for delivering results with
    // the correct watcher_id that the FE expects.
    watcher_map: HashMap<String, String>,
    // Results that arrived before the FE created a matching watcher.
    pending_results: Vec<proto_api::executor_api_pb::AllocationStreamResponse>,
}

impl AllocationRunner {
    fn new(
        client: FunctionExecutorGrpcClient,
        allocation: ServerAllocation,
        ctx: AllocationContext,
        timeout: Duration,
        cancel_token: CancellationToken,
    ) -> Self {
        let allocation_id = allocation.allocation_id.clone().unwrap_or_default();
        let uri_prefix = allocation
            .request_data_payload_uri_prefix
            .as_deref()
            .unwrap_or("file:///tmp/indexify-blobs")
            .to_string();
        Self {
            client,
            allocation,
            allocation_id,
            ctx,
            timeout,
            cancel_token,
            uri_prefix,
            request_error_blob_uri: None,
            seen_blob_ids: HashSet::new(),
            seen_function_call_ids: HashSet::new(),
            seen_op_ids: HashSet::new(),
            state_handler: RequestStateHandler::new(),
            output_blob_handles: Vec::new(),
            watcher_map: HashMap::new(),
            pending_results: Vec::new(),
        }
    }

    /// Delete allocation from FE and return a failure outcome, taking
    /// accumulated blob handles.
    ///
    /// When `likely_fe_crash` is true, skips the `delete_allocation` gRPC call
    /// (the FE is dead) and checks the process exit status to determine the
    /// accurate termination reason (OOM vs crash). This makes the allocation
    /// runner the single source of truth for both the failure reason and the
    /// termination reason, eliminating races with the health checker.
    async fn fail_with_cleanup(
        &mut self,
        reason: proto_api::executor_api_pb::AllocationFailureReason,
        error_message: impl Into<String>,
        likely_fe_crash: bool,
    ) -> AllocationOutcome {
        let (reason, termination_reason) = if likely_fe_crash {
            self.determine_crash_reason(reason).await
        } else {
            let _ = self.client.delete_allocation(&self.allocation_id).await;
            (reason, None)
        };
        AllocationOutcome::Failed {
            reason,
            error_message: error_message.into(),
            output_blob_handles: std::mem::take(&mut self.output_blob_handles),
            likely_fe_crash,
            termination_reason,
        }
    }

    /// Check the process exit status to determine accurate failure and
    /// termination reasons for a crashed FE.
    async fn determine_crash_reason(
        &self,
        default_reason: proto_api::executor_api_pb::AllocationFailureReason,
    ) -> (
        proto_api::executor_api_pb::AllocationFailureReason,
        Option<proto_api::executor_api_pb::FunctionExecutorTerminationReason>,
    ) {
        let exit_status = self
            .ctx
            .driver
            .get_exit_status(&self.ctx.process_handle)
            .await
            .ok()
            .flatten();
        if exit_status.as_ref().is_some_and(|s| s.oom_killed) {
            (
                proto_api::executor_api_pb::AllocationFailureReason::Oom,
                Some(proto_api::executor_api_pb::FunctionExecutorTerminationReason::Oom),
            )
        } else {
            (
                default_reason,
                Some(proto_api::executor_api_pb::FunctionExecutorTerminationReason::ProcessCrash),
            )
        }
    }

    /// Return a cancelled outcome, taking accumulated blob handles.
    ///
    /// Does NOT call delete_allocation on the FE — the cancel token only
    /// fires when the FE is being terminated (crash, OOM, shutdown), so the
    /// process is dead or about to be killed. Attempting the gRPC call would
    /// hang on a half-open TCP connection to the dead container.
    fn cancel_with_cleanup(&mut self) -> AllocationOutcome {
        AllocationOutcome::Cancelled {
            output_blob_handles: std::mem::take(&mut self.output_blob_handles),
        }
    }

    /// Top-level execution flow: create allocation on FE, open stream, run
    /// reconciliation loop, convert result.
    async fn run(&mut self, prepared: PreparedAllocation) -> AllocationOutcome {
        let start_time = Instant::now();

        let func_ref = self.allocation.function.as_ref();
        info!(
            allocation_id = %self.allocation_id,
            executor_id = %self.ctx.executor_id,
            request_id = ?self.allocation.request_id,
            function_call_id = ?self.allocation.function_call_id,
            namespace = ?func_ref.and_then(|f| f.namespace.as_deref()),
            app = ?func_ref.and_then(|f| f.application_name.as_deref()),
            "fn" = ?func_ref.and_then(|f| f.function_name.as_deref()),
            "Starting allocation execution"
        );

        // Register with the dispatcher to receive FunctionCallResults from the
        // allocation stream.
        let mut result_rx = self
            .ctx
            .allocation_result_dispatcher
            .register(self.allocation_id.clone())
            .await;

        // Store the request error blob URI from the prepared handle so we can
        // use the correct canonical URI during result conversion.
        self.request_error_blob_uri = prepared
            .request_error_blob_handle
            .as_ref()
            .map(|h| h.uri.clone());

        // Create allocation on FE
        let fe_allocation = function_executor_pb::Allocation {
            request_id: self.allocation.request_id.clone(),
            function_call_id: self.allocation.function_call_id.clone(),
            allocation_id: self.allocation.allocation_id.clone(),
            inputs: Some(prepared.inputs),
            result: None,
        };

        let create_request = CreateAllocationRequest {
            allocation: Some(fe_allocation),
        };

        if let Err(e) = self.client.create_allocation(create_request).await {
            error!(error = %e, "Failed to create allocation on FE");
            self.ctx
                .allocation_result_dispatcher
                .deregister(&self.allocation_id)
                .await;
            let (reason, termination_reason) = self
                .determine_crash_reason(
                    proto_api::executor_api_pb::AllocationFailureReason::InternalError,
                )
                .await;
            return AllocationOutcome::Failed {
                reason,
                error_message: e.to_string(),
                output_blob_handles: Vec::new(),
                likely_fe_crash: true,
                termination_reason,
            };
        }

        // Watch allocation state stream
        let mut stream = match self
            .client
            .watch_allocation_state(&self.allocation_id)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to open allocation state stream");
                self.ctx
                    .allocation_result_dispatcher
                    .deregister(&self.allocation_id)
                    .await;
                return self
                    .fail_with_cleanup(
                        proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                        e.to_string(),
                        true,
                    )
                    .await;
            }
        };

        // Reconciliation loop.
        //
        // The loop either:
        // - Sets `final_result` and breaks (allocation completed on FE), or
        // - Sets `error_outcome` and breaks (failure/cancel/timeout).
        let mut deadline = Instant::now() + self.timeout;
        let mut final_result: Option<function_executor_pb::AllocationResult> = None;
        let mut error_outcome: Option<AllocationOutcome> = None;

        loop {
            if self.cancel_token.is_cancelled() {
                info!(allocation_id = %self.allocation_id, "Allocation cancelled");
                error_outcome = Some(self.cancel_with_cleanup());
                break;
            }

            if Instant::now() > deadline {
                warn!("Allocation timed out");
                error_outcome = Some(
                    self.fail_with_cleanup(
                        proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                        "Allocation execution timed out",
                        false,
                    )
                    .await,
                );
                break;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    error_outcome = Some(self.cancel_with_cleanup());
                    break;
                }
                Some(response) = result_rx.recv() => {
                    // FunctionCallResult arrived from the allocation stream.
                    // Deliver it to the FE via send_allocation_update.
                    self.deliver_function_call_result(response).await;
                    // Reset deadline — receiving a result is progress.
                    deadline = Instant::now() + self.timeout;
                }
                message = tokio::time::timeout(remaining, stream.message()) => {
                    match message {
                        Ok(Ok(Some(state))) => {
                            if state.progress.is_some() {
                                deadline = Instant::now() + self.timeout;
                            }

                            self.reconcile(&state).await;

                            // Check for final result
                            if let Some(result) = state.result {
                                let outcome = result.outcome_code();
                                if outcome == function_executor_pb::AllocationOutcomeCode::Failure {
                                    let failure_reason = result.failure_reason();
                                    let has_request_error = result.request_error_output.is_some();
                                    let has_value = result.outputs.is_some();
                                    error!(
                                        outcome = ?result.outcome_code,
                                        failure_reason = ?failure_reason,
                                        has_request_error = has_request_error,
                                        has_value = has_value,
                                        "Allocation FAILED"
                                    );
                                } else {
                                    info!(
                                        allocation_id = %self.allocation_id,
                                        outcome = ?result.outcome_code,
                                        "Allocation completed"
                                    );
                                }
                                final_result = Some(result);
                                break;
                            }
                        }
                        Ok(Ok(None)) => {
                            warn!("Allocation state stream closed prematurely");
                            error_outcome = Some(self
                                .fail_with_cleanup(
                                    proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                                    "Allocation state stream closed without result",
                                    true,
                                )
                                .await);
                            break;
                        }
                        Ok(Err(e)) => {
                            // Only flag as likely FE crash for status codes that suggest
                            // the FE process itself is unhealthy. Transient network
                            // issues (deadline exceeded, cancelled, etc.) should not
                            // kill a potentially healthy FE.
                            let likely_fe_crash = matches!(
                                e.code(),
                                tonic::Code::Unavailable | tonic::Code::Internal | tonic::Code::Unknown
                            );
                            error!(
                                error = %e,
                                code = ?e.code(),
                                likely_fe_crash = likely_fe_crash,
                                "Allocation state stream error"
                            );
                            error_outcome = Some(self
                                .fail_with_cleanup(
                                    proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                                    e.to_string(),
                                    likely_fe_crash,
                                )
                                .await);
                            break;
                        }
                        Err(_) => {
                            warn!("Allocation timed out waiting for state");
                            error_outcome = Some(self
                                .fail_with_cleanup(
                                    proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                                    "Allocation timed out",
                                    false,
                                )
                                .await);
                            break;
                        }
                    }
                }
            }
        }

        // Deregister from the dispatcher.
        self.ctx
            .allocation_result_dispatcher
            .deregister(&self.allocation_id)
            .await;

        // If the allocation completed on the FE, produce the Completed outcome.
        if let Some(fe_result) = final_result {
            let execution_duration_ms = start_time.elapsed().as_millis() as u64;

            if let Err(e) = self.client.delete_allocation(&self.allocation_id).await {
                warn!(error = %e, "Failed to delete allocation from FE");
            }

            let server_result = convert_fe_result_to_server(
                &self.allocation,
                &fe_result,
                execution_duration_ms,
                &self.uri_prefix,
                &self.allocation_id,
                self.request_error_blob_uri.as_deref(),
            );
            return AllocationOutcome::Completed {
                result: server_result,
                fe_result: Some(fe_result),
                output_blob_handles: std::mem::take(&mut self.output_blob_handles),
            };
        }

        // Otherwise return the error/cancel outcome.
        error_outcome.unwrap_or_else(|| AllocationOutcome::Failed {
            reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError,
            error_message: "No result from allocation".to_string(),
            output_blob_handles: std::mem::take(&mut self.output_blob_handles),
            likely_fe_crash: false,
            termination_reason: None,
        })
    }

    /// Deliver a FunctionCallResult from the allocation stream to the FE.
    ///
    /// Converts the server-side `FunctionCallResult` into an FE-side
    /// `AllocationFunctionCallResult` and sends it via
    /// `send_allocation_update`.
    ///
    /// If no matching watcher exists yet (the FE hasn't registered it),
    /// the result is buffered in `pending_results` and delivered later
    /// when the watcher appears during reconciliation.
    async fn deliver_function_call_result(
        &mut self,
        response: proto_api::executor_api_pb::AllocationStreamResponse,
    ) {
        let fc_id = response
            .log_entry
            .as_ref()
            .and_then(|e| e.entry.as_ref())
            .and_then(|entry| match entry {
                proto_api::executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(r) => {
                    r.function_call_id.clone()
                }
                _ => None,
            });

        // Look up watcher_id from the map (keyed by function_call_id which
        // equals the watcher's root_function_call_id for blocking calls).
        let watcher_id = fc_id
            .as_deref()
            .and_then(|id| self.watcher_map.get(id))
            .cloned();

        if watcher_id.is_none() {
            debug!(
                allocation_id = %self.allocation_id,
                function_call_id = ?fc_id,
                "Buffering function call result (no watcher yet)"
            );
            self.pending_results.push(response);
            return;
        }

        self.deliver_function_call_result_inner(response, watcher_id)
            .await;
    }

    /// Actually deliver a result to the FE with the resolved watcher_id.
    async fn deliver_function_call_result_inner(
        &mut self,
        response: proto_api::executor_api_pb::AllocationStreamResponse,
        watcher_id: Option<String>,
    ) {
        let Some(log_entry) = response.log_entry else {
            return;
        };

        let Some(proto_api::executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
            fc_result,
        )) = log_entry.entry
        else {
            return;
        };

        let function_call_id = fc_result.function_call_id.clone();

        // Convert server DataPayloads to FE SerializedObjectInsideBlob + presigned
        // read BLOBs.
        let (value_output, value_blob) = if let Some(ref return_value) = fc_result.return_value {
            let so = proto_convert::data_payload_to_serialized_object_inside_blob(return_value);
            let blob = blob_reconciler::presign_read_only_blob_for_data_payload(
                return_value,
                &self.ctx.blob_store,
            )
            .await
            .ok();
            (Some(so), blob)
        } else {
            (None, None)
        };

        let (request_error_output, request_error_blob) = if let Some(ref request_error) =
            fc_result.request_error
        {
            let so = proto_convert::data_payload_to_serialized_object_inside_blob(request_error);
            let blob = blob_reconciler::presign_read_only_blob_for_data_payload(
                request_error,
                &self.ctx.blob_store,
            )
            .await
            .ok();
            (Some(so), blob)
        } else {
            (None, None)
        };

        let outcome_code = fc_result.outcome_code.map(|code| {
            let server_code = proto_api::executor_api_pb::AllocationOutcomeCode::try_from(code)
                .unwrap_or(proto_api::executor_api_pb::AllocationOutcomeCode::Unknown);
            proto_convert::convert_outcome_code_server_to_fe(server_code) as i32
        });

        let fe_fc_result = function_executor_pb::AllocationFunctionCallResult {
            function_call_id,
            watcher_id,
            outcome_code,
            value_output,
            value_blob,
            request_error_output,
            request_error_blob,
        };

        let update = make_allocation_update(
            &self.allocation_id,
            function_executor_pb::allocation_update::Update::FunctionCallResult(fe_fc_result),
        );

        if let Err(e) = self.client.send_allocation_update(update).await {
            warn!(error = %e, "Failed to deliver function call result to FE");
        }
    }

    /// Scan the FE's function_call_watchers to build the watcher_map and
    /// deliver any buffered results that now have a matching watcher.
    async fn reconcile_watchers(&mut self, state: &AllocationState) {
        // Update watcher_map from FE state.
        for w in &state.function_call_watchers {
            if let (Some(watcher_id), Some(root_fc_id)) = (&w.id, &w.root_function_call_id) {
                self.watcher_map
                    .entry(root_fc_id.clone())
                    .or_insert_with(|| watcher_id.clone());
            }
        }

        // Deliver any buffered results that now have a matching watcher.
        if !self.pending_results.is_empty() {
            let pending = std::mem::take(&mut self.pending_results);
            let mut still_pending = Vec::new();

            for response in pending {
                let fc_id = response
                    .log_entry
                    .as_ref()
                    .and_then(|e| e.entry.as_ref())
                    .and_then(|entry| match entry {
                        proto_api::executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(r) => {
                            r.function_call_id.clone()
                        }
                        _ => None,
                    });

                let watcher_id = fc_id
                    .as_deref()
                    .and_then(|id| self.watcher_map.get(id))
                    .cloned();

                if watcher_id.is_some() {
                    self.deliver_function_call_result_inner(response, watcher_id)
                        .await;
                } else {
                    still_pending.push(response);
                }
            }

            self.pending_results = still_pending;
        }
    }

    /// Delegate reconciliation to the focused modules.
    async fn reconcile(&mut self, state: &AllocationState) {
        blob_reconciler::reconcile_output_blobs(
            &mut self.client,
            &self.allocation_id,
            &self.uri_prefix,
            &self.ctx.blob_store,
            &mut self.seen_blob_ids,
            &mut self.output_blob_handles,
            state,
        )
        .await;

        function_call_reconciler::reconcile_function_calls(
            &mut self.client,
            &self.allocation_id,
            &self.allocation,
            &self.ctx.stream_tx,
            &mut self.seen_function_call_ids,
            &mut self.seen_op_ids,
            &self.ctx.metrics,
            state,
            &self.uri_prefix,
            &self.output_blob_handles,
            &self.ctx.blob_store,
            &self.ctx.executor_id,
        )
        .await;

        // Build watcher map from FE's function_call_watchers and deliver
        // any buffered results that now have a matching watcher.
        self.reconcile_watchers(state).await;

        self.state_handler
            .reconcile(
                state,
                &mut self.client,
                &self.ctx.blob_store,
                &self.allocation_id,
                &self.uri_prefix,
            )
            .await;
    }
}

// ---------------------------------------------------------------------------
// Free functions (shared helpers)
// ---------------------------------------------------------------------------

/// Convert FE AllocationResult to server AllocationResult.
fn convert_fe_result_to_server(
    allocation: &ServerAllocation,
    fe_result: &function_executor_pb::AllocationResult,
    execution_duration_ms: u64,
    uri_prefix: &str,
    allocation_id: &str,
    request_error_blob_uri: Option<&str>,
) -> ServerAllocationResult {
    use proto_api::executor_api_pb;

    use super::proto_convert;

    let outcome_code = proto_convert::convert_outcome_code_fe_to_server(fe_result.outcome_code());
    let failure_reason =
        proto_convert::convert_failure_reason_fe_to_server(fe_result.failure_reason());

    // Reconstruct canonical blob URIs from the blob ID rather than using
    // the presigned upload URLs stored in the blob chunks. Presigned URLs
    // are transient and cannot be used as the canonical data location.
    let output_blob_uri = fe_result
        .uploaded_function_outputs_blob
        .as_ref()
        .and_then(|b| b.id.as_deref())
        .map(|blob_id| format!("{}.{}.output_{}", uri_prefix, allocation_id, blob_id));

    let request_error_blob_uri = request_error_blob_uri.map(|s| s.to_string());

    let request_error = fe_result
        .request_error_output
        .as_ref()
        .map(|so| proto_convert::serialized_object_to_data_payload(so, request_error_blob_uri));

    let return_value = match &fe_result.outputs {
        Some(function_executor_pb::allocation_result::Outputs::Value(so)) => {
            Some(executor_api_pb::allocation_result::ReturnValue::Value(
                proto_convert::serialized_object_to_data_payload(so, output_blob_uri.clone()),
            ))
        }
        Some(function_executor_pb::allocation_result::Outputs::Updates(updates)) => {
            Some(executor_api_pb::allocation_result::ReturnValue::Updates(
                function_call_reconciler::convert_execution_plan_updates(
                    updates,
                    output_blob_uri.as_deref(),
                ),
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
