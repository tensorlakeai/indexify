//! Core allocation execution protocol.
//!
//! Executes an allocation on a function executor subprocess. This is the
//! "Running" phase of the 3-phase lifecycle (Preparing → Running → Finalizing).
//!
//! Prep and finalization are handled separately by the controller so that
//! only execution occupies a concurrency slot.
//!
//! Reconciliation of output blobs, function calls, and watchers is delegated
//! to the `blob_reconciler`, `function_call_reconciler`, and
//! `watcher_reconciler` modules respectively.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use proto_api::{
    executor_api_pb::{Allocation as ServerAllocation, AllocationResult as ServerAllocationResult},
    function_executor_pb::{self, AllocationState, AllocationUpdate, CreateAllocationRequest},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use super::{
    blob_reconciler,
    events::{AllocationOutcome, PreparedAllocation},
    fe_client::FunctionExecutorGrpcClient,
    function_call_reconciler,
    state_ops::RequestStateHandler,
    watcher_reconciler,
    watcher_registry::{WatcherRegistry, WatcherResult},
};
use crate::blob_ops::{BlobStore, MultipartUploadHandle};

/// Shared infrastructure context for allocation execution.
///
/// Bundles the dependencies that are common across the allocation runner,
/// reconcilers, and related helpers — reducing parameter counts.
pub struct AllocationContext {
    pub server_channel: Channel,
    pub blob_store: Arc<BlobStore>,
    pub watcher_registry: WatcherRegistry,
    pub metrics: Arc<crate::metrics::DataplaneMetrics>,
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
    // Reconciliation state
    seen_blob_ids: HashSet<String>,
    seen_function_call_ids: HashSet<String>,
    seen_watcher_ids: HashSet<String>,
    state_handler: RequestStateHandler,
    output_blob_handles: Vec<MultipartUploadHandle>,
    has_active_watchers: bool,
    // Channel for watcher results delivered by the registry
    watcher_result_tx: mpsc::UnboundedSender<WatcherResult>,
    watcher_result_rx: mpsc::UnboundedReceiver<WatcherResult>,
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
        let (watcher_result_tx, watcher_result_rx) = mpsc::unbounded_channel();

        Self {
            client,
            allocation,
            allocation_id,
            ctx,
            timeout,
            cancel_token,
            uri_prefix,
            seen_blob_ids: HashSet::new(),
            seen_function_call_ids: HashSet::new(),
            seen_watcher_ids: HashSet::new(),
            state_handler: RequestStateHandler::new(),
            output_blob_handles: Vec::new(),
            has_active_watchers: false,
            watcher_result_tx,
            watcher_result_rx,
        }
    }

    /// Delete allocation from FE and return a failure outcome, taking
    /// accumulated blob handles.
    async fn fail_with_cleanup(
        &mut self,
        reason: proto_api::executor_api_pb::AllocationFailureReason,
        error_message: impl Into<String>,
        likely_fe_crash: bool,
    ) -> AllocationOutcome {
        let _ = self.client.delete_allocation(&self.allocation_id).await;
        AllocationOutcome::Failed {
            reason,
            error_message: error_message.into(),
            output_blob_handles: std::mem::take(&mut self.output_blob_handles),
            likely_fe_crash,
        }
    }

    /// Delete allocation from FE and return a cancelled outcome, taking
    /// accumulated blob handles.
    async fn cancel_with_cleanup(&mut self) -> AllocationOutcome {
        let _ = self.client.delete_allocation(&self.allocation_id).await;
        AllocationOutcome::Cancelled {
            output_blob_handles: std::mem::take(&mut self.output_blob_handles),
        }
    }

    /// Top-level execution flow: create allocation on FE, open stream, run
    /// reconciliation loop, convert result.
    async fn run(&mut self, prepared: PreparedAllocation) -> AllocationOutcome {
        let start_time = Instant::now();

        info!("Starting allocation execution");

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
            return AllocationOutcome::Failed {
                reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError,
                error_message: e.to_string(),
                output_blob_handles: Vec::new(),
                likely_fe_crash: true,
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
                return self
                    .fail_with_cleanup(
                        proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                        e.to_string(),
                        true,
                    )
                    .await;
            }
        };

        // Reconciliation loop
        let mut deadline = Instant::now() + self.timeout;
        #[allow(unused_assignments)]
        let mut final_result: Option<function_executor_pb::AllocationResult> = None;

        loop {
            if self.cancel_token.is_cancelled() {
                info!("Allocation cancelled");
                return self.cancel_with_cleanup().await;
            }

            // Don't enforce the deadline while waiting for child functions.
            // Child functions have their own timeouts — the parent should wait
            // for them to complete (matching the Python executor behavior).
            if Instant::now() > deadline && !self.has_active_watchers {
                warn!("Allocation timed out");
                return self
                    .fail_with_cleanup(
                        proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                        "Allocation execution timed out",
                        false,
                    )
                    .await;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let message = tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    return self.cancel_with_cleanup().await;
                }
                Some(watcher_result) = self.watcher_result_rx.recv() => {
                    // Deliver watcher result to FE inline
                    watcher_reconciler::deliver_function_call_result_to_fe(
                        &mut self.client,
                        &self.allocation_id,
                        &watcher_result.watcher_id,
                        &watcher_result.watched_function_call_id,
                        watcher_result.fc_result,
                        &self.ctx.blob_store,
                    )
                    .await;
                    // Reset deadline so the parent function has time to finish
                    // processing after the child result is delivered.
                    deadline = Instant::now() + self.timeout;
                    continue;
                }
                result = tokio::time::timeout(remaining, stream.message()) => result,
            };

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
                    return self
                        .fail_with_cleanup(
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                            "Allocation state stream closed without result",
                            true,
                        )
                        .await;
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
                    return self
                        .fail_with_cleanup(
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionError,
                            e.to_string(),
                            likely_fe_crash,
                        )
                        .await;
                }
                Err(_) => {
                    if self.has_active_watchers {
                        continue;
                    }
                    warn!("Allocation timed out waiting for state");
                    return self
                        .fail_with_cleanup(
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout,
                            "Allocation timed out",
                            false,
                        )
                        .await;
                }
            }
        }

        let execution_duration_ms = start_time.elapsed().as_millis() as u64;

        if let Err(e) = self.client.delete_allocation(&self.allocation_id).await {
            warn!(error = %e, "Failed to delete allocation from FE");
        }

        match &final_result {
            Some(fe_result) => {
                let server_result =
                    convert_fe_result_to_server(&self.allocation, fe_result, execution_duration_ms);
                AllocationOutcome::Completed {
                    result: server_result,
                    fe_result: final_result,
                    output_blob_handles: std::mem::take(&mut self.output_blob_handles),
                }
            }
            None => AllocationOutcome::Failed {
                reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError,
                error_message: "No result from allocation".to_string(),
                output_blob_handles: std::mem::take(&mut self.output_blob_handles),
                likely_fe_crash: false,
            },
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
            &self.ctx.server_channel,
            &mut self.seen_function_call_ids,
            &self.ctx.metrics,
            state,
        )
        .await;

        watcher_reconciler::reconcile_watchers(
            &self.allocation,
            &self.ctx.watcher_registry,
            &mut self.seen_watcher_ids,
            &mut self.has_active_watchers,
            &self.watcher_result_tx,
            state,
        )
        .await;

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
) -> ServerAllocationResult {
    use proto_api::executor_api_pb;

    use super::proto_convert;

    let outcome_code = proto_convert::convert_outcome_code_fe_to_server(fe_result.outcome_code());
    let failure_reason =
        proto_convert::convert_failure_reason_fe_to_server(fe_result.failure_reason());

    let output_blob_uri = fe_result
        .uploaded_function_outputs_blob
        .as_ref()
        .and_then(|b| b.chunks.first())
        .and_then(|c| c.uri.clone());

    let request_error_blob_uri = fe_result
        .uploaded_request_error_blob
        .as_ref()
        .and_then(|b| b.chunks.first())
        .and_then(|c| c.uri.clone());

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
