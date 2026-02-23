//! Allocation lifecycle management for the AllocationController.
//!
//! Handles allocation preparation, scheduling, execution, and finalization.

use std::time::{Duration, Instant};

use anyhow::Result;
use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    AllocationFailureReason,
    AllocationResult as ServerAllocationResult,
};
use tracing::{Instrument, info, warn};

use super::{
    AllocationController,
    events::ACEvent,
    types::{AllocLogCtx, AllocationState, ContainerState, ManagedAllocation},
};
use crate::function_executor::{
    allocation_finalize,
    allocation_prep,
    allocation_runner::{self, AllocationContext},
    controller::timed_phase,
    events::{AllocationOutcome, FinalizationContext, PreparedAllocation},
    proto_convert,
};

impl AllocationController {
    /// Remove Done allocations that the server is no longer sending.
    ///
    /// Called during Reconcile: any allocation in Done state whose ID is NOT
    /// in the current batch has been acknowledged by the server and can be
    /// garbage-collected.
    pub(super) fn cleanup_done_allocations(
        &mut self,
        current_allocation_ids: &std::collections::HashSet<String>,
    ) {
        self.allocations.retain(|alloc_id, alloc| {
            if matches!(alloc.state, AllocationState::Done) {
                // Keep if the server is still sending it (hasn't processed result yet)
                current_allocation_ids.contains(alloc_id)
            } else {
                true // Keep all non-Done allocations
            }
        });
    }

    /// Add new allocations, spawning preparation tasks for each.
    pub(super) fn add_allocations(
        &mut self,
        new_allocations: Vec<(String, ServerAllocation, u64)>,
    ) {
        for (fe_id, allocation, command_seq) in new_allocations {
            let alloc_id = allocation.allocation_id.clone().unwrap_or_default();

            // Skip if already tracked
            if self.allocations.contains_key(&alloc_id) {
                continue;
            }

            self.config.metrics.counters.allocations_fetched.add(1, &[]);

            // If FE doesn't exist or is Terminated, fail immediately
            let fe_exists_and_alive = self
                .containers
                .get(&fe_id)
                .is_some_and(|fe| !matches!(fe.state, ContainerState::Terminated { .. }));

            if !fe_exists_and_alive {
                let lctx = AllocLogCtx::from_allocation(&allocation);
                warn!(
                    namespace = %lctx.namespace,
                    app = %lctx.app,
                    version = %lctx.version,
                    "fn" = %lctx.fn_name,
                    allocation_id = %alloc_id,
                    request_id = %lctx.request_id,
                    container_id = %fe_id,
                    "FE not found or terminated, failing allocation immediately -> Done"
                );
                // Ack the command so the server knows it was received.
                let ack = proto_convert::make_allocation_scheduled_response(&alloc_id, command_seq);
                let _ = self.config.result_tx.send(ack);

                let failure_reason = self
                    .containers
                    .get(&fe_id)
                    .and_then(|fe| match &fe.state {
                        ContainerState::Terminated { reason, .. } => {
                            Some(proto_convert::termination_to_failure_reason(*reason))
                        }
                        _ => None,
                    })
                    .unwrap_or(AllocationFailureReason::ContainerTerminated);
                let outcome = proto_convert::make_allocation_failed_outcome(
                    &allocation,
                    failure_reason,
                    None,
                    None,
                    Some(fe_id.clone()),
                );
                // Send failure via outcome channel — no blobs to clean up.
                proto_convert::record_outcome_metrics(&outcome, &self.config.metrics.counters);
                let _ = self.config.outcome_tx.send(outcome);
                let managed = ManagedAllocation {
                    allocation: allocation.clone(),
                    fe_id: fe_id.clone(),
                    state: AllocationState::Done,
                    created_at: Instant::now(),
                };
                self.allocations.insert(alloc_id.clone(), managed);
                continue;
            }

            // Send AllocationScheduled ack back on the command response channel
            let ack = proto_convert::make_allocation_scheduled_response(&alloc_id, command_seq);
            let _ = self.config.result_tx.send(ack);

            let cancel_token = self.cancel_token.child_token();

            {
                let lctx = AllocLogCtx::from_allocation(&allocation);
                info!(
                    namespace = %lctx.namespace,
                    app = %lctx.app,
                    version = %lctx.version,
                    "fn" = %lctx.fn_name,
                    allocation_id = %alloc_id,
                    request_id = %lctx.request_id,
                    container_id = %fe_id,
                    executor_id = %self.config.executor_id,
                    function_call_id = %lctx.function_call_id,
                    "Allocation added -> Preparing"
                );
            }

            let prep_container_id = fe_id.clone();
            let managed = ManagedAllocation {
                allocation: allocation.clone(),
                fe_id,
                state: AllocationState::Preparing {
                    cancel_token: cancel_token.clone(),
                },
                created_at: Instant::now(),
            };
            self.allocations.insert(alloc_id.clone(), managed);

            self.config
                .metrics
                .counters
                .allocation_preparations
                .add(1, &[]);
            self.config
                .metrics
                .up_down_counters
                .allocations_getting_prepared
                .add(1, &[]);

            // Spawn prep task with panic-safety wrapper
            let event_tx = self.event_tx.clone();
            let blob_store = self.config.blob_store.clone();
            let alloc_id_clone = alloc_id.clone();
            let request_id = allocation.request_id.clone().unwrap_or_default();
            let metrics = self.config.metrics.clone();
            let lctx = AllocLogCtx::from_allocation(&allocation);

            tokio::spawn(async move {
                let result = tokio::spawn(
                    async move {
                        timed_phase(
                            &metrics.histograms.allocation_preparation_latency_seconds,
                            &metrics.up_down_counters.allocations_getting_prepared,
                            Some(&metrics.counters.allocation_preparation_errors),
                            allocation_prep::prepare_allocation(&allocation, &blob_store),
                            |r: &Result<_>| r.is_err(),
                        )
                        .await
                    }
                    .instrument(tracing::info_span!(
                        "allocation_prep",
                        allocation_id = %alloc_id_clone,
                        request_id = %request_id,
                        namespace = %lctx.namespace,
                        app = %lctx.app,
                        "fn" = %lctx.fn_name,
                        container_id = %prep_container_id,
                    )),
                )
                .await;

                let event = match result {
                    Ok(inner_result) => ACEvent::AllocationPrepared {
                        allocation_id: alloc_id_clone,
                        result: inner_result,
                    },
                    Err(join_err) => ACEvent::AllocationPrepared {
                        allocation_id: alloc_id_clone,
                        result: Err(anyhow::anyhow!("Prep task panicked: {}", join_err)),
                    },
                };
                let _ = event_tx.send(event);
            });
        }
    }

    /// Handle preparation completion.
    pub(super) fn handle_allocation_prepared(
        &mut self,
        allocation_id: String,
        result: Result<PreparedAllocation>,
    ) {
        let Some(alloc) = self.allocations.get_mut(&allocation_id) else {
            return; // Already cleaned up
        };

        // Only handle if in Preparing state
        if !matches!(alloc.state, AllocationState::Preparing { .. }) {
            return;
        }

        match result {
            Ok(prepared) => {
                let fe_id = alloc.fe_id.clone();
                let lctx = AllocLogCtx::from_allocation(&alloc.allocation);

                // Check container state
                let container_state = self.containers.get(&fe_id).map(|fe| &fe.state);

                match container_state {
                    Some(ContainerState::Running { .. }) => {
                        info!(
                            namespace = %lctx.namespace,
                            app = %lctx.app,
                            version = %lctx.version,
                            "fn" = %lctx.fn_name,
                            allocation_id = %allocation_id,
                            request_id = %lctx.request_id,
                            container_id = %fe_id,
                            executor_id = %self.config.executor_id,
                            function_call_id = %lctx.function_call_id,
                            "Allocation prepared: Preparing -> WaitingForSlot"
                        );
                        alloc.state = AllocationState::WaitingForSlot;
                        self.config
                            .metrics
                            .up_down_counters
                            .runnable_allocations
                            .add(1, &[]);
                        self.waiting_queue
                            .entry(fe_id)
                            .or_default()
                            .push_back(allocation_id.clone());

                        // Store prepared data + finalization context in a side map.
                        // WaitingForSlot is a simple marker; the data is looked up
                        // at schedule time from prepared_data.
                        let finalization_ctx = FinalizationContext {
                            request_error_blob_handle: prepared.request_error_blob_handle.clone(),
                            output_blob_handles: Vec::new(),
                            fe_result: None,
                        };
                        self.prepared_data
                            .insert(allocation_id.clone(), (prepared, finalization_ctx));

                        self.try_schedule();
                    }
                    Some(ContainerState::Starting) => {
                        info!(
                            namespace = %lctx.namespace,
                            app = %lctx.app,
                            version = %lctx.version,
                            "fn" = %lctx.fn_name,
                            allocation_id = %allocation_id,
                            request_id = %lctx.request_id,
                            container_id = %fe_id,
                            executor_id = %self.config.executor_id,
                            function_call_id = %lctx.function_call_id,
                            "Allocation prepared, container not ready: Preparing -> WaitingForContainer"
                        );
                        alloc.state = AllocationState::WaitingForContainer;
                        self.config
                            .metrics
                            .up_down_counters
                            .runnable_allocations
                            .add(1, &[]);
                        self.waiting_queue
                            .entry(fe_id)
                            .or_default()
                            .push_back(allocation_id.clone());
                        let finalization_ctx = FinalizationContext {
                            request_error_blob_handle: prepared.request_error_blob_handle.clone(),
                            output_blob_handles: Vec::new(),
                            fe_result: None,
                        };
                        self.prepared_data
                            .insert(allocation_id, (prepared, finalization_ctx));
                    }
                    Some(ContainerState::Terminated { reason, .. }) => {
                        warn!(
                            namespace = %lctx.namespace,
                            app = %lctx.app,
                            version = %lctx.version,
                            "fn" = %lctx.fn_name,
                            allocation_id = %allocation_id,
                            request_id = %lctx.request_id,
                            container_id = %fe_id,
                            reason = ?reason,
                            "Allocation prepared but container terminated: Preparing -> Finalizing(failure)"
                        );
                        let failure_reason = proto_convert::termination_to_failure_reason(*reason);
                        let result =
                            proto_convert::make_failure_result(&alloc.allocation, failure_reason);
                        let ctx = FinalizationContext {
                            request_error_blob_handle: prepared.request_error_blob_handle,
                            output_blob_handles: Vec::new(),
                            fe_result: None,
                        };
                        self.start_finalization(&allocation_id, result, ctx, Some(fe_id.clone()));
                    }
                    None => {
                        warn!(
                            namespace = %lctx.namespace,
                            app = %lctx.app,
                            version = %lctx.version,
                            "fn" = %lctx.fn_name,
                            allocation_id = %allocation_id,
                            request_id = %lctx.request_id,
                            container_id = %fe_id,
                            "Allocation prepared but container gone: Preparing -> Finalizing(failure)"
                        );
                        let result = proto_convert::make_failure_result(
                            &alloc.allocation,
                            AllocationFailureReason::ContainerTerminated,
                        );
                        let ctx = FinalizationContext {
                            request_error_blob_handle: prepared.request_error_blob_handle,
                            output_blob_handles: Vec::new(),
                            fe_result: None,
                        };
                        self.start_finalization(&allocation_id, result, ctx, Some(fe_id.clone()));
                    }
                }
            }
            Err(e) => {
                let lctx = AllocLogCtx::from_allocation(&alloc.allocation);
                warn!(
                    namespace = %lctx.namespace,
                    app = %lctx.app,
                    version = %lctx.version,
                    "fn" = %lctx.fn_name,
                    allocation_id = %allocation_id,
                    request_id = %lctx.request_id,
                    container_id = %alloc.fe_id,
                    error = ?e,
                    "Allocation preparation failed: Preparing -> Finalizing(failure)"
                );
                let result = proto_convert::make_failure_result(
                    &alloc.allocation,
                    AllocationFailureReason::InternalError,
                );
                self.start_finalization(
                    &allocation_id,
                    result,
                    FinalizationContext::default(),
                    None,
                );
            }
        }
    }

    /// Try to schedule prepared allocations on containers with free slots.
    pub(super) fn try_schedule(&mut self) {
        let fe_ids: Vec<String> = self.containers.keys().cloned().collect();

        for fe_id in fe_ids {
            let Some(fe) = self.containers.get(&fe_id) else {
                continue;
            };

            // Only schedule on Running containers
            let (client, process_handle) = match &fe.state {
                ContainerState::Running { client, handle, .. } => (client.clone(), handle.clone()),
                _ => continue,
            };
            let max_concurrency = fe.max_concurrency;
            let allocation_timeout_ms = fe.description.allocation_timeout_ms.unwrap_or(300_000);

            let current_running = *self.running_count.get(&fe_id).unwrap_or(&0);
            let mut slots = max_concurrency.saturating_sub(current_running);

            // Drain candidate alloc_ids from the queue first to avoid holding
            // a mutable borrow on self.waiting_queue while touching other fields.
            let mut candidates = Vec::new();
            if let Some(queue) = self.waiting_queue.get_mut(&fe_id) {
                while slots > 0 {
                    match queue.pop_front() {
                        Some(id) => {
                            candidates.push(id);
                            slots -= 1;
                        }
                        None => break,
                    }
                }
            }

            for alloc_id in candidates {
                let Some(alloc) = self.allocations.get_mut(&alloc_id) else {
                    continue; // Cleaned up already
                };

                // Verify state is WaitingForSlot or WaitingForContainer
                match &alloc.state {
                    AllocationState::WaitingForSlot | AllocationState::WaitingForContainer => {}
                    _ => continue, // Stale entry
                }

                // Get prepared data
                let Some((prepared, finalization_ctx)) = self.prepared_data.remove(&alloc_id)
                else {
                    warn!(allocation_id = %alloc_id, "No prepared data for scheduled allocation");
                    let result = proto_convert::make_failure_result(
                        &alloc.allocation,
                        AllocationFailureReason::InternalError,
                    );
                    // Can't call self.start_finalization here directly due to
                    // borrow, so collect for deferred finalization
                    alloc.state = AllocationState::Finalizing {
                        result,
                        terminated_container_id: None,
                    };
                    self.config
                        .metrics
                        .counters
                        .allocation_finalizations
                        .add(1, &[]);
                    self.config
                        .metrics
                        .up_down_counters
                        .allocations_finalizing
                        .add(1, &[]);
                    self.config
                        .metrics
                        .up_down_counters
                        .runnable_allocations
                        .add(-1, &[]);
                    self.spawn_finalization_task(&alloc_id, FinalizationContext::default());
                    continue;
                };

                let cancel_token = self.cancel_token.child_token();

                {
                    let lctx = AllocLogCtx::from_allocation(&alloc.allocation);
                    let running = *self.running_count.get(&fe_id).unwrap_or(&0);
                    info!(
                        namespace = %lctx.namespace,
                        app = %lctx.app,
                        version = %lctx.version,
                        "fn" = %lctx.fn_name,
                        allocation_id = %alloc_id,
                        request_id = %lctx.request_id,
                        container_id = %fe_id,
                        executor_id = %self.config.executor_id,
                        function_call_id = %lctx.function_call_id,
                        running_count = running + 1,
                        max_concurrency = max_concurrency,
                        "Allocation scheduled: {} -> Running", alloc.state
                    );
                }

                // Transition to Running
                alloc.state = AllocationState::Running {
                    cancel_token: cancel_token.clone(),
                    finalization_ctx,
                };

                *self.running_count.entry(fe_id.clone()).or_insert(0) += 1;

                self.config.metrics.counters.allocation_runs.add(1, &[]);
                self.config
                    .metrics
                    .up_down_counters
                    .allocation_runs_in_progress
                    .add(1, &[]);
                self.config
                    .metrics
                    .up_down_counters
                    .runnable_allocations
                    .add(-1, &[]);

                let event_tx = self.event_tx.clone();
                let alloc_id_clone = alloc_id.clone();
                let alloc_id_span = alloc_id.clone();
                let metrics = self.config.metrics.clone();
                let allocation_timeout = Duration::from_millis(allocation_timeout_ms as u64);

                let ctx = AllocationContext {
                    server_channel: self.config.server_channel.clone(),
                    blob_store: self.config.blob_store.clone(),
                    metrics: metrics.clone(),
                    driver: self.config.driver.clone(),
                    process_handle: process_handle.clone(),
                    executor_id: self.config.executor_id.clone(),
                    stream_tx: self.config.activity_tx.clone(),
                    allocation_result_dispatcher: self.allocation_result_dispatcher.clone(),
                };

                let allocation = alloc.allocation.clone();
                let request_id = allocation.request_id.clone().unwrap_or_default();
                let client_clone = (*client).clone();
                let exec_lctx = AllocLogCtx::from_allocation(&allocation);
                let exec_container_id = fe_id.clone();

                tokio::spawn(
                    async move {
                        let result = timed_phase(
                            &metrics.histograms.allocation_run_latency_seconds,
                            &metrics.up_down_counters.allocation_runs_in_progress,
                            None,
                            allocation_runner::execute_allocation(
                                client_clone,
                                allocation,
                                prepared,
                                ctx,
                                allocation_timeout,
                                cancel_token,
                            ),
                            |_: &AllocationOutcome| false,
                        )
                        .await;

                        let _ = event_tx.send(ACEvent::AllocationExecutionFinished {
                            allocation_id: alloc_id_clone,
                            result,
                        });
                    }
                    .instrument(tracing::info_span!(
                        "allocation_exec",
                        allocation_id = %alloc_id_span,
                        request_id = %request_id,
                        namespace = %exec_lctx.namespace,
                        app = %exec_lctx.app,
                        "fn" = %exec_lctx.fn_name,
                        container_id = %exec_container_id,
                    )),
                );
            }
        }
    }

    /// Handle allocation execution completion.
    pub(super) fn handle_allocation_execution_finished(
        &mut self,
        allocation_id: String,
        outcome: AllocationOutcome,
    ) {
        let Some(alloc) = self.allocations.get_mut(&allocation_id) else {
            return;
        };

        // Only handle if in Running state
        if !matches!(alloc.state, AllocationState::Running { .. }) {
            return;
        }

        let fe_id = alloc.fe_id.clone();
        let lctx = AllocLogCtx::from_allocation(&alloc.allocation);

        // Decrement running count
        if let Some(count) = self.running_count.get_mut(&fe_id) {
            *count = count.saturating_sub(1);
        }

        let outcome_label = match &outcome {
            AllocationOutcome::Completed { .. } => "completed",
            AllocationOutcome::Cancelled { .. } => "cancelled",
            AllocationOutcome::Failed {
                likely_fe_crash, ..
            } if *likely_fe_crash => "failed(fe_crash)",
            AllocationOutcome::Failed { .. } => "failed",
        };
        info!(
            namespace = %lctx.namespace,
            app = %lctx.app,
            version = %lctx.version,
            "fn" = %lctx.fn_name,
            allocation_id = %allocation_id,
            request_id = %lctx.request_id,
            container_id = %fe_id,
            executor_id = %self.config.executor_id,
            function_call_id = %lctx.function_call_id,
            outcome = outcome_label,
            elapsed_ms = %alloc.created_at.elapsed().as_millis(),
            "Allocation execution finished: Running -> Finalizing"
        );

        // Take finalization context from Running state
        let finalization_ctx = match std::mem::replace(&mut alloc.state, AllocationState::Done) {
            AllocationState::Running {
                finalization_ctx, ..
            } => finalization_ctx,
            _ => unreachable!(), // We checked above
        };

        let mut ctx = finalization_ctx;
        let mut trigger_fe_termination = false;
        let mut fe_termination_reason =
            proto_api::executor_api_pb::ContainerTerminationReason::Unhealthy;

        if matches!(outcome, AllocationOutcome::Failed { .. }) {
            self.config
                .metrics
                .counters
                .allocation_run_errors
                .add(1, &[]);
        }

        let server_result = match outcome {
            AllocationOutcome::Completed {
                result,
                fe_result,
                output_blob_handles,
            } => {
                ctx.output_blob_handles = output_blob_handles;
                ctx.fe_result = fe_result;
                result
            }
            AllocationOutcome::Cancelled {
                output_blob_handles,
            } => {
                ctx.output_blob_handles = output_blob_handles;

                // Check if we are terminated
                let failure_reason = self
                    .containers
                    .get(&fe_id)
                    .and_then(|fe| match &fe.state {
                        ContainerState::Terminated { reason, .. } => {
                            Some(proto_convert::termination_to_failure_reason(*reason))
                        }
                        _ => None,
                    })
                    .unwrap_or(AllocationFailureReason::AllocationCancelled);

                proto_convert::make_failure_result(&alloc.allocation, failure_reason)
            }
            AllocationOutcome::Failed {
                reason,
                error_message,
                output_blob_handles,
                likely_fe_crash,
                termination_reason,
            } => {
                if likely_fe_crash {
                    warn!(
                        allocation_id = %allocation_id,
                        error = ?error_message,
                        termination_reason = ?termination_reason,
                        "Allocation failed due to likely FE crash"
                    );
                    trigger_fe_termination = true;
                    fe_termination_reason = termination_reason.unwrap_or(
                        proto_api::executor_api_pb::ContainerTerminationReason::Unhealthy,
                    );
                }
                ctx.output_blob_handles = output_blob_handles;

                // If the container is already known to be terminated (e.g. the health
                // checker fired before this outcome was processed), prefer the container's
                // known termination reason over the runner's reason.  This handles the race
                // where determine_crash_reason() inspects Docker before the OOMKilled flag
                // is set, but the health checker subsequently detects OOM and updates
                // container state first.
                let effective_reason = self
                    .containers
                    .get(&fe_id)
                    .and_then(|fe| match &fe.state {
                        ContainerState::Terminated { reason, .. } => {
                            Some(proto_convert::termination_to_failure_reason(*reason))
                        }
                        _ => None,
                    })
                    .unwrap_or(reason);

                proto_convert::make_failure_result(&alloc.allocation, effective_reason)
            }
        };

        self.start_finalization(&allocation_id, server_result, ctx, None);

        if trigger_fe_termination {
            let _ = self.event_tx.send(ACEvent::ContainerTerminated {
                fe_id,
                reason: fe_termination_reason,
                blamed_allocation_id: Some(allocation_id.clone()),
            });
        }

        // Free a slot — schedule next
        self.try_schedule();
    }

    /// Handle allocation finalization completion.
    pub(super) fn handle_allocation_finalization_finished(
        &mut self,
        allocation_id: String,
        is_success: bool,
    ) {
        let Some(alloc) = self.allocations.get_mut(&allocation_id) else {
            return;
        };

        // Only handle if in Finalizing state
        let (server_result, terminated_container_id) =
            match std::mem::replace(&mut alloc.state, AllocationState::Done) {
                AllocationState::Finalizing {
                    result,
                    terminated_container_id,
                } => (result, terminated_container_id),
                other => {
                    alloc.state = other;
                    return;
                }
            };

        let result = if !is_success {
            let mut err_result = proto_convert::make_failure_result(
                &alloc.allocation,
                AllocationFailureReason::InternalError,
            );
            err_result.execution_duration_ms = server_result.execution_duration_ms;
            err_result
        } else {
            server_result
        };

        {
            let lctx = AllocLogCtx::from_allocation(&alloc.allocation);
            info!(
                namespace = %lctx.namespace,
                app = %lctx.app,
                version = %lctx.version,
                "fn" = %lctx.fn_name,
                allocation_id = %allocation_id,
                request_id = %lctx.request_id,
                container_id = %alloc.fe_id,
                executor_id = %self.config.executor_id,
                function_call_id = %lctx.function_call_id,
                success = is_success,
                total_elapsed_ms = %alloc.created_at.elapsed().as_millis(),
                "Allocation finalized: Finalizing -> Done (result sent)"
            );
        }

        // Record metrics and send result via outcome channel (guaranteed delivery)
        let outcome = proto_convert::allocation_result_to_outcome(&result, terminated_container_id);
        proto_convert::record_outcome_metrics(&outcome, &self.config.metrics.counters);
        let _ = self.config.outcome_tx.send(outcome);

        // Keep the allocation in Done state — do NOT remove it.
        // The server may re-send this allocation before it processes the
        // result. Keeping it in the map lets add_allocations() skip it
        // via contains_key(). Done allocations are cleaned up in
        // cleanup_done_allocations() when the server stops sending
        // them.
    }

    /// Start finalization for an allocation.
    ///
    /// `terminated_container_id` should be `Some(fe_id)` when the container is
    /// dead (startup failure, runtime crash). It will be included in the
    /// `AllocationFailed` message so the scheduler skips this container.
    pub(super) fn start_finalization(
        &mut self,
        allocation_id: &str,
        server_result: ServerAllocationResult,
        ctx: FinalizationContext,
        terminated_container_id: Option<String>,
    ) {
        let Some(alloc) = self.allocations.get_mut(allocation_id) else {
            return;
        };

        self.config
            .metrics
            .counters
            .allocation_finalizations
            .add(1, &[]);
        self.config
            .metrics
            .up_down_counters
            .allocations_finalizing
            .add(1, &[]);

        alloc.state = AllocationState::Finalizing {
            result: server_result,
            terminated_container_id,
        };

        self.spawn_finalization_task(allocation_id, ctx);
    }

    /// Spawn the finalization tokio task. Separated from start_finalization
    /// to allow calling from contexts where the allocation state has already
    /// been set.
    fn spawn_finalization_task(&self, allocation_id: &str, ctx: FinalizationContext) {
        let event_tx = self.event_tx.clone();
        let blob_store = self.config.blob_store.clone();
        let alloc_id = allocation_id.to_string();
        let alloc_id_span = allocation_id.to_string();
        let alloc_for_ctx = self.allocations.get(allocation_id);
        let request_id = alloc_for_ctx
            .and_then(|a| a.allocation.request_id.clone())
            .unwrap_or_default();
        let fin_lctx = alloc_for_ctx
            .map(|a| AllocLogCtx::from_allocation(&a.allocation))
            .unwrap_or(AllocLogCtx {
                namespace: String::new(),
                app: String::new(),
                version: String::new(),
                fn_name: String::new(),
                request_id: String::new(),
                function_call_id: String::new(),
            });
        let fin_container_id = alloc_for_ctx.map(|a| a.fe_id.clone()).unwrap_or_default();
        let metrics = self.config.metrics.clone();

        tokio::spawn(
            async move {
                let is_success = timed_phase(
                    &metrics.histograms.allocation_finalization_latency_seconds,
                    &metrics.up_down_counters.allocations_finalizing,
                    Some(&metrics.counters.allocation_finalization_errors),
                    async {
                        allocation_finalize::finalize_allocation(
                            &alloc_id,
                            ctx.fe_result.as_ref(),
                            ctx.request_error_blob_handle.as_ref(),
                            &ctx.output_blob_handles,
                            &blob_store,
                        )
                        .await
                        .is_ok()
                    },
                    |success: &bool| !success,
                )
                .await;

                let _ = event_tx.send(ACEvent::AllocationFinalizationFinished {
                    allocation_id: alloc_id,
                    is_success,
                });
            }
            .instrument(tracing::info_span!(
                "allocation_finalize",
                allocation_id = %alloc_id_span,
                request_id = %request_id,
                namespace = %fin_lctx.namespace,
                app = %fin_lctx.app,
                "fn" = %fin_lctx.fn_name,
                container_id = %fin_container_id,
            )),
        );
    }

    /// Shut down all containers and allocations.
    pub(super) async fn shutdown_all(&mut self) {
        info!("Shutting down all containers and allocations");

        // Cancel all health checkers
        for fe in self.containers.values() {
            if let ContainerState::Running {
                health_checker_cancel,
                ..
            } = &fe.state
            {
                health_checker_cancel.cancel();
            }
        }

        // Cancel all running allocation tokens
        for alloc in self.allocations.values() {
            match &alloc.state {
                AllocationState::Preparing { cancel_token } |
                AllocationState::Running { cancel_token, .. } => {
                    cancel_token.cancel();
                }
                _ => {}
            }
        }

        // Fail all non-terminal allocations
        let alloc_ids: Vec<String> = self.allocations.keys().cloned().collect();

        for alloc_id in alloc_ids {
            let Some(alloc) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };

            match &alloc.state {
                AllocationState::Finalizing { .. } | AllocationState::Done => continue,
                _ => {}
            }

            // Extract finalization context: check prepared_data first (for
            // WaitingForSlot/WaitingForContainer), then fall back to the state
            // (for Running which carries its own ctx).
            let ctx = if let Some((_, finalization_ctx)) = self.prepared_data.remove(&alloc_id) {
                finalization_ctx
            } else {
                match std::mem::replace(&mut alloc.state, AllocationState::Done) {
                    AllocationState::Running {
                        finalization_ctx, ..
                    } => finalization_ctx,
                    _ => FinalizationContext::default(),
                }
            };

            let result = proto_convert::make_failure_result(
                &alloc.allocation,
                AllocationFailureReason::AllocationCancelled,
            );
            self.start_finalization(&alloc_id, result, ctx, None);
        }

        // Release GPUs but do NOT kill containers — they will be
        // adopted on next startup via the state file.
        let gpu_allocator = self.config.gpu_allocator.clone();
        for fe in self.containers.values_mut() {
            Self::return_gpus(&gpu_allocator, &mut fe.allocated_gpu_uuids);
        }

        // Wait for in-flight finalizations to complete (with timeout)
        let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self
            .allocations
            .values()
            .any(|a| matches!(a.state, AllocationState::Finalizing { .. }))
        {
            tokio::select! {
                _ = tokio::time::sleep_until(drain_deadline) => {
                    warn!("Timed out waiting for finalizations during shutdown");
                    break;
                }
                Some(event) = self.event_rx.recv() => {
                    if let ACEvent::AllocationFinalizationFinished { allocation_id, is_success } = event {
                        self.handle_allocation_finalization_finished(allocation_id, is_success);
                    }
                }
            }
        }

        // Send any remaining results as AllocationActivities
        for (_alloc_id, alloc) in self.allocations.drain() {
            if let AllocationState::Finalizing {
                result,
                terminated_container_id,
            } = alloc.state
            {
                // During shutdown all containers are dying, so include
                // container_id if it wasn't already set.
                let container_id = terminated_container_id.or_else(|| Some(alloc.fe_id.clone()));
                let outcome = proto_convert::allocation_result_to_outcome(&result, container_id);
                proto_convert::record_outcome_metrics(&outcome, &self.config.metrics.counters);
                let _ = self.config.outcome_tx.send(outcome);
            }
        }

        self.config
            .metrics
            .up_down_counters
            .containers_count
            .add(-(self.containers.len() as i64), &[]);
    }
}
