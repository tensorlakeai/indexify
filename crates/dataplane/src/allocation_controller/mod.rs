//! Unified AllocationController for function executor lifecycle management.
//!
//! Replaces the per-FE `FunctionExecutorController` model with a single
//! controller that manages all function executor containers and their
//! allocations through one `select!` loop.
//!
//! **"One brain, many hands"**: The controller is the single sequential
//! decision-maker. Tokio tasks are parallel workers that report back via
//! a shared `mpsc` channel.

mod allocation_lifecycle;
mod container_lifecycle;
pub mod events;
mod types;

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use proto_api::executor_api_pb::{
    AllocationFailureReason,
    FunctionExecutorState,
    FunctionExecutorStatus,
};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use self::{
    events::{ACCommand, ACEvent},
    types::{AllocLogCtx, AllocationState, ContainerState, ManagedAllocation, ManagedFE},
};
use crate::{
    allocation_result_dispatcher::AllocationResultDispatcher,
    function_executor::{controller::FESpawnConfig, events::FinalizationContext, proto_convert},
};

/// Handle returned to service.rs / StateReconciler for communicating with the
/// controller.
pub struct AllocationControllerHandle {
    pub command_tx: mpsc::UnboundedSender<ACCommand>,
    pub state_rx: watch::Receiver<Vec<FunctionExecutorState>>,
}

/// Unified controller managing all function executor containers and
/// allocations.
pub struct AllocationController {
    // -- Owned state --
    containers: HashMap<String, ManagedFE>,
    allocations: HashMap<String, ManagedAllocation>,

    // Scheduling: allocations waiting per FE + running count per FE
    waiting_queue: HashMap<String, VecDeque<String>>,
    running_count: HashMap<String, u32>,

    // Prepared data: stored between preparation and scheduling phases.
    // Keyed by allocation_id.
    prepared_data: HashMap<
        String,
        (
            crate::function_executor::events::PreparedAllocation,
            FinalizationContext,
        ),
    >,

    // -- Channels --
    command_rx: mpsc::UnboundedReceiver<ACCommand>,
    event_tx: mpsc::UnboundedSender<ACEvent>,
    event_rx: mpsc::UnboundedReceiver<ACEvent>,

    // -- Shared dependencies --
    config: FESpawnConfig,
    state_tx: watch::Sender<Vec<FunctionExecutorState>>,
    state_change_notify: Arc<Notify>,
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,

    // -- Shutdown --
    cancel_token: CancellationToken,
}

impl AllocationController {
    /// Spawn the controller as a tokio task. Returns a handle for
    /// communication.
    pub fn spawn(
        config: FESpawnConfig,
        cancel_token: CancellationToken,
        state_change_notify: Arc<Notify>,
        allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
    ) -> AllocationControllerHandle {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = watch::channel(Vec::new());
        let controller = Self {
            containers: HashMap::new(),
            allocations: HashMap::new(),
            waiting_queue: HashMap::new(),
            running_count: HashMap::new(),
            prepared_data: HashMap::new(),
            command_rx,
            event_tx,
            event_rx,
            config,
            state_tx,
            state_change_notify,
            allocation_result_dispatcher,
            cancel_token,
        };

        let executor_id = controller.config.executor_id.clone();
        tokio::spawn(async move {
            controller
                .run()
                .instrument(tracing::info_span!(
                    "allocation_controller",
                    executor_id = %executor_id,
                ))
                .await;
        });

        AllocationControllerHandle {
            command_tx,
            state_rx,
        }
    }

    /// Main event loop.
    async fn run(mut self) {
        info!("AllocationController starting");
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("AllocationController cancelled, shutting down");
                    self.shutdown_all().await;
                    break;
                }
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ACCommand::Reconcile { added_or_updated_fes, removed_fe_ids, new_allocations } => {
                            // Garbage-collect Done allocations the server no longer sends.
                            // Only run when we have allocation data — without
                            // it the set would be empty and we'd incorrectly
                            // remove ALL Done allocations.
                            if !new_allocations.is_empty() {
                                let current_ids: std::collections::HashSet<String> = new_allocations
                                    .iter()
                                    .map(|(_, a, _)| a.allocation_id.clone().unwrap_or_default())
                                    .collect();
                                self.cleanup_done_allocations(&current_ids);
                            }

                            self.reconcile_containers(added_or_updated_fes, removed_fe_ids).await;
                            self.add_allocations(new_allocations);
                            self.try_schedule();
                        }
                        ACCommand::Shutdown => {
                            info!("AllocationController shutdown requested");
                            self.shutdown_all().await;
                            break;
                        }
                    }
                }
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
        info!("AllocationController stopped");
    }

    /// Dispatch events to the appropriate handler.
    async fn handle_event(&mut self, event: ACEvent) {
        match event {
            ACEvent::ContainerStartupComplete { fe_id, result } => {
                self.handle_container_startup_complete(fe_id, result).await;
            }
            ACEvent::ContainerTerminated {
                fe_id,
                reason,
                blamed_allocation_id,
            } => {
                self.handle_container_terminated(fe_id, reason, blamed_allocation_id);
            }
            ACEvent::AllocationPrepared {
                allocation_id,
                result,
            } => {
                self.handle_allocation_prepared(allocation_id, result);
            }
            ACEvent::AllocationExecutionFinished {
                allocation_id,
                result,
            } => {
                self.handle_allocation_execution_finished(allocation_id, result);
            }
            ACEvent::AllocationFinalizationFinished {
                allocation_id,
                is_success,
            } => {
                self.handle_allocation_finalization_finished(allocation_id, is_success);
            }
        }
    }

    /// Broadcast container states via state_tx for heartbeat reporting.
    fn broadcast_state(&self) {
        let states: Vec<FunctionExecutorState> = self
            .containers
            .values()
            .map(|fe| {
                let (status, termination_reason) = match &fe.state {
                    ContainerState::Starting => (FunctionExecutorStatus::Pending, None),
                    ContainerState::Running { .. } => (FunctionExecutorStatus::Running, None),
                    ContainerState::Terminated { reason } => {
                        (FunctionExecutorStatus::Terminated, Some(*reason))
                    }
                };
                FunctionExecutorState {
                    description: Some(fe.description.clone()),
                    status: Some(status.into()),
                    termination_reason: termination_reason.map(|r| r.into()),
                }
            })
            .collect();
        let _ = self.state_tx.send(states);
        self.state_change_notify.notify_one();
    }

    /// Return GPUs to the allocator pool.
    fn return_gpus(
        gpu_allocator: &crate::gpu_allocator::GpuAllocator,
        gpu_uuids: &mut Vec<String>,
    ) {
        if !gpu_uuids.is_empty() {
            gpu_allocator.deallocate(&std::mem::take(gpu_uuids));
        }
    }

    /// Fire-and-forget kill a process handle.
    fn kill_process_fire_and_forget(&self, handle: ProcessHandle) {
        let driver = self.config.driver.clone();
        tokio::spawn(async move {
            let _ = driver.kill(&handle).await;
        });
    }

    /// Fail all non-terminal allocations for a given FE.
    ///
    /// `blamed_alloc_ids` identifies allocations that caused the container
    /// termination (e.g. a Running allocation that OOM'd). Blamed allocations
    /// get the specific `reason`; non-blamed allocations get
    /// `FunctionExecutorTerminated` so the server gives them a free retry.
    fn fail_allocations_for_fe(
        &mut self,
        fe_id: &str,
        reason: AllocationFailureReason,
        blamed_alloc_ids: &[String],
    ) {
        // Collect allocation IDs for this FE
        let alloc_ids: Vec<String> = self
            .allocations
            .iter()
            .filter(|(_, alloc)| alloc.fe_id == fe_id)
            .filter(|(_, alloc)| {
                !matches!(
                    alloc.state,
                    AllocationState::Done | AllocationState::Finalizing { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect();

        // Remove from waiting queue
        self.waiting_queue.remove(fe_id);

        if !alloc_ids.is_empty() {
            warn!(
                container_id = %fe_id,
                reason = ?reason,
                blamed = ?blamed_alloc_ids,
                count = alloc_ids.len(),
                "Failing {} allocations for terminated container", alloc_ids.len()
            );
        }

        for alloc_id in alloc_ids {
            let Some(alloc) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };

            // Blamed allocations get the specific failure reason (e.g. OOM);
            // non-blamed get FunctionExecutorTerminated (free retry on server).
            let alloc_reason =
                if blamed_alloc_ids.is_empty() || blamed_alloc_ids.contains(&alloc_id) {
                    reason
                } else {
                    AllocationFailureReason::FunctionExecutorTerminated
                };

            {
                let lctx = AllocLogCtx::from_allocation(&alloc.allocation);
                warn!(
                    namespace = %lctx.namespace,
                    app = %lctx.app,
                    version = %lctx.version,
                    fn_name = %lctx.fn_name,
                    allocation_id = %alloc_id,
                    request_id = %lctx.request_id,
                    container_id = %fe_id,
                    from_state = %alloc.state,
                    reason = ?alloc_reason,
                    blamed = blamed_alloc_ids.contains(&alloc_id),
                    "Failing allocation: {} -> Finalizing(failure)", alloc.state
                );
            }

            // Cancel any running tasks.
            // Running allocations are NOT failed here — their allocation
            // runners detect the gRPC stream break and return the accurate
            // failure reason (e.g. FunctionError). Failing them here would
            // race with the runner and could override its more specific reason
            // with a generic one derived from the FE termination reason.
            // We cancel the token as a safety net for half-open connections.
            match &alloc.state {
                AllocationState::Running { cancel_token, .. } => {
                    cancel_token.cancel();
                    continue;
                }
                AllocationState::Preparing { cancel_token } => {
                    cancel_token.cancel();
                }
                _ => {}
            }

            // Take finalization context: first check prepared_data side map,
            // then fall back to the state itself.
            let ctx = if let Some((_, finalization_ctx)) = self.prepared_data.remove(&alloc_id) {
                finalization_ctx
            } else {
                match std::mem::replace(&mut alloc.state, AllocationState::Done) {
                    AllocationState::Running {
                        finalization_ctx, ..
                    } => finalization_ctx,
                    AllocationState::Preparing { .. } |
                    AllocationState::WaitingForContainer |
                    AllocationState::WaitingForSlot => FinalizationContext::default(),
                    other => {
                        alloc.state = other;
                        continue;
                    }
                }
            };

            let result = proto_convert::make_failure_result(&alloc.allocation, alloc_reason);

            // If finalization context has no blobs to clean up, send result
            // directly to avoid the latency of spawning a finalization task.
            if ctx.request_error_blob_handle.is_none() &&
                ctx.output_blob_handles.is_empty() &&
                ctx.fe_result.is_none()
            {
                let activity = proto_convert::allocation_result_to_stream_request(&result);
                proto_convert::record_activity_metrics(&activity, &self.config.metrics.counters);
                let _ = self.config.activity_tx.send(activity);
                alloc.state = AllocationState::Done;
            } else {
                self.start_finalization(&alloc_id, result, ctx);
            }
        }
    }
}

// Pull in the Instrument trait for tracing spans on futures.
use tracing::Instrument;

use crate::driver::ProcessHandle;
