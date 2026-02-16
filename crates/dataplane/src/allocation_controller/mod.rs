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
use crate::function_executor::{
    controller::FESpawnConfig,
    events::FinalizationContext,
    proto_convert,
    watcher_registry::WatcherRegistry,
};

/// Handle returned to service.rs / StateReconciler for communicating with the
/// controller.
pub struct AllocationControllerHandle {
    pub command_tx: mpsc::UnboundedSender<ACCommand>,
    pub state_rx: watch::Receiver<Vec<FunctionExecutorState>>,
    pub watcher_registry: WatcherRegistry,
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
    watcher_registry: WatcherRegistry,
    state_tx: watch::Sender<Vec<FunctionExecutorState>>,
    state_change_notify: Arc<Notify>,

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
    ) -> AllocationControllerHandle {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (state_tx, state_rx) = watch::channel(Vec::new());
        let watcher_registry = WatcherRegistry::new();

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
            watcher_registry: watcher_registry.clone(),
            state_tx,
            state_change_notify,
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
            watcher_registry,
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
                        ACCommand::Reconcile { desired_fes, new_allocations } => {
                            // Garbage-collect Done allocations the server no longer sends.
                            let current_ids: std::collections::HashSet<String> = new_allocations
                                .iter()
                                .map(|(_, a)| a.allocation_id.clone().unwrap_or_default())
                                .collect();
                            self.cleanup_done_allocations(&current_ids);

                            self.reconcile_containers(desired_fes).await;
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
                let (status, termination_reason, blamed_alloc_ids) = match &fe.state {
                    ContainerState::Starting => (FunctionExecutorStatus::Pending, None, vec![]),
                    ContainerState::Running { .. } => {
                        (FunctionExecutorStatus::Running, None, vec![])
                    }
                    ContainerState::Terminated {
                        reason,
                        blamed_alloc_ids,
                    } => (
                        FunctionExecutorStatus::Terminated,
                        Some(*reason),
                        blamed_alloc_ids.clone(),
                    ),
                };
                FunctionExecutorState {
                    description: Some(fe.description.clone()),
                    status: Some(status.into()),
                    termination_reason: termination_reason.map(|r| r.into()),
                    allocation_ids_caused_termination: blamed_alloc_ids,
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

            // Cancel any running tasks
            match &alloc.state {
                AllocationState::Preparing { cancel_token } => {
                    cancel_token.cancel();
                }
                AllocationState::Running { cancel_token, .. } => {
                    cancel_token.cancel();
                    // Decrement running count
                    if let Some(count) = self.running_count.get_mut(fe_id) {
                        *count = count.saturating_sub(1);
                    }
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
                crate::function_executor::controller::record_allocation_metrics(
                    &result,
                    &self.config.metrics.counters,
                );
                let _ = self.config.result_tx.send(result);
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
