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
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Instant,
};

use proto_api::executor_api_pb::{
    AllocationFailureReason,
    ContainerState as ProtoContainerState,
    ContainerStatus,
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
    state_file::StateFile,
};

/// Handle returned to service.rs / StateReconciler for communicating with the
/// controller.
pub struct AllocationControllerHandle {
    pub command_tx: mpsc::UnboundedSender<ACCommand>,
    pub state_rx: watch::Receiver<Vec<ProtoContainerState>>,
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
    state_tx: watch::Sender<Vec<ProtoContainerState>>,
    state_change_notify: Arc<Notify>,
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,

    // -- State persistence --
    state_file: Arc<StateFile>,

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
        let state_file = config.state_file.clone();
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
            state_file,
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
                        ACCommand::Recover { reply } => {
                            info!("AllocationController recovery requested");
                            let recovered_handles = self.recover_containers().await;
                            let _ = reply.send(recovered_handles);
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
        let states: Vec<ProtoContainerState> = self
            .containers
            .values()
            .map(|fe| {
                let (status, termination_reason) = match &fe.state {
                    ContainerState::Starting => (ContainerStatus::Pending, None),
                    ContainerState::Running { .. } => (ContainerStatus::Running, None),
                    ContainerState::Terminated { reason } => {
                        (ContainerStatus::Terminated, Some(*reason))
                    }
                };
                ProtoContainerState {
                    description: Some(fe.description.clone()),
                    status: Some(status.into()),
                    termination_reason: termination_reason.map(|r| r.into()),
                }
            })
            .collect();
        let _ = self.state_tx.send(states);
        self.state_change_notify.notify_one();

        // Update FE state counts for the observable gauge
        let (mut starting, mut running, mut terminated) = (0u64, 0u64, 0u64);
        for fe in self.containers.values() {
            match &fe.state {
                ContainerState::Starting => starting += 1,
                ContainerState::Running { .. } => running += 1,
                ContainerState::Terminated { .. } => terminated += 1,
            }
        }
        let metrics = self.config.metrics.clone();
        tokio::spawn(async move {
            metrics
                .update_fe_state_counts(starting, running, terminated)
                .await;
        });
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

    /// Recover containers from state file entries.
    ///
    /// For each persisted container:
    /// 1. Check if process is alive via driver
    /// 2. Reconnect gRPC client
    /// 3. Insert as Running with health checker
    /// 4. Remove dead entries from state file
    ///
    /// Returns the set of recovered handle IDs (for orphan cleanup).
    async fn recover_containers(&mut self) -> HashSet<String> {
        let entries = self.state_file.get_all().await;
        let mut recovered_handles = HashSet::new();

        for entry in entries {
            match self.recover_single_entry(&entry).await {
                Some(handle_id) => {
                    recovered_handles.insert(handle_id);
                }
                None => {
                    // Dead or unrecoverable — already cleaned from state file
                }
            }
        }

        if !recovered_handles.is_empty() {
            info!(
                recovered = recovered_handles.len(),
                "AC recovered containers from state file"
            );
            self.broadcast_state();
        }

        recovered_handles
    }

    /// Try to recover a single container from a persisted state entry.
    ///
    /// Returns the handle ID on success, or `None` if the container is
    /// dead or unrecoverable (cleans up the state file entry on failure).
    async fn recover_single_entry(
        &mut self,
        entry: &crate::state_file::PersistedContainer,
    ) -> Option<String> {
        let handle = ProcessHandle {
            id: entry.handle_id.clone(),
            daemon_addr: Some(entry.daemon_addr.clone()),
            http_addr: Some(entry.http_addr.clone()),
            container_ip: entry.container_ip.clone(),
        };

        // Check if process is alive
        match self.config.driver.alive(&handle).await {
            Ok(true) => {}
            Ok(false) => {
                info!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    "AC container no longer alive, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
            Err(e) => {
                warn!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    error = ?e,
                    "Failed to check AC container status, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
        }

        // Reconnect gRPC client
        let addr = &entry.daemon_addr;
        let client = match crate::retry::retry_until_deadline(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(100),
            &format!("reconnecting to recovered FE at {}", addr),
            || crate::function_executor::fe_client::FunctionExecutorGrpcClient::connect(addr),
            || async { Ok(()) },
        )
        .await
        {
            Ok(client) => client,
            Err(e) => {
                warn!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    error = ?e,
                    "Failed to reconnect to AC container, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
        };

        // Decode description
        let description = match entry.decode_description() {
            Some(desc) => desc,
            None => {
                warn!(
                    container_id = %entry.container_id,
                    "No description stored in state file for AC container, skipping"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
        };

        let fe_id = entry.container_id.clone();
        let max_concurrency = description.max_concurrency.unwrap_or(1);

        // Spawn health checker
        let health_cancel = tokio_util::sync::CancellationToken::new();
        let health_cancel_clone = health_cancel.clone();
        let event_tx = self.event_tx.clone();
        let panic_event_tx = event_tx.clone();
        let health_client = client.clone();
        let health_driver = self.config.driver.clone();
        let health_handle = handle.clone();
        let health_fe_id = fe_id.clone();
        let panic_fe_id = fe_id.clone();
        let health_metrics = self.config.metrics.clone();

        tokio::spawn(async move {
            let result = tokio::spawn(async move {
                if let Some(reason) =
                    crate::function_executor::health_checker::run_health_check_loop(
                        health_client,
                        health_driver,
                        health_handle,
                        health_cancel_clone,
                        &health_fe_id,
                        health_metrics,
                    )
                    .await
                {
                    let _ = event_tx.send(ACEvent::ContainerTerminated {
                        fe_id: health_fe_id,
                        reason,
                        blamed_allocation_id: None,
                    });
                }
            })
            .await;

            if let Err(join_err) = result {
                tracing::error!(container_id = %panic_fe_id, error = %join_err, "Recovered health checker panicked");
                let _ = panic_event_tx.send(ACEvent::ContainerTerminated {
                    fe_id: panic_fe_id,
                    reason: proto_api::executor_api_pb::ContainerTerminationReason::Unknown,
                    blamed_allocation_id: None,
                });
            }
        });

        let managed = ManagedFE {
            description,
            state: ContainerState::Running {
                handle: handle.clone(),
                client: Box::new(client),
                health_checker_cancel: health_cancel,
            },
            max_concurrency,
            allocated_gpu_uuids: Vec::new(), // GPUs not recovered across restarts
            created_at: Instant::now(),
        };

        let handle_id = handle.id.clone();
        info!(
            container_id = %fe_id,
            handle_id = %handle_id,
            daemon_addr = %entry.daemon_addr,
            max_concurrency = max_concurrency,
            "Recovered AC container from state file"
        );

        self.containers.insert(fe_id.clone(), managed);

        // Notify the server so it can update container state
        let response =
            crate::function_executor::proto_convert::make_container_started_response(&fe_id);
        let _ = self.config.container_state_tx.send(response);

        self.config
            .metrics
            .up_down_counters
            .containers_count
            .add(1, &[]);

        Some(handle_id)
    }

    /// Fail all non-terminal allocations for a given FE.
    ///
    /// For startup failures (constructor crash/timeout/error/bad image), all
    /// non-running allocations get the startup failure reason so the server
    /// charges them a retry attempt — the constructor bug is systematic.
    ///
    /// For runtime failures (OOM, crash, unhealthy, etc.), non-running
    /// allocations get `FunctionExecutorTerminated` so the server gives them
    /// a free retry — they're innocent victims of the container dying.
    ///
    /// Running allocations are skipped — their runners detect the gRPC stream
    /// break and report the accurate failure reason themselves.
    fn fail_allocations_for_fe(&mut self, fe_id: &str, reason: AllocationFailureReason) {
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

        // For startup failures, the bug is systematic (every allocation would
        // hit the same constructor failure), so all get charged.
        // For runtime failures, only running allocations (handled by their
        // runners) get the real reason; non-running ones are innocent.
        let is_startup = matches!(
            reason,
            AllocationFailureReason::StartupFailedInternalError |
                AllocationFailureReason::StartupFailedFunctionError |
                AllocationFailureReason::StartupFailedFunctionTimeout |
                AllocationFailureReason::StartupFailedBadImage
        );
        let non_running_reason = if is_startup {
            reason
        } else {
            AllocationFailureReason::ContainerTerminated
        };

        if !alloc_ids.is_empty() {
            warn!(
                container_id = %fe_id,
                reason = ?reason,
                non_running_reason = ?non_running_reason,
                count = alloc_ids.len(),
                "Failing {} allocations for terminated container", alloc_ids.len()
            );
        }

        for alloc_id in alloc_ids {
            let Some(alloc) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };

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
                AllocationState::WaitingForContainer | AllocationState::WaitingForSlot => {
                    self.config
                        .metrics
                        .up_down_counters
                        .runnable_allocations
                        .add(-1, &[]);
                }
                _ => {}
            }

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
                    reason = ?non_running_reason,
                    "Failing allocation: {} -> Finalizing(failure)", alloc.state
                );
            }

            // Take finalization context: first check prepared_data side map,
            // then fall back to the state itself.
            let ctx = if let Some((_, finalization_ctx)) = self.prepared_data.remove(&alloc_id) {
                finalization_ctx
            } else {
                match std::mem::replace(&mut alloc.state, AllocationState::Done) {
                    AllocationState::Preparing { .. } |
                    AllocationState::WaitingForContainer |
                    AllocationState::WaitingForSlot => FinalizationContext::default(),
                    other => {
                        alloc.state = other;
                        continue;
                    }
                }
            };

            // If finalization context has no blobs to clean up, send result
            // directly to avoid the latency of spawning a finalization task.
            if ctx.request_error_blob_handle.is_none() &&
                ctx.output_blob_handles.is_empty() &&
                ctx.fe_result.is_none()
            {
                let outcome = proto_convert::make_allocation_failed_outcome(
                    &alloc.allocation,
                    non_running_reason,
                    None,
                    None,
                    Some(fe_id.to_string()),
                );
                proto_convert::record_outcome_metrics(&outcome, &self.config.metrics.counters);
                if self.config.outcome_tx.send(outcome).is_err() {
                    tracing::warn!("outcome_tx channel closed, allocation outcome lost");
                }
                alloc.state = AllocationState::Done;
            } else {
                let result =
                    proto_convert::make_failure_result(&alloc.allocation, non_running_reason);
                self.start_finalization(&alloc_id, result, ctx, Some(fe_id.to_string()));
            }
        }
    }
}

// Pull in the Instrument trait for tracing spans on futures.
use tracing::Instrument;

use crate::driver::ProcessHandle;
