use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
    vec,
};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use tokio::sync::Notify;
use tracing::{error, info, instrument, trace, warn};

use crate::{
    data_model::{
        self,
        Application,
        ApplicationState,
        ChangeType,
        ContainerPoolKey,
        ContainerState,
        SnapshotStatus,
        StateChange,
    },
    metrics::{Timer, low_latency_boundaries},
    processor::{
        buffer_reconciler::BufferReconciler,
        container_reconciler,
        function_run_creator,
        function_run_processor::FunctionRunProcessor,
        sandbox_processor::SandboxProcessor,
    },
    scheduler::{
        blocked::UnblockedWork,
        executor_class::ExecutorClass,
        placement::FeasibilityCache,
    },
    state_store::{
        ExecutorEvent,
        IndexifyState,
        requests::{
            CreateOrUpdateApplicationRequest,
            DeleteApplicationRequest,
            DeleteContainerPoolRequest,
            DeleteRequestRequest,
            FailSnapshotRequest,
            RequestPayload,
            SchedulerUpdatePayload,
            SchedulerUpdateRequest,
            StateMachineUpdateRequest,
        },
        state_changes,
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ns},
};

/// Result from processing a single state change. SchedulerUpdate results can
/// be merged across multiple events into a single batched write.
/// ImmediateWrite results (deletions, etc.) require their own write.
pub(crate) enum StateChangeResult {
    /// Batchable scheduler update — merge with other updates.
    SchedulerUpdate(SchedulerUpdateRequest),
    /// Non-batchable payload — write immediately.
    ImmediateWrite(StateMachineUpdateRequest),
}

pub struct ApplicationProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub write_sm_update_latency: Histogram<f64>,
    pub state_change_latency: Histogram<f64>,
    pub state_changes_total: Counter<u64>,
    pub state_transition_latency: Histogram<f64>,
    pub handle_state_change_latency: Histogram<f64>,
    pub allocate_function_runs_latency: Histogram<f64>,
    pub state_change_queue_depth: Gauge<u64>,
    pub queue_size: u32,
    pub cluster_vacuum_interval: Duration,
    pub cluster_vacuum_max_idle_age: Duration,
    pub snapshot_timeout: Duration,
}

impl ApplicationProcessor {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        queue_size: u32,
        cluster_vacuum_interval: Duration,
        cluster_vacuum_max_idle_age: Duration,
        snapshot_timeout: Duration,
    ) -> Self {
        let meter = opentelemetry::global::meter("processor_metrics");

        let write_sm_update_latency = meter
            .f64_histogram("indexify.application_processor.write_sm_update_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of writing state machine update in seconds")
            .build();

        let state_change_latency = meter
            .f64_histogram("indexify.application_processor.state_change_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state change processing in seconds")
            .build();

        let state_changes_total = meter
            .u64_counter("indexify.application_processor.state_changes_total")
            .with_description("Total number of state changes processed")
            .build();

        let state_transition_latency = meter
            .f64_histogram("indexify.application_processor.state_transition_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state transition since state change creation in seconds")
            .build();

        let handle_state_change_latency = meter
            .f64_histogram("indexify.application_processor.handle_state_change_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state change handling in seconds")
            .build();

        let allocate_function_runs_latency = meter
            .f64_histogram("indexify.application_processor.allocate_function_runs_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of function runs allocation in seconds")
            .build();

        let state_change_queue_depth = meter
            .u64_gauge("indexify.state_store.state_change_queue_depth")
            .with_description("Number of unprocessed state changes in the queue")
            .build();

        Self {
            indexify_state,
            write_sm_update_latency,
            state_change_latency,
            state_changes_total,
            state_transition_latency,
            handle_state_change_latency,
            allocate_function_runs_latency,
            state_change_queue_depth,
            queue_size,
            cluster_vacuum_interval,
            cluster_vacuum_max_idle_age,
            snapshot_timeout,
        }
    }

    pub async fn validate_app_constraints(&self) -> Result<()> {
        let updated_applications = {
            let state = self.indexify_state.app_state.load();
            let executor_catalog = &state.indexes.executor_catalog;
            state.indexes.applications.values().filter_map(|application| {
                let target_state = if application.can_be_scheduled(executor_catalog).is_ok() {
                        ApplicationState::Active
                } else {
                        ApplicationState::Disabled{reason: "The application contains functions that have unsatisfiable placement constraints".to_string()}
                };

                if target_state != application.state {
                    let mut updated_application = *application.clone();
                    updated_application.state = target_state;
                    Some(updated_application)
                } else {
                    None
                }
            })
                .collect::<Vec<Application>>()
        };

        for application in updated_applications {
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                        CreateOrUpdateApplicationRequest {
                            namespace: application.namespace.clone(),
                            application,
                            upgrade_requests_to_current_version: true,
                            container_pools: vec![],
                        },
                    )),
                })
                .await?;
        }
        Ok(())
    }

    #[instrument(skip(self, shutdown_rx))]
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut change_events_rx = self.indexify_state.change_events_rx.clone();
        // Used to run the loop when there are more than 1 change events queued up
        // The watch from the state store only notifies that there are N number of state
        // changes but if we only process one event from the queue then the
        // watch will not notify again
        let notify = Arc::new(Notify::new());
        // Tracks whether buffer reconciliation is needed. Set after any
        // state change that could affect pool container counts. Processed
        // only when no real state changes are pending (back-of-queue).
        let mut pool_reconciliation_pending = false;

        let vacuum_enabled = !self.cluster_vacuum_interval.is_zero();
        let mut vacuum_timer = tokio::time::interval(
            if vacuum_enabled {
                self.cluster_vacuum_interval
            } else {
                // Interval must be > 0; use a large dummy value (guarded by vacuum_enabled)
                Duration::from_secs(3600)
            },
        );
        // Consume the first immediate tick so vacuum doesn't fire on startup
        vacuum_timer.tick().await;

        loop {
            tokio::select! {
                _ = change_events_rx.changed() => {
                    change_events_rx.borrow_and_update();
                    match self.write_sm_update(&notify).await {
                        Ok(true) => { pool_reconciliation_pending = true; },
                        Ok(false) => {},
                        Err(err) => {
                            error!("error processing state change: {:?}", err);
                            continue;
                        }
                    }
                },
                _ = notify.notified() => {
                    match self.write_sm_update(&notify).await {
                        Ok(true) => { pool_reconciliation_pending = true; },
                        Ok(false) => {},
                        Err(err) => {
                            error!("error processing state change: {:?}", err);
                            continue;
                        }
                    }
                },
                _ = vacuum_timer.tick(), if vacuum_enabled => {
                    if let Err(err) = self.handle_cluster_vacuum().await {
                        error!("error during cluster vacuum: {:?}", err);
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("application processor shutting down");
                    break;
                }
            }

            // Run buffer reconciliation when the payload queue is drained.
            // This avoids blocking the critical scheduling path — pool
            // maintenance runs only when there are no pending real events.
            if pool_reconciliation_pending {
                // Check if the queue has more payloads before running reconciliation.
                let queue_empty = self
                    .indexify_state
                    .reader()
                    .read_pending_payloads()
                    .await
                    .map(|p| p.is_empty())
                    .unwrap_or(false);
                if queue_empty {
                    pool_reconciliation_pending = false;
                    if let Err(err) = self.run_pool_reconciliation().await {
                        error!("error during pool reconciliation: {:?}", err);
                    }
                }
            }
        }
    }

    /// Generate ephemeral state changes from a payload.
    ///
    /// Replicates the state change generation that
    /// `write_in_persistent_store()` performs, including secondary
    /// `AllocationOutputsIngested` events for `UpsertExecutor` payloads.
    fn generate_state_changes_for_payload(
        &self,
        payload: &RequestPayload,
    ) -> Result<Vec<StateChange>> {
        let request = StateMachineUpdateRequest {
            payload: payload.clone(),
        };
        let mut changes = request.state_changes(&self.indexify_state.state_change_id_seq)?;

        // For UpsertExecutor, also generate secondary AllocationOutputsIngested
        // events — replicating write_in_persistent_store logic.
        if let RequestPayload::UpsertExecutor(req) = payload {
            for allocation_output in &req.allocation_outputs {
                changes.extend(state_changes::task_outputs_ingested(
                    &self.indexify_state.state_change_id_seq,
                    allocation_output,
                )?);
            }
        }

        Ok(changes)
    }

    /// Process pending payloads from the RocksDB PayloadQueue.
    ///
    /// Returns `true` if payloads were processed, `false` if the queue was
    /// empty.
    #[instrument(skip_all)]
    pub async fn write_sm_update(&self, notify: &Arc<Notify>) -> Result<bool> {
        let metrics_kvs = &[KeyValue::new("op", "get")];
        let _timer_guard = Timer::start_with_labels(&self.write_sm_update_latency, metrics_kvs);

        // 1. Atomically read pending payloads and snapshot under write_mutex to prevent
        //    the race where a payload is committed to RocksDB but its ArcSwap publish
        //    hasn't happened yet.
        let (pending, current) = {
            let _write_guard = self.indexify_state.write_mutex.lock().await;
            let pending = self.indexify_state.reader().read_pending_payloads().await?;
            let current = self.indexify_state.app_state.load_full();
            (pending, current)
        };

        self.state_change_queue_depth
            .record(pending.len() as u64, &[]);

        // 2. If no pending payloads, return.
        if pending.is_empty() {
            return Ok(false);
        }

        let max_seq = pending.last().unwrap().0;

        // 3. Fire a notification so the event loop re-enters to check for
        // more payloads after this batch is done.
        notify.notify_one();

        // 4. Take ONE snapshot for the entire batch. All state changes in the
        // batch share these mutable clones. Processors mutate them inline so
        // subsequent events see prior decisions (container placements, etc.).
        let mut indexes = current.indexes.clone();
        let mut container_scheduler = current.scheduler.clone();

        let mut merged_update = SchedulerUpdateRequest::default();
        let mut feas_cache = FeasibilityCache::new();

        // 4b. Reap idle containers — frees resources so subsequent
        // allocations in this batch see available capacity.
        let reap_update = container_scheduler.reap_idle_containers(std::time::Duration::ZERO);
        if !reap_update.containers.is_empty() {
            let clock = indexes.clock;
            indexes.apply_scheduler_update(clock, &reap_update, "reap_idle")?;

            // 4c. Unblock work that was waiting on freed resources from reaped
            // containers. Without this, function runs blocked due to capacity
            // remain stuck — reap frees memory but nothing tells the
            // BlockedWorkTracker about it.
            let mut freed_per_class: HashMap<ExecutorClass, u64> = HashMap::new();
            for meta in reap_update.containers.values() {
                if matches!(
                    meta.desired_state,
                    data_model::ContainerState::Terminated { .. }
                ) {
                    let exec_class = container_scheduler.get_executor_class(&meta.executor_id);
                    *freed_per_class.entry(exec_class).or_default() +=
                        meta.function_container.resources.memory_mb;
                }
            }
            let mut unblocked = UnblockedWork::default();
            for (class, freed_mb) in &freed_per_class {
                let batch = container_scheduler
                    .blocked_work
                    .unblock_for_freed_resources(class, *freed_mb);
                unblocked.function_run_keys.extend(batch.function_run_keys);
                unblocked.sandbox_keys.extend(batch.sandbox_keys);
            }

            // Extend reap_update into merged FIRST. Allocations below may
            // restore a reaped container (via restore_reaped_container),
            // producing a Running entry that correctly overwrites the
            // Terminated entry from the reap (HashMap::extend is last-wins).
            // Without this ordering, reap's Terminated would overwrite
            // restore's Running, causing spurious ContainerRemoved events.
            merged_update.extend(reap_update);

            // Allocate unblocked function runs.
            if !unblocked.function_run_keys.is_empty() {
                let function_runs =
                    indexes.resolve_pending_function_runs(&unblocked.function_run_keys);
                if !function_runs.is_empty() {
                    info!(
                        num_unblocked = function_runs.len(),
                        "reap_idle: scheduling unblocked function runs"
                    );
                    let task_allocator = FunctionRunProcessor::new(self.queue_size);
                    let alloc_result = task_allocator.allocate_function_runs(
                        &indexes,
                        &mut container_scheduler,
                        function_runs,
                        &self.allocate_function_runs_latency,
                        &mut feas_cache,
                    )?;
                    indexes.apply_scheduler_update(clock, &alloc_result, "reap_unblock_alloc")?;
                    merged_update.extend(alloc_result);
                }
            }

            // Allocate unblocked sandboxes (batched apply).
            if !unblocked.sandbox_keys.is_empty() {
                let sandbox_processor = SandboxProcessor::new();
                let mut sb_batch = SchedulerUpdateRequest::default();
                for sandbox_key in &unblocked.sandbox_keys {
                    sb_batch.extend(sandbox_processor.allocate_sandbox_by_key(
                        &indexes,
                        &mut container_scheduler,
                        sandbox_key.namespace(),
                        sandbox_key.sandbox_id(),
                        &mut feas_cache,
                    )?);
                }
                indexes.apply_scheduler_update(clock, &sb_batch, "reap_unblock_sandbox")?;
                merged_update.extend(sb_batch);
            }
        }

        // 5. Process payloads from the queue. For each payload:
        // a) Apply raw state update (update_state + container_scheduler.update)
        //    so handle_state_change sees the data. write() no longer updates
        //    ArcSwap for enqueued payloads — the scheduler is the sole writer.
        // b) Generate ephemeral state changes and route through scheduling.
        let clock = self
            .indexify_state
            .state_change_id_seq
            .load(std::sync::atomic::Ordering::Relaxed);
        for (_seq, payload) in pending {
            // 5a. Apply raw payload state to local clones.
            let _ = indexes
                .update_state(clock, &payload, "scheduler")
                .map_err(|e| anyhow::anyhow!("error applying payload to in-memory state: {e:?}"))?;
            container_scheduler.update(&payload).map_err(|e| {
                anyhow::anyhow!("error applying payload to container scheduler: {e:?}")
            })?;

            // 5b. Generate ephemeral state changes for scheduling payloads.
            // Non-scheduling payloads (CompleteSnapshot, CreateNameSpace, etc.)
            // produce no state changes — their update_state above is sufficient.
            let state_changes = self.generate_state_changes_for_payload(&payload)?;

            for state_change in state_changes {
                let state_change_metrics_kvs = &[KeyValue::new(
                    "type",
                    if state_change.namespace.is_some() {
                        "ns"
                    } else {
                        "global"
                    },
                )];
                let _timer_guard =
                    Timer::start_with_labels(&self.state_change_latency, state_change_metrics_kvs);
                self.state_changes_total.add(1, state_change_metrics_kvs);

                let created_at = state_change.created_at;

                let result = self
                    .handle_state_change(
                        &mut indexes,
                        &mut container_scheduler,
                        &state_change,
                        &mut feas_cache,
                    )
                    .await;

                match result {
                    Ok(StateChangeResult::SchedulerUpdate(update)) => {
                        let clock = indexes.clock;
                        indexes.apply_scheduler_update(clock, &update, "batch_visibility")?;
                        container_scheduler.apply_container_update(&update);
                        merged_update.extend(update);
                    }
                    Ok(StateChangeResult::ImmediateWrite(sm_update)) => {
                        // Non-batchable payload (DeleteApplication, etc.) — write
                        // to RocksDB and apply to local clones directly. No ArcSwap
                        // refresh needed since we publish complete clones at the end.
                        let payload = sm_update.payload;
                        if let Err(err) = self
                            .indexify_state
                            .write_scheduler_output(StateMachineUpdateRequest {
                                payload: payload.clone(),
                            })
                            .await
                        {
                            error!(
                                "error writing immediate state change {}: {:?}",
                                state_change.change_type, err,
                            );
                        } else {
                            let clock = indexes.clock;
                            let _ = indexes.update_state(clock, &payload, "immediate")?;
                            container_scheduler.update(&payload)?;
                            // Emit FullSync to affected executors for deletions.
                            self.emit_immediate_events(&payload, &container_scheduler)
                                .await;
                        }
                    }
                    Err(err) => {
                        error!(
                            "error processing state change {}: {:?}",
                            state_change.change_type, err
                        );
                    }
                }

                self.state_transition_latency.record(
                    get_elapsed_time(created_at.into(), TimeUnit::Milliseconds),
                    state_change_metrics_kvs,
                );
            }
        }

        // 6. Snapshot pre-existing containers BEFORE publishing, so we can
        // distinguish ContainerAdded vs ContainerDescriptionChanged in events.
        let pre_existing_containers: HashSet<data_model::ContainerId> = merged_update
            .containers
            .keys()
            .filter(|id| current.scheduler.function_containers.contains_key(id))
            .cloned()
            .collect();

        // 6a. Write scheduler results to RocksDB (allocations, updated runs, etc.).
        if let Err(err) = self
            .indexify_state
            .write_scheduler_output(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                    update: Box::new(merged_update.clone()),
                }),
            })
            .await
        {
            error!("error writing batched scheduler update: {:?}", err);
        }

        // 6b. Publish complete clones to ArcSwap (sole writer).
        self.indexify_state
            .app_state
            .store(Arc::new(crate::state_store::AppState {
                indexes,
                scheduler: container_scheduler,
            }));

        // 6c. Emit events AFTER ArcSwap publish so long-poll consumers see
        // consistent state when they wake.
        self.emit_scheduler_events(&merged_update, &pre_existing_containers)
            .await;

        // 7. Dequeue processed payloads from the queue.
        self.indexify_state.dequeue_payloads(max_seq).await?;

        Ok(true)
    }

    #[instrument(skip_all)]
    pub async fn handle_state_change(
        &self,
        indexes_guard: &mut crate::state_store::in_memory_state::InMemoryState,
        container_scheduler_guard: &mut crate::processor::container_scheduler::ContainerScheduler,
        state_change: &StateChange,
        feas_cache: &mut FeasibilityCache,
    ) -> Result<StateChangeResult> {
        trace!("processing state change: {}", state_change);
        let kvs = &[KeyValue::new(
            "change_type",
            state_change.change_type.to_string(),
        )];
        let _timer_guard = Timer::start_with_labels(&self.handle_state_change_latency, kvs);
        let clock = indexes_guard.clock;

        let task_creator =
            function_run_creator::FunctionRunCreator::new(self.indexify_state.clone());
        let container_reconciler =
            container_reconciler::ContainerReconciler::new(clock, self.indexify_state.clone());
        let task_allocator = FunctionRunProcessor::new(self.queue_size);
        let sandbox_processor = SandboxProcessor::new();

        let scheduler_update = match &state_change.change_type {
            ChangeType::CreateFunctionCall(req) => {
                let mut scheduler_update = task_creator
                    .handle_blocking_function_call(indexes_guard, req)
                    .await?;
                // Apply so allocate_function_runs sees the new request_ctx.
                indexes_guard.apply_scheduler_update(
                    clock,
                    &scheduler_update,
                    "create_function_call",
                )?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                let alloc_result = task_allocator.allocate_function_runs(
                    indexes_guard,
                    container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                    feas_cache,
                )?;
                scheduler_update.extend(alloc_result);
                scheduler_update
            }
            ChangeType::InvokeApplication(ev) => task_allocator.allocate_request(
                indexes_guard,
                container_scheduler_guard,
                &ev.namespace,
                &ev.application,
                &ev.request_id,
                &self.allocate_function_runs_latency,
                feas_cache,
            )?,
            ChangeType::AllocationOutputsIngested(req) => {
                let mut scheduler_update = task_creator
                    .handle_allocation_ingestion(indexes_guard, container_scheduler_guard, req)
                    .await?;

                // Apply intermediate result so allocate_function_runs
                // sees updated request_ctx from handle_allocation_ingestion.
                indexes_guard.apply_scheduler_update(
                    clock,
                    &scheduler_update,
                    "alloc_output_ingested_intermediate",
                )?;

                // Allocate NEW function runs from output propagation.
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                if !unallocated_function_runs.is_empty() {
                    let alloc_result = task_allocator.allocate_function_runs(
                        indexes_guard,
                        container_scheduler_guard,
                        unallocated_function_runs,
                        &self.allocate_function_runs_latency,
                        feas_cache,
                    )?;
                    indexes_guard.apply_scheduler_update(
                        clock,
                        &alloc_result,
                        "alloc_output_ingested_new_runs",
                    )?;
                    scheduler_update.extend(alloc_result);
                }

                // Unblock previously-blocked work. The completed allocation
                // freed a slot on the container — unblock work that was
                // waiting on capacity for this executor's class.
                let cid = &req.allocation_target.container_id;
                let mut freed_memory_mb: u64 = 0;
                if let Some(fc) = container_scheduler_guard.function_containers.get(cid) {
                    let not_terminated = !matches!(
                        fc.desired_state,
                        data_model::ContainerState::Terminated { .. }
                    );
                    let capacity = self.queue_size * fc.function_container.max_concurrency;
                    if not_terminated && fc.allocations.len() < capacity as usize {
                        freed_memory_mb = fc.function_container.resources.memory_mb;
                    }
                }

                if freed_memory_mb > 0 {
                    let executor_class = container_scheduler_guard
                        .get_executor_class(&req.allocation_target.executor_id);
                    let unblocked = container_scheduler_guard
                        .blocked_work
                        .unblock_for_freed_resources(&executor_class, freed_memory_mb);

                    if !unblocked.function_run_keys.is_empty() {
                        let function_runs = indexes_guard
                            .resolve_pending_function_runs(&unblocked.function_run_keys);
                        if !function_runs.is_empty() {
                            scheduler_update.extend(task_allocator.allocate_function_runs(
                                indexes_guard,
                                container_scheduler_guard,
                                function_runs,
                                &self.allocate_function_runs_latency,
                                feas_cache,
                            )?);
                        }
                    }

                    for sandbox_key in &unblocked.sandbox_keys {
                        scheduler_update.extend(sandbox_processor.allocate_sandbox_by_key(
                            indexes_guard,
                            container_scheduler_guard,
                            sandbox_key.namespace(),
                            sandbox_key.sandbox_id(),
                            feas_cache,
                        )?);
                    }
                }

                scheduler_update
            }
            ChangeType::ExecutorUpserted(ev) => {
                let mut scheduler_update = container_reconciler
                    .reconcile_executor_state(
                        indexes_guard,
                        container_scheduler_guard,
                        &ev.executor_id,
                    )
                    .await?;

                // Layer 2: Capacity-aware scheduling skip.
                // Only run the full allocation pass if the reconciliation
                // produced capacity changes (new/removed containers or
                // updated executor states). This avoids O(all_pending) work
                // on heartbeats that only update liveness.
                let capacity_changed = !scheduler_update.containers.is_empty() ||
                    !scheduler_update.updated_executor_states.is_empty();

                if capacity_changed {
                    // Allocate NEW function runs from reconciliation (e.g.,
                    // re-queued runs from terminated containers).
                    let new_function_runs = scheduler_update.unallocated_function_runs();
                    if !new_function_runs.is_empty() {
                        let alloc_result = task_allocator.allocate_function_runs(
                            indexes_guard,
                            container_scheduler_guard,
                            new_function_runs,
                            &self.allocate_function_runs_latency,
                            feas_cache,
                        )?;
                        // Apply intermediate result so the unblocked-work
                        // allocation below sees updated request_ctx.
                        indexes_guard.apply_scheduler_update(
                            clock,
                            &alloc_result,
                            "executor_upserted_intermediate",
                        )?;
                        scheduler_update.extend(alloc_result);
                    }

                    // Layer 3: Targeted unblocking via BlockedWorkTracker.
                    // Compute the executor's class and unblock work that was
                    // previously blocked on this class. This ensures previously
                    // blocked work gets retried now that capacity is available.
                    let executor_class =
                        container_scheduler_guard.get_executor_class(&ev.executor_id);
                    let budget_mb = container_scheduler_guard
                        .executor_states
                        .get(&ev.executor_id)
                        .map(|s| s.free_resources.memory_bytes / (1024 * 1024))
                        .unwrap_or(0);
                    let unblocked = container_scheduler_guard
                        .blocked_work
                        .unblock_for_class(&executor_class, budget_mb);

                    if !unblocked.is_empty() {
                        // Resolve function run keys to actual FunctionRun objects
                        let function_runs = indexes_guard
                            .resolve_pending_function_runs(&unblocked.function_run_keys);

                        if !function_runs.is_empty() {
                            scheduler_update.extend(task_allocator.allocate_function_runs(
                                indexes_guard,
                                container_scheduler_guard,
                                function_runs,
                                &self.allocate_function_runs_latency,
                                feas_cache,
                            )?);
                        }

                        // Allocate unblocked sandboxes by key
                        for sandbox_key in &unblocked.sandbox_keys {
                            scheduler_update.extend(sandbox_processor.allocate_sandbox_by_key(
                                indexes_guard,
                                container_scheduler_guard,
                                sandbox_key.namespace(),
                                sandbox_key.sandbox_id(),
                                feas_cache,
                            )?);
                        }
                    }
                }
                scheduler_update
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut scheduler_update = container_reconciler.deregister_executor(
                    indexes_guard,
                    container_scheduler_guard,
                    &ev.executor_id,
                )?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                scheduler_update.extend(task_allocator.allocate_function_runs(
                    indexes_guard,
                    container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                    feas_cache,
                )?);
                scheduler_update
            }
            ChangeType::TombstoneApplication(request) => {
                return Ok(StateChangeResult::ImmediateWrite(
                    StateMachineUpdateRequest {
                        payload: RequestPayload::DeleteApplicationRequest(
                            DeleteApplicationRequest {
                                namespace: request.namespace.clone(),
                                name: request.application.clone(),
                            },
                        ),
                    },
                ));
            }
            ChangeType::TombstoneRequest(request) => {
                return Ok(StateChangeResult::ImmediateWrite(
                    StateMachineUpdateRequest {
                        payload: RequestPayload::DeleteRequestRequest(DeleteRequestRequest {
                            namespace: request.namespace.clone(),
                            application: request.application.clone(),
                            request_id: request.request_id.clone(),
                        }),
                    },
                ));
            }
            ChangeType::CreateSandbox(ev) => sandbox_processor.allocate_sandbox_by_key(
                indexes_guard,
                container_scheduler_guard,
                &ev.namespace,
                ev.sandbox_id.get(),
                feas_cache,
            )?,
            ChangeType::TerminateSandbox(ev) => sandbox_processor.terminate_sandbox(
                indexes_guard,
                container_scheduler_guard,
                &ev.namespace,
                ev.sandbox_id.get(),
            )?,
            ChangeType::SnapshotSandbox(ev) => {
                info!(
                    namespace = %ev.namespace,
                    sandbox_id = %ev.sandbox_id,
                    snapshot_id = %ev.snapshot_id,
                    "processing SnapshotSandbox event"
                );

                // Emit SnapshotContainer event to the executor running this
                // sandbox so the dataplane can snapshot the container filesystem.
                let sandbox_key = data_model::SandboxKey::new(&ev.namespace, ev.sandbox_id.get());
                if let Some(sandbox) = indexes_guard.sandboxes.get(&sandbox_key) {
                    if let (Some(container_id), Some(executor_id)) =
                        (&sandbox.container_id, &sandbox.executor_id)
                    {
                        let connections = self.indexify_state.executor_connections.read().await;
                        IndexifyState::send_event(
                            &connections,
                            executor_id,
                            ExecutorEvent::SnapshotContainer {
                                container_id: container_id.clone(),
                                snapshot_id: ev.snapshot_id.get().to_string(),
                                upload_uri: ev.upload_uri.clone(),
                            },
                        );
                    } else {
                        warn!(
                            namespace = %ev.namespace,
                            sandbox_id = %ev.sandbox_id,
                            "SnapshotSandbox: sandbox has no container_id or executor_id"
                        );
                    }
                } else {
                    warn!(
                        namespace = %ev.namespace,
                        sandbox_id = %ev.sandbox_id,
                        "SnapshotSandbox: sandbox not found in state"
                    );
                }

                // No container or pool changes — return empty batchable update.
                return Ok(StateChangeResult::SchedulerUpdate(
                    SchedulerUpdateRequest::default(),
                ));
            }
            ChangeType::CreateContainerPool(ev) => {
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing CreateContainerPool event"
                );
                SchedulerUpdateRequest::default()
            }
            ChangeType::UpdateContainerPool(ev) => {
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing UpdateContainerPool event"
                );
                SchedulerUpdateRequest::default()
            }
            ChangeType::DeleteContainerPool(ev) => {
                // Container termination is handled by container_scheduler.update()
                // when the DeleteContainerPool payload is applied
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing DeleteContainerPool event, issuing storage deletion"
                );
                return Ok(StateChangeResult::ImmediateWrite(
                    StateMachineUpdateRequest {
                        payload: RequestPayload::DeleteContainerPool(DeleteContainerPoolRequest {
                            namespace: ev.namespace.clone(),
                            pool_id: ev.pool_id.clone(),
                        }),
                    },
                ));
            }
            ChangeType::DataplaneResultsIngested(ev) => {
                info!(
                    executor_id = %ev.executor_id,
                    num_allocations = ev.allocation_events.len(),
                    num_container_updates = ev.container_state_updates.len(),
                    "processing DataplaneResultsIngested event"
                );
                let mut scheduler_update = SchedulerUpdateRequest::default();
                let mut freed_memory_mb: u64 = 0;

                // Step 1: Terminate containers (lifecycle only).
                // Marks containers as terminated and removes them from the
                // scheduler so Step 3 won't schedule to dead containers.
                //
                // Allocation failure is NOT handled here — the dataplane
                // sends individual AllocationFailed messages with the correct
                // blame-resolved failure reasons, and those are processed in
                // Step 2 via handle_allocation_ingestion.
                for csu in &ev.container_state_updates {
                    let termination_reason = csu
                        .termination_reason
                        .unwrap_or(data_model::ContainerTerminationReason::Unknown);
                    let mut container_update = SchedulerUpdateRequest::default();

                    // Mark the container itself as terminated in the scheduler
                    // so it is excluded from the executor's desired state. This
                    // wakes up the sandbox/allocation stream loops to send a
                    // removal notification to the dataplane.
                    let term_info = container_scheduler_guard
                        .function_containers
                        .get(&csu.container_id)
                        .map(|existing_meta| {
                            let mut fc = existing_meta.function_container.clone();
                            let terminated_state = data_model::ContainerState::Terminated {
                                reason: termination_reason,
                            };
                            // Set terminated state on the container so
                            // terminate_sandbox_for_container reads the correct
                            // termination reason for the sandbox outcome.
                            fc.state = terminated_state.clone();
                            (
                                fc.clone(),
                                data_model::ContainerServerMetadata::new(
                                    ev.executor_id.clone(),
                                    fc,
                                    terminated_state,
                                ),
                            )
                        });
                    if let Some((fc, terminated_meta)) = term_info {
                        // Track freed memory for BlockedWorkTracker unblocking
                        freed_memory_mb += fc.resources.memory_mb;

                        container_update
                            .containers
                            .insert(csu.container_id.clone(), Box::new(terminated_meta));

                        // Remove container from executor metadata so
                        // free_resources are updated correctly.
                        if let Some(exec_state) = container_scheduler_guard
                            .executor_states
                            .get(&ev.executor_id)
                        {
                            let mut updated_exec = (**exec_state).clone();
                            if updated_exec.remove_container(&fc).is_ok() {
                                container_update
                                    .updated_executor_states
                                    .insert(ev.executor_id.clone(), Box::new(updated_exec));
                            }
                        }

                        // Terminate the associated sandbox (if any) so it
                        // transitions to Terminated in the state store.
                        let sandbox_update = container_reconciler
                            .terminate_sandbox_for_container(indexes_guard, &fc)?;
                        container_update.extend(sandbox_update);
                    }

                    // Apply incremental updates so subsequent containers/allocations
                    // see the updated state.
                    container_scheduler_guard.apply_container_update(&container_update);
                    indexes_guard.apply_scheduler_update(
                        clock,
                        &container_update,
                        "dataplane_results_container_term",
                    )?;
                    scheduler_update.extend(container_update);
                }

                // Step 1b: Promote containers and sandboxes for started containers.
                // When the dataplane reports ContainerStarted, update the
                // container state from Pending to Running and promote the
                // associated sandbox (if any) as well.
                for container_id in &ev.container_started_ids {
                    let promote_update = container_reconciler
                        .promote_sandbox_for_started_container(
                            indexes_guard,
                            container_scheduler_guard,
                            container_id,
                        )?;
                    if !promote_update.updated_sandboxes.is_empty() ||
                        !promote_update.containers.is_empty()
                    {
                        container_scheduler_guard.apply_container_update(&promote_update);
                        indexes_guard.apply_scheduler_update(
                            clock,
                            &promote_update,
                            "dataplane_results_container_started",
                        )?;
                        scheduler_update.extend(promote_update);
                    }
                }

                // Step 2: Process allocation results and accumulate freed capacity.
                let mut counted_containers: HashSet<data_model::ContainerId> = HashSet::new();
                for alloc_event in &ev.allocation_events {
                    let alloc_update = task_creator
                        .handle_allocation_ingestion(
                            indexes_guard,
                            container_scheduler_guard,
                            alloc_event,
                        )
                        .await?;

                    // Apply to in-memory state so subsequent iterations see
                    // updated request_ctx (important when multiple allocs
                    // complete for the same request).
                    indexes_guard.apply_scheduler_update(
                        clock,
                        &alloc_update,
                        "alloc_ingestion",
                    )?;
                    scheduler_update.extend(alloc_update);

                    // Accumulate freed capacity (was Step 2a).
                    let cid = &alloc_event.allocation_target.container_id;
                    if counted_containers.insert(cid.clone()) &&
                        let Some(fc) = container_scheduler_guard.function_containers.get(cid)
                    {
                        let not_terminated = !matches!(
                            fc.desired_state,
                            data_model::ContainerState::Terminated { .. }
                        );
                        let capacity = self.queue_size * fc.function_container.max_concurrency;
                        if not_terminated && fc.allocations.len() < capacity as usize {
                            freed_memory_mb += fc.function_container.resources.memory_mb;
                        }
                    }
                }

                // Step 2b: Unblock previously-blocked work for this executor's class.
                // Container terminations in Step 1 or allocation completions in
                // Step 2 freed resources; unblock work that was waiting on
                // capacity for this executor's class.
                let mut unblocked = UnblockedWork::default();
                if freed_memory_mb > 0 {
                    let executor_class =
                        container_scheduler_guard.get_executor_class(&ev.executor_id);
                    unblocked = container_scheduler_guard
                        .blocked_work
                        .unblock_for_freed_resources(&executor_class, freed_memory_mb);
                }

                // Step 3: Allocate NEW function runs from output propagation (Step 2).
                // These are runs that were just created, not previously blocked.
                let new_function_runs = scheduler_update.unallocated_function_runs();
                if !new_function_runs.is_empty() {
                    info!(
                        num_new = new_function_runs.len(),
                        new_fns = ?new_function_runs.iter().map(|r| format!("{}:{}", r.name, r.id)).collect::<Vec<_>>(),
                        "DataplaneResultsIngested Step 3: scheduling new function runs from output propagation"
                    );
                    let alloc_result = task_allocator.allocate_function_runs(
                        indexes_guard,
                        container_scheduler_guard,
                        new_function_runs,
                        &self.allocate_function_runs_latency,
                        feas_cache,
                    )?;
                    info!(
                        new_allocations = alloc_result.new_allocations.len(),
                        updated_runs = alloc_result.updated_function_runs.len(),
                        new_containers = alloc_result.containers.len(),
                        "DataplaneResultsIngested Step 3: allocation results"
                    );
                    // Apply intermediate result so Step 3b/4 see updated
                    // request_ctx (prevents stale snapshot overwrite).
                    indexes_guard.apply_scheduler_update(
                        clock,
                        &alloc_result,
                        "dataplane_step3_intermediate",
                    )?;
                    scheduler_update.extend(alloc_result);
                }

                // Step 3b: Allocate unblocked function runs (previously blocked, now retried).
                if !unblocked.function_run_keys.is_empty() {
                    let function_runs =
                        indexes_guard.resolve_pending_function_runs(&unblocked.function_run_keys);
                    if !function_runs.is_empty() {
                        info!(
                            num_unblocked = function_runs.len(),
                            "DataplaneResultsIngested Step 3b: scheduling unblocked function runs"
                        );
                        scheduler_update.extend(task_allocator.allocate_function_runs(
                            indexes_guard,
                            container_scheduler_guard,
                            function_runs,
                            &self.allocate_function_runs_latency,
                            feas_cache,
                        )?);
                    }
                }

                // Step 4: Allocate unblocked sandboxes.
                // Only retry sandboxes that were previously blocked and just
                // unblocked by the freed resources.
                for sandbox_key in &unblocked.sandbox_keys {
                    scheduler_update.extend(sandbox_processor.allocate_sandbox_by_key(
                        indexes_guard,
                        container_scheduler_guard,
                        sandbox_key.namespace(),
                        sandbox_key.sandbox_id(),
                        feas_cache,
                    )?);
                }

                scheduler_update
            }
        };

        // Buffer reconciliation is deferred — it runs in the event loop
        // when no real state changes are pending, avoiding blocking the
        // critical scheduling path.

        Ok(StateChangeResult::SchedulerUpdate(scheduler_update))
    }

    /// Emit container, allocation, and executor notification events from a
    /// scheduler update. Must be called AFTER ArcSwap publish so that
    /// long-poll consumers see consistent state when they wake.
    async fn emit_scheduler_events(
        &self,
        update: &SchedulerUpdateRequest,
        pre_existing_containers: &HashSet<data_model::ContainerId>,
    ) {
        let connections = self.indexify_state.executor_connections.read().await;
        let mut changed_executors: HashSet<data_model::ExecutorId> = HashSet::new();

        // Container events FIRST (before allocations, so consumer sees
        // AddContainer before RunAllocation for the same container).
        // Removals are emitted before additions so the dataplane frees
        // resources (e.g. GPUs) before trying to allocate them for new
        // containers — prevents transient resource exhaustion races.
        for (container_id, meta) in &update.containers {
            if matches!(meta.desired_state, ContainerState::Terminated { .. }) {
                IndexifyState::send_event(
                    &connections,
                    &meta.executor_id,
                    ExecutorEvent::ContainerRemoved(container_id.clone()),
                );
            }
        }
        for (container_id, meta) in &update.containers {
            if matches!(meta.desired_state, ContainerState::Terminated { .. }) {
                continue;
            }
            if !pre_existing_containers.contains(container_id) {
                IndexifyState::send_event(
                    &connections,
                    &meta.executor_id,
                    ExecutorEvent::ContainerAdded(container_id.clone()),
                );
            } else {
                IndexifyState::send_event(
                    &connections,
                    &meta.executor_id,
                    ExecutorEvent::ContainerDescriptionChanged(container_id.clone()),
                );
            }
        }

        // Allocation events
        for allocation in &update.new_allocations {
            IndexifyState::send_event(
                &connections,
                &allocation.target.executor_id,
                ExecutorEvent::AllocationCreated(Box::new(allocation.clone())),
            );
        }

        for executor_id in update.updated_executor_states.keys() {
            changed_executors.insert(executor_id.clone());
        }

        for container_meta in update.containers.values() {
            changed_executors.insert(container_meta.executor_id.clone());
        }

        drop(connections);

        // Notify executors with state changes
        let mut executor_states = self.indexify_state.executor_states.write().await;
        for executor_id in &changed_executors {
            info!(
                executor_id = executor_id.get(),
                "notifying executor of state change"
            );
            if let Some(executor_state) = executor_states.get_mut(executor_id) {
                executor_state.notify();
            }
        }
    }

    /// Emit events for ImmediateWrite payloads (DeleteContainerPool,
    /// DeleteApplicationRequest). These send FullSync to affected executors.
    async fn emit_immediate_events(
        &self,
        payload: &RequestPayload,
        container_scheduler: &crate::processor::container_scheduler::ContainerScheduler,
    ) {
        let connections = self.indexify_state.executor_connections.read().await;
        let mut changed_executors: HashSet<data_model::ExecutorId> = HashSet::new();

        if let RequestPayload::DeleteContainerPool(delete_req) = payload {
            let pool_key = ContainerPoolKey::new(&delete_req.namespace, &delete_req.pool_id);
            for (_, meta) in container_scheduler.function_containers.iter() {
                if meta.function_container.belongs_to_pool(&pool_key) {
                    changed_executors.insert(meta.executor_id.clone());
                    IndexifyState::send_event(
                        &connections,
                        &meta.executor_id,
                        ExecutorEvent::FullSync,
                    );
                }
            }
        }

        if let RequestPayload::DeleteApplicationRequest(delete_req) = payload {
            for (_, meta) in container_scheduler.function_containers.iter() {
                if meta.function_container.namespace == delete_req.namespace &&
                    meta.function_container.application_name == delete_req.name
                {
                    changed_executors.insert(meta.executor_id.clone());
                    IndexifyState::send_event(
                        &connections,
                        &meta.executor_id,
                        ExecutorEvent::FullSync,
                    );
                }
            }
        }

        drop(connections);

        if !changed_executors.is_empty() {
            let mut executor_states = self.indexify_state.executor_states.write().await;
            for executor_id in &changed_executors {
                info!(
                    executor_id = executor_id.get(),
                    "notifying executor of state change"
                );
                if let Some(executor_state) = executor_states.get_mut(executor_id) {
                    executor_state.notify();
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn handle_cluster_vacuum(&self) -> Result<()> {
        // Reap idle containers on the timer so quiescent systems (no incoming
        // state changes) still free resources. The event-loop reaper in
        // write_sm_update uses min_idle_age=0 for immediate capacity recovery;
        // here we use the configured age threshold for background cleanup.
        let current = self.indexify_state.app_state.load_full();
        let mut container_scheduler = current.scheduler.clone();

        let reap_update =
            container_scheduler.reap_idle_containers(self.cluster_vacuum_max_idle_age);
        if !reap_update.containers.is_empty() {
            let mut indexes = current.indexes.clone();
            let clock = indexes.clock;
            indexes.apply_scheduler_update(clock, &reap_update, "vacuum_reap")?;

            // Unblock work that was waiting on freed resources from reaped
            // containers. Without this, the write() below removes the
            // containers from function_containers (via update_scheduler_update
            // → remove_container_from_indices), so subsequent
            // DataplaneResultsIngested events can't find them to trigger
            // unblocking — causing blocked function runs to be stuck forever.
            let mut freed_per_class: HashMap<ExecutorClass, u64> = HashMap::new();
            for meta in reap_update.containers.values() {
                if matches!(
                    meta.desired_state,
                    data_model::ContainerState::Terminated { .. }
                ) {
                    let exec_class = container_scheduler.get_executor_class(&meta.executor_id);
                    *freed_per_class.entry(exec_class).or_default() +=
                        meta.function_container.resources.memory_mb;
                }
            }
            let mut unblocked = UnblockedWork::default();
            for (class, freed_mb) in &freed_per_class {
                let batch = container_scheduler
                    .blocked_work
                    .unblock_for_freed_resources(class, *freed_mb);
                unblocked.function_run_keys.extend(batch.function_run_keys);
                unblocked.sandbox_keys.extend(batch.sandbox_keys);
            }

            // Extend reap_update into merged FIRST so allocations below can
            // restore reaped containers (last-wins ordering).
            let mut merged_update = reap_update;
            let mut feas_cache = FeasibilityCache::new();

            // Allocate unblocked function runs.
            if !unblocked.function_run_keys.is_empty() {
                let function_runs =
                    indexes.resolve_pending_function_runs(&unblocked.function_run_keys);
                if !function_runs.is_empty() {
                    info!(
                        num_unblocked = function_runs.len(),
                        "cluster_vacuum: scheduling unblocked function runs"
                    );
                    let task_allocator = FunctionRunProcessor::new(self.queue_size);
                    let alloc_result = task_allocator.allocate_function_runs(
                        &indexes,
                        &mut container_scheduler,
                        function_runs,
                        &self.allocate_function_runs_latency,
                        &mut feas_cache,
                    )?;
                    indexes.apply_scheduler_update(clock, &alloc_result, "vacuum_unblock_alloc")?;
                    merged_update.extend(alloc_result);
                }
            }

            // Allocate unblocked sandboxes (batched apply).
            if !unblocked.sandbox_keys.is_empty() {
                let sandbox_processor = SandboxProcessor::new();
                let mut sb_batch = SchedulerUpdateRequest::default();
                for sandbox_key in &unblocked.sandbox_keys {
                    sb_batch.extend(sandbox_processor.allocate_sandbox_by_key(
                        &indexes,
                        &mut container_scheduler,
                        sandbox_key.namespace(),
                        sandbox_key.sandbox_id(),
                        &mut feas_cache,
                    )?);
                }
                indexes.apply_scheduler_update(clock, &sb_batch, "vacuum_unblock_sandbox")?;
                merged_update.extend(sb_batch);
            }

            // Determine pre-existing containers for event emission.
            let pre_existing_containers: HashSet<data_model::ContainerId> = merged_update
                .containers
                .keys()
                .filter(|id| current.scheduler.function_containers.contains_key(id))
                .cloned()
                .collect();

            // Write scheduler results to RocksDB.
            self.indexify_state
                .write_scheduler_output(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(merged_update.clone()),
                    }),
                })
                .await?;

            // Publish to ArcSwap (sole writer). The next batch loads from
            // ArcSwap and gets the updated blocked_work automatically.
            self.indexify_state
                .app_state
                .store(Arc::new(crate::state_store::AppState {
                    indexes,
                    scheduler: container_scheduler,
                }));

            // Emit events after ArcSwap publish.
            self.emit_scheduler_events(&merged_update, &pre_existing_containers)
                .await;
        }

        // Fail snapshots that have been stuck in InProgress for too long
        self.handle_snapshot_vacuum().await?;

        Ok(())
    }

    /// Run buffer reconciliation for warm pool containers.
    /// Called from the event loop only when no real state changes are pending.
    pub async fn run_pool_reconciliation(&self) -> Result<()> {
        let current = self.indexify_state.app_state.load_full();
        let indexes = current.indexes.clone();
        let mut container_scheduler = current.scheduler.clone();

        let buffer_reconciler = BufferReconciler::new();
        let mut feas_cache = FeasibilityCache::new();
        let buffer_update =
            buffer_reconciler.reconcile(&indexes, &mut container_scheduler, &mut feas_cache)?;

        if !buffer_update.containers.is_empty() || !buffer_update.updated_executor_states.is_empty()
        {
            // Determine pre-existing containers for event emission.
            let pre_existing_containers: HashSet<data_model::ContainerId> = buffer_update
                .containers
                .keys()
                .filter(|id| current.scheduler.function_containers.contains_key(id))
                .cloned()
                .collect();

            // Write to RocksDB.
            self.indexify_state
                .write_scheduler_output(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(buffer_update.clone()),
                    }),
                })
                .await?;

            // Publish to ArcSwap (sole writer).
            let mut indexes = indexes;
            let clock = indexes.clock;
            indexes.apply_scheduler_update(clock, &buffer_update, "pool_reconciliation")?;
            container_scheduler.apply_container_update(&buffer_update);
            self.indexify_state
                .app_state
                .store(Arc::new(crate::state_store::AppState {
                    indexes,
                    scheduler: container_scheduler,
                }));

            // Emit events after ArcSwap publish.
            self.emit_scheduler_events(&buffer_update, &pre_existing_containers)
                .await;
        }

        Ok(())
    }

    /// Detect and fail snapshots stuck in `InProgress` beyond the configured
    /// timeout. This prevents sandboxes from being permanently stuck in the
    /// `Snapshotting` state if a dataplane crashes mid-snapshot.
    async fn handle_snapshot_vacuum(&self) -> Result<()> {
        if self.snapshot_timeout.is_zero() {
            return Ok(());
        }

        let now_ns = get_epoch_time_in_ns();
        let timeout_ns = self.snapshot_timeout.as_nanos();

        let stale_snapshot_ids: Vec<_> = {
            let state = self.indexify_state.app_state.load();
            state
                .indexes
                .snapshots
                .values()
                .filter(|s| {
                    matches!(s.status, SnapshotStatus::InProgress) &&
                        now_ns.saturating_sub(s.creation_time_ns) > timeout_ns
                })
                .map(|s| s.id.clone())
                .collect()
        };

        for snapshot_id in stale_snapshot_ids {
            warn!(
                snapshot_id = snapshot_id.get(),
                timeout_secs = self.snapshot_timeout.as_secs(),
                "Failing stale snapshot stuck in InProgress"
            );
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FailSnapshot(FailSnapshotRequest {
                        snapshot_id,
                        error: "Snapshot timed out".to_string(),
                    }),
                })
                .await?;
        }

        Ok(())
    }
}
