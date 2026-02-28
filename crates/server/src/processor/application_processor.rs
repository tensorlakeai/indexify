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
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    data_model::{self, Application, ApplicationState, ChangeType, SnapshotStatus, StateChange},
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
            let in_memory_state = self.indexify_state.in_memory_state.load();
            let executor_catalog = &in_memory_state.executor_catalog;
            in_memory_state.applications.values().filter_map(|application| {
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
        let mut cached_state_changes: Vec<StateChange> = vec![];
        let mut change_events_rx = self.indexify_state.change_events_rx.clone();
        let mut last_global_state_change_cursor: Option<Vec<u8>> = None;
        let mut last_namespace_state_change_cursor: Option<Vec<u8>> = None;
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
                    match self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        Ok(()) => { pool_reconciliation_pending = true; },
                        Err(err) => {
                            error!("error processing state change: {:?}", err);
                            continue;
                        }
                    }
                },
                _ = notify.notified() => {
                    match self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        Ok(()) => { pool_reconciliation_pending = true; },
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

            // Run buffer reconciliation when the state change queue is drained.
            // This avoids blocking the critical scheduling path — pool
            // maintenance runs only when there are no pending real events.
            if pool_reconciliation_pending && cached_state_changes.is_empty() {
                pool_reconciliation_pending = false;
                if let Err(err) = self.run_pool_reconciliation().await {
                    error!("error during pool reconciliation: {:?}", err);
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn write_sm_update(
        &self,
        cached_state_changes: &mut Vec<StateChange>,
        application_events_cursor: &mut Option<Vec<u8>>,
        executor_events_cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        debug!("Waking up to process state changes; cached_state_changes={cached_state_changes:?}");
        let metrics_kvs = &[KeyValue::new("op", "get")];
        let _timer_guard = Timer::start_with_labels(&self.write_sm_update_latency, metrics_kvs);

        // 1. First load state changes. Process the `global` state changes first
        // and then the `ns_` state changes
        if cached_state_changes.is_empty() {
            let unprocessed_state_changes = self
                .indexify_state
                .reader()
                .unprocessed_state_changes(executor_events_cursor, application_events_cursor)
                .await?;
            if let Some(cursor) = unprocessed_state_changes.application_state_change_cursor {
                application_events_cursor.replace(cursor);
            };
            if let Some(cursor) = unprocessed_state_changes.executor_state_change_cursor {
                executor_events_cursor.replace(cursor);
            };
            let mut state_changes = unprocessed_state_changes.changes;
            state_changes.reverse();
            cached_state_changes.extend(state_changes);
        }

        self.state_change_queue_depth
            .record(cached_state_changes.len() as u64, &[]);

        // 2. If there are no state changes to process, return
        if cached_state_changes.is_empty() {
            return Ok(());
        }

        // 3. Fire a notification so the event loop re-enters to fetch more
        // state changes after this batch is done.
        notify.notify_one();

        // 4. Take ONE snapshot for the entire batch. All state changes in the
        // batch share these mutable clones. Processors mutate them inline so
        // subsequent events see prior decisions (container placements, etc.).
        let current_state = self.indexify_state.in_memory_state.load_full();
        let mut indexes = (*current_state).clone();
        let current_sched = self.indexify_state.container_scheduler.load_full();
        let mut container_scheduler = (*current_sched).clone();

        let mut snapshot_clock = indexes.clock;

        let mut merged_update = SchedulerUpdateRequest::default();
        let mut processed_state_changes: Vec<StateChange> = Vec::new();
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

        // 5. Process cached state changes, accumulating results.
        // State changes whose created_at_clock exceeds the snapshot are
        // deferred: write() commits to RocksDB before publishing the
        // ArcSwap, so a state change can be visible in RocksDB while its
        // data is not yet in our in-memory snapshot. Deferred changes are
        // left in cached_state_changes for the next batch.
        let mut deferred: Vec<StateChange> = Vec::new();
        while let Some(state_change) = cached_state_changes.pop() {
            if state_change
                .created_at_clock()
                .map_or(false, |c| c > snapshot_clock)
            {
                debug!(
                    snapshot_clock,
                    state_change_clock = ?state_change.created_at_clock(),
                    change_type = %state_change.change_type,
                    "deferring state change ahead of in-memory snapshot"
                );
                deferred.push(state_change);
                continue;
            }
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

            // Capture creation time before potential moves.
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
                    // Apply to indexes first (by reference) so subsequent
                    // events in this batch see updated function run / sandbox
                    // states (e.g. unallocated_function_runs is current).
                    // This fixes cross-event stale snapshot overwrites.
                    let clock = indexes.clock;
                    indexes.apply_scheduler_update(clock, &update, "batch_visibility")?;
                    merged_update.extend(update);
                    processed_state_changes.push(state_change);
                }
                Ok(StateChangeResult::ImmediateWrite(sm_update)) => {
                    // Non-batchable payload (DeleteApplication, etc.) — write
                    // immediately and refresh local clones so subsequent events
                    // in this batch see the updated state.
                    if let Err(err) = self.indexify_state.write(sm_update).await {
                        error!(
                            "error writing state change {}, marking as processed: {:?}",
                            state_change.change_type, err,
                        );
                        self.indexify_state
                            .write(StateMachineUpdateRequest {
                                payload: RequestPayload::ProcessStateChanges(vec![state_change]),
                            })
                            .await?;
                    }
                    // Refresh clones to see the written state, preserving
                    // ephemeral batch state across the refresh so blocked work
                    // and reaped container tracking is not lost. Also advance
                    // snapshot_clock so deferred changes become eligible.
                    let refreshed_state = self.indexify_state.in_memory_state.load_full();
                    indexes = (*refreshed_state).clone();
                    snapshot_clock = indexes.clock;
                    let blocked_work = std::mem::take(&mut container_scheduler.blocked_work);
                    let reaped_containers =
                        std::mem::take(&mut container_scheduler.reaped_containers);
                    let refreshed_sched = self.indexify_state.container_scheduler.load_full();
                    container_scheduler = (*refreshed_sched).clone();
                    container_scheduler.blocked_work = blocked_work;
                    container_scheduler.reaped_containers = reaped_containers;
                }
                Err(err) => {
                    if err.to_string().contains("Operation timed out") {
                        warn!(
                            "transient error processing state change {}, retrying later: {:?}",
                            state_change.change_type, err
                        );
                        cached_state_changes.push(state_change);
                        // Flush accumulated work before retrying.
                        if !processed_state_changes.is_empty() {
                            self.indexify_state
                                .write(StateMachineUpdateRequest {
                                    payload: RequestPayload::SchedulerUpdate(
                                        SchedulerUpdatePayload {
                                            update: Box::new(merged_update),
                                            processed_state_changes,
                                        },
                                    ),
                                })
                                .await?;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        return Ok(());
                    }
                    error!(
                        "error processing state change {}, marking as processed: {:?}",
                        state_change.change_type, err
                    );
                    // Mark the failing state change as processed (NOOP).
                    processed_state_changes.push(state_change);
                }
            }

            self.state_transition_latency.record(
                get_elapsed_time(created_at.into(), TimeUnit::Milliseconds),
                state_change_metrics_kvs,
            );
        }

        // Put deferred state changes back for the next batch.
        cached_state_changes.extend(deferred);

        // 6. ONE RocksDB write for the entire batch.
        if !processed_state_changes.is_empty() &&
            let Err(err) = self
                .indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(merged_update),
                        processed_state_changes: processed_state_changes.clone(),
                    }),
                })
                .await
        {
            error!(
                "error writing batched state changes, marking as processed: {:?}",
                err,
            );
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::ProcessStateChanges(processed_state_changes),
                })
                .await?;
        }

        // 7. Persist BlockedWorkTracker state from batch processing.
        // The batch clone's blocked_work reflects all block/unblock
        // mutations from this batch. Merge it into the persisted
        // ContainerScheduler so the next batch sees it.
        {
            let current = self.indexify_state.container_scheduler.load_full();
            let mut next = (*current).clone();
            next.blocked_work = container_scheduler.blocked_work;
            self.indexify_state
                .container_scheduler
                .store(Arc::new(next));
        }

        Ok(())
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
                        payload: RequestPayload::DeleteApplicationRequest((
                            DeleteApplicationRequest {
                                namespace: request.namespace.clone(),
                                name: request.application.clone(),
                            },
                            vec![state_change.clone()],
                        )),
                    },
                ));
            }
            ChangeType::TombstoneRequest(request) => {
                return Ok(StateChangeResult::ImmediateWrite(
                    StateMachineUpdateRequest {
                        payload: RequestPayload::DeleteRequestRequest((
                            DeleteRequestRequest {
                                namespace: request.namespace.clone(),
                                application: request.application.clone(),
                                request_id: request.request_id.clone(),
                            },
                            vec![state_change.clone()],
                        )),
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
                        payload: RequestPayload::DeleteContainerPool((
                            DeleteContainerPoolRequest {
                                namespace: ev.namespace.clone(),
                                pool_id: ev.pool_id.clone(),
                            },
                            vec![state_change.clone()],
                        )),
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

    #[instrument(skip(self))]
    async fn handle_cluster_vacuum(&self) -> Result<()> {
        // Reap idle containers on the timer so quiescent systems (no incoming
        // state changes) still free resources. The event-loop reaper in
        // write_sm_update uses min_idle_age=0 for immediate capacity recovery;
        // here we use the configured age threshold for background cleanup.
        let current_sched = self.indexify_state.container_scheduler.load_full();
        let mut container_scheduler = (*current_sched).clone();

        let reap_update =
            container_scheduler.reap_idle_containers(self.cluster_vacuum_max_idle_age);
        if !reap_update.containers.is_empty() {
            let current_state = self.indexify_state.in_memory_state.load_full();
            let mut indexes = (*current_state).clone();
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

            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(merged_update),
                        processed_state_changes: vec![],
                    }),
                })
                .await?;

            // Persist BlockedWorkTracker state so the next batch sees the
            // unblock mutations made above.
            {
                let current = self.indexify_state.container_scheduler.load_full();
                let mut next = (*current).clone();
                next.blocked_work = container_scheduler.blocked_work;
                self.indexify_state
                    .container_scheduler
                    .store(Arc::new(next));
            }
        }

        // Fail snapshots that have been stuck in InProgress for too long
        self.handle_snapshot_vacuum().await?;

        Ok(())
    }

    /// Run buffer reconciliation for warm pool containers.
    /// Called from the event loop only when no real state changes are pending.
    pub async fn run_pool_reconciliation(&self) -> Result<()> {
        let current_state = self.indexify_state.in_memory_state.load_full();
        let indexes = (*current_state).clone();
        let current_sched = self.indexify_state.container_scheduler.load_full();
        let mut container_scheduler = (*current_sched).clone();

        let buffer_reconciler = BufferReconciler::new();
        let mut feas_cache = FeasibilityCache::new();
        let buffer_update =
            buffer_reconciler.reconcile(&indexes, &mut container_scheduler, &mut feas_cache)?;

        if !buffer_update.containers.is_empty() || !buffer_update.updated_executor_states.is_empty()
        {
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(buffer_update),
                        processed_state_changes: vec![],
                    }),
                })
                .await?;
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
            let in_memory = self.indexify_state.in_memory_state.load();
            in_memory
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
