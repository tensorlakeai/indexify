use std::{sync::Arc, time::Duration, vec};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Gauge, Histogram},
};
use tokio::sync::Notify;
use tracing::{debug, error, info, instrument, trace, warn};

const MAX_WRITE_RETRIES: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 100;

use crate::{
    data_model::{Application, ApplicationState, ChangeType, FunctionURI, StateChange},
    manual_timer,
    metrics::{Timer, low_latency_boundaries},
    processor::{
        function_executor_manager,
        function_run_creator,
        function_run_processor::FunctionRunProcessor,
    },
    state_store::{
        IndexifyState,
        blocked_runs::ExecutorClass,
        in_memory_state::InMemoryState,
        requests::{
            CreateOrUpdateApplicationRequest,
            DeleteApplicationRequest,
            DeleteRequestRequest,
            RequestPayload,
            SchedulerUpdateRequest,
            StateMachineUpdateRequest,
        },
    },
    utils::{TimeUnit, get_elapsed_time},
};

/// Result of processing a state change - distinguishes scheduler updates from
/// delete operations
pub enum StateChangeResult {
    /// A scheduler update that can be batched with other scheduler updates
    SchedulerUpdate(Box<SchedulerUpdateRequest>),
    /// A delete application request that must be processed immediately
    DeleteApplication(DeleteApplicationRequest),
    /// A delete request that must be processed immediately
    DeleteRequest(DeleteRequestRequest),
}

pub struct ApplicationProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub state_transition_latency: Histogram<f64>,
    pub processor_processing_latency: Histogram<f64>,
    pub batch_processing_duration: Histogram<f64>,
    pub unprocessed_state_changes: Gauge<u64>,
    pub queue_size: u32,
}

impl ApplicationProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>, queue_size: u32) -> Self {
        let meter = opentelemetry::global::meter("processor_metrics");

        let processor_processing_latency = meter
            .f64_histogram("indexify.processor_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("processor task processing latency in seconds")
            .build();

        let state_transition_latency = meter
            .f64_histogram("indexify.state_transition_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state transitions before processing in seconds")
            .build();

        let batch_processing_duration = meter
            .f64_histogram("indexify.batch_processing_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Total time to process a batch of state changes in seconds")
            .build();

        let unprocessed_state_changes = meter
            .u64_gauge("indexify.unprocessed_state_changes")
            .with_description("Number of unprocessed state changes pending processing")
            .build();

        Self {
            indexify_state,
            state_transition_latency,
            processor_processing_latency,
            batch_processing_duration,
            unprocessed_state_changes,
            queue_size,
        }
    }

    pub async fn validate_app_constraints(&self) -> Result<()> {
        let updated_applications = {
            let in_memory_state = self.indexify_state.in_memory_state.read().await;
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
        loop {
            tokio::select! {
                _ = change_events_rx.changed() => {
                    change_events_rx.borrow_and_update();
                    if let Err(err) = self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        error!("error processing state change: {:?}", err);
                        continue
                    }
                },
                _ = notify.notified() => {
                    if let Err(err) = self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        error!("error processing state change: {:?}", err);
                        continue
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("application processor shutting down");
                    break;
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
        let timer_kvs = &[KeyValue::new("op", "get")];
        let _timer_guard = Timer::start_with_labels(&self.processor_processing_latency, timer_kvs);

        // 1. Load state changes if cache is empty
        if cached_state_changes.is_empty() {
            let unprocessed = self
                .indexify_state
                .reader()
                .unprocessed_state_changes(executor_events_cursor, application_events_cursor)
                .await?;
            if let Some(cursor) = unprocessed.application_state_change_cursor {
                application_events_cursor.replace(cursor);
            }
            if let Some(cursor) = unprocessed.executor_state_change_cursor {
                executor_events_cursor.replace(cursor);
            }
            let mut state_changes = unprocessed.changes;
            state_changes.reverse();
            cached_state_changes.extend(state_changes);
        }

        self.unprocessed_state_changes
            .record(cached_state_changes.len() as u64, &[]);

        if cached_state_changes.is_empty() {
            return Ok(());
        }

        let batch_timer = manual_timer!(self.batch_processing_duration, &[]);

        // 2. Clone in_memory_state ONCE for the entire batch so state changes see each
        //    other's effects
        let indexes = self.indexify_state.in_memory_state.read().await.clone();
        let mut indexes_guard = indexes.write().await;

        // 3. Initialize batch accumulators
        let mut combined_update = SchedulerUpdateRequest::default();
        let mut batch_state_changes: Vec<StateChange> = Vec::new();

        // 4. Process all state changes using the shared in_memory_state
        for state_change in std::mem::take(cached_state_changes) {
            match self
                .handle_state_change(&state_change, &mut indexes_guard)
                .await
            {
                Ok(StateChangeResult::SchedulerUpdate(update)) => {
                    // Update blocked runs index so subsequent state changes in this batch
                    // can see which functions have blocked runs (avoids redundant allocation
                    // attempts)
                    for (run_key, fn_uri, requirements) in &update.blocked_runs_to_add {
                        indexes_guard.blocked_runs_index.block(
                            run_key.clone(),
                            fn_uri.clone(),
                            requirements.clone(),
                        );
                    }
                    for run_key in &update.blocked_runs_to_remove {
                        indexes_guard.blocked_runs_index.unblock(run_key);
                    }
                    combined_update.extend(*update);
                    batch_state_changes.push(state_change.clone());
                    self.record_latency(&state_change);
                }
                Ok(StateChangeResult::DeleteApplication(req)) => {
                    self.flush_batch(&mut combined_update, &mut batch_state_changes)
                        .await?;
                    let payload =
                        RequestPayload::DeleteApplicationRequest((req, vec![state_change.clone()]));
                    self.write_with_fallback(payload, state_change).await?;
                }
                Ok(StateChangeResult::DeleteRequest(req)) => {
                    self.flush_batch(&mut combined_update, &mut batch_state_changes)
                        .await?;
                    let payload =
                        RequestPayload::DeleteRequestRequest((req, vec![state_change.clone()]));
                    self.write_with_fallback(payload, state_change).await?;
                }
                Err(err) => {
                    error!(
                        "error processing {}, marking as processed: {:?}",
                        state_change.change_type, err
                    );
                    self.mark_as_processed(vec![state_change]).await?;
                }
            }
        }

        // 5. Flush remaining batch
        self.flush_batch(&mut combined_update, &mut batch_state_changes)
            .await?;

        batch_timer.stop();
        notify.notify_one();
        Ok(())
    }

    /// Checks if an error is a transient RocksDB error that should be retried.
    /// Based on https://docs.rs/rocksdb/latest/rocksdb/enum.ErrorKind.html
    fn is_transient_error(err: &anyhow::Error) -> bool {
        let err_str = err.to_string();
        err_str.contains("TimedOut") || err_str.contains("Busy") || err_str.contains("timed out")
    }

    /// Writes to the state machine with retry and exponential backoff for
    /// transient errors. Returns Ok(true) if write succeeded, Ok(false) if
    /// failed (non-transient or retries exhausted).
    async fn write_with_retry(&self, payload: RequestPayload) -> Result<bool> {
        let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

        for attempt in 0..MAX_WRITE_RETRIES {
            match self
                .indexify_state
                .write(StateMachineUpdateRequest {
                    payload: payload.clone(),
                })
                .await
            {
                Ok(()) => return Ok(true),
                Err(err) => {
                    if Self::is_transient_error(&err) && attempt < MAX_WRITE_RETRIES - 1 {
                        warn!(
                            attempt = attempt + 1,
                            max_retries = MAX_WRITE_RETRIES,
                            delay_ms = delay.as_millis(),
                            error = %err,
                            "transient RocksDB error, retrying with backoff"
                        );
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                    } else {
                        error!(
                            attempt = attempt + 1,
                            error = %err,
                            "rocksdb write failed"
                        );
                        return Ok(false);
                    }
                }
            }
        }
        Ok(false)
    }

    async fn flush_batch(
        &self,
        combined_update: &mut SchedulerUpdateRequest,
        state_changes: &mut Vec<StateChange>,
    ) -> Result<()> {
        if state_changes.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(state_changes);
        let update = std::mem::take(combined_update);
        let payload = RequestPayload::SchedulerUpdate((Box::new(update), batch.clone()));

        if !self.write_with_retry(payload).await? {
            error!(
                "batch write failed, marking {} state changes as processed",
                batch.len()
            );
            self.mark_as_processed(batch).await?;
        }
        Ok(())
    }

    async fn write_with_fallback(
        &self,
        payload: RequestPayload,
        state_change: StateChange,
    ) -> Result<()> {
        if !self.write_with_retry(payload).await? {
            error!(
                "write failed for {}, marking as processed",
                state_change.change_type
            );
            self.mark_as_processed(vec![state_change]).await?;
        }
        Ok(())
    }

    /// Marks state changes as processed with retry.
    async fn mark_as_processed(&self, state_changes: Vec<StateChange>) -> Result<()> {
        let payload = RequestPayload::ProcessStateChanges(state_changes);
        if !self.write_with_retry(payload).await? {
            error!("failed to mark state changes as processed after retries");
        }
        Ok(())
    }

    fn record_latency(&self, state_change: &StateChange) {
        self.state_transition_latency.record(
            get_elapsed_time(state_change.created_at.into(), TimeUnit::Milliseconds),
            &[KeyValue::new(
                "type",
                if state_change.namespace.is_some() {
                    "ns"
                } else {
                    "global"
                },
            )],
        );
    }

    #[instrument(skip_all)]
    pub async fn handle_state_change(
        &self,
        state_change: &StateChange,
        indexes_guard: &mut InMemoryState,
    ) -> Result<StateChangeResult> {
        trace!("processing state change: {}", state_change);
        let clock = indexes_guard.clock;
        let task_creator = function_run_creator::FunctionRunCreator::new(clock);
        let fe_manager =
            function_executor_manager::FunctionExecutorManager::new(clock, self.queue_size);
        let task_allocator = FunctionRunProcessor::new(clock, &fe_manager);

        let result = match &state_change.change_type {
            ChangeType::CreateFunctionCall(req) => {
                let mut scheduler_update = task_creator
                    .handle_blocking_function_call(indexes_guard, req)
                    .await?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();

                // Try to allocate and block failures
                let alloc_update = task_allocator
                    .allocate_function_runs(indexes_guard, unallocated_function_runs)?;
                scheduler_update.extend(alloc_update);

                StateChangeResult::SchedulerUpdate(Box::new(scheduler_update))
            }
            ChangeType::InvokeApplication(ev) => {
                let scheduler_update = task_allocator.allocate_request(
                    indexes_guard,
                    &ev.namespace,
                    &ev.application,
                    &ev.request_id,
                )?;

                StateChangeResult::SchedulerUpdate(Box::new(scheduler_update))
            }
            ChangeType::AllocationOutputsIngested(req) => {
                let mut scheduler_update = task_creator
                    .handle_allocation_ingestion(indexes_guard, req)
                    .await?;

                // Allocate child runs created by handle_allocation_ingestion.
                // Use scheduler_update.unallocated_function_runs() NOT indexes_guard -
                // we only want runs from THIS allocation completion, not ALL unallocated runs.
                let child_runs = scheduler_update.unallocated_function_runs();

                if !child_runs.is_empty() {
                    let alloc_update =
                        task_allocator.allocate_function_runs(indexes_guard, child_runs)?;
                    scheduler_update.extend(alloc_update);
                }

                // Also check blocked runs - when allocation completes, capacity is freed
                // and we should try to unblock waiting runs.
                let Some(allocation) = indexes_guard.get_allocation_by_id(&req.allocation_id)
                else {
                    return Ok(StateChangeResult::SchedulerUpdate(Box::new(
                        scheduler_update,
                    )));
                };

                let fn_uri = FunctionURI {
                    namespace: allocation.namespace.clone(),
                    application: allocation.application.clone(),
                    function: allocation.function.clone(),
                    version: allocation.application_version.clone(),
                };

                // First try O(1) lookup for same function (prioritize same function to reduce
                // churn)
                if indexes_guard
                    .blocked_runs_index
                    .has_blocked_for_function(&fn_uri)
                {
                    // Calculate available capacity on the FE that just freed up
                    let available_slots = indexes_guard
                        .function_executors_by_fn_uri
                        .get(&fn_uri)
                        .and_then(|fes| fes.get(&allocation.target.function_executor_id))
                        .map(|fe_meta| {
                            let capacity =
                                self.queue_size * fe_meta.function_executor.max_concurrency;
                            let current = indexes_guard
                                .allocations_by_executor
                                .get(&allocation.target.executor_id)
                                .and_then(|by_fe| {
                                    by_fe.get(&allocation.target.function_executor_id)
                                })
                                .map(|allocs| allocs.len() as u32)
                                .unwrap_or(0);
                            capacity.saturating_sub(current) as usize
                        })
                        .unwrap_or(1);

                    if available_slots > 0 {
                        let blocked_keys = indexes_guard
                            .blocked_runs_index
                            .get_runs_for_function(&fn_uri, available_slots);
                        let candidate_runs = indexes_guard.get_function_runs_by_keys(&blocked_keys);
                        if !candidate_runs.is_empty() {
                            let alloc_update =
                                task_allocator.retry_blocked_runs(indexes_guard, candidate_runs)?;
                            scheduler_update.extend(alloc_update);
                        }
                    }
                    return Ok(StateChangeResult::SchedulerUpdate(Box::new(
                        scheduler_update,
                    )));
                }

                // No blocked runs for same function - check other functions by executor class
                let Some(executor) = indexes_guard.executors.get(&allocation.target.executor_id)
                else {
                    return Ok(StateChangeResult::SchedulerUpdate(Box::new(
                        scheduler_update,
                    )));
                };

                let executor_class = ExecutorClass::from_executor(executor);
                let blocked_keys = indexes_guard
                    .blocked_runs_index
                    .get_runs_for_class(&executor_class);

                if blocked_keys.is_empty() {
                    return Ok(StateChangeResult::SchedulerUpdate(Box::new(
                        scheduler_update,
                    )));
                }

                // Limit to queue_size - one FE's worth of capacity
                let limited_keys: Vec<_> = blocked_keys
                    .into_iter()
                    .take(self.queue_size as usize)
                    .collect();
                let candidate_runs = indexes_guard.get_function_runs_by_keys(&limited_keys);

                if !candidate_runs.is_empty() {
                    let alloc_update =
                        task_allocator.retry_blocked_runs(indexes_guard, candidate_runs)?;
                    scheduler_update.extend(alloc_update);
                }

                StateChangeResult::SchedulerUpdate(Box::new(scheduler_update))
            }
            ChangeType::ExecutorUpserted(ev) => {
                let mut scheduler_update =
                    fe_manager.reconcile_executor_state(indexes_guard, &ev.executor_id)?;

                // First, allocate any runs created by reconcile_executor_state (e.g., retries)
                let reconcile_runs = scheduler_update.unallocated_function_runs();
                if !reconcile_runs.is_empty() {
                    debug!(
                        executor_id = %ev.executor_id,
                        reconcile_runs = reconcile_runs.len(),
                        "Allocating runs from reconcile_executor_state"
                    );

                    let alloc_update =
                        task_allocator.allocate_function_runs(indexes_guard, reconcile_runs)?;
                    scheduler_update.extend(alloc_update);
                }

                // O(1) lookup: Get blocked runs for this executor's class
                if let Some(executor) = indexes_guard.executors.get(&ev.executor_id) {
                    let executor_class = ExecutorClass::from_executor(executor);
                    let blocked_keys = indexes_guard
                        .blocked_runs_index
                        .get_runs_for_class(&executor_class);

                    if !blocked_keys.is_empty() {
                        debug!(
                            executor_id = %ev.executor_id,
                            executor_class_cpu = executor_class.cpu_ms,
                            executor_class_mem = executor_class.memory_bytes,
                            blocked_count = blocked_keys.len(),
                            "O(1) lookup: found blocked runs for executor class"
                        );

                        // Limit how many blocked runs we try to avoid O(k) work
                        const MAX_BLOCKED_RUNS_TO_TRY: usize = 100;
                        let limited_keys: Vec<_> = blocked_keys
                            .into_iter()
                            .take(MAX_BLOCKED_RUNS_TO_TRY)
                            .collect();
                        let candidate_runs = indexes_guard.get_function_runs_by_keys(&limited_keys);

                        if !candidate_runs.is_empty() {
                            debug!(
                                executor_id = %ev.executor_id,
                                candidate_runs = candidate_runs.len(),
                                "Attempting to allocate blocked jobs for executor"
                            );

                            // Use retry_blocked_runs since these are from blocked index
                            let alloc_update =
                                task_allocator.retry_blocked_runs(indexes_guard, candidate_runs)?;
                            scheduler_update.extend(alloc_update);
                        }
                    }
                } else {
                    // Fallback to scanning if executor not found (shouldn't happen)
                    warn!(
                        executor_id = %ev.executor_id,
                        "Executor not found in state, falling back to full scan"
                    );
                    let candidate_runs = indexes_guard.unallocated_function_runs();
                    if !candidate_runs.is_empty() {
                        // These are unallocated runs, not blocked retries
                        let alloc_update =
                            task_allocator.allocate_function_runs(indexes_guard, candidate_runs)?;
                        scheduler_update.extend(alloc_update);
                    }
                }

                StateChangeResult::SchedulerUpdate(Box::new(scheduler_update))
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut scheduler_update =
                    fe_manager.deregister_executor(indexes_guard, &ev.executor_id)?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();

                // Try to allocate and block failures
                let alloc_update = task_allocator
                    .allocate_function_runs(indexes_guard, unallocated_function_runs)?;

                scheduler_update.extend(alloc_update);
                StateChangeResult::SchedulerUpdate(Box::new(scheduler_update))
            }
            ChangeType::TombstoneApplication(request) => {
                StateChangeResult::DeleteApplication(DeleteApplicationRequest {
                    namespace: request.namespace.clone(),
                    name: request.application.clone(),
                })
            }
            ChangeType::TombstoneRequest(request) => {
                StateChangeResult::DeleteRequest(DeleteRequestRequest {
                    namespace: request.namespace.clone(),
                    application: request.application.clone(),
                    request_id: request.request_id.clone(),
                })
            }
        };
        Ok(result)
    }
}
