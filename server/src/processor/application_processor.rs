use std::{sync::Arc, vec};

use anyhow::Result;
use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::sync::Notify;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    data_model::{Application, ApplicationState, ChangeType, StateChange},
    metrics::{Timer, low_latency_boundaries},
    processor::{
        function_executor_manager,
        function_run_creator,
        function_run_processor::FunctionRunProcessor,
    },
    state_store::{
        IndexifyState,
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
    SchedulerUpdate(SchedulerUpdateRequest),
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
    pub batch_write_duration: Histogram<f64>,
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

        let batch_write_duration = meter
            .f64_histogram("indexify.batch_write_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time spent writing batch to state machine in seconds")
            .build();

        Self {
            indexify_state,
            state_transition_latency,
            processor_processing_latency,
            batch_processing_duration,
            batch_write_duration,
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

        if cached_state_changes.is_empty() {
            return Ok(());
        }

        let batch_start = std::time::Instant::now();
        let num_state_changes = cached_state_changes.len();

        // 2. Clone in_memory_state ONCE for the entire batch so state changes see each
        //    other's effects
        let indexes = self.indexify_state.in_memory_state.read().await.clone();
        let mut indexes_guard = indexes.write().await;

        // 3. Initialize batch accumulators
        let mut combined_update = SchedulerUpdateRequest::default();
        let mut batch_state_changes: Vec<StateChange> = Vec::new();
        let mut total_write_time = std::time::Duration::ZERO;

        // 4. Process all state changes using the shared in_memory_state
        for state_change in cached_state_changes.drain(..).collect::<Vec<_>>() {
            match self
                .handle_state_change(&state_change, &mut indexes_guard)
                .await
            {
                Ok(StateChangeResult::SchedulerUpdate(update)) => {
                    combined_update.extend(update);
                    batch_state_changes.push(state_change.clone());
                    self.record_latency(&state_change);
                }
                Ok(StateChangeResult::DeleteApplication(req)) => {
                    let write_time = self
                        .flush_batch(&mut combined_update, &mut batch_state_changes)
                        .await?;
                    total_write_time += write_time;
                    let payload =
                        RequestPayload::DeleteApplicationRequest((req, vec![state_change.clone()]));
                    self.write_with_fallback(payload, state_change).await?;
                }
                Ok(StateChangeResult::DeleteRequest(req)) => {
                    let write_time = self
                        .flush_batch(&mut combined_update, &mut batch_state_changes)
                        .await?;
                    total_write_time += write_time;
                    let payload =
                        RequestPayload::DeleteRequestRequest((req, vec![state_change.clone()]));
                    self.write_with_fallback(payload, state_change).await?;
                }
                Err(err) if err.to_string().contains("Operation timed out") => {
                    warn!(
                        "transient error processing {}, retrying: {:?}",
                        state_change.change_type, err
                    );
                    cached_state_changes.push(state_change);
                    self.flush_batch(&mut combined_update, &mut batch_state_changes)
                        .await?;
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    return Ok(());
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
        let write_time = self
            .flush_batch(&mut combined_update, &mut batch_state_changes)
            .await?;
        total_write_time += write_time;

        let total_time = batch_start.elapsed();
        self.batch_processing_duration.record(
            total_time.as_secs_f64(),
            &[KeyValue::new("num_state_changes", num_state_changes as i64)],
        );
        self.batch_write_duration.record(
            total_write_time.as_secs_f64(),
            &[KeyValue::new("num_state_changes", num_state_changes as i64)],
        );

        notify.notify_one();
        Ok(())
    }

    /// Flushes accumulated scheduler updates. Falls back to individual
    /// processing on error. Returns the time spent writing to the state
    /// machine.
    async fn flush_batch(
        &self,
        combined_update: &mut SchedulerUpdateRequest,
        state_changes: &mut Vec<StateChange>,
    ) -> Result<std::time::Duration> {
        if state_changes.is_empty() {
            return Ok(std::time::Duration::ZERO);
        }

        let batch = std::mem::take(state_changes);
        let batch_size = batch.len();
        let update = std::mem::take(combined_update);
        let payload = RequestPayload::SchedulerUpdate((Box::new(update), batch.clone()));

        let write_start = std::time::Instant::now();
        if let Err(err) = self
            .indexify_state
            .write(StateMachineUpdateRequest { payload })
            .await
        {
            error!("batch write failed, falling back to individual: {:?}", err);
            // Re-acquire fresh state for fallback processing
            let indexes = self.indexify_state.in_memory_state.read().await.clone();
            let mut indexes_guard = indexes.write().await;
            for sc in batch {
                match self.handle_state_change(&sc, &mut indexes_guard).await {
                    Ok(StateChangeResult::SchedulerUpdate(upd)) => {
                        let p = RequestPayload::SchedulerUpdate((Box::new(upd), vec![sc.clone()]));
                        self.write_with_fallback(p, sc).await?;
                    }
                    _ => self.mark_as_processed(vec![sc]).await?,
                }
            }
        }
        let write_time = write_start.elapsed();
        debug!(
            batch_size = batch_size,
            write_time_ms = write_time.as_millis(),
            "flushed batch to state machine"
        );
        Ok(write_time)
    }

    /// Writes a request, marking state change as processed on failure.
    async fn write_with_fallback(
        &self,
        payload: RequestPayload,
        state_change: StateChange,
    ) -> Result<()> {
        if let Err(err) = self
            .indexify_state
            .write(StateMachineUpdateRequest { payload })
            .await
        {
            error!(
                "write failed for {}, marking as processed: {:?}",
                state_change.change_type, err
            );
            self.mark_as_processed(vec![state_change]).await?;
        }
        Ok(())
    }

    /// Marks state changes as processed (NOOP).
    async fn mark_as_processed(&self, state_changes: Vec<StateChange>) -> Result<()> {
        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::ProcessStateChanges(state_changes),
            })
            .await
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

                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(indexes_guard, unallocated_function_runs)?,
                );
                StateChangeResult::SchedulerUpdate(scheduler_update)
            }
            ChangeType::InvokeApplication(ev) => {
                let scheduler_update = task_allocator.allocate_request(
                    indexes_guard,
                    &ev.namespace,
                    &ev.application,
                    &ev.request_id,
                )?;
                StateChangeResult::SchedulerUpdate(scheduler_update)
            }
            ChangeType::AllocationOutputsIngested(req) => {
                let mut scheduler_update = task_creator
                    .handle_allocation_ingestion(indexes_guard, req)
                    .await?;
                let unallocated_function_runs = indexes_guard.unallocated_function_runs();
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(indexes_guard, unallocated_function_runs)?,
                );
                StateChangeResult::SchedulerUpdate(scheduler_update)
            }
            ChangeType::ExecutorUpserted(ev) => {
                let mut scheduler_update =
                    fe_manager.reconcile_executor_state(indexes_guard, &ev.executor_id)?;
                let unallocated_function_runs = indexes_guard.unallocated_function_runs();
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(indexes_guard, unallocated_function_runs)?,
                );
                StateChangeResult::SchedulerUpdate(scheduler_update)
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut scheduler_update =
                    fe_manager.deregister_executor(indexes_guard, &ev.executor_id)?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(indexes_guard, unallocated_function_runs)?,
                );
                StateChangeResult::SchedulerUpdate(scheduler_update)
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
