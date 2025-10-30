use std::{sync::Arc, vec};

use anyhow::Result;
use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::sync::Notify;
use tracing::{debug, error, info, instrument, trace};

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
        requests::{
            CreateOrUpdateApplicationRequest,
            DeleteApplicationRequest,
            DeleteRequestRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
    },
    utils::{TimeUnit, get_elapsed_time},
};

pub struct ApplicationProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub state_transition_latency: Histogram<f64>,
    pub processor_processing_latency: Histogram<f64>,
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

        Self {
            indexify_state,
            state_transition_latency,
            processor_processing_latency,
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
        last_global_state_change_cursor: &mut Option<Vec<u8>>,
        last_namespace_state_change_cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        debug!("Waking up to process state changes; cached_state_changes={cached_state_changes:?}");
        let timer_kvs = &[KeyValue::new("op", "get")];
        let _timer_guard = Timer::start_with_labels(&self.processor_processing_latency, timer_kvs);

        // 1. First load 100 state changes. Process the `global` state changes first
        // and then the `ns_` state changes
        if cached_state_changes.is_empty() {
            let unprocessed_state_changes = self
                .indexify_state
                .reader()
                .unprocessed_state_changes(&None, &None)?;
            if let Some(cursor) = unprocessed_state_changes.last_global_state_change_cursor {
                last_global_state_change_cursor.replace(cursor);
            };
            if let Some(cursor) = unprocessed_state_changes.last_namespace_state_change_cursor {
                last_namespace_state_change_cursor.replace(cursor);
            };
            let mut state_changes = unprocessed_state_changes.changes;
            state_changes.reverse();
            cached_state_changes.extend(state_changes);
        }

        // 2. If there are no state changes to process, return
        // and wait for the scheduler to wake us up again when there are state changes
        if cached_state_changes.is_empty() {
            return Ok(());
        }

        // 3. Fire a notification when handling multiple
        // state changes in order to fill in the cache when
        // the next state change is processed.
        //
        // 0 = we fetched all current state changes
        // > 1 = we are processing cached state changes, we need to fetch more when done
        if !cached_state_changes.is_empty() {
            notify.notify_one();
        }

        // 4. Process the next state change from the queue
        let state_change = cached_state_changes.pop().unwrap();
        let sm_update = self.handle_state_change(&state_change).await;

        // 5. Write the state change to the state store
        // If there is an error processing the state change, we write a NOOP state
        // change That way this problematic state change will never be processed
        // again This most likely is a bug but in production we want to move
        // along.
        let sm_update = match sm_update {
            Ok(sm_update) => sm_update,
            Err(err) => {
                // TODO: Determine if error is transient or not to determine if retrying should
                // be done.
                error!(
                    "error processing state change {}, marking as processed: {:?}",
                    state_change.change_type, err
                );

                // Sending NOOP SM Update
                StateMachineUpdateRequest {
                    payload: RequestPayload::ProcessStateChanges(vec![state_change.clone()]),
                }
            }
        };

        // 6. Write the state change
        if let Err(err) = self.indexify_state.write(sm_update).await {
            // TODO: Determine if error is transient or not to determine if retrying should
            // be done.
            error!(
                "error writing state change {}, marking as processed: {:?}",
                state_change.change_type, err,
            );
            // 7. If SM update fails for whatever reason, lets just write a NOOP state
            //    change
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::ProcessStateChanges(vec![state_change.clone()]),
                })
                .await?;
        }

        // Record the state transition latency
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
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn handle_state_change(
        &self,
        state_change: &StateChange,
    ) -> Result<StateMachineUpdateRequest> {
        trace!("processing state change: {}", state_change);
        let indexes = self.indexify_state.in_memory_state.read().await.clone();
        let mut indexes_guard = indexes.write().await;
        let clock = indexes_guard.clock;
        let task_creator =
            function_run_creator::FunctionRunCreator::new(self.indexify_state.clone(), clock);
        let fe_manager =
            function_executor_manager::FunctionExecutorManager::new(clock, self.queue_size);
        let task_allocator = FunctionRunProcessor::new(clock, &fe_manager);

        let req = match &state_change.change_type {
            ChangeType::CreateFunctionCall(req) => {
                let mut scheduler_update =
                    task_creator.handle_blocking_function_call(&mut indexes_guard, req)?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();

                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(&mut indexes_guard, unallocated_function_runs)?,
                );
                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::InvokeApplication(ev) => {
                let scheduler_update = task_allocator.allocate_request(
                    &mut indexes_guard,
                    &ev.namespace,
                    &ev.application,
                    &ev.request_id,
                )?;

                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::AllocationOutputsIngested(req) => {
                let mut scheduler_update = task_creator
                    .handle_allocation_ingestion(&mut indexes_guard, req)
                    .await?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(&mut indexes_guard, unallocated_function_runs)?,
                );
                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::ExecutorUpserted(ev) => {
                let mut scheduler_update =
                    fe_manager.reconcile_executor_state(&mut indexes_guard, &ev.executor_id)?;
                let unallocated_function_runs =
                    indexes_guard.unallocated_function_runs_for_executor_catalog(&ev.executor_id);
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(&mut indexes_guard, unallocated_function_runs)?,
                );

                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut scheduler_update =
                    fe_manager.deregister_executor(&mut indexes_guard, &ev.executor_id)?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                scheduler_update.extend(
                    task_allocator
                        .allocate_function_runs(&mut indexes_guard, unallocated_function_runs)?,
                );

                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::TombstoneApplication(request) => StateMachineUpdateRequest {
                payload: RequestPayload::DeleteApplicationRequest((
                    DeleteApplicationRequest {
                        namespace: request.namespace.clone(),
                        name: request.application.clone(),
                    },
                    vec![state_change.clone()],
                )),
            },
            ChangeType::TombstoneRequest(request) => StateMachineUpdateRequest {
                payload: RequestPayload::DeleteRequestRequest((
                    DeleteRequestRequest {
                        namespace: request.namespace.clone(),
                        application: request.application.clone(),
                        request_id: request.request_id.clone(),
                    },
                    vec![state_change.clone()],
                )),
            },
        };
        Ok(req)
    }
}
