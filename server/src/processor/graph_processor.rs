use std::{sync::Arc, vec};

use anyhow::Result;
use opentelemetry::{metrics::Histogram, KeyValue};
use tokio::sync::Notify;
use tracing::{debug, error, info, trace};

use crate::{
    data_model::{ChangeType, ComputeGraph, ComputeGraphState, StateChange},
    metrics::{low_latency_boundaries, Timer},
    processor::{
        function_executor_manager,
        task_allocator::TaskAllocationProcessor,
        task_cache,
        task_creator,
    },
    state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            DeleteComputeGraphRequest,
            DeleteInvocationRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        IndexifyState,
    },
    utils::{get_elapsed_time, TimeUnit},
};

pub struct GraphProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub task_cache: Arc<task_cache::TaskCache>,
    pub state_transition_latency: Histogram<f64>,
    pub processor_processing_latency: Histogram<f64>,
    pub queue_size: u32,
}

impl GraphProcessor {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        task_cache: Arc<task_cache::TaskCache>,
        queue_size: u32,
    ) -> Self {
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
            task_cache,
            state_transition_latency,
            processor_processing_latency,
            queue_size,
        }
    }

    pub async fn validate_graph_constraints(&self) -> Result<()> {
        let updated_compute_graphs = {
            let in_memory_state = self.indexify_state.in_memory_state.read().await;
            let executor_catalog = &in_memory_state.executor_catalog;
            in_memory_state.compute_graphs.values().filter_map(|compute_graph| {
                let target_state = if executor_catalog.allows_any_labels() ||
                    compute_graph.can_be_scheduled(&executor_catalog.entries).is_ok() {
                        ComputeGraphState::Active
                } else {
                        ComputeGraphState::Disabled{reason: "The compute graph contains functions that have unsatisfiable placement constraints".to_string()}
                };

                if target_state != compute_graph.state {
                    let mut updated_graph = *compute_graph.clone();
                    updated_graph.state = target_state;
                    Some(updated_graph)
                } else {
                    None
                }
            })
                .collect::<Vec<ComputeGraph>>()
        };

        for compute_graph in updated_compute_graphs {
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateComputeGraph(
                        CreateOrUpdateComputeGraphRequest {
                            namespace: compute_graph.namespace.clone(),
                            compute_graph,
                            upgrade_tasks_to_current_version: true,
                        },
                    ),
                })
                .await?;
        }
        Ok(())
    }

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
                    info!("graph processor shutting down");
                    break;
                }
            }
        }
    }

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

    pub async fn handle_state_change(
        &self,
        state_change: &StateChange,
    ) -> Result<StateMachineUpdateRequest> {
        trace!("processing state change: {}", state_change);
        let indexes = self.indexify_state.in_memory_state.read().await.clone();
        let mut indexes_guard = indexes.write().await;
        let clock = indexes_guard.clock;
        let mut task_creator = task_creator::TaskCreator::new(self.indexify_state.clone(), clock);
        let fe_manager =
            function_executor_manager::FunctionExecutorManager::new(clock, self.queue_size);
        let task_allocator = TaskAllocationProcessor::new(clock, &fe_manager);

        let req = match &state_change.change_type {
            ChangeType::InvokeComputeGraph(_) | ChangeType::AllocationOutputsIngested(_) => {
                if let ChangeType::AllocationOutputsIngested(req) = &state_change.change_type {
                    self.task_cache
                        .handle_task_outputs(req, &indexes_guard)
                        .await;
                }

                let mut scheduler_update = task_creator
                    .invoke(&mut indexes_guard, &state_change.change_type)
                    .await?;

                scheduler_update.extend(self.task_cache.try_allocate(&mut indexes_guard).await);
                scheduler_update.extend(task_allocator.allocate(&mut indexes_guard)?);

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
                scheduler_update.extend(task_allocator.allocate(&mut indexes_guard)?);

                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::ExecutorRemoved(_) => {
                let scheduler_update = task_allocator.allocate(&mut indexes_guard)?;

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
                scheduler_update.extend(task_allocator.allocate(&mut indexes_guard)?);

                StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate((
                        Box::new(scheduler_update),
                        vec![state_change.clone()],
                    )),
                }
            }
            ChangeType::TombstoneComputeGraph(request) => StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraphRequest((
                    DeleteComputeGraphRequest {
                        namespace: request.namespace.clone(),
                        name: request.compute_graph.clone(),
                    },
                    vec![state_change.clone()],
                )),
            },
            ChangeType::TombstoneInvocation(request) => StateMachineUpdateRequest {
                payload: RequestPayload::DeleteInvocationRequest((
                    DeleteInvocationRequest {
                        namespace: request.namespace.clone(),
                        compute_graph: request.compute_graph.clone(),
                        invocation_id: request.invocation_id.clone(),
                    },
                    vec![state_change.clone()],
                )),
            },
        };
        Ok(req)
    }
}
