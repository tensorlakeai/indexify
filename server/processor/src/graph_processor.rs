use std::{sync::Arc, vec};

use anyhow::Result;
use data_model::{ChangeType, StateChange};
use state_store::{
    requests::{
        DeleteComputeGraphRequest,
        DeleteInvocationRequest,
        FinalizeTaskRequest,
        MutateClusterTopologyRequest,
        NamespaceProcessorUpdateRequest,
        ReductionTasks,
        RequestPayload,
        StateMachineUpdateRequest,
        TaskAllocationUpdateRequest,
    },
    IndexifyState,
};
use tokio::sync::Notify;
use tracing::{error, info};

use crate::{
    task_allocator::{self, TaskPlacementResult},
    task_creator::{self, TaskCreationResult},
};

pub struct GraphProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub task_allocator: Arc<task_allocator::TaskAllocationProcessor>,
    pub task_creator: Arc<task_creator::TaskCreator>,
}

impl GraphProcessor {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        task_allocator: Arc<task_allocator::TaskAllocationProcessor>,
        task_creator: Arc<task_creator::TaskCreator>,
    ) -> Self {
        Self {
            indexify_state,
            task_allocator,
            task_creator,
        }
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
        // 1. First load 100 state changes. Process the `global` state changes first
        // and then the `ns_` state changes
        if cached_state_changes.is_empty() {
            let unprocessed_state_changes =
                self.indexify_state.reader().unprocessed_state_changes(
                    &last_global_state_change_cursor,
                    &last_namespace_state_change_cursor,
                )?;
            let _ = match unprocessed_state_changes.last_global_state_change_cursor {
                Some(cursor) => {
                    last_global_state_change_cursor.replace(cursor);
                }
                None => {}
            };
            let _ = match unprocessed_state_changes.last_namespace_state_change_cursor {
                Some(cursor) => {
                    last_namespace_state_change_cursor.replace(cursor);
                }
                None => {}
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

        // 3. Fire a notification to wake ourselves up to see if there are more than 100
        //    state changes
        notify.notify_one();

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
                    payload: RequestPayload::Noop,
                    processed_state_changes: vec![state_change.clone()],
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
                    payload: RequestPayload::Noop,
                    processed_state_changes: vec![state_change.clone()],
                })
                .await?;
        }
        Ok(())
    }

    pub async fn handle_state_change(
        &self,
        state_change: &StateChange,
    ) -> Result<StateMachineUpdateRequest> {
        info!("processing state change: {}", state_change.change_type);
        match &state_change.change_type {
            ChangeType::InvokeComputeGraph(event) => {
                info!("invoking compute graph: {:?}", event);
                let task_creation_result = self
                    .task_creator
                    .handle_invoke_compute_graph(event.clone())
                    .await?;
                Ok(task_creation_result_to_sm_update(
                    &event.namespace,
                    &event.compute_graph,
                    &event.invocation_id,
                    task_creation_result,
                    &state_change,
                ))
            }
            ChangeType::TaskOutputsIngested(event) => Ok(StateMachineUpdateRequest {
                payload: RequestPayload::FinalizeTask(FinalizeTaskRequest {
                    namespace: event.namespace.clone(),
                    compute_graph: event.compute_graph.clone(),
                    compute_fn: event.compute_fn.clone(),
                    invocation_id: event.invocation_id.clone(),
                    task_id: event.task_id.clone(),
                    task_outcome: event.outcome.clone(),
                    executor_id: event.executor_id.clone(),
                    diagnostics: event.diagnostic.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            }),
            ChangeType::TaskFinalized(event) => {
                let task_creation_result = self
                    .task_creator
                    .handle_task_finished_inner(self.indexify_state.clone(), event)
                    .await?;
                Ok(task_creation_result_to_sm_update(
                    &event.namespace,
                    &event.compute_graph,
                    &event.invocation_id,
                    task_creation_result,
                    &state_change,
                ))
            }
            ChangeType::TombstoneComputeGraph(request) => Ok(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraphRequest(DeleteComputeGraphRequest {
                    namespace: request.namespace.clone(),
                    name: request.compute_graph.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            }),
            ChangeType::TombstoneInvocation(request) => Ok(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteInvocationRequest(DeleteInvocationRequest {
                    namespace: request.namespace.clone(),
                    compute_graph: request.compute_graph.clone(),
                    invocation_id: request.invocation_id.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            }),
            ChangeType::ExecutorAdded(_event) => {
                if let Err(err) = self.task_allocator.refresh_executors() {
                    error!("error refreshing executors: {:?}", err);
                }
                let result = self.task_allocator.schedule_unplaced_tasks();
                if let Ok(result) = result {
                    Ok(task_placement_result_to_sm_update(result, &state_change))
                } else {
                    error!("error scheduling unplaced tasks: {:?}", result.err());
                    Ok(StateMachineUpdateRequest {
                        payload: RequestPayload::Noop,
                        processed_state_changes: vec![state_change.clone()],
                    })
                }
            }
            ChangeType::ExecutorRemoved(_event) => {
                if let Err(err) = self.task_allocator.refresh_executors() {
                    error!("error refreshing executors: {:?}", err);
                }
                let task_allocation = self.task_allocator.schedule_unplaced_tasks()?;
                if task_allocation.task_placements.is_empty() {
                    Ok(StateMachineUpdateRequest {
                        payload: RequestPayload::Noop,
                        processed_state_changes: vec![state_change.clone()],
                    })
                } else {
                    Ok(task_placement_result_to_sm_update(
                        task_allocation,
                        &state_change,
                    ))
                }
            }
            ChangeType::TombStoneExecutor(event) => {
                info!("tombstone executor {:?}", event);
                Ok(StateMachineUpdateRequest {
                    payload: RequestPayload::MutateClusterTopology(MutateClusterTopologyRequest {
                        executor_removed: event.executor_id.clone(),
                    }),
                    processed_state_changes: vec![state_change.clone()],
                })
            }
            ChangeType::TaskCreated(event) => {
                let result = self
                    .task_allocator
                    .schedule_tasks(vec![event.task.clone()])?;
                Ok(task_placement_result_to_sm_update(result, &state_change))
            }
        }
    }
}

fn task_creation_result_to_sm_update(
    ns: &str,
    compute_graph: &str,
    invocation_id: &str,
    task_creation_result: TaskCreationResult,
    state_change: &StateChange,
) -> StateMachineUpdateRequest {
    StateMachineUpdateRequest {
        payload: RequestPayload::NamespaceProcessorUpdate(NamespaceProcessorUpdateRequest {
            namespace: ns.to_string(),
            compute_graph: compute_graph.to_string(),
            invocation_id: invocation_id.to_string(),
            task_requests: task_creation_result.tasks,
            reduction_tasks: ReductionTasks {
                new_reduction_tasks: task_creation_result.new_reduction_tasks,
                processed_reduction_tasks: task_creation_result.processed_reduction_tasks,
            },
        }),
        processed_state_changes: vec![state_change.clone()],
    }
}

fn task_placement_result_to_sm_update(
    task_placement_result: TaskPlacementResult,
    state_change: &StateChange,
) -> StateMachineUpdateRequest {
    StateMachineUpdateRequest {
        payload: RequestPayload::TaskAllocationProcessorUpdate(TaskAllocationUpdateRequest {
            allocations: task_placement_result.task_placements,
            unplaced_task_keys: task_placement_result.unplaced_task_keys,
            placement_diagnostics: task_placement_result.placement_diagnostics,
        }),
        processed_state_changes: vec![state_change.clone()],
    }
}
