use std::{sync::Arc, vec};

use anyhow::Result;
use data_model::{ChangeType, StateChange};
use state_store::{
    requests::{
        DeleteComputeGraphRequest,
        DeleteInvocationRequest,
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
        // Used to run the loop when there are more than 1 change events queued up
        // The watch from the state store only notifies that there are N number of state
        // changes but if we only process one event from the queue then the
        // watch will not notify again
        let notify = Arc::new(Notify::new());
        loop {
            tokio::select! {
                _ = change_events_rx.changed() => {
                    change_events_rx.borrow_and_update();
                    let sm_update = self.handle_state_change(&mut cached_state_changes).await;
                    if let Err(err) = &sm_update {
                        tracing::error!("error processing state change: {:?}", err);
                        continue
                    }
                    if let Ok(Some(sm_update)) = sm_update {
                        notify.notify_one();
                        if let Err(err) = self.indexify_state.write(sm_update).await {
                            tracing::error!("error writing state change: {:?}", err);
                        }
                    }
                },
                _ = notify.notified() => {
                    let sm_update = self.handle_state_change(&mut cached_state_changes).await;
                    if let Err(err) = &sm_update {
                        tracing::error!("error processing state change: {:?}", err);
                        continue
                    }
                    if let Ok(Some(sm_update)) = sm_update {
                        notify.notify_one();
                        if let Err(err) = self.indexify_state.write(sm_update).await {
                            tracing::error!("error writing state change: {:?}", err);
                        }
                    }
                },
                _ = shutdown_rx.changed() => {
                    tracing::info!("graph processor shutting down");
                    break;
                }
            }
        }
    }

    pub async fn handle_state_change(
        &self,
        cached_state_changes: &mut Vec<StateChange>,
    ) -> Result<Option<StateMachineUpdateRequest>> {
        if cached_state_changes.is_empty() {
            cached_state_changes.extend(self.indexify_state.reader().unprocessed_state_changes()?);
        }
        if cached_state_changes.is_empty() {
            return Ok(None);
        }
        let state_change = cached_state_changes.pop().unwrap();
        let scheduler_update = match &state_change.change_type {
            ChangeType::InvokeComputeGraph(event) => {
                let task_creation_result = self
                    .task_creator
                    .handle_invoke_compute_graph(event.clone())
                    .await?;
                Ok(Some(task_creation_result_to_sm_update(
                    &event.namespace,
                    &event.compute_graph,
                    &event.invocation_id,
                    task_creation_result,
                    &state_change,
                )))
            }
            ChangeType::TaskFinished(event) => {
                let task_creation_result = self
                    .task_creator
                    .handle_task_finished_inner(self.indexify_state.clone(), event)
                    .await?;
                Ok(Some(task_creation_result_to_sm_update(
                    &event.namespace,
                    &event.compute_graph,
                    &event.invocation_id,
                    task_creation_result,
                    &state_change,
                )))
            }
            ChangeType::TombstoneComputeGraph(request) => Ok(Some(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraphRequest(DeleteComputeGraphRequest {
                    namespace: request.namespace.clone(),
                    name: request.compute_graph.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            })),
            ChangeType::TombstoneInvocation(request) => Ok(Some(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteInvocationRequest(DeleteInvocationRequest {
                    namespace: request.namespace.clone(),
                    compute_graph: request.compute_graph.clone(),
                    invocation_id: request.invocation_id.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            })),
            ChangeType::ExecutorAdded => {
                let result = self.task_allocator.schedule_unplaced_tasks()?;
                Ok(Some(task_placement_result_to_sm_update(
                    result,
                    &state_change,
                )))
            }
            ChangeType::ExecutorRemoved(event) => Ok(Some(StateMachineUpdateRequest {
                payload: RequestPayload::MutateClusterTopology(MutateClusterTopologyRequest {
                    executor_removed: event.executor_id.clone(),
                }),
                processed_state_changes: vec![state_change.clone()],
            })),
            ChangeType::TaskCreated(event) => {
                let result = self
                    .task_allocator
                    .schedule_tasks(vec![event.task.clone()])?;
                Ok(Some(task_placement_result_to_sm_update(
                    result,
                    &state_change,
                )))
            }
        };
        scheduler_update
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
