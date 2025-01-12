use std::sync::Arc;

use anyhow::Result;
use data_model::{ChangeType, ProcessorId, ProcessorType, StateChange};
use state_store::{
    requests::{
        MutateClusterTopologyRequest,
        NamespaceProcessorUpdateRequest,
        ProcessedStateChange,
        ReductionTasks,
        RequestPayload,
        StateMachineUpdateRequest,
        TaskAllocationUpdateRequest,
    },
    IndexifyState,
};
use tokio::sync::Notify;

use crate::{
    dispatcher::Dispatcher,
    namespace::TaskCreationResult,
    task_allocator1::{self, TaskPlacementResult},
};

pub struct GraphProcessor {
    pub indexify_state: Arc<IndexifyState<Dispatcher>>,
    pub task_allocator: task_allocator1::TaskAllocationProcessor,
}

impl GraphProcessor {
    pub fn new(
        indexify_state: Arc<IndexifyState<Dispatcher>>,
        task_allocator: task_allocator1::TaskAllocationProcessor,
    ) -> Self {
        Self {
            indexify_state,
            task_allocator,
        }
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut change_events_rx = self.indexify_state.change_events_rx.clone();
        // Used to run the loop when there are more than 1 change events queued up
        // The watch from the state store only notifies that there are N number of state
        // changes but if we only process one event from the queue then the
        // watch will not notify again
        let notify = Arc::new(Notify::new());
        loop {
            change_events_rx.borrow_and_update();
            tokio::select! {
                _ = change_events_rx.changed() => {
                    let sm_update = self.handle_state_change().await;
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
                    let sm_update = self.handle_state_change().await;
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

    pub async fn handle_state_change(&self) -> Result<Option<StateMachineUpdateRequest>> {
        let state_change = self.indexify_state.reader().get_next_state_change()?;
        if state_change.is_none() {
            return Ok(None);
        }
        let state_change = state_change.unwrap();
        let scheduler_update = match &state_change.change_type {
            ChangeType::InvokeComputeGraph(event) => {
                let task_creation_result = super::namespace::handle_invoke_compute_graph(
                    self.indexify_state.clone(),
                    event.clone(),
                )
                .await?;
                Ok(Some(task_creation_result_to_sm_update(
                    task_creation_result,
                    &state_change,
                )))
            }
            ChangeType::TaskFinished(event) => {
                let task_creation_result = super::namespace::handle_task_finished_inner(
                    self.indexify_state.clone(),
                    event,
                )
                .await?;
                Ok(Some(task_creation_result_to_sm_update(
                    task_creation_result,
                    &state_change,
                )))
            }
            ChangeType::TombstoneComputeGraph(_) => Ok(None),
            ChangeType::TombstoneInvocation(_) => Ok(None),
            ChangeType::ExecutorAdded => Ok(None),
            ChangeType::ExecutorRemoved(event) => Ok(Some(StateMachineUpdateRequest {
                payload: RequestPayload::MutateClusterTopology(MutateClusterTopologyRequest {
                    executor_removed: event.executor_id.clone(),
                }),
                process_state_change: Some(ProcessedStateChange {
                    processor_id: ProcessorId::new(ProcessorType::Namespace),
                    state_changes: vec![state_change.clone()],
                }),
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
    task_creation_result: TaskCreationResult,
    state_change: &StateChange,
) -> StateMachineUpdateRequest {
    StateMachineUpdateRequest {
        payload: RequestPayload::NamespaceProcessorUpdate(NamespaceProcessorUpdateRequest {
            task_requests: task_creation_result.tasks,
            reduction_tasks: ReductionTasks {
                new_reduction_tasks: task_creation_result.new_reduction_tasks,
                processed_reduction_tasks: task_creation_result.processed_reduction_tasks,
            },
        }),
        process_state_change: Some(ProcessedStateChange {
            state_changes: vec![state_change.clone()],
            processor_id: ProcessorId::new(ProcessorType::Namespace),
        }),
    }
}

fn task_placement_result_to_sm_update(
    task_placement_result: TaskPlacementResult,
    state_change: &StateChange,
) -> StateMachineUpdateRequest {
    StateMachineUpdateRequest {
        payload: RequestPayload::TaskAllocationProcessorUpdate(TaskAllocationUpdateRequest {
            allocations: task_placement_result.task_placements,
            placement_diagnostics: task_placement_result.placement_diagnostics,
        }),
        process_state_change: Some(ProcessedStateChange {
            processor_id: ProcessorId::new(ProcessorType::Namespace),
            state_changes: vec![state_change.clone()],
        }),
    }
}
