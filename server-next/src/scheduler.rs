use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    InvokeComputeGraphEvent,
    OutputPayload,
    StateChangeId,
    Task,
    TaskFinishedEvent,
};
use state_store::{
    requests::{
        CreateTasksRequest,
        RequestPayload,
        SchedulerUpdateRequest,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};
use task_scheduler::TaskScheduler;
use tokio::{self, sync::watch::Receiver};
use tracing::{error, info};

#[derive(Debug)]
struct TaskCreationResult {
    namespace: String,
    compute_graph: String,
    tasks: Vec<Task>,
    invocation_finished: bool,
    invocation_id: String,
}
pub struct Scheduler {
    indexify_state: Arc<IndexifyState>,
    task_allocator: Arc<TaskScheduler>,
}

impl Scheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let task_allocator = Arc::new(TaskScheduler::new(indexify_state.clone()));
        Self {
            indexify_state,
            task_allocator,
        }
    }

    async fn handle_invoke_compute_graph(
        &self,
        event: InvokeComputeGraphEvent,
    ) -> Result<TaskCreationResult> {
        let compute_graph = self
            .indexify_state
            .reader()
            .get_compute_graph(&event.namespace, &event.compute_graph)?;
        if compute_graph.is_none() {
            error!(
                "compute graph not found: {:?} {:?}",
                event.namespace, event.compute_graph
            );
            return Ok(TaskCreationResult {
                tasks: vec![],
                namespace: event.namespace.clone(),
                invocation_id: event.invocation_id.clone(),
                compute_graph: event.compute_graph.clone(),
                invocation_finished: false,
            });
        }
        let compute_graph = compute_graph.unwrap();
        // Crate a task for the compute graph
        let task = compute_graph.start_fn.create_task(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            &event.invocation_id,
        )?;
        Ok(TaskCreationResult {
            namespace: event.namespace.clone(),
            compute_graph: event.compute_graph.clone(),
            invocation_id: event.invocation_id.clone(),
            tasks: vec![task],
            invocation_finished: false,
        })
    }

    async fn handle_task_finished(
        &self,
        task_finished_event: &TaskFinishedEvent,
    ) -> Result<TaskCreationResult> {
        let task = self
            .indexify_state
            .reader()
            .get_task(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.invocation_id,
                &task_finished_event.compute_fn,
                &task_finished_event.task_id.to_string(),
            )?
            .ok_or(anyhow!("task not found: {:?}", task_finished_event))?;

        let compute_graph = self
            .indexify_state
            .reader()
            .get_compute_graph(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
            )?
            .ok_or(anyhow!(
                "compute graph not found: {:?} {:?}",
                task_finished_event.namespace,
                task_finished_event.compute_graph
            ))?;
        // Find the edges
        let edges = compute_graph.edges.get(&task_finished_event.compute_fn);
        if edges.is_none() {
            let invocation_ctx = self.indexify_state.reader().invocation_ctx(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.invocation_id,
            )?;
            if invocation_ctx.outstanding_tasks == 0 {
                info!(
                    "compute graph completed: {:?}",
                    task_finished_event.compute_graph
                );
                return Ok(TaskCreationResult {
                    namespace: task_finished_event.namespace.clone(),
                    compute_graph: task_finished_event.compute_graph.clone(),
                    invocation_id: task_finished_event.invocation_id.clone(),
                    tasks: vec![],
                    invocation_finished: true,
                });
            }
        }

        let mut out_edges = Vec::from_iter(edges.iter().cloned().flatten());

        // Get all the outputs of the compute fn
        let outputs = self.indexify_state.reader().get_task_outputs(
            &task_finished_event.namespace,
            &task_finished_event.task_id.to_string(),
        )?;
        for output in &outputs {
            if let OutputPayload::Router(router_output) = &output.payload {
                for edge in &router_output.edges {
                    out_edges.push(edge);
                }
            }
        }

        let mut new_tasks = vec![];
        for edge in out_edges {
            for output in &outputs {
                let compute_fn = compute_graph.nodes.get(edge);
                if compute_fn.is_none() {
                    error!("compute fn not found: {:?}", edge);
                    continue;
                }
                let compute_fn = compute_fn.unwrap();
                let new_task = compute_fn.create_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &output.id,
                    &task.invocation_id,
                )?;
                new_tasks.push(new_task);
            }
        }
        Ok(TaskCreationResult {
            namespace: task_finished_event.namespace.clone(),
            compute_graph: task_finished_event.compute_graph.clone(),
            invocation_id: task_finished_event.invocation_id.clone(),
            tasks: new_tasks,
            invocation_finished: false,
        })
    }

    pub async fn run_scheduler(&self) -> Result<()> {
        let state_changes = self
            .indexify_state
            .reader()
            .get_unprocessed_state_changes()?;
        let mut create_task_requests = vec![];
        let mut processed_state_changes = vec![];
        for state_change in &state_changes {
            processed_state_changes.push(state_change.id.clone());
            let result = match &state_change.change_type {
                ChangeType::InvokeComputeGraph(invoke_compute_graph_event) => Some(
                    self.handle_invoke_compute_graph(invoke_compute_graph_event.clone())
                        .await?,
                ),
                ChangeType::TaskFinished(task_finished_event) => {
                    Some(self.handle_task_finished(task_finished_event).await?)
                }
                _ => None,
            };
            if let Some(result) = result {
                let request = CreateTasksRequest {
                    namespace: result.namespace.clone(),
                    invocation_id: result.invocation_id.clone(),
                    compute_graph: result.compute_graph.clone(),
                    invocation_finished: result.invocation_finished,
                    tasks: result.tasks,
                };
                create_task_requests.push(request);
            }
        }
                let mut new_allocations = vec![];
        for state_change in state_changes {
            let allocations = match state_change.change_type {
                ChangeType::TaskCreated => Some(self.task_allocator.schedule_unplaced_tasks()?),
                ChangeType::ExecutorAdded => None,
                ChangeType::ExecutorRemoved => {
                    let executor_id = &state_change.object_id;
                    Some(self.task_allocator.reschedule_tasks(executor_id)?)
                }
                _ => None,
            };
            if let Some(allocations) = allocations {
                new_allocations.extend(allocations);
            }
        }
        let scheduler_update_request = StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdateRequest {
                task_requests: create_task_requests,
                allocations: new_allocations,
            }),
            state_changes_processed: processed_state_changes,
        };
        self.indexify_state.write(scheduler_update_request).await
    }

    pub async fn start(
        &self,
        mut shutdown_rx: Receiver<()>,
        mut state_watcher_rx: Receiver<StateChangeId>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = state_watcher_rx.changed() => {
                       let _state_change = *state_watcher_rx.borrow_and_update();
                       if let Err(err) = self.run_scheduler().await {
                              error!("error processing and distributing work: {:?}", err);
                       }
                },
                _ = shutdown_rx.changed() => {
                    info!("scheduler shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use data_model::test_objects::tests::{mock_invocation_payload, TEST_NAMESPACE};
    use state_store::test_state_store::tests::TestStateStore;

    use super::*;

    #[tokio::test]
    async fn test_invoke_compute_graph_event_creates_tasks() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(
                TEST_NAMESPACE,
                "graph_A",
                &state_store.invocation_payload_id,
                None,
                None,
            )
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let unprocessed_state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes()
            .unwrap();
        assert_eq!(unprocessed_state_changes.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn create_tasks_when_after_fn_finishes() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(
                TEST_NAMESPACE,
                "graph_A",
                &state_store.invocation_payload_id,
                None,
                None,
            )
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task_id = &tasks[0].id;

        // Finish the task and check if new tasks are created
        state_store
            .finalize_task(&mock_invocation_payload().id, task_id)
            .await
            .unwrap();
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(
                TEST_NAMESPACE,
                "graph_A",
                &state_store.invocation_payload_id,
                None,
                None,
            )
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3);
        let unprocessed_state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes()
            .unwrap();
        assert_eq!(unprocessed_state_changes.len(), 0);
        Ok(())
    }
}
