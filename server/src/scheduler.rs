use std::{sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{ChangeType, StateChangeId};
use state_store::{
    requests::{
        CreateTasksRequest,
        ReductionTasks,
        RequestPayload,
        SchedulerUpdateRequest,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};
use task_scheduler::{
    task_creator::{handle_invoke_compute_graph, handle_task_finished},
    TaskScheduler,
};
use tokio::{self, sync::watch::Receiver};
use tracing::{error, info};

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

    pub async fn run_scheduler(&self) -> Result<()> {
        let state_changes = self
            .indexify_state
            .reader()
            .get_unprocessed_state_changes()?;
        let mut create_task_requests = vec![];
        let mut processed_state_changes = vec![];
        let mut new_reduction_tasks = vec![];
        let mut processed_reduction_tasks = vec![];
        let mut diagnostic_msgs = vec![];
        for state_change in &state_changes {
            processed_state_changes.push(state_change.id.clone());
            let result = match &state_change.change_type {
                ChangeType::InvokeComputeGraph(invoke_compute_graph_event) => Some(
                    handle_invoke_compute_graph(
                        self.indexify_state.clone(),
                        invoke_compute_graph_event.clone(),
                    )
                    .await?,
                ),
                ChangeType::TaskFinished(task_finished_event) => {
                    let task = self
                        .indexify_state
                        .reader()
                        .get_task_from_finished_event(&task_finished_event)?
                        .ok_or(anyhow!("task not found {}", task_finished_event.task_id))?;
                    let compute_graph = self
                        .indexify_state
                        .reader()
                        .get_compute_graph(&task.namespace, &task.compute_graph_name)?
                        .ok_or(anyhow!("compute graph not found"))?;
                    Some(
                        handle_task_finished(self.indexify_state.clone(), task, compute_graph)
                            .await?,
                    )
                }
                _ => None,
            };
            if let Some(result) = result {
                let request = CreateTasksRequest {
                    namespace: result.namespace.clone(),
                    invocation_id: result.invocation_id.clone(),
                    compute_graph: result.compute_graph.clone(),
                    tasks: result.tasks,
                };
                create_task_requests.push(request);
                new_reduction_tasks.extend(result.new_reduction_tasks);
                processed_reduction_tasks.extend(result.processed_reduction_tasks);
            }
        }
        let mut new_allocations = vec![];
        for state_change in &state_changes {
            let task_placement_result = match state_change.change_type {
                ChangeType::TaskCreated |
                ChangeType::ExecutorAdded |
                ChangeType::ExecutorRemoved => Some(self.task_allocator.schedule_unplaced_tasks()?),
                _ => None,
            };
            if let Some(task_placement_result) = task_placement_result {
                new_allocations.extend(task_placement_result.task_placements);
                diagnostic_msgs.extend(task_placement_result.diagnostic_msgs);
            }
        }

        let scheduler_update_request = StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdateRequest {
                task_requests: create_task_requests,
                allocations: new_allocations,
                reduction_tasks: ReductionTasks {
                    new_reduction_tasks,
                    processed_reduction_tasks,
                },
                diagnostic_msgs,
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
    use std::time::Duration;

    use data_model::{
        test_objects::tests::{
            mock_executor,
            mock_executor_id,
            mock_invocation_payload_graph_b,
            TEST_NAMESPACE,
        },
        ExecutorId,
        TaskOutcome,
    };
    use state_store::test_state_store::tests::TestStateStore;

    use super::*;
    use crate::executors::{self, ExecutorManager};

    #[tokio::test]
    async fn test_invoke_compute_graph_event_creates_tasks() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let invocation_id = state_store.with_simple_graph().await;
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let unprocessed_state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes()
            .unwrap();
        // Processes the invoke cg event and creates a task created event
        assert_eq!(unprocessed_state_changes.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn create_tasks_when_after_fn_finishes() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let invocation_id = state_store.with_simple_graph().await;
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        // Finish the task and check if new tasks are created
        state_store
            .finalize_task(task, 1, TaskOutcome::Success, false)
            .await
            .unwrap();
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3);
        let unprocessed_state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes()
            .unwrap();

        // has task crated state change in it.
        assert_eq!(unprocessed_state_changes.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn handle_failed_tasks() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let invocation_id = state_store.with_simple_graph().await;
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];

        // Finish the task and check if new tasks are created
        state_store
            .finalize_task(task, 1, TaskOutcome::Failure, false)
            .await
            .unwrap();
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let task = &tasks[0];

        let invocation_ctx = indexify_state
            .reader()
            .invocation_ctx(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            )
            .unwrap();

        assert!(invocation_ctx.completed);

        Ok(())
    }

    pub async fn schedule_all(indexify_state: &IndexifyState, scheduler: &Scheduler) -> Result<()> {
        let time = std::time::Instant::now();
        loop {
            if time.elapsed().as_secs() > 10 {
                return Err(anyhow!("timeout"));
            }
            let unprocessed_state_changes =
                indexify_state.reader().get_unprocessed_state_changes()?;
            if unprocessed_state_changes.is_empty() {
                return Ok(());
            }
            scheduler.run_scheduler().await?;
        }
    }

    #[tokio::test]
    async fn test_task_remove() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let ex = Arc::new(ExecutorManager::new(indexify_state.clone()).await);
        let invocation_id = state_store.with_simple_graph().await;
        ex.register_executor(mock_executor()).await?;

        schedule_all(&indexify_state, &scheduler).await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        let task = tasks.first().unwrap();
        state_store
            .finalize_task(&task, 1, TaskOutcome::Success, false)
            .await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 0);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_task_unassign() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let ex = Arc::new(ExecutorManager::new(indexify_state.clone()).await);
        let invocation_id = state_store.with_simple_graph().await;
        ex.register_executor(mock_executor()).await?;

        schedule_all(&indexify_state, &scheduler).await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        ex.deregister_executor(mock_executor_id()).await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 0);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_executor() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let invocation_id = state_store.with_simple_graph().await;
        let ex = Arc::new(ExecutorManager::new(indexify_state.clone()).await);

        schedule_all(&indexify_state, &scheduler).await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 1);

        ex.register_executor(mock_executor()).await?;

        schedule_all(&indexify_state, &scheduler).await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        executors::schedule_deregister(ex.clone(), mock_executor_id(), Duration::from_secs(1));

        let mut executor = mock_executor();
        executor.id = ExecutorId::new("2".to_string());
        ex.register_executor(executor.clone()).await?;

        let time = std::time::Instant::now();
        loop {
            schedule_all(&indexify_state, &scheduler).await?;

            let executor_tasks = indexify_state
                .reader()
                .get_tasks_by_executor(&executor.id, 10)?;
            if executor_tasks.len() == 1 {
                break;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;

            if time.elapsed().as_secs() > 10 {
                return Err(anyhow!("timeout"));
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_tasks_for_router_tasks() {
        let state_store = TestStateStore::new().await.unwrap();
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        let invocation_id = state_store.with_router_graph().await;
        scheduler.run_scheduler().await.unwrap();
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task_id = &tasks[0].id;

        // Finish the task and check if new tasks are created
        state_store
            .finalize_task_graph_b(&mock_invocation_payload_graph_b().id, task_id)
            .await
            .unwrap();
        scheduler.run_scheduler().await.unwrap();
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 2);
        let unprocessed_state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes()
            .unwrap();
        // has task crated state change in it.
        assert_eq!(unprocessed_state_changes.len(), 1);

        // Now finish the router task and we should have 3 tasks
        // The last one would be for the edge which the router picks
        let task_id = &tasks[1].id;
        state_store
            .finalize_router_x(&mock_invocation_payload_graph_b().id, task_id)
            .await
            .unwrap();
        scheduler.run_scheduler().await.unwrap();
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3);
    }
}
