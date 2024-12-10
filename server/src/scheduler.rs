use std::{sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{ChangeType, StateChangeId};
use metrics::{scheduler_stats, Timer};
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
use tracing::{error, info, instrument, span};

pub struct Scheduler {
    indexify_state: Arc<IndexifyState>,
    task_allocator: Arc<TaskScheduler>,
    metrics: Arc<scheduler_stats::Metrics>,
}

impl Scheduler {
    pub fn new(indexify_state: Arc<IndexifyState>, metrics: Arc<scheduler_stats::Metrics>) -> Self {
        let task_allocator = Arc::new(TaskScheduler::new(indexify_state.clone()));
        Self {
            indexify_state,
            task_allocator,
            metrics,
        }
    }

    #[instrument(skip(self))]
    pub async fn run_scheduler(&self) -> Result<()> {
        let _timer = Timer::start(&self.metrics.scheduler_invocations);
        let state_changes = self
            .indexify_state
            .reader()
            .get_unprocessed_state_changes()?;

        for state_change in &state_changes {
            let mut create_task_requests = vec![];
            let mut processed_state_change_ids = vec![];
            let mut new_reduction_tasks = vec![];
            let mut processed_reduction_tasks = vec![];
            let mut diagnostic_msgs = vec![];
            let requires_task_allocation = state_change.change_type == ChangeType::TaskCreated ||
                state_change.change_type == ChangeType::ExecutorAdded ||
                state_change.change_type == ChangeType::ExecutorRemoved;
            let span = span!(
                tracing::Level::INFO,
                "process_state_change",
                state_change_id = state_change.id.to_string(),
                change_type = state_change.change_type.as_ref(),
            );
            let _enter = span.enter();
            match self.process_state_change(state_change).await {
                Ok(result) => {
                    processed_state_change_ids.push(state_change.id);

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
                Err(err) => {
                    error!("error processing state change: {:?}", err);
                    continue;
                }
            }

            let mut new_allocations = vec![];
            if requires_task_allocation {
                let task_placement_result = self.task_allocator.schedule_unplaced_tasks()?;
                new_allocations.extend(task_placement_result.task_placements);
                diagnostic_msgs.extend(task_placement_result.diagnostic_msgs);
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
                state_changes_processed: processed_state_change_ids,
            };
            if let Err(err) = self.indexify_state.write(scheduler_update_request).await {
                error!("error writing scheduler update request: {:?}", err);
            }
        }

        Ok(())
    }

    async fn process_state_change(
        &self,
        state_change: &data_model::StateChange,
    ) -> Result<Option<task_scheduler::TaskCreationResult>> {
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
                    .get_task_from_finished_event(task_finished_event)
                    .map_err(|e| {
                        error!("error getting task from finished event: {:?}", e);
                        e
                    })?;
                if task.is_none() {
                    error!(
                        "task not found for task finished event: {}",
                        task_finished_event.task_id
                    );
                    return Ok(None);
                }
                let task =
                    task.ok_or(anyhow!("task not found: {}", task_finished_event.task_id))?;
                let compute_graph = self
                    .indexify_state
                    .reader()
                    .get_compute_graph(&task.namespace, &task.compute_graph_name)
                    .map_err(|e| {
                        error!("error getting compute graph: {:?}", e);
                        e
                    })?;
                if compute_graph.is_none() {
                    error!(
                        "compute graph not found: {:?} {:?}",
                        task.namespace, task.compute_graph_name
                    );
                    return Ok(None);
                }
                let compute_graph = compute_graph.ok_or(anyhow!(
                    "compute graph not found: {:?} {:?}",
                    task.namespace,
                    task.compute_graph_name
                ))?;
                Some(handle_task_finished(self.indexify_state.clone(), task, compute_graph).await?)
            }
            _ => None,
        };
        Ok(result)
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
    use std::{collections::HashMap, time::Duration};

    use data_model::{
        test_objects::tests::{
            mock_executor,
            mock_executor_id,
            mock_invocation_payload_graph_b,
            mock_node_fn_output,
            reducer_fn,
            test_compute_fn,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        ComputeGraph,
        ComputeGraphCode,
        DataPayload,
        ExecutorId,
        GraphVersion,
        InvocationPayloadBuilder,
        Node,
        RuntimeInformation,
        StateChange,
        Task,
        TaskId,
        TaskOutcome,
    };
    use state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            FinalizeTaskRequest,
            InvokeComputeGraphRequest,
        },
        serializer::{JsonEncode, JsonEncoder},
        state_machine::IndexifyObjectsColumns,
        test_state_store::tests::TestStateStore,
    };
    use tracing_subscriber::{layer::SubscriberExt, Layer};

    use super::*;
    use crate::executors::{self, ExecutorManager};

    #[tokio::test]
    async fn test_invoke_compute_graph_event_creates_tasks() -> Result<()> {
        let state_store = TestStateStore::new().await?;
        let indexify_state = state_store.indexify_state.clone();
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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

        assert!(invocation_ctx.unwrap().completed);

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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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
            .finalize_task(task, 1, TaskOutcome::Success, false)
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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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
        let scheduler = Scheduler::new(
            indexify_state.clone(),
            Arc::new(scheduler_stats::Metrics::new(
                indexify_state.metrics.clone(),
            )),
        );
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

    // TODO write edge case test case when all fn_map finish state changes are
    // handled in the same runloop of the executor!

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::new("trace");
        // ignore error when set in multiple tests.
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;
        let state = IndexifyState::new(temp_dir.path().join("state")).await?;
        let scheduler = Scheduler::new(
            state.clone(),
            Arc::new(scheduler_stats::Metrics::new(state.metrics.clone())),
        );

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", None);
            let fn_map = test_compute_fn("fn_map", None);
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", None);
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion(1),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks: Vec<Task> = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(3, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        state_changes_processed: vec![],
                    })
                    .await?;
            }
            scheduler.run_scheduler().await?;
        }

        // complete all fn_reduce tasks
        for _ in 0..3 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert_eq!(graph_ctx.completed, true);
        }

        Ok(())
    }

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_first_reducer_finish_before_parent_finish() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::new("trace");
        // ignore error when set in multiple tests.
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;
        let state = IndexifyState::new(temp_dir.path().join("state")).await?;
        let scheduler = Scheduler::new(
            state.clone(),
            Arc::new(scheduler_stats::Metrics::new(state.metrics.clone())),
        );

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", None);
            let fn_map = test_compute_fn("fn_map", None);
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", None);
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion(1),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks: Vec<Task> = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}", pending_tasks);

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &reduce_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(2, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        state_changes_processed: vec![],
                    })
                    .await?;

                // FIXME: Batching all tasks will currently break the reducing.
                scheduler.run_scheduler().await?;
            }
        }

        for _ in 0..2 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
            let pending_task = pending_tasks.first().unwrap();
            assert_eq!(pending_task.compute_fn_name, "fn_convert");

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }
        {
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_first_reducer_error_before_parent_finish() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::new("trace");
        // ignore error when set in multiple tests.
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;
        let state = IndexifyState::new(temp_dir.path().join("state")).await?;
        let scheduler = Scheduler::new(
            state.clone(),
            Arc::new(scheduler_stats::Metrics::new(state.metrics.clone())),
        );

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", None);
            let fn_map = test_compute_fn("fn_map", None);
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", None);
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion(1),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks: Vec<Task> = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}", pending_tasks);

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            // Completing all fn_map tasks
            let request = FinalizeTaskRequest {
                namespace: invocation_payload.namespace.clone(),
                compute_graph: invocation_payload.compute_graph_name.clone(),
                compute_fn: "fn_reduce".to_string(),
                invocation_id: invocation_payload.id.clone(),
                task_id: reduce_task.id.clone(),
                node_outputs: vec![],
                task_outcome: TaskOutcome::Failure, // Failure!
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(2, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        state_changes_processed: vec![],
                    })
                    .await?;

                // FIXME: Batching all tasks will currently break the reducing.
                scheduler.run_scheduler().await?;
            }
        }

        {
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    // test_reducer_graph_reducer_finalize_just_before_map
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce (reduce task finalize before next map tasks)
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_reducer_finalize_just_before_map() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::new("trace");
        // ignore error when set in multiple tests.
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;
        let state = IndexifyState::new(temp_dir.path().join("state")).await?;
        let scheduler = Scheduler::new(
            state.clone(),
            Arc::new(scheduler_stats::Metrics::new(state.metrics.clone())),
        );

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", None);
            let fn_map = test_compute_fn("fn_map", None);
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", None);
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion(1),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:#?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks: Vec<Task> = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        let pending_map_tasks = check_pending_tasks(3, "fn_map")?;
        {
            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let task = pending_map_tasks[1].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            // NOT running scheduler yet
        }

        // HACK: We need to finalize the reducer task before the map task, but without
        // saving it.
        let all_unprocessed_state_changes_reduce = {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                2,
                "pending tasks: {:#?}",
                pending_tasks
            );

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            let all_unprocessed_state_changes_before =
                state.reader().get_unprocessed_state_changes()?;

            let request = make_finalize_request("fn_reduce", &reduce_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            let all_unprocessed_state_changes_reduce: Vec<StateChange> = state
                .reader()
                .get_unprocessed_state_changes()?
                .iter()
                .filter(|sc| {
                    !all_unprocessed_state_changes_before
                        .iter()
                        .any(|sc_before| sc_before.id == sc.id)
                })
                .cloned()
                .collect();

            // Need to finalize without persisting the state change
            for state_change in all_unprocessed_state_changes_reduce.clone() {
                state.db.delete_cf(
                    &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&state.db),
                    &state_change.id.to_key(),
                )?;
            }

            let ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(
                ctx.get_task_analytics("fn_reduce").unwrap().pending_tasks,
                0
            );
            assert_eq!(
                ctx.get_task_analytics("fn_reduce")
                    .unwrap()
                    .successful_tasks,
                1
            );

            // running scheduler for previous map
            scheduler.run_scheduler().await?;

            all_unprocessed_state_changes_reduce
        };

        {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 2, "pending tasks: {:?}", pending_tasks);

            let task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_map")
                .unwrap();

            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            // Persisting state change
            for state_change in all_unprocessed_state_changes_reduce {
                let serialized_state_change = JsonEncoder::encode(&state_change)?;
                state.db.put_cf(
                    &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&state.db),
                    &state_change.id.to_key(),
                    serialized_state_change,
                )?;
            }

            // running scheduler for reducer
            scheduler.run_scheduler().await?;
        }

        for _ in 0..2 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }
        {
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reducer_graph_reducer_parent_errors() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::new("trace");
        // ignore error when set in multiple tests.
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;
        let state = IndexifyState::new(temp_dir.path().join("state")).await?;
        let scheduler = Scheduler::new(
            state.clone(),
            Arc::new(scheduler_stats::Metrics::new(state.metrics.clone())),
        );

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", None);
            let fn_map = test_compute_fn("fn_map", None);
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", None);
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion(1),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
            let tasks = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )
                .unwrap()
                .0;

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let tasks: Vec<Task> = state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        {
            let pending_tasks = check_pending_tasks(3, "fn_map")?;

            let request =
                make_finalize_request(&pending_tasks[0].compute_fn_name, &pending_tasks[0].id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            // FAIL second fn_map task
            let request = FinalizeTaskRequest {
                namespace: invocation_payload.namespace.clone(),
                compute_graph: invocation_payload.compute_graph_name.clone(),
                compute_fn: "fn_map".to_string(),
                invocation_id: invocation_payload.id.clone(),
                task_id: pending_tasks[1].id.clone(),
                node_outputs: vec![],
                task_outcome: TaskOutcome::Failure, // Failure!
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            let request =
                make_finalize_request(&pending_tasks[2].compute_fn_name, &pending_tasks[2].id, 1);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;

            scheduler.run_scheduler().await?;
        }

        // Expect no more tasks and a completed graph
        {
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }
}
