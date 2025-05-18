use std::sync::Arc;

use anyhow::Result;
use blob_store::BlobStorageConfig;
use data_model::{
    test_objects::tests::mock_node_fn_output,
    DataPayload,
    ExecutorId,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionExecutorState,
    Task,
    TaskDiagnostics,
    TaskOutcome,
    TaskStatus,
};
use nanoid::nanoid;
use state_store::{
    in_memory_state::DesiredExecutorState,
    requests::{
        DeregisterExecutorRequest,
        IngestTaskOutputsRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    state_machine::IndexifyObjectsColumns,
};
use tracing::subscriber;
use tracing_subscriber::{layer::SubscriberExt, Layer};

use crate::{
    config::ServerConfig,
    executor_api::executor_api_pb::{TaskAllocation, TaskResult},
    service::Service,
};

pub struct TaskStateAssertions {
    pub total: usize,
    pub allocated: usize,
    pub unallocated: usize,
    pub completed_success: usize,
}

impl Default for TaskStateAssertions {
    fn default() -> Self {
        Self {
            total: 0,
            allocated: 0,
            unallocated: 0,
            completed_success: 0,
        }
    }
}

pub struct ExecutorStateAssertions {
    pub num_func_executors: usize,
    pub num_allocated_tasks: usize,
}

impl Default for ExecutorStateAssertions {
    fn default() -> Self {
        Self {
            num_func_executors: 0,
            num_allocated_tasks: 0,
        }
    }
}

pub struct TestService {
    pub service: Service,
    // keeping a reference to the temp dir to ensure it is not deleted
    #[allow(dead_code)]
    temp_dir: tempfile::TempDir,
}

impl TestService {
    pub async fn new() -> Result<Self> {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("trace"));
        let _ = subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;

        let cfg = ServerConfig {
            state_store_path: temp_dir
                .path()
                .join("state_store")
                .to_str()
                .unwrap()
                .to_string(),
            blob_storage: BlobStorageConfig {
                path: format!(
                    "file://{}",
                    temp_dir.path().join("blob_store").to_str().unwrap()
                ),
            },
            ..Default::default()
        };
        let srv = Service::new(cfg).await?;

        Ok(Self {
            service: srv,
            temp_dir,
        })
    }

    pub async fn process_all_state_changes(&self) -> Result<()> {
        while !self
            .service
            .indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?
            .changes
            .is_empty()
        {
            self.process_graph_processor().await?;
        }
        Ok(())
    }

    async fn process_graph_processor(&self) -> Result<()> {
        let notify = Arc::new(tokio::sync::Notify::new());
        let mut cached_state_changes = self
            .service
            .indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?
            .changes;
        while !cached_state_changes.is_empty() {
            self.service
                .graph_processor
                .write_sm_update(&mut cached_state_changes, &mut None, &mut None, &notify)
                .await?;
        }
        Ok(())
    }

    pub async fn create_executor(&self, executor: ExecutorMetadata) -> Result<TestExecutor> {
        let e = TestExecutor {
            executor_id: executor.id.clone(),
            test_service: self,
        };

        e.heartbeat(executor).await?;

        Ok(e)
    }

    pub async fn assert_task_states(&self, assertions: TaskStateAssertions) -> Result<()> {
        let tasks = self
            .service
            .indexify_state
            .reader()
            .get_all_rows_from_cf::<Task>(IndexifyObjectsColumns::Tasks)?
            .iter()
            .map(|r| r.1.clone())
            .collect::<Vec<_>>();
        assert_eq!(tasks.len(), assertions.total, "Total Tasks: {:#?}", tasks);

        let allocated_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Running)
            .collect::<Vec<_>>();
        assert_eq!(
            allocated_tasks.len(),
            assertions.allocated,
            "Allocated tasks: {}/{} - {:#?}",
            allocated_tasks.len(),
            tasks.len(),
            allocated_tasks,
        );

        let pending_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .collect::<Vec<_>>();
        assert_eq!(
            pending_tasks.len(),
            assertions.unallocated,
            "Pending tasks: {:#?}",
            pending_tasks
        );

        let pending_tasks_memory = self
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .tasks
            .clone();
        let pending_tasks_memory = pending_tasks_memory
            .iter()
            .filter(|(_k, t)| t.status == TaskStatus::Pending)
            .collect::<Vec<_>>();
        assert_eq!(
            pending_tasks_memory.len(),
            assertions.unallocated,
            "Pending tasks in mem store",
        );

        let completed_success_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Completed && t.outcome == TaskOutcome::Success)
            .collect::<Vec<_>>();
        assert_eq!(
            completed_success_tasks.len(),
            assertions.completed_success,
            "Tasks completed successfully: {:#?}",
            completed_success_tasks
        );

        let unallocated_tasks = self
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .unallocated_tasks
            .clone();
        assert_eq!(
            unallocated_tasks.len(),
            assertions.unallocated,
            "Unallocated tasks in mem store",
        );

        Ok(())
    }
}

pub struct FinalizeTaskArgs {
    pub num_outputs: i32,
    pub task_outcome: TaskOutcome,
    pub reducer_fn: Option<String>,
    pub diagnostics: Option<TaskDiagnostics>,
    pub allocation_id: String,
}

impl FinalizeTaskArgs {
    pub fn new(allocation_id: String) -> FinalizeTaskArgs {
        FinalizeTaskArgs {
            num_outputs: 1,
            task_outcome: TaskOutcome::Success,
            reducer_fn: None,
            diagnostics: None,
            allocation_id,
        }
    }

    pub fn task_outcome(mut self, task_outcome: TaskOutcome) -> FinalizeTaskArgs {
        self.task_outcome = task_outcome;
        self
    }

    pub fn diagnostics(mut self, stdout: bool, stderr: bool) -> FinalizeTaskArgs {
        self.diagnostics = Some(TaskDiagnostics {
            stdout: if stdout {
                Some(DataPayload {
                    path: format!("stdout_{}", uuid::Uuid::new_v4().to_string()),
                    size: 0,
                    sha256_hash: "".to_string(),
                })
            } else {
                None
            },
            stderr: if stderr {
                Some(DataPayload {
                    path: format!("stderr_{}", uuid::Uuid::new_v4().to_string()),
                    size: 0,
                    sha256_hash: "".to_string(),
                })
            } else {
                None
            },
        });
        self
    }
}

pub struct TestExecutor<'a> {
    pub executor_id: ExecutorId,
    pub test_service: &'a TestService,
}

impl TestExecutor<'_> {
    pub async fn heartbeat(&self, executor: ExecutorMetadata) -> Result<()> {
        self.test_service
            .service
            .executor_manager
            .heartbeat(executor)
            .await?;
        Ok(())
    }

    pub async fn update_function_executors(&self, functions: Vec<FunctionExecutor>) -> Result<()> {
        // First, get current executor state
        let mut executor = self.get_executor_server_state().await?;

        // Update function executors, preserving the status (important for unhealthy
        // function executor tests)
        executor.function_executors = functions.into_iter().map(|f| (f.id.clone(), f)).collect();

        // Update state hash and send heartbeat
        executor.state_hash = nanoid!();
        self.heartbeat(executor).await?;

        Ok(())
    }

    pub async fn mark_function_executors_as_running(&self) -> Result<()> {
        let fes = self
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|mut fe| {
                fe.state = FunctionExecutorState::Running;
                fe
            })
            .collect();

        self.update_function_executors(fes).await?;

        // Process state changes to ensure changes take effect
        self.test_service.process_all_state_changes().await?;

        Ok(())
    }

    pub async fn get_executor_server_state(&self) -> Result<ExecutorMetadata> {
        // Get the in-memory state first to check if executor exists
        let indexes = self
            .test_service
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .clone();

        // Get executor from in-memory state - this is the base executor without
        // complete function executors
        let base_executor = indexes
            .read()
            .unwrap()
            .executors
            .get(&self.executor_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Executor not found in state store"))?;

        // Clone base executor
        let mut executor = *base_executor.clone();

        // Use executor_manager to get current state - this has the most up-to-date
        // function executors and tasks
        let desired_executor_state = self
            .test_service
            .service
            .executor_manager
            .get_executor_state(&self.executor_id)
            .await;

        // Convert function executors from desired state to ExecutorMetadata format
        executor.function_executors = desired_executor_state
            .function_executors
            .into_iter()
            .filter_map(|desc| {
                // Create FunctionExecutor
                let function_executor: FunctionExecutor = desc.try_into().unwrap();

                Some((function_executor.id.clone(), function_executor))
            })
            .collect();

        Ok(executor)
    }

    pub async fn assert_state(&self, assertions: ExecutorStateAssertions) -> Result<()> {
        // Get desired state from executor manager
        let desired_state = self
            .test_service
            .service
            .executor_manager
            .get_executor_state(&self.executor_id)
            .await;

        // Check function executor count
        let func_executors_count = desired_state.function_executors.len();
        assert_eq!(
            assertions.num_func_executors, func_executors_count,
            "function executors: expected {}, got {}",
            assertions.num_func_executors, func_executors_count
        );

        // Check task allocation count
        let tasks_count = desired_state.task_allocations.len();
        assert_eq!(
            assertions.num_allocated_tasks, tasks_count,
            "tasks: expected {}, got {}",
            assertions.num_allocated_tasks, tasks_count
        );

        Ok(())
    }

    pub async fn desired_state(
        &self,
    ) -> crate::executor_api::executor_api_pb::DesiredExecutorState {
        let desired_state = self
            .test_service
            .service
            .executor_manager
            .get_executor_state(&self.executor_id)
            .await;
        desired_state
    }

    pub async fn deregister(&self) -> Result<()> {
        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: self.executor_id.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }

    pub async fn finalize_task(
        &self,
        task_allocation: &TaskAllocation,
        args: FinalizeTaskArgs,
    ) -> Result<()> {
        let node_outputs = (0..args.num_outputs)
            .map(|_| {
                mock_node_fn_output(
                    task_allocation.task.as_ref().unwrap().graph_invocation_id(),
                    task_allocation.task.as_ref().unwrap().graph_name(),
                    task_allocation.task.as_ref().unwrap().function_name(),
                    args.reducer_fn.clone(),
                )
            })
            .collect();

        // get the task from the state store
        let mut task = self
            .test_service
            .service
            .indexify_state
            .reader()
            .get_task(
                &task_allocation.task.as_ref().unwrap().namespace(),
                &task_allocation.task.as_ref().unwrap().graph_name(),
                &task_allocation.task.as_ref().unwrap().graph_invocation_id(),
                &task_allocation.task.as_ref().unwrap().function_name(),
                &task_allocation.task.as_ref().unwrap().id(),
            )
            .unwrap()
            .unwrap();

        task.outcome = args.task_outcome.clone();
        task.status = TaskStatus::Completed;
        task.diagnostics = args.diagnostics.clone();

        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::IngestTaskOutputs(IngestTaskOutputsRequest {
                    namespace: task.namespace.clone(),
                    compute_graph: task.compute_graph_name.clone(),
                    compute_fn: task.compute_fn_name.clone(),
                    invocation_id: task.invocation_id.clone(),
                    task,
                    node_outputs,
                    executor_id: self.executor_id.clone(),
                    allocation_id: args.allocation_id.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }
}
