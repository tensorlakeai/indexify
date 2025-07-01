use std::sync::Arc;

use anyhow::Result;
use blob_store::BlobStorageConfig;
use data_model::{
    test_objects::tests::test_node_fn_output,
    Allocation,
    DataPayload,
    ExecutorId,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionExecutorState,
    GraphVersion,
    Task,
    TaskDiagnostics,
    TaskOutcome,
    TaskStatus,
};
use nanoid::nanoid;
use state_store::{
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
    executor_api::executor_api_pb::TaskAllocation,
    service::Service,
};

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

    pub async fn get_all_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self
            .service
            .indexify_state
            .reader()
            .get_all_rows_from_cf::<Task>(IndexifyObjectsColumns::Tasks)?
            .iter()
            .map(|r| r.1.clone())
            .collect::<Vec<_>>();
        Ok(tasks)
    }

    pub async fn get_allocated_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self.get_all_tasks().await?;
        let allocated_tasks = tasks
            .into_iter()
            .filter(|t| t.status == TaskStatus::Running)
            .collect::<Vec<_>>();
        Ok(allocated_tasks)
    }

    pub async fn get_pending_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self.get_all_tasks().await?;
        let pending_tasks = tasks
            .into_iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .collect::<Vec<_>>();

        let pending_count = pending_tasks.len();

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
            pending_count,
            "Pending tasks in mem store",
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
            pending_count,
            "Unallocated tasks in mem store",
        );

        Ok(pending_tasks)
    }

    pub async fn get_completed_success_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self.get_all_tasks().await?;
        let completed_success_tasks = tasks
            .into_iter()
            .filter(|t| t.status == TaskStatus::Completed && t.outcome == TaskOutcome::Success)
            .collect::<Vec<_>>();
        Ok(completed_success_tasks)
    }
}

// Declarative macros for task state assertions
#[macro_export]
macro_rules! assert_task_counts {
    ($test_srv:expr, total: $total:expr, allocated: $allocated:expr, pending: $pending:expr, completed_success: $completed_success:expr) => {{
        let all_tasks = $test_srv.get_all_tasks().await?;
        let allocated_tasks = $test_srv.get_allocated_tasks().await?;
        let pending_tasks = $test_srv.get_pending_tasks().await?;
        let completed_success_tasks = $test_srv.get_completed_success_tasks().await?;

        assert_eq!(all_tasks.len(), $total, "Total Tasks: {:#?}", all_tasks);
        assert_eq!(
            allocated_tasks.len(),
            $allocated,
            "Allocated tasks: {:#?}",
            allocated_tasks
        );
        assert_eq!(
            pending_tasks.len(),
            $pending,
            "Pending tasks: {:#?}",
            pending_tasks
        );
        assert_eq!(
            completed_success_tasks.len(),
            $completed_success,
            "Tasks completed successfully: {:#?}",
            completed_success_tasks
        );
    }};
}

#[macro_export]
macro_rules! assert_executor_state {
    ($executor:expr, num_func_executors: $num_func_executors:expr, num_allocated_tasks: $num_allocated_tasks:expr) => {{
        // Get desired state from executor manager
        let desired_state = $executor
            .test_service
            .service
            .executor_manager
            .get_executor_state(&$executor.executor_id)
            .await;

        // Check function executor count
        let func_executors_count = desired_state.function_executors.len();
        assert_eq!(
            $num_func_executors, func_executors_count,
            "function executors: expected {}, got {}",
            $num_func_executors, func_executors_count
        );

        // Check task allocation count
        let tasks_count = desired_state.task_allocations.len();
        assert_eq!(
            $num_allocated_tasks, tasks_count,
            "tasks: expected {}, got {}",
            $num_allocated_tasks, tasks_count
        );
    }};
}

pub struct FinalizeTaskArgs {
    pub num_outputs: i32,
    pub task_outcome: TaskOutcome,
    pub reducer_fn: Option<String>,
    pub diagnostics: Option<TaskDiagnostics>,
    pub allocation_key: String,
}

pub fn allocation_key_from_proto(allocation: &TaskAllocation) -> String {
    Allocation::key_from(
        allocation.task.as_ref().unwrap().namespace(),
        allocation.task.as_ref().unwrap().graph_name(),
        allocation.task.as_ref().unwrap().graph_invocation_id(),
        allocation.allocation_id.as_ref().unwrap(),
    )
}

impl FinalizeTaskArgs {
    pub fn new(allocation_key: String) -> FinalizeTaskArgs {
        FinalizeTaskArgs {
            num_outputs: 1,
            task_outcome: TaskOutcome::Success,
            reducer_fn: None,
            diagnostics: None,
            allocation_key,
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
        let function_executor_diagnostics = vec![];
        self.test_service
            .service
            .executor_manager
            .heartbeat(executor, function_executor_diagnostics)
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

    pub async fn set_function_executor_states(&self, state: FunctionExecutorState) -> Result<()> {
        let fes = self
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|mut fe| {
                fe.state = state;
                fe
            })
            .collect();

        self.update_function_executors(fes).await?;

        // Process state changes to ensure changes take effect
        self.test_service.process_all_state_changes().await?;

        Ok(())
    }

    pub async fn mark_function_executors_as_running(&self) -> Result<()> {
        self.set_function_executor_states(FunctionExecutorState::Running)
            .await
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
        let executor = indexes
            .read()
            .await
            .executors
            .get(&self.executor_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Executor not found in state store"))?;

        let executor_server_metadata = indexes
            .read()
            .await
            .executor_states
            .get(&self.executor_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Executor not found in state store"))?;

        // Clone base executor
        let mut executor = *executor.clone();

        executor.function_executors = executor_server_metadata
            .function_executors
            .into_iter()
            .map(|(id, fe)| (id, fe.function_executor))
            .collect();

        Ok(executor)
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
        let allocation_id = task_allocation.allocation_id.clone().unwrap();
        let graph_version = self
            .test_service
            .service
            .indexify_state
            .reader()
            .get_compute_graph_version(
                &task_allocation.task.as_ref().unwrap().namespace(),
                &task_allocation.task.as_ref().unwrap().graph_name(),
                &GraphVersion(
                    task_allocation
                        .task
                        .as_ref()
                        .unwrap()
                        .graph_version()
                        .to_string(),
                ),
            )?
            .unwrap();
        let node_output = test_node_fn_output(
            task_allocation.task.as_ref().unwrap().graph_invocation_id(),
            task_allocation.task.as_ref().unwrap().graph_name(),
            task_allocation.task.as_ref().unwrap().function_name(),
            args.reducer_fn.clone(),
            args.num_outputs as usize,
            allocation_id,
            graph_version
                .edges
                .get(
                    &task_allocation
                        .task
                        .as_ref()
                        .unwrap()
                        .function_name()
                        .to_string(),
                )
                .cloned()
                .unwrap_or_default(),
        );

        // get the task from the state store
        let task = self
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

        let mut allocation = self
            .test_service
            .service
            .indexify_state
            .reader()
            .get_allocation(&args.allocation_key)
            .unwrap()
            .unwrap();

        allocation.outcome = args.task_outcome.clone();
        allocation.diagnostics = args.diagnostics.clone();

        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::IngestTaskOutputs(IngestTaskOutputsRequest {
                    namespace: task.namespace.clone(),
                    compute_graph: task.compute_graph_name.clone(),
                    compute_fn: task.compute_fn_name.clone(),
                    invocation_id: task.invocation_id.clone(),
                    node_output,
                    task,
                    executor_id: self.executor_id.clone(),
                    allocation_key: args.allocation_key.clone(),
                    allocation,
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }
}
