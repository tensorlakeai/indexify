use std::sync::Arc;

use anyhow::Result;
use nanoid::nanoid;
use tracing::subscriber;
use tracing_subscriber::{layer::SubscriberExt, Layer};

use crate::{
    blob_store::BlobStorageConfig,
    config::{ExecutorCatalogEntry, ServerConfig},
    data_model::{
        Allocation,
        DataPayload,
        ExecutorId,
        ExecutorMetadata,
        FunctionExecutor,
        FunctionExecutorState,
        FunctionRun,
        FunctionRunOutcome,
        FunctionRunStatus,
        GraphInvocationCtx,
    },
    executor_api::executor_api_pb::TaskAllocation,
    service::Service,
    state_store::{
        requests::{
            AllocationOutput,
            DeregisterExecutorRequest,
            GraphUpdates,
            RequestPayload,
            StateMachineUpdateRequest,
            UpsertExecutorRequest,
        },
        state_machine::IndexifyObjectsColumns,
    },
};

pub struct TestService {
    pub service: Service,
    // keeping a reference to the temp dir to ensure it is not deleted
    #[allow(dead_code)]
    temp_dir: tempfile::TempDir,
}

impl TestService {
    pub async fn new() -> Result<Self> {
        Self::new_with_executor_catalog(vec![]).await
    }

    pub async fn new_with_executor_catalog(
        executor_catalog: Vec<ExecutorCatalogEntry>,
    ) -> Result<Self> {
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
                region: None,
            },
            executor_catalog,
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

    pub async fn create_executor(&self, executor: ExecutorMetadata) -> Result<TestExecutor<'_>> {
        let mut e = TestExecutor {
            executor_id: executor.id.clone(),
            executor_metadata: executor.clone(),
            test_service: self,
        };

        e.heartbeat(executor.clone()).await?;

        Ok(e)
    }

    pub async fn get_all_function_runs(&self) -> Result<Vec<Box<FunctionRun>>> {
        let function_runs = self
            .service
            .indexify_state
            .reader()
            .get_all_rows_from_cf::<GraphInvocationCtx>(IndexifyObjectsColumns::GraphInvocationCtx)
            .unwrap()
            .into_iter()
            .map(|(_, ctx)| ctx.function_runs.values().cloned().collect::<Vec<_>>())
            .flatten()
            .map(|fr| Box::new(fr))
            .collect::<Vec<_>>();
        Ok(function_runs)
    }

    pub async fn get_allocated_tasks(&self) -> Result<Vec<Box<FunctionRun>>> {
        let tasks = self.get_all_function_runs().await?;
        let allocated_tasks = tasks
            .into_iter()
            .filter(|t| matches!(t.status, FunctionRunStatus::Running(_)))
            .collect::<Vec<_>>();
        Ok(allocated_tasks)
    }

    pub async fn get_pending_function_runs(&self) -> Result<Vec<Box<FunctionRun>>> {
        let tasks = self.get_all_function_runs().await?;
        let pending_tasks = tasks
            .into_iter()
            .filter(|t| t.status == FunctionRunStatus::Pending)
            .collect::<Vec<_>>();

        let pending_count = pending_tasks.len();

        let pending_tasks_memory = self
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .function_runs
            .clone();

        let pending_tasks_memory = pending_tasks_memory
            .iter()
            .filter(|(_k, t)| t.status == FunctionRunStatus::Pending)
            .collect::<Vec<_>>();

        assert_eq!(
            pending_tasks_memory.len(),
            pending_count,
            "Pending tasks in mem store",
        );

        let unallocated_function_runs = self
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .unallocated_function_runs
            .clone();

        assert_eq!(
            unallocated_function_runs.len(),
            pending_count,
            "Unallocated function runs in mem store",
        );

        Ok(pending_tasks)
    }

    pub async fn get_success_function_runs(&self) -> Result<Vec<Box<FunctionRun>>> {
        let function_runs = self.get_all_function_runs().await?;
        let completed_success_tasks = function_runs
            .into_iter()
            .filter(|t| {
                t.status == FunctionRunStatus::Completed &&
                    t.outcome == Some(FunctionRunOutcome::Success)
            })
            .collect::<Vec<_>>();
        Ok(completed_success_tasks)
    }
}

// Declarative macros for task state assertions
#[macro_export]
macro_rules! assert_task_counts {
    ($test_srv:expr, total: $total:expr, allocated: $allocated:expr, pending: $pending:expr, completed_success: $completed_success:expr) => {{
        let all_function_runs = $test_srv.get_all_function_runs().await?;
        let allocated_tasks = $test_srv.get_allocated_tasks().await?;
        let pending_tasks = $test_srv.get_pending_function_runs().await?;
        let completed_success_tasks = $test_srv.get_success_function_runs().await?;

        assert_eq!(
            all_function_runs.len(),
            $total,
            "Total Tasks: {:#?}",
            all_function_runs
        );
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
    pub task_outcome: FunctionRunOutcome,
    pub allocation_key: String,
    pub graph_updates: Option<GraphUpdates>,
    pub data_payload: Option<DataPayload>,
}

pub fn allocation_key_from_proto(allocation: &TaskAllocation) -> String {
    Allocation::key_from(
        &allocation
            .function
            .as_ref()
            .unwrap()
            .namespace
            .as_ref()
            .unwrap(),
        &allocation
            .function
            .as_ref()
            .unwrap()
            .application_name
            .as_ref()
            .unwrap(),
        &allocation.request_id.as_ref().unwrap(),
        &allocation.allocation_id.as_ref().unwrap(),
    )
}

impl FinalizeTaskArgs {
    pub fn new(
        allocation_key: String,
        graph_updates: Option<GraphUpdates>,
        data_payload: Option<DataPayload>,
    ) -> FinalizeTaskArgs {
        FinalizeTaskArgs {
            task_outcome: FunctionRunOutcome::Success,
            allocation_key,
            graph_updates,
            data_payload,
        }
    }

    pub fn task_outcome(mut self, task_outcome: FunctionRunOutcome) -> FinalizeTaskArgs {
        self.task_outcome = task_outcome;
        self
    }
}

pub struct TestExecutor<'a> {
    pub executor_id: ExecutorId,
    pub executor_metadata: ExecutorMetadata,
    pub test_service: &'a TestService,
}

impl TestExecutor<'_> {
    pub async fn heartbeat(&mut self, executor: ExecutorMetadata) -> Result<()> {
        let update_executor_state = self
            .test_service
            .service
            .executor_manager
            .heartbeat(&executor)
            .await?;
        self.executor_metadata = executor.clone();

        let request = UpsertExecutorRequest::build(
            executor,
            vec![],
            update_executor_state,
            self.test_service.service.indexify_state.clone(),
        )?;

        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::UpsertExecutor(request),
        };
        self.test_service
            .service
            .indexify_state
            .write(sm_req)
            .await?;
        Ok(())
    }

    pub async fn update_function_executors(
        &mut self,
        functions: Vec<FunctionExecutor>,
    ) -> Result<()> {
        // First, get current executor state
        let mut executor = self.get_executor_server_state().await?;

        // Update function executors, preserving the status (important for unhealthy
        // function executor tests)
        executor.function_executors = functions.into_iter().map(|f| (f.id.clone(), f)).collect();

        // Update state hash and send heartbeat
        executor.state_hash = nanoid!();
        self.heartbeat(executor.clone()).await?;

        Ok(())
    }

    pub async fn set_function_executor_states(
        &mut self,
        state: FunctionExecutorState,
    ) -> Result<()> {
        let fes = self
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|mut fe| {
                fe.state = state.clone();
                fe
            })
            .collect();

        self.update_function_executors(fes).await?;

        // Process state changes to ensure changes take effect
        self.test_service.process_all_state_changes().await?;

        Ok(())
    }

    pub async fn mark_function_executors_as_running(&mut self) -> Result<()> {
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
            })
            .await?;
        Ok(())
    }

    pub async fn finalize_task(
        &self,
        task_allocation: &TaskAllocation,
        args: FinalizeTaskArgs,
    ) -> Result<()> {
        let mut allocation = self
            .test_service
            .service
            .indexify_state
            .reader()
            .get_allocation(&args.allocation_key)
            .unwrap()
            .unwrap();

        allocation.outcome = args.task_outcome;

        let ingest_task_outputs_request = AllocationOutput {
            request_exception: None,
            graph_updates: args.graph_updates.clone().map(|g| GraphUpdates {
                graph_updates: g.graph_updates,
                output_function_call_id: g.output_function_call_id,
            }),
            executor_id: self.executor_id.clone(),
            invocation_id: task_allocation.request_id.clone().unwrap(),
            data_payload: args.data_payload,
            allocation,
        };

        let request = UpsertExecutorRequest::build(
            self.executor_metadata.clone(),
            vec![ingest_task_outputs_request],
            false,
            self.test_service.service.indexify_state.clone(),
        )?;

        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::UpsertExecutor(request),
            })
            .await?;
        Ok(())
    }
}
