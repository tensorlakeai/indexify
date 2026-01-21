use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::Result;
use nanoid::nanoid;
use tracing::subscriber;
use tracing_subscriber::{Layer, layer::SubscriberExt};

use crate::{
    blob_store::BlobStorageConfig,
    config::{ExecutorCatalogEntry, ServerConfig},
    data_model::{
        Allocation,
        Container,
        ContainerState,
        DataPayload,
        ExecutorId,
        ExecutorMetadata,
        FunctionCallId,
        FunctionRun,
        FunctionRunOutcome,
        FunctionRunStatus,
        RequestCtx,
        test_objects::tests::mock_blocking_function_call,
    },
    executor_api::executor_api_pb::Allocation as AllocationPb,
    executors,
    processor::request_state_change_processor::RequestStateChangeProcessor,
    service::Service,
    state_store::{
        driver::rocksdb::RocksDBConfig,
        executor_watches::ExecutorWatch,
        requests::{
            AllocationOutput,
            DeregisterExecutorRequest,
            FunctionCallRequest,
            RequestPayload,
            RequestUpdates,
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
            rocksdb_config: RocksDBConfig::default(),
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
            .unprocessed_state_changes(&None, &None)
            .await?
            .changes
            .is_empty()
        {
            self.process_graph_processor().await?;
        }
        Ok(())
    }

    /// Drain all pending request state change events from RocksDB.
    /// This simulates what the RequestStateChangeProcessor would do.
    pub async fn drain_request_state_change_events(&self) -> Result<()> {
        let processor = RequestStateChangeProcessor::new(self.service.indexify_state.clone());
        processor.drain_all_events().await
    }

    async fn process_graph_processor(&self) -> Result<()> {
        let notify = Arc::new(tokio::sync::Notify::new());
        let mut cached_state_changes = self
            .service
            .indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)
            .await?
            .changes;
        while !cached_state_changes.is_empty() {
            self.service
                .application_processor
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
            .get_all_rows_from_cf::<RequestCtx>(IndexifyObjectsColumns::RequestCtx)
            .await?
            .into_iter()
            .flat_map(|(_, ctx)| ctx.function_runs.values().cloned().collect::<Vec<_>>())
            .map(Box::new)
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
macro_rules! assert_function_run_counts {
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
            .await
            .unwrap();

        // Check function executor count
        let func_executors_count = desired_state.function_executors.len();
        assert_eq!(
            $num_func_executors, func_executors_count,
            "function executors: expected {}, got {}",
            $num_func_executors, func_executors_count
        );

        // Check task allocation count
        let tasks_count = desired_state.allocations.len();
        assert_eq!(
            $num_allocated_tasks, tasks_count,
            "tasks: expected {}, got {}",
            $num_allocated_tasks, tasks_count
        );
    }};
}

pub struct FinalizeFunctionRunArgs {
    pub task_outcome: FunctionRunOutcome,
    pub allocation_key: String,
    pub graph_updates: Option<RequestUpdates>,
    pub data_payload: Option<DataPayload>,
    pub execution_duration_ms: Option<u64>,
}

pub fn allocation_key_from_proto(allocation: &AllocationPb) -> String {
    Allocation::key_from(
        allocation
            .function
            .as_ref()
            .unwrap()
            .namespace
            .as_ref()
            .unwrap(),
        allocation
            .function
            .as_ref()
            .unwrap()
            .application_name
            .as_ref()
            .unwrap(),
        allocation.request_id.as_ref().unwrap(),
        allocation.allocation_id.as_ref().unwrap(),
    )
}

impl FinalizeFunctionRunArgs {
    pub fn new(
        allocation_key: String,
        graph_updates: Option<RequestUpdates>,
        data_payload: Option<DataPayload>,
    ) -> FinalizeFunctionRunArgs {
        FinalizeFunctionRunArgs {
            task_outcome: FunctionRunOutcome::Success,
            allocation_key,
            graph_updates,
            data_payload,
            execution_duration_ms: None,
        }
    }

    pub fn function_run_outcome(
        mut self,
        task_outcome: FunctionRunOutcome,
    ) -> FinalizeFunctionRunArgs {
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
            HashSet::new(),
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

    pub async fn update_function_executors(&mut self, functions: Vec<Container>) -> Result<()> {
        // First, get current executor state
        let mut executor = self.get_executor_server_state().await?;

        // Update function executors, preserving the status (important for unhealthy
        // function executor tests)
        executor.containers = functions.into_iter().map(|f| (f.id.clone(), f)).collect();

        // Update state hash and send heartbeat
        executor.state_hash = nanoid!();
        self.heartbeat(executor.clone()).await?;

        Ok(())
    }

    pub async fn set_function_executor_states(&mut self, state: ContainerState) -> Result<()> {
        let fes = self
            .get_executor_server_state()
            .await?
            .containers
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
        self.set_function_executor_states(ContainerState::Running)
            .await
    }

    pub async fn get_executor_server_state(&self) -> Result<ExecutorMetadata> {
        // Get the in-memory state first to check if executor exists
        let container_scheduler = self
            .test_service
            .service
            .indexify_state
            .container_scheduler
            .read()
            .await;

        // Get executor from in-memory state - this is the base executor without
        // complete function executors
        let executor = container_scheduler
            .executors
            .get(&self.executor_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Executor not found in state store"))?;

        let executor_server_metadata = container_scheduler
            .executor_states
            .get(&self.executor_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Executor not found in state store"))?;

        // Clone base executor
        let mut executor = *executor.clone();

        let mut function_containers = HashMap::new();
        for container_id in executor_server_metadata.function_container_ids {
            let Some(fc) = container_scheduler.function_containers.get(&container_id) else {
                continue;
            };
            function_containers.insert(container_id, fc.function_container.clone());
        }
        executor.containers = function_containers;

        Ok(executor)
    }

    pub async fn desired_state(
        &self,
    ) -> crate::executor_api::executor_api_pb::DesiredExecutorState {
        self.test_service
            .service
            .executor_manager
            .get_executor_state(&self.executor_id)
            .await
            .unwrap()
    }

    pub async fn deregister(&self) -> Result<()> {
        let executor_last_seq = self
            .test_service
            .service
            .indexify_state
            .state_change_id_seq
            .clone();
        let state_changes =
            executors::tombstone_executor(&executor_last_seq, &self.executor_id).unwrap();
        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: self.executor_id.clone(),
                    state_changes,
                }),
            })
            .await?;
        Ok(())
    }

    pub async fn invoke_blocking_function_call(
        &self,
        function_name: &str,
        namespace: &str,
        application: &str,
        request_id: &str,
        source_function_call_id: FunctionCallId,
    ) -> Result<FunctionCallId> {
        use crate::data_model::ComputeOp;
        let graph_updates = mock_blocking_function_call(function_name, &source_function_call_id);
        let function_call_id =
            if let Some(ComputeOp::FunctionCall(fc)) = graph_updates.request_updates.first() {
                fc.function_call_id.clone()
            } else {
                return Err(anyhow::anyhow!("No function call in graph updates"));
            };
        let request = FunctionCallRequest {
            namespace: namespace.to_string(),
            application_name: application.to_string(),
            request_id: request_id.to_string(),
            graph_updates,
            source_function_call_id,
        };
        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateFunctionCall(request),
            })
            .await?;
        Ok(function_call_id)
    }

    pub async fn update_watches(&self, executor_watches: HashSet<ExecutorWatch>) -> Result<()> {
        let request = UpsertExecutorRequest::build(
            self.executor_metadata.clone(),
            vec![],
            false,
            executor_watches,
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

    pub async fn finalize_allocation(
        &self,
        allocation_pb: &AllocationPb,
        args: FinalizeFunctionRunArgs,
    ) -> Result<()> {
        let mut allocation = self
            .test_service
            .service
            .indexify_state
            .reader()
            .get_allocation(&args.allocation_key)
            .await
            .unwrap()
            .unwrap();

        allocation.outcome = args.task_outcome;

        allocation.execution_duration_ms = args.execution_duration_ms;
        if allocation.execution_duration_ms.is_none() {
            allocation.execution_duration_ms = Some(1000);
        }

        let ingest_task_outputs_request = AllocationOutput {
            request_exception: None,
            graph_updates: args.graph_updates.clone().map(|g| RequestUpdates {
                request_updates: g.request_updates,
                output_function_call_id: g.output_function_call_id,
            }),
            executor_id: self.executor_id.clone(),
            request_id: allocation_pb.request_id.clone().unwrap(),
            data_payload: args.data_payload,
            allocation,
        };

        let request = UpsertExecutorRequest::build(
            self.executor_metadata.clone(),
            vec![ingest_task_outputs_request],
            false,
            HashSet::new(),
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
