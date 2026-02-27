use std::sync::Arc;

use anyhow::Result;
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
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        test_objects::tests::mock_blocking_function_call,
    },
    executor_api::{
        executor_api_pb,
        executor_api_pb::Allocation as AllocationPb,
        sync_executor_full_state,
    },
    executors,
    service::Service,
    state_store::{
        driver::rocksdb::RocksDBConfig,
        requests::{
            DeregisterExecutorRequest,
            FunctionCallRequest,
            RequestPayload,
            RequestUpdates,
            StateMachineUpdateRequest,
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
        // Run deferred pool reconciliation after all state changes are
        // processed, matching the event loop behavior in start().
        self.service
            .application_processor
            .run_pool_reconciliation()
            .await?;
        Ok(())
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
            command_emitter: crate::executor_api::CommandEmitter::new(),
        };

        // Initial registration — mirrors the production flow where an executor
        // connects and sends its full state on the first heartbeat.
        e.sync_executor_state(executor.clone()).await?;

        Ok(e)
    }

    pub async fn get_all_function_runs(&self) -> Result<Vec<Box<FunctionRun>>> {
        let function_runs = self
            .service
            .indexify_state
            .reader()
            .get_all_rows_from_cf::<FunctionRun>(IndexifyObjectsColumns::FunctionRuns)
            .await?
            .into_iter()
            .map(|(_, fr)| Box::new(fr))
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
            .load()
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

        let in_memory_state = self.service.indexify_state.in_memory_state.load();

        let unallocated_function_runs = in_memory_state.unallocated_function_runs.clone();

        assert_eq!(
            unallocated_function_runs.len(),
            pending_count,
            "Unallocated function runs in mem store",
        );

        // Verify pending_resources histogram count matches
        let pending_resources = in_memory_state.get_pending_resources();
        let histogram_total: u64 = pending_resources.function_runs.profiles.values().sum();
        assert_eq!(
            histogram_total as usize, pending_count,
            "Pending resources histogram count mismatch: histogram={}, pending={}",
            histogram_total, pending_count,
        );

        drop(in_memory_state);

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
        let snapshot = $executor
            .test_service
            .service
            .executor_manager
            .get_executor_state(&$executor.executor_id)
            .await
            .unwrap();

        // Check function executor count
        let func_executors_count = snapshot.containers.len();
        assert_eq!(
            $num_func_executors, func_executors_count,
            "function executors: expected {}, got {}",
            $num_func_executors, func_executors_count
        );

        // Check task allocation count
        let tasks_count = snapshot.allocations.len();
        assert_eq!(
            $num_allocated_tasks, tasks_count,
            "tasks: expected {}, got {}",
            $num_allocated_tasks, tasks_count
        );
    }};
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

#[derive(Default, Debug)]
pub struct ReceivedCommands {
    pub add_containers: Vec<executor_api_pb::ContainerDescription>,
    pub remove_containers: Vec<String>,
    pub run_allocations: Vec<AllocationPb>,
}

pub struct TestExecutor<'a> {
    pub executor_id: ExecutorId,
    pub executor_metadata: ExecutorMetadata,
    pub test_service: &'a TestService,
    pub command_emitter: crate::executor_api::CommandEmitter,
}

impl TestExecutor<'_> {
    /// Get commands emitted since last call, based on current desired state.
    pub async fn pending_commands(&mut self) -> Vec<crate::executor_api::executor_api_pb::Command> {
        let snapshot = self
            .test_service
            .service
            .executor_manager
            .get_executor_state(&self.executor_id)
            .await
            .unwrap();
        let commands = self.command_emitter.emit_commands(&snapshot);
        self.command_emitter.commit_snapshot(&snapshot);
        commands
    }

    /// Receive pending commands from the CommandEmitter, categorized by type.
    /// Mirrors production: executor receives commands and acts on them.
    pub async fn recv_commands(&mut self) -> ReceivedCommands {
        let commands = self.pending_commands().await;
        let mut received = ReceivedCommands::default();
        for cmd in commands {
            match cmd.command {
                Some(executor_api_pb::command::Command::AddContainer(c)) => {
                    if let Some(container) = c.container {
                        received.add_containers.push(container);
                    }
                }
                Some(executor_api_pb::command::Command::RemoveContainer(r)) => {
                    received.remove_containers.push(r.container_id);
                }
                Some(executor_api_pb::command::Command::RunAllocation(r)) => {
                    if let Some(allocation) = r.allocation {
                        received.run_allocations.push(allocation);
                    }
                }
                _ => {}
            }
        }
        received
    }

    /// Lightweight heartbeat — v2 liveness only.
    /// Mirrors the production heartbeat RPC when no full_state is included.
    /// Use this in tests that need to keep the executor alive without
    /// changing its state (e.g., timeout/lapse tests).
    pub async fn heartbeat(&mut self) -> Result<()> {
        self.test_service
            .service
            .executor_manager
            .heartbeat_v2(&self.executor_id)
            .await?;
        Ok(())
    }

    /// Full state sync — mirrors the production heartbeat RPC when
    /// full_state IS included (initial connect or state change).
    /// Calls the same `sync_executor_full_state` that the production
    /// RPC handler uses, plus `heartbeat_v2` for liveness.
    pub async fn sync_executor_state(&mut self, executor: ExecutorMetadata) -> Result<()> {
        // v2 liveness: update heartbeat deadline
        self.test_service
            .service
            .executor_manager
            .heartbeat_v2(&executor.id)
            .await?;

        self.executor_metadata = executor.clone();

        // Same code path as production handle_v2_full_state
        sync_executor_full_state(
            &self.test_service.service.executor_manager,
            self.test_service.service.indexify_state.clone(),
            executor,
        )
        .await?;

        Ok(())
    }

    pub async fn update_function_executors(&mut self, functions: Vec<Container>) -> Result<()> {
        // First, get current executor state
        let mut executor = self.get_executor_server_state().await?;

        // Update function executors, preserving the status (important for unhealthy
        // function executor tests)
        executor.containers = functions.into_iter().map(|f| (f.id.clone(), f)).collect();

        // State changed — do full state sync (like production executor
        // reporting updated container states via heartbeat with full_state)
        self.sync_executor_state(executor.clone()).await?;

        Ok(())
    }

    pub async fn set_container_states(&mut self, state: ContainerState) -> Result<()> {
        let fes = self
            .get_executor_server_state()
            .await?
            .containers
            .into_iter()
            .map(|(_, mut fe)| {
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
        self.set_container_states(ContainerState::Running).await
    }

    pub async fn get_executor_server_state(&self) -> Result<ExecutorMetadata> {
        // Get the in-memory state first to check if executor exists
        let container_scheduler = self
            .test_service
            .service
            .indexify_state
            .container_scheduler
            .load();

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

        let mut function_containers = imbl::HashMap::new();
        for container_id in executor_server_metadata.function_container_ids {
            let Some(fc) = container_scheduler.function_containers.get(&container_id) else {
                continue;
            };
            function_containers.insert(container_id, fc.function_container.clone());
        }
        executor.containers = function_containers;

        Ok(executor)
    }

    /// Returns the server's desired executor state. Use only for assertions,
    /// not for driving side effects. Use `recv_commands()` to get allocations
    /// for responding with AllocationCompleted/AllocationFailed.
    pub async fn srv_executor_state(&self) -> crate::executors::ExecutorStateSnapshot {
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

    /// Process allocation outcomes through the internal
    /// `process_allocation_completed` / `process_allocation_failed` paths.
    pub async fn report_allocation_activities(
        &self,
        outcomes: Vec<executor_api_pb::AllocationOutcome>,
    ) -> Result<()> {
        for item in outcomes {
            match item.outcome {
                Some(executor_api_pb::allocation_outcome::Outcome::Completed(c)) => {
                    crate::executor_api::process_allocation_completed(
                        &self.test_service.service.indexify_state,
                        &self.test_service.service.blob_storage_registry,
                        &self.executor_id,
                        c,
                    )
                    .await?;
                }
                Some(executor_api_pb::allocation_outcome::Outcome::Failed(f)) => {
                    crate::executor_api::process_allocation_failed(
                        &self.test_service.service.indexify_state,
                        &self.test_service.service.blob_storage_registry,
                        &self.executor_id,
                        f,
                    )
                    .await?;
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Build an `AllocationCompleted` wrapped in `AllocationOutcome`
    /// from test data.
    pub fn make_allocation_completed(
        allocation: &AllocationPb,
        graph_updates: Option<RequestUpdates>,
        data_payload: Option<DataPayload>,
        execution_duration_ms: Option<u64>,
    ) -> executor_api_pb::AllocationOutcome {
        let function = allocation.function.clone();
        // Extract namespace from the allocation's FunctionRef for child compute ops
        let namespace = function
            .as_ref()
            .and_then(|f| f.namespace.clone())
            .unwrap_or_default();
        let return_value = if let Some(updates) = graph_updates {
            let ns = namespace.clone();
            Some(executor_api_pb::allocation_completed::ReturnValue::Updates(
                executor_api_pb::ExecutionPlanUpdates {
                    updates: updates
                        .request_updates
                        .into_iter()
                        .map(|op| internal_compute_op_to_proto(op, &ns))
                        .collect(),
                    root_function_call_id: Some(updates.output_function_call_id.to_string()),
                    start_at: None,
                },
            ))
        } else if let Some(dp) = data_payload {
            Some(executor_api_pb::allocation_completed::ReturnValue::Value(
                internal_data_payload_to_proto(dp),
            ))
        } else {
            None
        };

        let completed = executor_api_pb::AllocationCompleted {
            allocation_id: allocation.allocation_id.clone().unwrap_or_default(),
            function,
            function_call_id: allocation.function_call_id.clone(),
            request_id: allocation.request_id.clone(),
            return_value,
            execution_duration_ms,
        };

        executor_api_pb::AllocationOutcome {
            outcome: Some(executor_api_pb::allocation_outcome::Outcome::Completed(
                completed,
            )),
        }
    }

    /// Build an `AllocationFailed` wrapped in `AllocationOutcome`
    /// from test data.
    pub fn make_allocation_failed(
        allocation: &AllocationPb,
        reason: FunctionRunFailureReason,
        request_error: Option<DataPayload>,
        execution_duration_ms: Option<u64>,
    ) -> executor_api_pb::AllocationOutcome {
        let function = allocation.function.clone();
        let proto_reason = internal_failure_reason_to_proto(&reason);

        let failed = executor_api_pb::AllocationFailed {
            allocation_id: allocation.allocation_id.clone().unwrap_or_default(),
            reason: proto_reason.into(),
            function,
            function_call_id: allocation.function_call_id.clone(),
            request_id: allocation.request_id.clone(),
            request_error: request_error.map(internal_data_payload_to_proto),
            execution_duration_ms,
            container_id: None,
        };

        executor_api_pb::AllocationOutcome {
            outcome: Some(executor_api_pb::allocation_outcome::Outcome::Failed(failed)),
        }
    }
}

/// Convert an internal `DataPayload` to its proto representation.
fn internal_data_payload_to_proto(dp: DataPayload) -> executor_api_pb::DataPayload {
    let encoding = crate::pb_helpers::string_to_data_payload_encoding(&dp.encoding);
    executor_api_pb::DataPayload {
        uri: Some(dp.path),
        encoding: Some(encoding.into()),
        encoding_version: Some(0),
        content_type: None,
        metadata_size: Some(dp.metadata_size),
        offset: Some(dp.offset),
        size: Some(dp.size),
        sha256_hash: Some(dp.sha256_hash),
        source_function_call_id: None,
        id: Some(dp.id),
    }
}

/// Convert an internal `ComputeOp` to a proto `ExecutionPlanUpdate`.
fn internal_compute_op_to_proto(
    op: crate::data_model::ComputeOp,
    namespace: &str,
) -> executor_api_pb::ExecutionPlanUpdate {
    match op {
        crate::data_model::ComputeOp::FunctionCall(fc) => executor_api_pb::ExecutionPlanUpdate {
            op: Some(executor_api_pb::execution_plan_update::Op::FunctionCall(
                executor_api_pb::FunctionCall {
                    id: Some(fc.function_call_id.to_string()),
                    target: Some(executor_api_pb::FunctionRef {
                        namespace: Some(namespace.to_string()),
                        application_name: None,
                        function_name: Some(fc.fn_name),
                        application_version: None,
                    }),
                    args: fc
                        .inputs
                        .into_iter()
                        .map(internal_function_arg_to_proto)
                        .collect(),
                    call_metadata: Some(fc.call_metadata.into()),
                },
            )),
        },
        crate::data_model::ComputeOp::Reduce(reduce) => executor_api_pb::ExecutionPlanUpdate {
            op: Some(executor_api_pb::execution_plan_update::Op::Reduce(
                executor_api_pb::ReduceOp {
                    id: Some(reduce.function_call_id.to_string()),
                    collection: reduce
                        .collection
                        .into_iter()
                        .map(internal_function_arg_to_proto)
                        .collect(),
                    reducer: Some(executor_api_pb::FunctionRef {
                        namespace: Some(namespace.to_string()),
                        application_name: None,
                        function_name: Some(reduce.fn_name),
                        application_version: None,
                    }),
                    call_metadata: Some(reduce.call_metadata.into()),
                },
            )),
        },
    }
}

/// Convert an internal `FunctionArgs` to a proto `FunctionArg`.
fn internal_function_arg_to_proto(
    arg: crate::data_model::FunctionArgs,
) -> executor_api_pb::FunctionArg {
    match arg {
        crate::data_model::FunctionArgs::FunctionRunOutput(fc_id) => executor_api_pb::FunctionArg {
            source: Some(executor_api_pb::function_arg::Source::FunctionCallId(
                fc_id.to_string(),
            )),
        },
        crate::data_model::FunctionArgs::DataPayload(dp) => executor_api_pb::FunctionArg {
            source: Some(executor_api_pb::function_arg::Source::InlineData(
                internal_data_payload_to_proto(dp),
            )),
        },
    }
}

/// Convert an internal `FunctionRunFailureReason` to a proto
/// `AllocationFailureReason`.
fn internal_failure_reason_to_proto(
    reason: &FunctionRunFailureReason,
) -> executor_api_pb::AllocationFailureReason {
    match reason {
        FunctionRunFailureReason::Unknown => executor_api_pb::AllocationFailureReason::Unknown,
        FunctionRunFailureReason::InternalError => {
            executor_api_pb::AllocationFailureReason::InternalError
        }
        FunctionRunFailureReason::FunctionError => {
            executor_api_pb::AllocationFailureReason::FunctionError
        }
        FunctionRunFailureReason::FunctionTimeout => {
            executor_api_pb::AllocationFailureReason::FunctionTimeout
        }
        FunctionRunFailureReason::RequestError => {
            executor_api_pb::AllocationFailureReason::RequestError
        }
        FunctionRunFailureReason::FunctionRunCancelled => {
            executor_api_pb::AllocationFailureReason::AllocationCancelled
        }
        FunctionRunFailureReason::FunctionExecutorTerminated => {
            executor_api_pb::AllocationFailureReason::ContainerTerminated
        }
        FunctionRunFailureReason::ConstraintUnsatisfiable => {
            executor_api_pb::AllocationFailureReason::ConstraintUnsatisfiable
        }
        FunctionRunFailureReason::ExecutorRemoved => {
            executor_api_pb::AllocationFailureReason::ExecutorRemoved
        }
        FunctionRunFailureReason::ContainerStartupFunctionError => {
            executor_api_pb::AllocationFailureReason::StartupFailedFunctionError
        }
        FunctionRunFailureReason::ContainerStartupFunctionTimeout => {
            executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout
        }
        FunctionRunFailureReason::ContainerStartupInternalError => {
            executor_api_pb::AllocationFailureReason::StartupFailedInternalError
        }
        FunctionRunFailureReason::OutOfMemory => executor_api_pb::AllocationFailureReason::Oom,
        FunctionRunFailureReason::ContainerStartupBadImage => {
            executor_api_pb::AllocationFailureReason::StartupFailedBadImage
        }
    }
}
