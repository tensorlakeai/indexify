use std::sync::Arc;

use anyhow::Result;
use blob_store::BlobStorageConfig;
use data_model::{
    test_objects::tests::mock_node_fn_output,
    DataPayload,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionURI,
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

use crate::{config::ServerConfig, service::Service};

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
            executor,
            test_service: self,
        };

        e.heartbeat().await?;

        Ok(e)
    }

    pub async fn assert_task_states(
        &self,
        total: usize,
        allocated: usize,
        unallocated: usize,
        completed_success: usize,
    ) -> Result<()> {
        let tasks = self
            .service
            .indexify_state
            .reader()
            .get_all_rows_from_cf::<Task>(IndexifyObjectsColumns::Tasks)?
            .iter()
            .map(|r| r.1.clone())
            .collect::<Vec<_>>();
        assert_eq!(tasks.len(), total, "Total Tasks: {:#?}", tasks);

        let allocated_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Running)
            .collect::<Vec<_>>();
        assert_eq!(
            allocated_tasks.len(),
            allocated,
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
            unallocated,
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
            unallocated,
            "Pending tasks in mem store",
        );

        let completed_success_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Completed && t.outcome == TaskOutcome::Success)
            .collect::<Vec<_>>();
        assert_eq!(
            completed_success_tasks.len(),
            completed_success,
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
            unallocated,
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
}

impl FinalizeTaskArgs {
    pub fn new() -> FinalizeTaskArgs {
        FinalizeTaskArgs {
            num_outputs: 1,
            task_outcome: TaskOutcome::Success,
            reducer_fn: None,
            diagnostics: None,
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
    pub executor: ExecutorMetadata,
    pub test_service: &'a TestService,
}

impl TestExecutor<'_> {
    pub async fn heartbeat(&self) -> Result<()> {
        self.test_service
            .service
            .executor_manager
            .heartbeat(self.executor.clone())
            .await?;
        Ok(())
    }

    pub async fn update_config(
        &mut self,
        dev_mode: Option<bool>,
        functions: Option<Option<Vec<FunctionURI>>>,
    ) -> Result<()> {
        if let Some(dev_mode) = dev_mode {
            self.executor.development_mode = dev_mode;
        }
        if let Some(functions) = functions {
            self.executor.function_allowlist = functions;
        }
        self.executor.state_hash = nanoid!();
        self.heartbeat().await?;
        Ok(())
    }

    pub async fn update_function_executors(
        &mut self,
        function_executors: Vec<FunctionExecutor>,
    ) -> Result<()> {
        self.executor.function_executors = function_executors
            .into_iter()
            .map(|fe| (fe.id.clone(), fe))
            .collect();
        self.executor.state_hash = nanoid!();
        self.heartbeat().await?;
        Ok(())
    }

    pub async fn deregister(&self) -> Result<()> {
        self.test_service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: self.executor.id.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }

    pub async fn assert_state(&self, num_func_executors: usize, num_tasks: usize) -> Result<()> {
        let indexes = self
            .test_service
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await;

        let func_executors = indexes
            .function_executors_by_executor
            .get(&self.executor.id)
            .iter()
            .flat_map(|inner_map| inner_map.iter())
            .map(|(_key, value)| value)
            .collect::<Vec<_>>();

        assert_eq!(
            num_func_executors,
            func_executors.len(),
            "function executors: {:#?}",
            func_executors
        );

        let tasks = indexes.active_tasks_for_executor(&self.executor.id);
        assert_eq!(num_tasks, tasks.len(), "tasks: {:#?}", tasks);

        Ok(())
    }

    pub async fn get_function_executors(&self) -> Result<Vec<FunctionExecutor>> {
        Ok(self.executor.function_executors.values().cloned().collect())
    }

    pub async fn get_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self
            .test_service
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .active_tasks_for_executor(&self.executor.id);

        Ok(tasks.into_iter().map(|task| (*task).clone()).collect())
    }

    pub async fn finalize_task(&self, task: &Task, args: FinalizeTaskArgs) -> Result<()> {
        let node_outputs = (0..args.num_outputs)
            .map(|_| {
                mock_node_fn_output(
                    task.invocation_id.as_str(),
                    task.compute_graph_name.as_str(),
                    task.compute_fn_name.as_str(),
                    args.reducer_fn.clone(),
                )
            })
            .collect();

        let mut task = task.clone();
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
                    executor_id: self.executor.id.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }
}
