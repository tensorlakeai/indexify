use std::{sync::Arc, time::Duration};

use anyhow::Result;
use blob_store::BlobStorageConfig;
use data_model::{
    test_objects::tests::{mock_executor, mock_node_fn_output},
    DataPayload,
    ExecutorId,
    ExecutorMetadata,
    Task,
    TaskDiagnostics,
    TaskOutcome,
    TaskStatus,
};
use state_store::{
    requests::{
        DeregisterExecutorRequest,
        IngestTaskOutputsRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        UpsertExecutorRequest,
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
        Self::new_with_executor_timeout(Duration::from_secs(30)).await
    }

    pub async fn new_with_executor_timeout(executor_timeout: Duration) -> Result<Self> {
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
            executor_timeout,
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

    pub async fn register_executor(&self, executor_id: ExecutorId) -> Result<TestExecutor> {
        let mut executor = mock_executor();
        executor.id = executor_id.clone();
        self.service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::UpsertExecutor(UpsertExecutorRequest {
                    executor: executor.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        Ok(TestExecutor {
            executor: executor.clone(),
            service: self,
        })
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

        let unallocated_tasks = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .collect::<Vec<_>>();
        assert_eq!(
            unallocated_tasks.len(),
            unallocated,
            "Unallocated tasks: {:#?}",
            unallocated_tasks
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
    pub service: &'a TestService,
}

impl TestExecutor<'_> {
    pub async fn deregister(&self) -> Result<()> {
        self.service
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

    pub async fn get_tasks(&self) -> Result<Vec<Task>> {
        let tasks = self
            .service
            .service
            .indexify_state
            .in_memory_state
            .read()
            .await
            .active_tasks_for_executor(&self.executor.id.get());

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

        self.service
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
