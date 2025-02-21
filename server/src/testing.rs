use std::sync::Arc;

use anyhow::Result;
use blob_store::BlobStorageConfig;
use data_model::{
    test_objects::tests::{mock_executor, mock_node_fn_output},
    ExecutorId,
    ExecutorMetadata,
    Task,
    TaskOutcome,
};
use state_store::requests::{
    DeregisterExecutorRequest,
    IngestTaskOutputsRequest,
    RegisterExecutorRequest,
    RequestPayload,
    StateMachineUpdateRequest,
};
use tracing::subscriber;
use tracing_subscriber::{layer::SubscriberExt, Layer};

use crate::{config::ServerConfig, service::Service};

pub struct TestService {
    pub service: Service,
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

        Ok(Self { service: srv })
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

    pub async fn process_graph_processor(&self) -> Result<()> {
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
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
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
}

pub struct FinalizeTaskArgs {
    pub num_outputs: i32,
    pub task_outcome: TaskOutcome,
    pub reducer_fn: Option<String>,
}

impl FinalizeTaskArgs {
    pub fn new() -> FinalizeTaskArgs {
        FinalizeTaskArgs {
            num_outputs: 1,
            task_outcome: TaskOutcome::Success,
            reducer_fn: None,
        }
    }

    pub fn task_outcome(mut self, task_outcome: TaskOutcome) -> FinalizeTaskArgs {
        self.task_outcome = task_outcome;
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
            .reader()
            .get_tasks_by_executor(&self.executor.id, 100)?;

        Ok(tasks)
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

        self.service
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::IngestTaskOutputs(IngestTaskOutputsRequest {
                    namespace: task.namespace.clone(),
                    compute_graph: task.compute_graph_name.clone(),
                    compute_fn: task.compute_fn_name.clone(),
                    invocation_id: task.invocation_id.clone(),
                    task: task.clone(),
                    node_outputs,
                    task_outcome: args.task_outcome,
                    executor_id: self.executor.id.clone(),
                    diagnostics: None,
                }),
                processed_state_changes: vec![],
            })
            .await?;
        Ok(())
    }
}
