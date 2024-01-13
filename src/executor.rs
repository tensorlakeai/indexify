use std::{
    collections::HashMap,
    fmt,
    sync::{atomic::AtomicBool, Arc},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use nanoid::nanoid;
use serde_json::json;
use tracing::{error, info};

use crate::{
    content_reader::ContentReader,
    coordinator_client::CoordinatorClient,
    extractor::extractor_runner::ExtractorRunner,
    indexify_coordinator::{self, RegisterExecutorRequest},
    internal_api::{
        self,
        Content,
        ExecutorInfo,
        ExtractorDescription,
        Task,
        TaskResult,
        TaskState,
    },
    server_config::ExecutorConfig,
    task_store::TaskStore,
};

fn create_executor_id() -> String {
    let host_name = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_default();

    format!("{}_{}", nanoid!(), host_name)
}

pub struct ExtractorExecutor {
    executor_config: Arc<ExecutorConfig>,
    executor_id: String,
    extractor_runner: Arc<ExtractorRunner>,
    extractor_description: ExtractorDescription,
    listen_addr: String,

    task_store: Arc<TaskStore>,
    requires_registration: AtomicBool,
}

impl fmt::Debug for ExtractorExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtractorExecutor")
            .field("config", &self.executor_config)
            .field("executor_id", &self.executor_id)
            .finish()
    }
}

impl ExtractorExecutor {
    #[tracing::instrument]
    pub async fn new(
        executor_config: Arc<ExecutorConfig>,
        extractor_runner: ExtractorRunner,
        listen_addr: String,
        task_store: Arc<TaskStore>,
    ) -> Result<Self> {
        let executor_id = create_executor_id();
        let extractor_description = extractor_runner.info()?.into();
        let extractor_runner = Arc::new(extractor_runner);
        let extractor_executor = Self {
            executor_config,
            executor_id,
            extractor_runner,
            extractor_description,
            listen_addr,
            task_store,
            requires_registration: AtomicBool::new(true),
        };
        Ok(extractor_executor)
    }

    #[tracing::instrument]
    pub fn get_executor_info(&self) -> ExecutorInfo {
        ExecutorInfo {
            id: self.executor_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            addr: self.executor_config.listen_if.clone().into(),
            extractor: self.extractor_runner.info().unwrap().into(),
        }
    }

    #[tracing::instrument]
    pub async fn extract(
        &self,
        content: Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<Vec<Content>, anyhow::Error> {
        let extracted_content = self
            .extractor_runner
            .extract(vec![content], input_params.unwrap_or(json!({})))?;
        let content = extracted_content
            .first()
            .ok_or(anyhow!("no content was extracted"))?
            .to_owned();
        Ok(content)
    }

    #[tracing::instrument(skip(self))]
    pub async fn execute_pending_tasks(&self) -> Result<(), anyhow::Error> {
        let tasks = self.task_store.pending_tasks();
        let mut results = Vec::new();
        for task in tasks {
            info!("performing task: {}", &task.id);
            let content = get_content(task.content_metadata).await?;
            let extracted_content_batch = self
                .extractor_runner
                .extract(vec![content], task.input_params.clone())?;

            for extracted_content_list in extracted_content_batch {
                let work_status = TaskResult {
                    task_id: task.id.clone(),
                    status: TaskState::Completed,
                    extracted_content: extracted_content_list,
                };
                results.push(work_status);
            }
        }
        self.task_store.update(results);
        Ok(())
    }

    pub async fn heartbeat(&self, coordinator_client: Arc<CoordinatorClient>) -> Result<()> {
        if self
            .requires_registration
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let req = RegisterExecutorRequest {
                executor_id: self.executor_id.clone(),
                addr: self.listen_addr.clone(),
                extractor: Some(self.extractor_description.clone().into()),
            };
            let _ = coordinator_client
                .get()
                .await?
                .register_executor(req)
                .await?;
            self.requires_registration
                .store(false, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
        let req = indexify_coordinator::HeartbeatRequest {
            executor_id: self.executor_id.clone(),
        };
        let resp = coordinator_client
            .get()
            .await?
            .heartbeat(req)
            .await?
            .into_inner();
        let mut tasks = Vec::new();
        for task in resp.tasks {
            if self.task_store.has_finished(&task.id) {
                continue;
            }
            let task: Result<Task> = task.try_into();
            if let Ok(task) = task {
                tasks.push(task);
            } else {
                error!("unable to parse task: {:?}", task);
            }
        }
        if !tasks.is_empty() {
            self.task_store.add(tasks);
        }
        Ok(())
    }
}

async fn get_content(content_metadata: internal_api::ContentMetadata) -> Result<Content> {
    let content_reader = ContentReader::new(content_metadata.clone());
    let data = content_reader.read().await?;
    let extracted_content = Content {
        mime: content_metadata.content_type,
        bytes: data,
        feature: None,
        metadata: HashMap::new(),
    };
    Ok(extracted_content)
}
