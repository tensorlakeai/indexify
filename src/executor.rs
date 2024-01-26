use std::{collections::HashMap, fmt, sync::Arc, time::SystemTime};

use anyhow::{anyhow, Context, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{HeartbeatRequest, RegisterExecutorRequest};
use nanoid::nanoid;
use serde_json::json;
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, StreamExt};
use tracing::{error, info};

use crate::{
    blob_storage::{BlobStorage, BlobStorageReader},
    coordinator_client::CoordinatorClient,
    extractor::extractor_runner::ExtractorRunner,
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
    pub executor_id: String,
    extractor_runner: Arc<ExtractorRunner>,
    extractor_description: internal_api::ExtractorDescription,
    listen_addr: String,

    task_store: Arc<TaskStore>,
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
        };
        Ok(extractor_executor)
    }

    pub fn matches_mime_type(
        &self,
        content: &internal_api::Content,
    ) -> Result<bool, anyhow::Error> {
        self.extractor_runner.matches_mime_type(content)
    }

    #[tracing::instrument]
    pub fn get_executor_info(&self) -> internal_api::ExecutorInfo {
        internal_api::ExecutorInfo {
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
        content: internal_api::Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<Vec<internal_api::Content>, anyhow::Error> {
        let extracted_content = self
            // the ExtractorRunner will handle filtering out content that does not match the
            // extractor input mime types
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
            let content = get_content(task.content_metadata).await;
            if let Err(err) = &content {
                info!("failed to get content: {}", err);
                results.push(internal_api::TaskResult::failed(
                    &task.id,
                    Some(err.to_string()),
                ));
                continue;
            }
            let content = content.unwrap();
            let extracted_content_batch = self
                .extractor_runner
                .extract(vec![content], task.input_params.clone());
            if let Err(err) = &extracted_content_batch {
                info!("failed to extract content: {}", err);
                results.push(internal_api::TaskResult::failed(
                    &task.id,
                    Some(err.to_string()),
                ));
                continue;
            }

            for extracted_content_list in extracted_content_batch.unwrap() {
                results.push(internal_api::TaskResult::success(
                    &task.id,
                    extracted_content_list,
                ));
            }
        }
        self.task_store.update(results);
        Ok(())
    }

    pub async fn register(&self, coordinator_client: Arc<CoordinatorClient>) -> Result<()> {
        let req = RegisterExecutorRequest {
            executor_id: self.executor_id.clone(),
            addr: self.listen_addr.clone(),
            extractor: Some(self.extractor_description.clone().into()),
        };
        let _resp = coordinator_client
            .get()
            .await?
            .register_executor(req)
            .await
            .map_err(|e| anyhow!("unable to register executor: {:?}", e))?;
        Ok(())
    }
}

async fn get_content(
    content_metadata: internal_api::ContentMetadata,
) -> Result<internal_api::Content> {
    let data = BlobStorage::new()
        .get(&[&content_metadata.storage_url])
        .await?
        .into_iter()
        .next()
        .context("no data returned. Invalid error")?;
    let extracted_content = internal_api::Content {
        mime: content_metadata.content_type,
        bytes: data,
        feature: None,
        labels: HashMap::new(),
    };
    Ok(extracted_content)
}

pub async fn heartbeat(
    task_store: Arc<TaskStore>,
    coordinator_client: Arc<CoordinatorClient>,
    heartbeat_rx: watch::Receiver<HeartbeatRequest>,
) -> Result<()> {
    let req_stream = WatchStream::new(heartbeat_rx);
    let response = coordinator_client
        .get()
        .await?
        .heartbeat(req_stream)
        .await?;
    let mut resp_stream = response.into_inner();
    info!("starting heartbeat");
    while let Some(received) = resp_stream.next().await {
        if let Err(err) = received {
            error!("unable to receive heartbeat: {:?}", err);
            break;
        }
        let hb_resp = received.map_err(|e| anyhow!("error receiving heartbeat: {:?}", e))?;
        let mut tasks = Vec::new();
        for task in hb_resp.tasks {
            if task_store.has_finished(&task.id) {
                continue;
            }
            let task: Result<internal_api::Task> = task.try_into();
            if let Ok(task) = task {
                tasks.push(task);
            } else {
                error!("unable to parse task: {:?}", task);
            }
        }
        if !tasks.is_empty() {
            task_store.add(tasks);
        }
    }
    Ok(())
}
