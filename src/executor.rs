use std::{collections::HashMap, fmt, sync::Arc, time::SystemTime};

use anyhow::{anyhow, Ok, Result};
use bollard::grpc::error;
use nanoid::nanoid;
use serde_json::json;
use tracing::{error, info};

use crate::{
    attribute_index::AttributeIndexManager,
    content_reader::ContentReader,
    extractor::{self, python_path, ExtractorTS},
    internal_api::{
        self,
        Content,
        ExecutorHeartbeatResponse,
        ExecutorMetadata,
        ExtractorDescription,
        ExtractorHeartbeat,
        Task,
        WorkState,
        TaskStatus,
    },
    persistence::Repository,
    server_config::{ExecutorConfig, ExtractorConfig},
    vector_index::VectorIndexManager,
    work_store::WorkStore,
};

fn create_executor_id() -> String {
    let host_name = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_default();

    format!("{}_{}", nanoid!(), host_name)
}

pub struct ExtractorExecutor {
    executor_config: Arc<ExecutorConfig>,
    extractor_config: Arc<ExtractorConfig>,
    executor_id: String,
    extractor: ExtractorTS,
    listen_addr: String,

    work_store: WorkStore,
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
        extractor_config_path: &str,
        listen_addr: String,
    ) -> Result<Self> {
        let executor_id = create_executor_id();
        let extractor_config = Arc::new(ExtractorConfig::from_path(extractor_config_path)?);
        info!("looking up extractor at path: {}", &extractor_config_path);
        python_path::set_python_path(extractor_config_path)?;

        let extractor =
            extractor::create_extractor(&extractor_config.module, &extractor_config.name)?;
        let extractor_executor = Self {
            executor_config,
            extractor_config,
            executor_id,
            extractor,
            listen_addr,
            work_store: WorkStore::new(),
        };
        Ok(extractor_executor)
    }

    #[tracing::instrument]
    pub fn new_test(
        repository: Arc<Repository>,
        executor_config: Arc<ExecutorConfig>,
        extractor_config: Arc<ExtractorConfig>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
    ) -> Result<Self> {
        let extractor =
            extractor::create_extractor(&extractor_config.module, &extractor_config.name)?;
        let executor_id = create_executor_id();
        Ok(Self {
            executor_config,
            extractor_config,
            executor_id,
            extractor,
            listen_addr: "127.0.0.0:9000".to_string(),
            work_store: WorkStore::new(),
        })
    }

    #[tracing::instrument]
    pub fn get_executor_info(&self) -> ExecutorMetadata {
        let extractor_info = self.extractor.schemas().unwrap();
        let mut output_schemas = HashMap::new();
        for (output_name, embedding_schema) in extractor_info.embedding_schemas {
            let extractor::EmbeddingSchema {
                dim,
                distance_metric,
            } = embedding_schema;
            let distance_metric = distance_metric.to_string();
            output_schemas.insert(
                output_name,
                internal_api::OutputSchema::Embedding {
                    dim,
                    distance_metric,
                },
            );
        }
        ExecutorMetadata {
            id: self.executor_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            addr: self.executor_config.listen_if.clone().into(),
            extractor: ExtractorDescription {
                name: self.extractor_config.name.clone(),
                description: self.extractor_config.description.clone(),
                input_params: extractor_info.input_params,
                schema: internal_api::ExtractorSchema {
                    output: output_schemas,
                },
            },
        }
    }

    #[tracing::instrument]
    pub async fn sync_repo(&self) -> Result<(), anyhow::Error> {
        let extractor_schema = self.extractor.schemas().unwrap();
        let executor_info = self.get_executor_info();
        let extractor_description = ExtractorDescription {
            name: self.extractor_config.name.clone(),
            description: self.extractor_config.description.clone(),
            input_params: extractor_schema.input_params,
            schema: executor_info.extractor.schema,
        };
        let hb_request = ExtractorHeartbeat {
            executor_id: self.executor_id.clone(),
            extractor: extractor_description,
            addr: self.listen_addr.clone(),
        };
        let resp = reqwest::Client::new()
            .post(&format!(
                "http://{}/heartbeat",
                &self.executor_config.coordinator_addr
            ))
            .json(&hb_request)
            .send()
            .await
            .map_err(|e| anyhow!("unable to send heartbeat: {}", e))?
            .text()
            .await
            .map_err(|e| anyhow!("unable to extract text from heartbeat response: {}", e))?;

        let resp: ExecutorHeartbeatResponse = serde_json::from_str(&resp)
            .map_err(|e| anyhow!("unable to parse heartbeat response: {}", e))?;

        self.work_store.add_work_list(resp.content_to_process);

        Ok(())
    }

    #[tracing::instrument]
    pub async fn sync_repo_test(&self, work_list: Vec<Task>) -> Result<u64, anyhow::Error> {
        self.work_store.add_work_list(work_list);
        if let Err(err) = self.perform_work().await {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        Ok(0)
    }

    #[tracing::instrument]
    pub async fn extract(
        &self,
        content: Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<Vec<Content>, anyhow::Error> {
        let extracted_content = self
            .extractor
            .extract(vec![content], input_params.unwrap_or(json!({})))?;
        let content = extracted_content
            .get(0)
            .ok_or(anyhow!("no content was extracted"))?
            .to_owned();
        Ok(content)
    }

    #[tracing::instrument(skip(self))]
    pub async fn perform_work(&self) -> Result<(), anyhow::Error> {
        let tasks: Vec<Task> = self.work_store.pending_work();
        let mut task_statuses = Vec::new();
        for task in tasks {
            info!("performing work: {}", &task.id);
            let content = self
                .create_content_from_metadata(task.content_metadata)
                .await?;
            let extracted_content_batch = self
                .extractor
                .extract(vec![content], task.input_params.clone())?;

            for extracted_content_list in extracted_content_batch {
                let task_status = TaskStatus {
                    work_id: task.id.clone(),
                    status: WorkState::Completed,
                    extracted_content: extracted_content_list,
                };
                task_statuses.push(task_status);
            }
        }
        self.work_store.update_work_status(task_statuses);
        Ok(())
    }

    async fn create_content_from_metadata(
        &self,
        content_metadata: internal_api::ContentMetadata,
    ) -> Result<Content, anyhow::Error> {
        let content_reader = ContentReader::new(content_metadata.clone());
        let data = content_reader.read().await?;
        let extracted_content = Content {
            content_type: content_metadata.content_type,
            source: data,
            feature: None,
        };
        Ok(extracted_content)
    }
}
