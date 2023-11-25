use crate::content_reader::ContentReader;
use crate::internal_api::{self, Content, WorkState, WorkStatus};
use crate::server_config::ExtractorConfig;
use crate::work_store::WorkStore;
use crate::{
    attribute_index::AttributeIndexManager,
    extractor::{self, ExtractorTS},
    internal_api::{ExecutorInfo, ExtractorDescription, SyncExecutor, SyncWorkerResponse, Work},
    persistence::Repository,
    server_config::ExecutorConfig,
    vector_index::VectorIndexManager,
};
use anyhow::{anyhow, Ok, Result};
use nanoid::nanoid;
use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::error;
use tracing::info;

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
        extractor_config: Arc<ExtractorConfig>,
        listen_addr: String,
    ) -> Result<Self> {
        let executor_id = create_executor_id();

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
    pub fn get_executor_info(&self) -> ExecutorInfo {
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
        ExecutorInfo {
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
    pub async fn sync_repo(&self) -> Result<u64, anyhow::Error> {
        let completed_work = self.work_store.completed_work();
        let extractor_schema = self.extractor.schemas().unwrap();
        let executor_info = self.get_executor_info();
        let extractor_description = ExtractorDescription {
            name: self.extractor_config.name.clone(),
            description: self.extractor_config.description.clone(),
            input_params: extractor_schema.input_params,
            schema: executor_info.extractor.schema,
        };
        let sync_executor_req = SyncExecutor {
            executor_id: self.executor_id.clone(),
            extractor: extractor_description,
            addr: self.listen_addr.clone(),
            work_status: completed_work,
        };
        let json_resp = reqwest::Client::new()
            .post(&format!(
                "http://{}/sync_executor",
                &self.executor_config.coordinator_addr
            ))
            .json(&sync_executor_req)
            .send()
            .await?
            .text()
            .await?;

        let resp: Result<SyncWorkerResponse, serde_json::Error> = serde_json::from_str(&json_resp);
        if let Err(err) = resp {
            return Err(anyhow!(
                "unable to parse server response: err: {:?}, resp: {}",
                err,
                &json_resp
            ));
        }

        self.work_store.clear_completed_work();

        self.work_store
            .add_work_list(resp.unwrap().content_to_process);

        if let Err(err) = self.perform_work().await {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        Ok(0)
    }

    #[tracing::instrument]
    pub async fn sync_repo_test(&self, work_list: Vec<Work>) -> Result<u64, anyhow::Error> {
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
    ) -> Result<Vec<Content>, anyhow::Error> {
        let extracted_content = self.extractor.extract(vec![content], json!({}))?;
        let content = extracted_content
            .get(0)
            .ok_or(anyhow!("no content was extracted"))?
            .to_owned();
        Ok(content)
    }

    #[tracing::instrument(skip(self))]
    pub async fn perform_work(&self) -> Result<(), anyhow::Error> {
        let work_list: Vec<Work> = self.work_store.pending_work();
        let mut work_status_list = Vec::new();
        for work in work_list {
            info!("performing work: {}", &work.id);
            let content = self
                .create_content_from_payload(work.content_payload)
                .await?;
            let extracted_content_batch =
                self.extractor.extract(vec![content], work.params.clone())?;

            for extracted_content_list in extracted_content_batch {
                let work_status = WorkStatus {
                    work_id: work.id.clone(),
                    status: WorkState::Completed,
                    extracted_content: extracted_content_list,
                };
                work_status_list.push(work_status);
            }
        }
        self.work_store.update_work_status(work_status_list);
        Ok(())
    }

    async fn create_content_from_payload(
        &self,
        content_payload: internal_api::ContentPayload,
    ) -> Result<Content, anyhow::Error> {
        let content_reader = ContentReader::new(content_payload.clone());
        let data = content_reader.read().await?;
        let extracted_content = Content {
            content_type: content_payload.content_type,
            source: data,
            feature: None,
        };
        Ok(extracted_content)
    }
}
