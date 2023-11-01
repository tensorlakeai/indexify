use crate::internal_api::{ExtractedContent, ExtractedData, WorkState, WorkStatus};
use crate::server_config::ExtractorConfig;
use crate::work_store::WorkStore;
use crate::{
    attribute_index::AttributeIndexManager,
    extractors::{self, Content, ExtractorTS},
    internal_api::{ExecutorInfo, SyncExecutor, SyncWorkerResponse, Work},
    persistence::{ExtractorDescription, ExtractorOutputSchema, Repository},
    server_config::ExecutorConfig,
    vector_index::VectorIndexManager,
};
use anyhow::{anyhow, Result};
use nanoid::nanoid;
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
    config: Arc<ExecutorConfig>,
    executor_id: String,
    extractor: ExtractorTS,
    listen_addr: String,

    work_store: WorkStore,
}

impl fmt::Debug for ExtractorExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtractorExecutor")
            .field("config", &self.config)
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

        let extractor = extractors::create_extractor(extractor_config)?;
        let extractor_executor = Self {
            config: executor_config,
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
        let extractor = extractors::create_extractor(extractor_config)?;
        let executor_id = create_executor_id();
        Ok(Self {
            config: executor_config,
            executor_id,
            extractor,
            listen_addr: "127.0.0.0:9000".to_string(),
            work_store: WorkStore::new(),
        })
    }

    #[tracing::instrument]
    pub fn get_executor_info(&self) -> ExecutorInfo {
        let extractor_info = self.extractor.info().unwrap();
        ExecutorInfo {
            id: self.executor_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            addr: self.config.listen_if.clone().into(),
            extractor: ExtractorDescription {
                name: extractor_info.name,
                description: extractor_info.description,
                input_params: extractor_info.input_params,
                output_schema: extractor_info.output_schema,
            },
        }
    }

    #[tracing::instrument]
    pub async fn sync_repo(&self) -> Result<u64, anyhow::Error> {
        let completed_work = self.work_store.completed_work();
        let sync_executor_req = SyncExecutor {
            executor_id: self.executor_id.clone(),
            extractor: self.extractor.info().unwrap(),
            addr: self.listen_addr.clone(),
            work_status: completed_work,
        };
        let json_resp = reqwest::Client::new()
            .post(&format!(
                "http://{}/sync_executor",
                &self.config.coordinator_addr
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
    pub async fn embed_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error> {
        let embedding = self.extractor.extract_embedding_query(query)?;
        Ok(embedding)
    }

    #[tracing::instrument(skip(self))]
    pub async fn perform_work(&self) -> Result<(), anyhow::Error> {
        let work_list: Vec<Work> = self.work_store.pending_work();
        for work in work_list {
            info!(
                "performing work: {}, content: {}",
                &work.id, &work.content_id
            );
            let content =
                Content::form_content_payload(work.content_id, work.content_payload.clone())
                    .await?;
            if let ExtractorOutputSchema::Embedding { .. } = self.extractor.info()?.output_schema {
                let extracted_embeddings = self
                    .extractor
                    .extract_embedding(vec![content.clone()], work.params.clone())?;
                let work_status = extracted_embeddings
                    .iter()
                    .map(|e| WorkStatus {
                        work_id: work.id.clone(),
                        status: WorkState::Completed,
                        data: Some(ExtractedContent {
                            content_id: e.content_id.clone(),
                            source: e.text.clone(),
                            extracted_data: ExtractedData::Embeddings {
                                embedding: e.embeddings.clone(),
                            },
                        }),
                    })
                    .collect::<Vec<WorkStatus>>();
                self.work_store.update_work_status(work_status);
            }

            if let ExtractorOutputSchema::Attributes { .. } = self.extractor.info()?.output_schema {
                let extracted_attributes = self
                    .extractor
                    .extract_attributes(vec![content], work.params.clone())?;
                let work_status = extracted_attributes
                    .iter()
                    .filter(|e| e.json.is_some())
                    .map(|e| WorkStatus {
                        work_id: work.id.clone(),
                        status: WorkState::Completed,
                        data: Some(ExtractedContent {
                            content_id: e.content_id.clone(),
                            source: e.text.clone(),
                            extracted_data: ExtractedData::Attributes {
                                attributes: e.json.clone().unwrap(),
                            },
                        }),
                    })
                    .collect::<Vec<WorkStatus>>();
                self.work_store.update_work_status(work_status);
            }
        }
        Ok(())
    }
}
