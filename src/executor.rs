use crate::{
    api::IndexifyAPIError,
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageBuilder,
    content_reader,
    extractors::{self, Content, ExtractorTS},
    persistence::{ExtractedAttributes, Work, WorkState},
    persistence::{ExtractorConfig, ExtractorOutputSchema, Repository},
    vector_index::VectorIndexManager,
    vectordbs, ExecutorInfo, ServerConfig, SyncExecutor, SyncWorkerResponse,
};
use anyhow::{anyhow, Result};
use axum::{extract::State, routing::get, routing::post, Router};
use dashmap::DashMap;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::SystemTime,
};
use tokio::{signal, sync::mpsc};
use tracing::error;
use tracing::info;

struct WorkStore {
    allocated_work: Arc<RwLock<HashMap<String, Work>>>,
}

impl WorkStore {
    fn new() -> Self {
        Self {
            allocated_work: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn remove_finished_work(&self) {
        let mut allocated_work = self.allocated_work.write().unwrap();
        allocated_work.retain(|_, work| !work.terminal_state());
    }

    fn add_work_list(&self, work_list: Vec<Work>) {
        let mut allocated_work = self.allocated_work.write().unwrap();
        for work in work_list {
            allocated_work.insert(work.id.clone(), work);
        }
    }

    fn update_work_state(&self, work_id: &str, work_state: WorkState) {
        let mut allocated_work = self.allocated_work.write().unwrap();
        if let Some(work) = allocated_work.get_mut(work_id) {
            work.work_state = work_state;
        }
    }
}

pub struct ExtractorExecutor {
    repository: Arc<Repository>,
    config: Arc<ServerConfig>,
    executor_id: String,
    extractors: DashMap<String, ExtractorTS>,
    extractor_info_list: Vec<ExtractorConfig>,
    vector_index_manager: Arc<VectorIndexManager>,
    attribute_index_manager: Arc<AttributeIndexManager>,
    content_reader_builder: content_reader::ContentReaderBuilder,

    work_store: WorkStore,
}

impl ExtractorExecutor {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self> {
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let executor_id = get_host_name(config.clone())?;
        let vector_db = vectordbs::create_vectordb(config.index_config.clone())?;
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            config.clone(),
            repository.clone(),
            vector_db,
        ));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));

        let blob_storage = BlobStorageBuilder::new(config.clone()).build()?;
        let content_reader_builder =
            content_reader::ContentReaderBuilder::new(blob_storage.clone());

        let available_extractors: DashMap<String, ExtractorTS> = DashMap::new();
        let mut extractor_info_list: Vec<ExtractorConfig> = vec![];
        for extractor_config in &config.extractors {
            let extractor = extractors::create_extractor(extractor_config.clone())?;
            let extractor_name = extractor.info()?.name.clone();
            extractor_info_list.push(extractor.info()?);
            available_extractors.insert(extractor_name, extractor);
        }
        let extractor_executor = Self {
            repository,
            config,
            executor_id,
            extractors: available_extractors,
            extractor_info_list,
            vector_index_manager,
            attribute_index_manager,
            content_reader_builder,
            work_store: WorkStore::new(),
        };
        Ok(extractor_executor)
    }

    pub fn new_test(
        repository: Arc<Repository>,
        config: Arc<ServerConfig>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
    ) -> Result<Self> {
        let available_extractors: DashMap<String, ExtractorTS> = DashMap::new();
        let mut extractor_info_list: Vec<ExtractorConfig> = vec![];
        for extractor_config in &config.extractors {
            let extractor = extractors::create_extractor(extractor_config.clone())?;
            let extractor_name = extractor.info()?.name.clone();
            extractor_info_list.push(extractor.info()?);
            available_extractors.insert(extractor_name, extractor);
        }
        let blob_storage = BlobStorageBuilder::new(config.clone()).build()?;
        let content_reader_builder =
            content_reader::ContentReaderBuilder::new(blob_storage.clone());

        let executor_id = get_host_name(config.clone()).unwrap();
        Ok(Self {
            repository,
            config,
            executor_id,
            extractors: available_extractors,
            extractor_info_list,
            vector_index_manager,
            attribute_index_manager,
            content_reader_builder,
            work_store: WorkStore::new(),
        })
    }

    pub fn get_executor_info(&self) -> ExecutorInfo {
        ExecutorInfo {
            id: self.executor_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            available_extractors: self.extractor_info_list.clone(),
        }
    }

    pub async fn sync_repo(&self) -> Result<u64, anyhow::Error> {
        let work_status: Vec<Work> = self
            .work_store
            .allocated_work
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let sync_executor_req = SyncExecutor {
            executor_id: self.executor_id.clone(),
            available_extractors: self.extractor_info_list.clone(),
            work_status: work_status.clone(),
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

        self.work_store.remove_finished_work();

        self.work_store
            .add_work_list(resp.unwrap().content_to_process);

        if let Err(err) = self.perform_work().await {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        Ok(0)
    }

    pub async fn sync_repo_test(&self, work_list: Vec<Work>) -> Result<u64, anyhow::Error> {
        self.work_store.add_work_list(work_list);
        if let Err(err) = self.perform_work().await {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        Ok(0)
    }

    pub async fn perform_work(&self) -> Result<(), anyhow::Error> {
        let work_list: Vec<Work> = self
            .work_store
            .allocated_work
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for work in work_list {
            info!(
                "performing work: {}, extractor: {}",
                &work.id, &work.extractor
            );
            let extractor = self
                .extractors
                .get(&work.extractor)
                .unwrap()
                .value()
                .clone();
            let content = self
                .repository
                .content_from_repo(&work.content_id, &work.repository_id)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;

            let content =
                Content::form_content_payload(content.clone(), &self.content_reader_builder)
                    .await?;
            if let ExtractorOutputSchema::Embedding { .. } = extractor.info()?.output_schema {
                info!(
                    "extracting embedding - repository: {}, extractor: {}, index: {}, content id: {}",
                    &work.repository_id, &work.extractor, &work.index_name, &content.id
                );
                let extracted_embeddings = extractor
                    .extract_embedding(vec![content.clone()], work.extractor_params.clone())?;
                self.vector_index_manager
                    .add_embedding(&work.repository_id, &work.index_name, extracted_embeddings)
                    .await?;
                self.work_store
                    .update_work_state(&work.id, WorkState::Completed);
            }

            if let ExtractorOutputSchema::Attributes { .. } = extractor.info()?.output_schema {
                info!(
                    "extracting attributes - repository: {}, extractor: {}, index: {}, content id: {}",
                    &work.repository_id, &work.extractor, &work.index_name, &content.id
                );
                let extracted_attributes = extractor
                    .extract_attributes(vec![content], work.extractor_params.clone())?
                    .into_iter()
                    .map(|d| {
                        ExtractedAttributes::new(
                            &d.content_id,
                            d.json.unwrap_or_default(),
                            &work.extractor,
                        )
                    })
                    .collect::<Vec<ExtractedAttributes>>();
                for extracted_attribute in &extracted_attributes {
                    self.attribute_index_manager
                        .add_index(
                            &work.repository_id,
                            &work.index_name,
                            extracted_attribute.clone(),
                        )
                        .await?;
                }
                self.work_store
                    .update_work_state(&work.id, WorkState::Completed);
            }
        }
        Ok(())
    }
}

enum TickerMessage {
    Shutdown,
    Heartbeat,
}

async fn heartbeat(
    tx: mpsc::Sender<TickerMessage>,
    mut rx: mpsc::Receiver<TickerMessage>,
    executor: Arc<ExtractorExecutor>,
) -> Result<()> {
    info!("starting executor heartbeat");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        let message = rx.recv().await;
        match message {
            Some(TickerMessage::Shutdown) => {
                info!("received shutdown signal");
                break;
            }
            Some(TickerMessage::Heartbeat) => {
                if let Err(err) = executor.sync_repo().await {
                    error!("unable to sync repo: {}", err.to_string());
                }
                interval.tick().await;
                if let Err(err) = tx.try_send(TickerMessage::Heartbeat) {
                    error!("unable to send heartbeat: {:?}", err.to_string());
                }
            }
            None => {
                info!("ticker channel closed");
                break;
            }
        }
    }
    Ok(())
}

pub struct ExecutorServer {
    config: Arc<ServerConfig>,
    executor: Arc<ExtractorExecutor>,
}

impl ExecutorServer {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self> {
        let executor = Arc::new(ExtractorExecutor::new(config.clone()).await?);
        Ok(Self { config, executor })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let app = Router::new().route("/", get(root)).route(
            "/sync_executor",
            post(sync_worker).with_state(self.executor.clone()),
        );
        let addr: SocketAddr = self.config.executor_config.server_listen_addr.parse()?;
        info!("starting executor server on: {}", &addr);
        let (tx, rx) = mpsc::channel(32);
        if let Err(err) = tx.send(TickerMessage::Heartbeat).await {
            error!("unable to send heartbeat: {:?}", err.to_string());
        }
        tokio::spawn(heartbeat(tx.clone(), rx, self.executor.clone()));
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal(tx.clone()))
            .await?;
        Ok(())
    }
}

async fn root() -> &'static str {
    "Indexify Extractor Server"
}

#[axum_macros::debug_handler]
async fn sync_worker(
    extractor_executor: State<Arc<ExtractorExecutor>>,
) -> Result<(), IndexifyAPIError> {
    let extractor_executor = extractor_executor;
    tokio::spawn(async move {
        let _ = extractor_executor.sync_repo().await;
    });
    Ok(())
}

fn get_host_name(config: Arc<ServerConfig>) -> Result<String> {
    Ok(config
        .executor_config
        .executor_id
        .clone()
        .unwrap_or_else(|| {
            let hostname = hostname::get().unwrap();
            hostname.to_string_lossy().to_string()
        }))
}

async fn shutdown_signal(tx: mpsc::Sender<TickerMessage>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            let _ = tx.try_send(TickerMessage::Shutdown);
        },
        _ = terminate => {
            let _ = tx.try_send(TickerMessage::Shutdown);
        },
    }
    info!("signal received, shutting down server gracefully");
}
