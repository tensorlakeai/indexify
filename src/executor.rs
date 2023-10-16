use crate::{
    api::IndexifyAPIError,
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageBuilder,
    content_reader,
    coordinator::{ExecutorInfo, SyncExecutor, SyncWorkerResponse},
    extractors::{self, Content, ExtractorTS},
    internal_api::{EmbedQueryRequest, EmbedQueryResponse},
    persistence::{ExtractedAttributes, Work, WorkState},
    persistence::{ExtractorConfig, ExtractorOutputSchema, Repository},
    server_config::ExecutorConfig,
    vector_index::VectorIndexManager,
    vectordbs,
};
use anyhow::{anyhow, Result};
use axum::{extract::State, routing::get, routing::post, Json, Router};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use reqwest::StatusCode;
use std::fmt;
use std::net::TcpListener;
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
    config: Arc<ExecutorConfig>,
    executor_id: String,
    extractor: ExtractorTS,
    vector_index_manager: Arc<VectorIndexManager>,
    attribute_index_manager: Arc<AttributeIndexManager>,
    content_reader_builder: content_reader::ContentReaderBuilder,
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
    pub async fn new(config: Arc<ExecutorConfig>, listen_addr: String) -> Result<Self> {
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let executor_id = get_host_name(config.clone())?;
        let vector_db = vectordbs::create_vectordb(
            config.index_config.clone(),
            repository.get_db_conn_clone(),
        )?;
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            repository.clone(),
            vector_db,
            config.coordinator_addr.clone().to_string(),
        ));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));

        let blob_storage =
            BlobStorageBuilder::new(Arc::new(config.blob_storage.clone())).build()?;
        let content_reader_builder =
            content_reader::ContentReaderBuilder::new(blob_storage.clone());

        let extractor = extractors::create_extractor(config.extractor.clone())?;
        let extractor_executor = Self {
            repository,
            config,
            executor_id,
            extractor,
            vector_index_manager,
            attribute_index_manager,
            content_reader_builder,
            listen_addr,
            work_store: WorkStore::new(),
        };
        Ok(extractor_executor)
    }

    #[tracing::instrument]
    pub fn new_test(
        repository: Arc<Repository>,
        config: Arc<ExecutorConfig>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
    ) -> Result<Self> {
        let extractor = extractors::create_extractor(config.extractor.clone())?;
        let blob_storage =
            BlobStorageBuilder::new(Arc::new(config.blob_storage.clone())).build()?;
        let content_reader_builder =
            content_reader::ContentReaderBuilder::new(blob_storage.clone());

        let executor_id = get_host_name(config.clone()).unwrap();
        Ok(Self {
            repository,
            config,
            executor_id,
            extractor,
            vector_index_manager,
            attribute_index_manager,
            content_reader_builder,
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
            addr: self.config.listen_addr.clone().into(),
            extractor: ExtractorConfig {
                name: extractor_info.name,
                description: extractor_info.description,
                input_params: extractor_info.input_params,
                output_schema: extractor_info.output_schema,
            },
        }
    }

    #[tracing::instrument]
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
            extractor: self.extractor.info().unwrap(),
            addr: self.listen_addr.clone(),
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

    #[tracing::instrument]
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
            let content = self
                .repository
                .content_from_repo(&work.content_id, &work.repository_id)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;

            let content =
                Content::form_content_payload(content.clone(), &self.content_reader_builder)
                    .await?;
            if let ExtractorOutputSchema::Embedding { .. } = self.extractor.info()?.output_schema {
                info!(
                    "extracting embedding - repository: {}, extractor: {}, index: {}, content id: {}",
                    &work.repository_id, &work.extractor, &work.index_name, &content.id
                );
                let extracted_embeddings = self
                    .extractor
                    .extract_embedding(vec![content.clone()], work.extractor_params.clone())?;
                self.vector_index_manager
                    .add_embedding(&work.repository_id, &work.index_name, extracted_embeddings)
                    .await?;
                self.work_store
                    .update_work_state(&work.id, WorkState::Completed);
            }

            if let ExtractorOutputSchema::Attributes { .. } = self.extractor.info()?.output_schema {
                info!(
                    "extracting attributes - repository: {}, extractor: {}, index: {}, content id: {}",
                    &work.repository_id, &work.extractor, &work.index_name, &content.id
                );
                let extracted_attributes = self
                    .extractor
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
    config: Arc<ExecutorConfig>,
}

impl ExecutorServer {
    pub async fn new(config: Arc<ExecutorConfig>) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let addr: SocketAddr = self.config.listen_addr_sock()?;
        let listener = TcpListener::bind(addr)?;
        let listen_addr = listener.local_addr()?.to_string();
        let executor =
            Arc::new(ExtractorExecutor::new(self.config.clone(), listen_addr.clone()).await?);
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .route("/", get(root))
            .route(
                "/sync_executor",
                post(sync_worker).with_state(executor.clone()),
            )
            .route("/embed_query", post(query).with_state(executor.clone()))
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics);

        info!("starting executor server on: {}", listen_addr);
        let (tx, rx) = mpsc::channel(32);
        if let Err(err) = tx.send(TickerMessage::Heartbeat).await {
            error!("unable to send heartbeat: {:?}", err.to_string());
        }
        tokio::spawn(heartbeat(tx.clone(), rx, executor.clone()));
        axum::Server::from_tcp(listener)?
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal(tx.clone()))
            .await?;
        Ok(())
    }
}

#[tracing::instrument]
async fn root() -> &'static str {
    "Indexify Extractor Server"
}

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn query(
    extractor_executor: State<Arc<ExtractorExecutor>>,
    Json(query): Json<EmbedQueryRequest>,
) -> Result<Json<EmbedQueryResponse>, IndexifyAPIError> {
    let embedding = extractor_executor
        .embed_query(&query.text)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(EmbedQueryResponse { embedding }))
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

#[tracing::instrument]
fn get_host_name(config: Arc<ExecutorConfig>) -> Result<String> {
    Ok(config.executor_id.clone().unwrap_or_else(|| {
        let hostname = hostname::get().unwrap();
        hostname.to_string_lossy().to_string()
    }))
}

#[tracing::instrument]
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
