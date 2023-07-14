use crate::{
    api::IndexifyAPIError,
    extractors::{EmbeddingExtractor, Extractor},
    index::IndexManager,
    persistence::Work,
    persistence::{ExtractorType, Repository},
    EmbeddingRouter, ServerConfig, SyncWorker, SyncWorkerResponse,
};
use anyhow::{anyhow, Result};
use axum::{extract::State, routing::get, routing::post, Router};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
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
}

pub struct ExtractorExecutor {
    repository: Arc<Repository>,
    embedding_extractor: Arc<EmbeddingExtractor>,
    config: Arc<ServerConfig>,

    work_store: WorkStore,
}

impl ExtractorExecutor {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self> {
        let embedding_router = Arc::new(EmbeddingRouter::new(config.clone())?);
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let index_manager = Arc::new(IndexManager::new(
            repository.clone(),
            config.index_config.clone(),
            embedding_router.clone(),
        )?);
        let embedding_extractor = Arc::new(EmbeddingExtractor::new(index_manager));
        let extractor_executor = Self {
            repository,
            embedding_extractor,
            config,
            work_store: WorkStore::new(),
        };
        Ok(extractor_executor)
    }

    pub fn new_test(
        repository: Arc<Repository>,
        index_manager: Arc<IndexManager>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Self {
            repository,
            embedding_extractor: Arc::new(EmbeddingExtractor::new(index_manager)),
            config,
            work_store: WorkStore::new(),
        }
    }

    pub async fn sync_repo(&self) -> Result<u64, anyhow::Error> {
        let work_status = self
            .work_store
            .allocated_work
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        let sync_executor_req = SyncWorker {
            worker_id: "unknown_id".into(),
            available_models: vec![],
            work_status,
        };
        let resp = reqwest::Client::new()
            .post(&format!(
                "http://{}/sync_executor",
                &self.config.coordinator_addr
            ))
            .json(&sync_executor_req)
            .send()
            .await?
            .json::<SyncWorkerResponse>()
            .await?;

        let work_list: Vec<Work> = resp.content_to_process;
        if let Err(err) = self.perform_work(work_list).await {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        self.work_store.allocated_work.write().unwrap().clear();
        Ok(0)
    }

    pub async fn perform_work(&self, work_list: Vec<Work>) -> Result<(), anyhow::Error> {
        for work in work_list {
            info!("performing work: {}", &work.id);
            let extractor = self
                .repository
                .get_extractor(&work.repository_id, &work.extractor)
                .await?;
            let content = self
                .repository
                .content_from_repo(&work.content_id, &work.repository_id)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            if let ExtractorType::Embedding { .. } = extractor.extractor_type {
                let index_name: String = format!("{}/{}", work.repository_id, extractor.name);
                info!(
                    "extracting embedding - extractor: {}, index: {}, content id: {}",
                    &extractor.name, &index_name, &content.id
                );
                self.embedding_extractor
                    .extract_and_store(content, &index_name)
                    .await?;
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
                info!("received heartbeat signal");
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
        let addr: SocketAddr = self.config.executor_addr.parse()?;
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
