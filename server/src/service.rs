use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum_server::Handle;
use blob_store::BlobStorage;
use metrics::init_provider;
use processor::{gc::Gc, graph_processor::GraphProcessor, task_allocator, task_creator};
use prometheus::Registry;
use state_store::{kv::KVS, IndexifyState};
use tokio::{
    self,
    signal,
    sync::{watch, Mutex},
};
use tracing::info;

use super::routes::RouteState;
use crate::{config::ServerConfig, executors::ExecutorManager, routes::create_routes};

#[derive(Clone)]
#[allow(dead_code)]
pub struct Service {
    pub config: ServerConfig,
    pub shutdown_tx: watch::Sender<()>,
    pub shutdown_rx: watch::Receiver<()>,
    pub blob_storage: Arc<BlobStorage>,
    pub indexify_state: Arc<IndexifyState>,
    pub executor_manager: Arc<ExecutorManager>,
    pub kvs: Arc<KVS>,
    pub gc_executor: Arc<Mutex<Gc>>,
    pub metrics_registry: Arc<Registry>,
    pub task_allocator: Arc<task_allocator::TaskAllocationProcessor>,
    pub task_creator: Arc<task_creator::TaskCreator>,
    pub graph_processor: Arc<GraphProcessor>,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let registry = init_provider()?;
        let metrics_registry = Arc::new(registry);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let blob_storage = Arc::new(
            BlobStorage::new(config.blob_storage.clone())
                .context("error initializing BlobStorage")?,
        );

        let indexify_state = IndexifyState::new(config.state_store_path.parse()?).await?;
        let executor_manager = Arc::new(ExecutorManager::new(indexify_state.clone()).await);

        let gc_executor = Arc::new(Mutex::new(Gc::new(
            indexify_state.clone(),
            blob_storage.clone(),
            shutdown_rx.clone(),
        )));

        let kvs = Arc::new(
            KVS::new(blob_storage.clone(), "graph_ctx_state")
                .await
                .context("error initializing KVS")?,
        );
        let task_allocator = Arc::new(task_allocator::TaskAllocationProcessor::new());
        let task_creator = Arc::new(task_creator::TaskCreator::new(indexify_state.clone()));
        let graph_processor = Arc::new(GraphProcessor::new(
            indexify_state.clone(),
            task_allocator.clone(),
            task_creator.clone(),
        ));
        Ok(Self {
            config,
            shutdown_tx,
            shutdown_rx,
            blob_storage,
            indexify_state,
            executor_manager,
            kvs,
            gc_executor,
            metrics_registry,
            task_allocator,
            task_creator,
            graph_processor,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let shutdown_rx = self.shutdown_rx.clone();
        let graph_processor = self.graph_processor.clone();
        tokio::spawn(async move {
            graph_processor.start(shutdown_rx).await;
        });

        let global_meter = opentelemetry::global::meter("server-http");
        let otel_metrics_service_layer = tower_otel_http_metrics::HTTPMetricsLayerBuilder::new()
            .with_meter(global_meter)
            .build()
            .unwrap();

        let route_state = RouteState {
            indexify_state: self.indexify_state.clone(),
            kvs: self.kvs.clone(),
            blob_storage: self.blob_storage.clone(),
            executor_manager: self.executor_manager.clone(),
            registry: self.metrics_registry.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
        };
        let gc_executor = self.gc_executor.clone();
        tokio::spawn(async move {
            let gc_executor_guard = gc_executor.lock().await;
            gc_executor_guard.start().await;
        });

        let handle = Handle::new();
        let handle_sh = handle.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        let kvs = self.kvs.clone();
        tokio::spawn(async move {
            shutdown_signal(handle_sh, shutdown_tx).await;
            info!("graceful shutdown signal received, shutting down server gracefully");
            if let Err(err) = kvs.close_db().await {
                tracing::error!("error closing kv store: {:?}", err);
            }
        });

        let addr: SocketAddr = self.config.listen_addr.parse()?;
        info!("server api listening on {}", self.config.listen_addr);
        let routes = create_routes(route_state).layer(otel_metrics_service_layer);
        axum_server::bind(addr)
            .handle(handle)
            .serve(routes.into_make_service())
            .await?;

        Ok(())
    }
}

async fn shutdown_signal(handle: Handle, shutdown_tx: watch::Sender<()>) {
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
        },
        _ = terminate => {
        },
    }
    handle.shutdown();
    shutdown_tx.send(()).unwrap();
    info!("signal received, shutting down server gracefully");
}
