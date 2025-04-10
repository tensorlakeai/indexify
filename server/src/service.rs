use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum_server::Handle;
use blob_store::BlobStorage;
use metrics::init_provider;
use processor::{gc::Gc, graph_processor::GraphProcessor};
use prometheus::Registry;
use state_store::{kv::KVS, IndexifyState};
use tokio::{
    self,
    signal,
    sync::{watch, Mutex},
};
use tonic::transport::Server;
use tracing::info;

use super::routes::RouteState;
use crate::{
    config::ServerConfig,
    executor_api::{executor_api_pb::executor_api_server::ExecutorApiServer, ExecutorAPIService},
    executors::ExecutorManager,
    routes::create_routes,
};

pub mod executor_api_descriptor {
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("executor_api_descriptor");
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct Service {
    pub config: Arc<ServerConfig>,
    pub shutdown_tx: watch::Sender<()>,
    pub shutdown_rx: watch::Receiver<()>,
    pub blob_storage: Arc<BlobStorage>,
    pub indexify_state: Arc<IndexifyState>,
    pub executor_manager: Arc<ExecutorManager>,
    pub kvs: Arc<KVS>,
    pub gc_executor: Arc<Mutex<Gc>>,
    pub metrics_registry: Arc<Registry>,
    pub graph_processor: Arc<GraphProcessor>,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let config = Arc::new(config);
        let registry = init_provider()?;
        let metrics_registry = Arc::new(registry);
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let blob_storage = Arc::new(
            BlobStorage::new(config.blob_storage.clone())
                .context("error initializing BlobStorage")?,
        );

        let indexify_state = IndexifyState::new(config.state_store_path.parse()?).await?;
        let executor_manager = ExecutorManager::new(indexify_state.clone()).await;

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
        let graph_processor = Arc::new(GraphProcessor::new(indexify_state.clone()));
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
            graph_processor,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let tokio_rt = tokio::runtime::Handle::current();

        let graph_processor = self.graph_processor.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        // spawn the graph processor in a blocking thread
        // to avoid blocking the tokio runtime when working with
        // in-memory data structures.
        tokio::task::spawn_blocking(move || {
            tokio_rt.block_on(async move {
                graph_processor.start(shutdown_rx).await;
            });
        });

        // Spawn monitoring task with shutdown receiver
        let monitor = self.executor_manager.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            monitor.start_heartbeat_monitor(shutdown_rx).await;
        });

        let global_meter = opentelemetry::global::meter("server-http");
        let otel_metrics_service_layer =
            tower_otel_http_metrics::HTTPMetricsLayerBuilder::builder()
                .with_meter(global_meter)
                .build()
                .unwrap();

        let api_metrics = Arc::new(metrics::api_io_stats::Metrics::new());

        let route_state = RouteState {
            config: self.config.clone(),
            indexify_state: self.indexify_state.clone(),
            kvs: self.kvs.clone(),
            blob_storage: self.blob_storage.clone(),
            executor_manager: self.executor_manager.clone(),
            registry: self.metrics_registry.clone(),
            metrics: api_metrics.clone(),
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

        let addr_grpc: SocketAddr = self.config.listen_addr_grpc.parse()?;
        let mut shutdown_rx = self.shutdown_rx.clone();
        let indexify_state = self.indexify_state.clone();
        let executor_manager = self.executor_manager.clone();
        let blob_storage = self.blob_storage.clone();
        tokio::spawn(async move {
            info!("server grpc listening on {}", addr_grpc);
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(executor_api_descriptor::FILE_DESCRIPTOR_SET)
                .build_v1()
                .unwrap();
            Server::builder()
                .add_service(ExecutorApiServer::new(ExecutorAPIService::new(
                    indexify_state,
                    executor_manager,
                    api_metrics,
                    blob_storage,
                )))
                .add_service(reflection_service)
                .serve_with_shutdown(addr_grpc, async move {
                    shutdown_rx.changed().await.ok();
                })
                .await
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
