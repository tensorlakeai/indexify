use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{extract::DefaultBodyLimit, Router};
use axum_server::Handle;
use axum_tracing_opentelemetry::middleware::{OtelAxumLayer, OtelInResponseLayer};
use hyper::Method;
use tokio::{
    self,
    signal,
    sync::{watch, Mutex},
};
use tonic::transport::Server;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::{
    blob_store::{registry::BlobStorageRegistry, BlobStorage},
    config::ServerConfig,
    executor_api::{executor_api_pb::executor_api_server::ExecutorApiServer, ExecutorAPIService},
    executors::ExecutorManager,
    metrics::{self, init_provider},
    processor::{gc::Gc, graph_processor::GraphProcessor, task_cache},
    routes::routes_state::RouteState,
    routes_internal::configure_internal_routes,
    routes_v1::configure_v1_routes,
    state_store::{kv::KVS, IndexifyState},
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
    pub blob_storage_registry: Arc<BlobStorageRegistry>,
    pub indexify_state: Arc<IndexifyState>,
    pub executor_manager: Arc<ExecutorManager>,
    pub kvs: Arc<KVS>,
    pub gc_executor: Arc<Mutex<Gc>>,
    pub task_cache: Arc<task_cache::TaskCache>,
    pub graph_processor: Arc<GraphProcessor>,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let config = Arc::new(config);
        init_provider(
            config.telemetry.enable_metrics,
            config.telemetry.endpoint.as_ref(),
            config.telemetry.metrics_interval,
            config.telemetry.instance_id.as_ref(),
            env!("CARGO_PKG_VERSION"),
        )?;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let kv_storage = Arc::new(
            BlobStorage::new(config.kv_storage.clone()).context("error initializing KVStorage")?,
        );

        let executor_catalog = crate::state_store::ExecutorCatalog {
            entries: config.executor_catalog.clone(),
        };
        if executor_catalog.allows_any_labels() {
            info!("No configured executor label sets; allowing all executors");
        }
        let indexify_state =
            IndexifyState::new(config.state_store_path.parse()?, executor_catalog).await?;

        let blob_storage_registry = Arc::new(BlobStorageRegistry::new(
            config.blob_storage.path.as_str(),
            config.blob_storage.region.clone(),
        )?);

        let namespaces = indexify_state.reader().get_all_namespaces()?;
        for namespace in namespaces {
            if let Some(blob_storage_bucket) = namespace.blob_storage_bucket {
                blob_storage_registry.create_new_blob_store(
                    &namespace.name,
                    &blob_storage_bucket,
                    namespace.blob_storage_region.clone(),
                )?;
            }
        }

        let executor_manager =
            ExecutorManager::new(indexify_state.clone(), blob_storage_registry.clone()).await;

        let gc_executor = Arc::new(Mutex::new(Gc::new(
            indexify_state.clone(),
            blob_storage_registry.clone(),
            shutdown_rx.clone(),
        )));

        let kvs = Arc::new(
            KVS::new(kv_storage, "graph_ctx_state")
                .await
                .context("error initializing KVS")?,
        );
        let task_cache = Arc::new(task_cache::TaskCache::new(indexify_state.clone()));
        let graph_processor = Arc::new(GraphProcessor::new(
            indexify_state.clone(),
            task_cache.clone(),
            config.queue_size,
        ));
        graph_processor.validate_graph_constraints().await?;
        Ok(Self {
            config,
            shutdown_tx,
            shutdown_rx,
            blob_storage_registry,
            indexify_state,
            executor_manager,
            kvs,
            gc_executor,
            task_cache,
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
            blob_storage: self.blob_storage_registry.clone(),
            executor_manager: self.executor_manager.clone(),
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
        let blog_storage_registry = self.blob_storage_registry.clone();
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
                    blog_storage_registry,
                )))
                .add_service(reflection_service)
                .serve_with_shutdown(addr_grpc, async move {
                    shutdown_rx.changed().await.ok();
                })
                .await
        });

        let addr: SocketAddr = self.config.listen_addr.parse()?;
        info!("server api listening on {}", self.config.listen_addr);
        let internal_routes = configure_internal_routes(route_state.clone());
        let v1_routes = configure_v1_routes(route_state.clone());
        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_origin(Any)
            .allow_headers(Any);
        let router = Router::new()
            .merge(internal_routes)
            .merge(v1_routes)
            .layer(otel_metrics_service_layer)
            .layer(OtelInResponseLayer)
            .layer(OtelAxumLayer::default())
            .layer(cors)
            .layer(DefaultBodyLimit::disable());
        axum_server::bind(addr)
            .handle(handle)
            .serve(router.into_make_service())
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
