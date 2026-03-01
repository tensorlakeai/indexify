use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::Router;
use axum_server::Handle;
use axum_tracing_opentelemetry::middleware::{OtelAxumLayer, OtelInResponseLayer};
use hyper::Method;
use otlp_logs_exporter::{OtlpLogsExporter, runtime::Tokio};
pub use proto_api::descriptor as executor_api_descriptor;
use tokio::{
    self,
    signal,
    sync::{mpsc, watch},
};
use tonic::transport::Server;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{Instrument, info, info_span};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    config::ServerConfig,
    executor_api::{ExecutorAPIService, executor_api_pb::executor_api_server::ExecutorApiServer},
    executors::ExecutorManager,
    metrics::{self, init_provider},
    middleware::InstanceRequestSpan,
    processor::{
        application_processor::ApplicationProcessor,
        request_state_change_processor::RequestStateChangeProcessor,
        usage_processor::UsageProcessor,
    },
    queue::Queue,
    routes::routes_state::RouteState,
    routes_internal::{configure_helper_router, configure_internal_routes},
    routes_v1::configure_v1_routes,
    state_store::{
        IndexifyState,
        request_event_buffers::RequestEventBuffers,
        request_events::RequestStateChangeEvent,
    },
};

fn otel_axum_filter(path: &str) -> bool {
    path.starts_with("/healthz") || path.starts_with("/docs") || path.starts_with("/ui")
}

#[allow(dead_code)]
pub struct Service {
    pub config: Arc<ServerConfig>,
    pub shutdown_tx: watch::Sender<()>,
    pub shutdown_rx: watch::Receiver<()>,
    pub blob_storage_registry: Arc<BlobStorageRegistry>,
    pub indexify_state: Arc<IndexifyState>,
    pub executor_manager: Arc<ExecutorManager>,
    pub application_processor: Arc<ApplicationProcessor>,
    pub usage_processor: Arc<UsageProcessor>,
    pub request_state_change_processor: Arc<RequestStateChangeProcessor>,
    /// File-dump receiver and path, held until the processor worker is spawned.
    local_request_events_log: Option<(mpsc::UnboundedReceiver<RequestStateChangeEvent>, String)>,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let config = Arc::new(config);
        init_provider(
            config.telemetry.enable_metrics,
            config.telemetry.endpoint.as_ref(),
            config.telemetry.metrics_interval,
            &config.instance_id(),
            env!("CARGO_PKG_VERSION"),
        )?;
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        let executor_catalog = crate::state_store::ExecutorCatalog {
            entries: config.executor_catalog.clone(),
        };
        if executor_catalog.empty() {
            info!("No configured executor label sets; allowing all executors");
        }

        let (request_event_buffers, local_request_events_log) =
            RequestEventBuffers::from_cloud_events_config(config.cloud_events.as_ref());

        let indexify_state = IndexifyState::new(
            config.state_store_path.parse()?,
            config.rocksdb_config.clone(),
            executor_catalog,
            request_event_buffers,
        )
        .await?;

        let blob_storage_registry = Arc::new(
            BlobStorageRegistry::new(
                config.blob_storage.path.as_str(),
                config.blob_storage.region.clone(),
            )
            .await?,
        );

        let namespaces = indexify_state.reader().get_all_namespaces().await?;
        for namespace in namespaces {
            if let Some(blob_storage_bucket) = namespace.blob_storage_bucket {
                blob_storage_registry
                    .create_new_blob_store(
                        &namespace.name,
                        blob_storage_bucket.as_str(),
                        namespace.blob_storage_region.clone(),
                    )
                    .await?;
            }
        }

        let executor_manager =
            ExecutorManager::new(indexify_state.clone(), blob_storage_registry.clone()).await;

        let application_processor = Arc::new(ApplicationProcessor::new(
            indexify_state.clone(),
            config.queue_size,
            std::time::Duration::from_secs(config.cluster_vacuum_interval_secs),
            std::time::Duration::from_secs(config.snapshot_timeout_secs),
        ));
        application_processor.validate_app_constraints().await?;

        let usage_queue = Arc::new(match &config.usage_queue {
            Some(cfg) => Some(Queue::new(cfg.clone()).await?),
            None => None,
        });

        let usage_processor =
            Arc::new(UsageProcessor::new(usage_queue, indexify_state.clone()).await?);

        let request_state_change_processor = Arc::new(RequestStateChangeProcessor::new(
            indexify_state.clone(),
            blob_storage_registry.clone(),
        ));

        Ok(Self {
            config,
            shutdown_tx,
            shutdown_rx,
            blob_storage_registry,
            indexify_state,
            executor_manager,
            application_processor,
            usage_processor,
            request_state_change_processor,
            local_request_events_log,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let span = info_span!(
            "service",
            env = self.config.env,
            "indexify-instance" = self.config.instance_id()
        );
        let tokio_rt = tokio::runtime::Handle::current();

        let application_processor = self.application_processor.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        // spawn the application processor in a blocking thread
        // to avoid blocking the tokio runtime when working with
        // in-memory data structures.
        let env = self.config.env.clone();
        let instance_id = self.config.instance_id();
        tokio::task::spawn_blocking(move || {
            let span = info_span!(
                "application processor",
                env,
                "indexify-instance" = instance_id
            );
            tokio_rt.block_on(
                async move {
                    application_processor.start(shutdown_rx).await;
                }
                .instrument(span.clone()),
            );
        });

        let usage_processor = self.usage_processor.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        let env = self.config.env.clone();
        let instance_id = self.config.instance_id();
        tokio::spawn(async move {
            let span = info_span!(
                "Initializing Usage Processor",
                env,
                "indexify-instance" = instance_id
            );

            let _ = {
                usage_processor.start(shutdown_rx).await;
                ().instrument(span.clone())
            };
        });

        let request_state_change_processor = self.request_state_change_processor.clone();
        let local_request_events_log = self.local_request_events_log.take();
        let cloud_events_config = self.config.cloud_events.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        let env = self.config.env.clone();
        let instance_id = self.config.instance_id();
        tokio::spawn(async move {
            let span = info_span!(
                "Initializing Request State Change Processor",
                env,
                "indexify-instance" = instance_id
            );

            let cloud_events_exporter = if let Some(config) = &cloud_events_config {
                info!(?config, "Initializing CloudEvents exporter");
                match OtlpLogsExporter::with_default_retry(Tokio, &config.endpoint).await {
                    Ok(exporter) => Some(exporter),
                    Err(err) => {
                        tracing::error!(?err, "Failed to create CloudEvents exporter");
                        None
                    }
                }
            } else {
                None
            };

            let _ = {
                request_state_change_processor
                    .start(cloud_events_exporter, local_request_events_log, shutdown_rx)
                    .await;
                ().instrument(span.clone())
            };
        });

        // Spawn monitoring task with shutdown receiver
        let monitor = self.executor_manager.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(
            async move {
                monitor.start_heartbeat_monitor(shutdown_rx).await;
            }
            .instrument(span.clone()),
        );

        let api_metrics = Arc::new(metrics::api_io_stats::Metrics::new());

        let route_state = RouteState {
            indexify_state: self.indexify_state.clone(),
            blob_storage: self.blob_storage_registry.clone(),
            executor_manager: self.executor_manager.clone(),
            metrics: api_metrics.clone(),
            config: self.config.clone(),
        };

        let handle = Handle::new();
        let handle_sh = handle.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(
            async move {
                shutdown_signal(handle_sh, shutdown_tx).await;
                info!("graceful shutdown signal received, shutting down server gracefully");
            }
            .instrument(span.clone()),
        );

        let addr_grpc: SocketAddr = self.config.listen_addr_grpc.parse()?;
        let mut shutdown_rx = self.shutdown_rx.clone();
        let indexify_state = self.indexify_state.clone();
        let executor_manager = self.executor_manager.clone();
        let blog_storage_registry = self.blob_storage_registry.clone();

        let instance_span = InstanceRequestSpan::new(&self.config.env, &self.config.instance_id());
        let instance_span_clone = instance_span.clone();

        tokio::spawn(
            async move {
                info!("server grpc listening on {}", addr_grpc);
                let instance_trace = TraceLayer::new_for_grpc().make_span_with(instance_span_clone);
                let reflection_service = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(
                        executor_api_descriptor::EXECUTOR_API_FILE_DESCRIPTOR_SET,
                    )
                    .build_v1()
                    .unwrap();
                // 4GB max message size for large execution plans
                const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024 * 1024;

                Server::builder()
                    .layer(instance_trace)
                    .add_service(
                        ExecutorApiServer::new(ExecutorAPIService::new(
                            indexify_state,
                            executor_manager,
                            blog_storage_registry,
                        ))
                        .max_decoding_message_size(MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(MAX_MESSAGE_SIZE),
                    )
                    .add_service(reflection_service)
                    .serve_with_shutdown(addr_grpc, async move {
                        shutdown_rx.changed().await.ok();
                    })
                    .await
            }
            .instrument(span.clone()),
        );

        let addr: SocketAddr = self.config.listen_addr.parse()?;
        info!("server api listening on {}", self.config.listen_addr);
        let v1_routes = configure_v1_routes(route_state.clone());
        let internal_routes = configure_internal_routes(route_state.clone());
        let helper_routes = configure_helper_router(route_state.clone());

        let mut router = Router::new().merge(v1_routes).merge(internal_routes);

        // Only apply OpenTelemetry middleware layers when telemetry is enabled
        if self.config.telemetry.tracing_enabled() {
            router = router
                .layer(OtelInResponseLayer)
                .layer(OtelAxumLayer::default().filter(otel_axum_filter));
        }

        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::DELETE])
            .allow_origin(Any)
            .allow_headers(Any);
        let instance_trace = TraceLayer::new_for_http().make_span_with(instance_span);

        let router = router
            .merge(helper_routes)
            .layer(instance_trace)
            .layer(cors);
        axum_server::bind(addr)
            .handle(handle)
            .serve(router.into_make_service())
            .await
            .map_err(Into::into)
    }
}

async fn shutdown_signal(handle: Handle<SocketAddr>, shutdown_tx: watch::Sender<()>) {
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
