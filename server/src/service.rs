use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum_server::Handle;
use blob_store::BlobStorage;
use metrics::{init_provider, processors_metrics};
use processor::{
    dispatcher::Dispatcher,
    gc::Gc,
    namespace::NamespaceProcessor,
    runner::ProcessorRunner,
    system_tasks::SystemTasksExecutor,
    task_allocator::TaskAllocationProcessor,
};
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
    pub indexify_state: Arc<IndexifyState<Dispatcher>>,
    pub dispatcher: Arc<Dispatcher>,
    pub executor_manager: Arc<ExecutorManager>,
    pub kvs: Arc<KVS>,
    pub task_allocator: Arc<ProcessorRunner<TaskAllocationProcessor>>,
    pub system_tasks_executor: Arc<Mutex<SystemTasksExecutor>>,
    pub gc_executor: Arc<Mutex<Gc>>,
    pub namespace_processor: Arc<ProcessorRunner<NamespaceProcessor>>,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let blob_storage = Arc::new(
            BlobStorage::new(config.blob_storage.clone())
                .context("error initializing BlobStorage")?,
        );

        let dispatcher_metrics = Arc::new(processors_metrics::Metrics::new());
        let dispatcher = Arc::new(Dispatcher::new(dispatcher_metrics.clone())?);

        let indexify_state =
            IndexifyState::new(config.state_store_path.parse()?, dispatcher.clone()).await?;
        let executor_manager =
            Arc::new(ExecutorManager::new(indexify_state.clone(), dispatcher.clone()).await);

        let task_allocator = Arc::new(TaskAllocationProcessor::new(indexify_state.clone()));
        let task_allocator_runner = Arc::new(ProcessorRunner::new(
            task_allocator.clone(),
            dispatcher_metrics.clone(),
        ));
        dispatcher.add_processor(task_allocator_runner.clone());

        let namespace_processor = Arc::new(NamespaceProcessor::new(indexify_state.clone()));
        let namespace_processor_runner = Arc::new(ProcessorRunner::new(
            namespace_processor.clone(),
            dispatcher_metrics.clone(),
        ));
        dispatcher.add_processor(namespace_processor_runner.clone());

        let system_tasks_executor = Arc::new(Mutex::new(SystemTasksExecutor::new(
            indexify_state.clone(),
            shutdown_rx.clone(),
        )));

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

        Ok(Self {
            config,
            shutdown_tx,
            shutdown_rx,
            blob_storage,
            indexify_state,
            dispatcher,
            executor_manager,
            kvs,
            task_allocator: task_allocator_runner,
            system_tasks_executor,
            gc_executor,
            namespace_processor: namespace_processor_runner,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let scheduler = self.task_allocator.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            scheduler.start(shutdown_rx).await;
        });

        let (registry, _provider) = init_provider()?;

        let global_meter = opentelemetry::global::meter("indexify-server");
        let otel_metrics_service_layer = tower_otel_http_metrics::HTTPMetricsLayerBuilder::new()
            .with_meter(global_meter)
            .build()
            .unwrap();

        let metrics_registry = Arc::new(registry);
        let route_state = RouteState {
            indexify_state: self.indexify_state.clone(),
            dispatcher: self.dispatcher.clone(),
            kvs: self.kvs.clone(),
            blob_storage: self.blob_storage.clone(),
            executor_manager: self.executor_manager.clone(),
            registry: metrics_registry.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
        };
        let namespace_processor = self.namespace_processor.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move { namespace_processor.start(shutdown_rx).await });

        let system_tasks_executor = self.system_tasks_executor.clone();
        tokio::spawn(async move {
            let system_tasks_executor_guard = system_tasks_executor.lock().await;
            system_tasks_executor_guard.start().await;
        });

        let gc_executor = self.gc_executor.clone();
        tokio::spawn(async move {
            let gc_executor_guard = gc_executor.lock().await;
            gc_executor_guard.start().await;
        });

        let handle = Handle::new();
        let handle_sh = handle.clone();
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown_signal(handle_sh, shutdown_tx).await;
            info!("graceful shutdown signal received, shutting down server gracefully");
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
