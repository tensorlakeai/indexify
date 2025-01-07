use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum_server::Handle;
use blob_store::BlobStorage;
use metrics::{init_provider, scheduler_stats::Metrics as SchedulerMetrics};
use prometheus::Registry;
use state_store::{kv::KVS, IndexifyState};
use tokio::{self, signal, sync::watch};
use tracing::info;

use super::{routes::RouteState, scheduler::Scheduler};
use crate::{
    config::ServerConfig,
    executors::ExecutorManager,
    gc::Gc,
    routes::create_routes,
    system_tasks::SystemTasksExecutor,
};

pub struct Service {
    pub config: ServerConfig,
    pub indexify_state: Arc<IndexifyState>,
    metrics_registry: Arc<Registry>,
    sched_metrics: Arc<SchedulerMetrics>,
    // This is a handle to the metrics provider, which we should not drop until the end of the
    // program.
    #[allow(dead_code)]
    metrics_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl Service {
    pub async fn new(config: ServerConfig) -> Result<Self> {
        let (metrics_registry, metrics_provider) = init_provider();
        let metrics_registry = Arc::new(metrics_registry);
        let indexify_state = IndexifyState::new(config.state_store_path.parse()?).await?;
        let sched_metrics = Arc::new(SchedulerMetrics::new(indexify_state.metrics.clone()));
        Ok(Self {
            config,
            indexify_state,
            metrics_registry,
            metrics_provider,
            sched_metrics,
        })
    }

    pub async fn start(&self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let blob_storage = Arc::new(
            BlobStorage::new(self.config.blob_storage.clone())
                .context("error initializing BlobStorage")?,
        );
        let executor_manager = Arc::new(ExecutorManager::new(self.indexify_state.clone()).await);
        let kvs = KVS::new(blob_storage.clone(), "graph_ctx_state")
            .await
            .context("error initializing KVS")?;
        let route_state = RouteState {
            indexify_state: self.indexify_state.clone(),
            kvs: Arc::new(kvs),
            blob_storage: blob_storage.clone(),
            executor_manager,
            registry: self.metrics_registry.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
        };
        let app = create_routes(route_state);
        let handle = Handle::new();
        let handle_sh = handle.clone();
        let scheduler = Scheduler::new(self.indexify_state.clone(), self.sched_metrics.clone());

        let mut gc = Gc::new(
            self.indexify_state.clone(),
            blob_storage.clone(),
            shutdown_rx.clone(),
        );
        let mut system_tasks_executor =
            SystemTasksExecutor::new(self.indexify_state.clone(), shutdown_rx.clone());

        let state_watcher_rx = self.indexify_state.get_state_change_watcher();
        tokio::spawn(async move {
            info!("starting scheduler");
            let _ = scheduler.start(shutdown_rx, state_watcher_rx).await;
            info!("scheduler shutdown");
        });
        tokio::spawn(async move {
            info!("starting garbage collector");
            let _ = gc.start().await;
            info!("garbage collector shutdown");
        });
        tokio::spawn(async move {
            info!("starting system tasks executor");
            let _ = system_tasks_executor.start().await;
            info!("system tasks executor shutdown");
        });

        tokio::spawn(async move {
            shutdown_signal(handle_sh, shutdown_tx).await;
            info!("received graceful shutdown signal. Telling tasks to shutdown");
        });

        let addr: SocketAddr = self.config.listen_addr.parse()?;
        info!("server api listening on {}", self.config.listen_addr);
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
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
