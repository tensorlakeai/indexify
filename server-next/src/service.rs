use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum_server::Handle;
use blob_store::BlobStorage;
use state_store::IndexifyState;
use tokio::{self, signal, sync::watch};
use tracing::info;

use super::{routes::RouteState, scheduler::Scheduler};
use crate::{config::ServerConfig, executors::ExecutorManager, gc::Gc, routes::create_routes};

pub struct Service {
    pub config: ServerConfig,
}

impl Service {
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    pub async fn start(&self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let indexify_state = IndexifyState::new(self.config.state_store_path.parse()?)?;
        let blob_storage = Arc::new(BlobStorage::new(self.config.blob_storage.clone())?);
        let executor_manager = Arc::new(ExecutorManager::new(indexify_state.clone()));
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: blob_storage.clone(),
            executor_manager,
        };
        let app = create_routes(route_state);
        let handle = Handle::new();
        let handle_sh = handle.clone();
        let scheduler = Scheduler::new(indexify_state.clone());

        let mut gc = Gc::new(indexify_state.clone(), blob_storage, shutdown_rx.clone());

        let state_watcher_rx = indexify_state.get_state_change_watcher();
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
            shutdown_signal(handle_sh, shutdown_tx).await;
            info!("received graceful shutdown signal. Telling tasks to shutdown");
        });
        let addr: SocketAddr = self.config.listen_addr.parse()?;
        info!("server api listening on {}", self.config.listen_addr);
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .unwrap();
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
