use super::routes::RouteState;
use super::scheduler::Scheduler;
use crate::{config::ServerConfig, routes::create_routes};
use anyhow::Result;
use axum_server::Handle;
use state_store::IndexifyState;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio;
use tokio::signal;
use tokio::sync::watch;
use tracing::info;

pub struct Service {
    pub config: ServerConfig,
}

impl Service {
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    pub async fn start(&self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let indexify_state = Arc::new(IndexifyState::new(self.config.state_store_path.parse()?)?);
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
        };
        let app = create_routes(route_state);
        let handle = Handle::new();
        let handle_sh = handle.clone();
        let scheduler = Scheduler::new(indexify_state.clone());
        tokio::spawn(async move {
            info!("starting scheduler");
            let _ = scheduler
                .start(shutdown_rx, indexify_state.get_state_change_watcher())
                .await;
            info!("scheduler shutdown");
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
