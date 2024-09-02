use std::net::SocketAddr;

use super::routes::RouteState;
use crate::{config::ServerConfig, routes::create_routes};
use anyhow::Result;
use axum_server::Handle;
use state_store::IndexifyState;
use tokio;
use tokio::signal;
use tracing::info;

pub struct Service {
    pub config: ServerConfig,
}

impl Service {
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    pub async fn start(&self) -> Result<()> {
        let indexify_state = IndexifyState {};
        let route_state = RouteState { indexify_state };
        let app = create_routes(route_state);
        let handle = Handle::new();
        let handle_sh = handle.clone();
        tokio::spawn(async move {
            shutdown_signal(handle_sh).await;
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

async fn shutdown_signal(handle: Handle) {
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
    info!("signal received, shutting down server gracefully");
}
