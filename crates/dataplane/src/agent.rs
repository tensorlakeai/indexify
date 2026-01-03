use crate::config::Config;
use anyhow::Result;
use tokio::signal;
use tracing::info;

pub struct Agent {
    #[allow(dead_code)]
    config: Config,
}

impl Agent {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Dataplane agent started, press Ctrl+C to shutdown");
        
        shutdown_signal().await;
        
        info!("Dataplane agent shutting down gracefully");
        Ok(())
    }
}

async fn shutdown_signal() {
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}