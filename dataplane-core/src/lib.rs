use std::sync::Arc;
use tokio::sync::watch;
use tracing::{error, info};

use crate::{config::Config, heartbeat::HeartbeatService};

pub mod config;
pub mod executor_client;
mod heartbeat;
pub mod containers;
mod hardware_probe;

pub struct DataplaneService {
    heartbeat_service: Arc<heartbeat::HeartbeatService>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DataplaneService {
    pub fn new(config: Config) -> Self {
        let heartbeat_service = Arc::new(HeartbeatService::new(Arc::new(config)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        DataplaneService {
            heartbeat_service,
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let heartbeat_service = self.heartbeat_service.clone();
        let shutdown_rx = self.shutdown_rx.clone();

        // Spawn the heartbeat service in a separate task
        let heartbeat_handle = tokio::spawn(async move {
            if let Err(e) = heartbeat_service.start(shutdown_rx).await {
                error!(error = %e, "Heartbeat service error");
            }
        });

        // Wait for the heartbeat service to complete
        heartbeat_handle.await?;

        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Initiating DataplaneService shutdown");
        self.shutdown_tx.send(true)?;
        Ok(())
    }
}