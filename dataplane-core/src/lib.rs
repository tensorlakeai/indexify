use std::sync::Arc;

use tokio::sync::watch;
use tracing::{error, info};

use crate::{
    config::Config, function_executor_manager::FunctionExecutorManager,
    heartbeat::HeartbeatService,
};

pub mod config;
pub mod containers;
pub mod executor_client;
pub mod function_executor_manager;
mod hardware_probe;
mod heartbeat;

pub struct DataplaneService {
    heartbeat_service: Arc<heartbeat::HeartbeatService>,
    function_executor_manager: Arc<FunctionExecutorManager>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl DataplaneService {
    pub fn new(config: Config) -> Self {
        let config_arc = Arc::new(config);

        let heartbeat_service = Arc::new(HeartbeatService::new(config_arc.clone()));
        let function_executor_manager = Arc::new(FunctionExecutorManager::new());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        DataplaneService {
            heartbeat_service,
            function_executor_manager,
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let heartbeat_service = self.heartbeat_service.clone();
        let function_executor_manager = self.function_executor_manager.clone();
        let shutdown_rx = self.shutdown_rx.clone();

        // Spawn the heartbeat service in a separate task
        let heartbeat_handle = tokio::spawn(async move {
            if let Err(e) = heartbeat_service.start(shutdown_rx, function_executor_manager).await {
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
