use std::sync::Arc;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

use crate::config::Config;
use crate::executor_client::{
    executor_api_pb::{ExecutorState, ExecutorStatus, ExecutorUpdate, ReportExecutorStateRequest},
    ExecutorClient,
};
use crate::hardware_probe::HardwareProbe;

pub struct HeartbeatService {
    config: Arc<Config>,
    hardware_probe: HardwareProbe,
}

impl HeartbeatService {
    pub fn new(config: Arc<Config>) -> Self {
        let hardware_probe = HardwareProbe::new();
        HeartbeatService {
            config,
            hardware_probe,
        }
    }

    fn compute_state_hash(state: &ExecutorState) -> String {
        // Serialize the state to bytes (deterministically)
        let mut buf = Vec::new();
        state.encode(&mut buf).unwrap();

        // Compute SHA256 hash
        let mut hasher = Sha256::new();
        hasher.update(&buf);
        let result = hasher.finalize();

        // Return hex string
        format!("{:x}", result)
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<bool>) -> anyhow::Result<()> {
        let executor_id = self.config.executor_id.clone();
        let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs);

        info!(
            executor_id = %executor_id,
            interval = ?heartbeat_interval,
            "Starting heartbeat service"
        );

        // Connect to the executor API
        let mut client = ExecutorClient::connect(&self.config).await?;

        loop {
            // Check for shutdown signal before sending heartbeat
            if *shutdown_rx.borrow() {
                info!("Heartbeat service received shutdown signal");
                break;
            }

            // Get system resources (cached, no re-probing)
            let resources = self.hardware_probe.get_resources();

            // Create executor state (without state_hash and server_clock first)
            let mut executor_state = ExecutorState {
                executor_id: Some(executor_id.clone()),
                hostname: hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok()),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                status: Some(ExecutorStatus::Running as i32),
                total_resources: Some(resources.clone()),
                total_function_executor_resources: Some(resources),
                allowed_functions: vec![],
                function_executor_states: vec![],
                labels: self.config.labels.iter()
                    .filter_map(|label| {
                        let parts: Vec<&str> = label.splitn(2, '=').collect();
                        if parts.len() == 2 {
                            Some((parts[0].to_string(), parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect(),
                state_hash: None,
                server_clock: None,
                catalog_entry_name: self.config.catalog_entry_name.clone(),
            };

            // Compute and set state_hash
            executor_state.state_hash = Some(Self::compute_state_hash(&executor_state));

            // Set server_clock after state_hash (not included in hash)
            executor_state.server_clock = Some(0);

            // Create executor update (empty allocation results for heartbeat)
            let executor_update = ExecutorUpdate {
                executor_id: Some(executor_id.clone()),
                allocation_results: vec![],
            };

            // Report executor state
            let request = ReportExecutorStateRequest {
                executor_state: Some(executor_state),
                executor_update: Some(executor_update),
                function_call_watches: vec![],
            };

            match client.report_executor_state(request).await {
                Ok(_response) => {
                    info!("Heartbeat sent successfully");
                }
                Err(e) => {
                    error!(error = %e, "Failed to send heartbeat");
                    // Continue running despite errors
                }
            }

            // Wait for the next heartbeat interval or shutdown signal
            tokio::select! {
                _ = sleep(heartbeat_interval) => {
                    // Time to send next heartbeat
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Heartbeat service received shutdown signal during sleep");
                        break;
                    }
                }
            }
        }

        info!("Heartbeat service stopped");
        Ok(())
    }
}