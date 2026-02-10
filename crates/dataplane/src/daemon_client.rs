//! gRPC client for communicating with the container daemon over TCP.

use std::time::Duration;

use anyhow::{Context, Result};
use proto_api::container_daemon_pb::{
    HealthRequest,
    SendSignalRequest,
    container_daemon_client::ContainerDaemonClient,
};
use tonic::transport::Channel;
use tracing::{debug, warn};

/// Client for communicating with the container daemon.
#[derive(Clone)]
pub struct DaemonClient {
    client: ContainerDaemonClient<Channel>,
}

impl DaemonClient {
    /// Create a new daemon client connected to the given address (e.g.,
    /// "172.17.0.2:9500").
    pub async fn connect(addr: &str) -> Result<Self> {
        debug!(addr = %addr, "Connecting to daemon");
        let channel = crate::grpc::connect_channel(addr).await?;
        let client = ContainerDaemonClient::new(channel);
        Ok(Self { client })
    }

    /// Try to connect to the daemon, retrying until timeout.
    pub async fn connect_with_retry(addr: &str, timeout: Duration) -> Result<Self> {
        crate::retry::retry_until_deadline(
            timeout,
            crate::grpc::POLL_INTERVAL,
            &format!("connecting to daemon at {}", addr),
            || Self::connect(addr),
            || async { Ok(()) },
        )
        .await
    }

    /// Check if the daemon is healthy and ready.
    pub async fn health(&mut self) -> Result<bool> {
        let response = self
            .client
            .health(HealthRequest {})
            .await
            .context("Health check failed")?
            .into_inner();

        let healthy = response.healthy.unwrap_or(false);
        if healthy {
            debug!(
                version = response.version,
                uptime_secs = response.uptime_secs,
                "Daemon is healthy"
            );
        }
        Ok(healthy)
    }

    /// Send a signal to the managed process.
    pub async fn send_signal(&mut self, signal: i32) -> Result<()> {
        let request = SendSignalRequest {
            signal: Some(signal),
        };

        let response = self
            .client
            .send_signal(request)
            .await
            .context("SendSignal RPC failed")?
            .into_inner();

        if !response.success.unwrap_or(false) {
            let error = response
                .error
                .unwrap_or_else(|| "Unknown error".to_string());
            anyhow::bail!("Failed to send signal: {}", error);
        }

        Ok(())
    }

    /// Wait for the daemon to be ready (health check passes).
    pub async fn wait_for_ready(&mut self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            match self.health().await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    warn!("Daemon health check returned false");
                }
                Err(e) => {
                    debug!(error = %e, "Health check error, retrying...");
                }
            }
            tokio::time::sleep(crate::grpc::POLL_INTERVAL).await;
        }

        anyhow::bail!("Timeout waiting for daemon to be ready after {:?}", timeout)
    }

    /// Create a mock DaemonClient for testing purposes.
    /// This client will fail on any actual operations, but can be used
    /// to create Running state containers for testing timeout logic.
    #[cfg(test)]
    pub fn new_for_testing() -> Self {
        use tonic::transport::Endpoint;

        // Create a channel that will fail on any actual connection attempt
        // This is fine for tests that only check state, not actual daemon operations
        let channel = Endpoint::from_static("http://[::1]:1").connect_lazy();

        Self {
            client: ContainerDaemonClient::new(channel),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_connect_fails_with_invalid_address() {
        // Connection should fail for an address with no server
        let result = DaemonClient::connect("127.0.0.1:19999").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_with_retry_fails_after_timeout() {
        // Should fail after timeout since no server is running
        let result =
            DaemonClient::connect_with_retry("127.0.0.1:19998", Duration::from_millis(200)).await;
        assert!(result.is_err());
    }
}
