//! gRPC client for communicating with function executor subprocesses.
//!
//! This is analogous to `DaemonClient` but speaks the FunctionExecutor proto
//! instead of the ContainerDaemon proto.

use std::time::Duration;

use anyhow::{Context, Result};
use proto_api::function_executor_pb::{
    AllocationState,
    AllocationUpdate,
    CreateAllocationRequest,
    DeleteAllocationRequest,
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializeRequest,
    InitializeResponse,
    WatchAllocationStateRequest,
    function_executor_client::FunctionExecutorClient,
};
use tonic::{Streaming, transport::Channel};
use tracing::debug;

/// Timeout for connecting to the function executor.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Interval for polling FE availability.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Client for communicating with a function executor subprocess.
#[derive(Clone)]
pub struct FunctionExecutorGrpcClient {
    client: FunctionExecutorClient<Channel>,
}

impl FunctionExecutorGrpcClient {
    /// Create a new client connected to the given address (e.g.,
    /// "127.0.0.1:9600").
    pub async fn connect(addr: &str) -> Result<Self> {
        let endpoint = format!("http://{}", addr);
        debug!(endpoint = %endpoint, "Connecting to function executor");

        let channel = Channel::from_shared(endpoint.clone())
            .context("Invalid endpoint")?
            .connect_timeout(CONNECT_TIMEOUT)
            .connect()
            .await
            .context("Failed to connect to function executor")?;

        let client = FunctionExecutorClient::new(channel);
        Ok(Self { client })
    }

    /// Try to connect to the FE, retrying until timeout.
    pub async fn connect_with_retry(addr: &str, timeout: Duration) -> Result<Self> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            match Self::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    debug!(error = %e, addr = %addr, "FE connection failed, retrying...");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }

        anyhow::bail!(
            "Timeout connecting to function executor at {} after {:?}",
            addr,
            timeout
        )
    }

    /// Initialize the function executor for a particular function.
    pub async fn initialize(&mut self, request: InitializeRequest) -> Result<InitializeResponse> {
        let response = self
            .client
            .initialize(request)
            .await
            .context("FE initialize RPC failed")?
            .into_inner();
        Ok(response)
    }

    /// Create and start an allocation on the function executor.
    pub async fn create_allocation(&mut self, request: CreateAllocationRequest) -> Result<()> {
        self.client
            .create_allocation(request)
            .await
            .context("FE create_allocation RPC failed")?;
        Ok(())
    }

    /// Watch allocation state updates (streaming).
    pub async fn watch_allocation_state(
        &mut self,
        allocation_id: &str,
    ) -> Result<Streaming<AllocationState>> {
        let request = WatchAllocationStateRequest {
            allocation_id: Some(allocation_id.to_string()),
        };
        let response = self
            .client
            .watch_allocation_state(request)
            .await
            .context("FE watch_allocation_state RPC failed")?;
        Ok(response.into_inner())
    }

    /// Send an update to an allocation (output blob, function call result,
    /// etc.).
    pub async fn send_allocation_update(&mut self, update: AllocationUpdate) -> Result<()> {
        self.client
            .send_allocation_update(update)
            .await
            .context("FE send_allocation_update RPC failed")?;
        Ok(())
    }

    /// Delete a finished allocation.
    pub async fn delete_allocation(&mut self, allocation_id: &str) -> Result<()> {
        let request = DeleteAllocationRequest {
            allocation_id: Some(allocation_id.to_string()),
        };
        self.client
            .delete_allocation(request)
            .await
            .context("FE delete_allocation RPC failed")?;
        Ok(())
    }

    /// Check health of the function executor.
    pub async fn check_health(&mut self) -> Result<HealthCheckResponse> {
        let response = self
            .client
            .check_health(HealthCheckRequest {})
            .await
            .context("FE check_health RPC failed")?
            .into_inner();
        Ok(response)
    }

    /// Get information about the function executor (version, SDK info).
    pub async fn get_info(&mut self) -> Result<InfoResponse> {
        let response = self
            .client
            .get_info(InfoRequest {})
            .await
            .context("FE get_info RPC failed")?
            .into_inner();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_connect_fails_with_invalid_address() {
        let result = FunctionExecutorGrpcClient::connect("127.0.0.1:19997").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_with_retry_fails_after_timeout() {
        let result = FunctionExecutorGrpcClient::connect_with_retry(
            "127.0.0.1:19996",
            Duration::from_millis(200),
        )
        .await;
        assert!(result.is_err());
    }
}
