//! Shared gRPC connection helpers.

use std::time::Duration;

use anyhow::{Context, Result};
use tonic::transport::Channel;

/// Default timeout for establishing a gRPC connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default polling interval for retry loops.
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Connect to a gRPC server at the given address (e.g., "127.0.0.1:9600").
///
/// Prepends `http://` and applies the standard connect timeout.
pub async fn connect_channel(addr: &str) -> Result<Channel> {
    let endpoint = format!("http://{}", addr);
    Channel::from_shared(endpoint)
        .context("Invalid endpoint")?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await
        .with_context(|| format!("Failed to connect to {}", addr))
}
