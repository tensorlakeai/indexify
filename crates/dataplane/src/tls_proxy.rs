//! TLS Proxy server with SNI-based routing and TCP passthrough.
//!
//! Accepts TLS connections, terminates TLS, extracts routing info from the
//! HTTP Host header, then does TCP passthrough to the container.
//!
//! This supports any protocol that starts with an HTTP request:
//! - HTTP/HTTPS
//! - WebSockets (HTTP Upgrade)
//! - gRPC (HTTP/2)
//!
//! ## Hostname Format
//!
//! ```text
//! {port}-{sandbox_id}.sandboxes.tensorlake.ai  →  container:{port}
//! {sandbox_id}.sandboxes.tensorlake.ai         →  container:9501 (daemon API)
//! ```

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{config::TlsProxyConfig, function_container_manager::FunctionContainerManager};

/// Default port for daemon API when no port prefix in hostname.
const DAEMON_API_PORT: u16 = 9501;

/// TLS proxy server with SNI-based routing and TCP passthrough.
pub struct TlsProxy {
    config: TlsProxyConfig,
    container_manager: Arc<FunctionContainerManager>,
}

impl TlsProxy {
    pub fn new(config: TlsProxyConfig, container_manager: Arc<FunctionContainerManager>) -> Self {
        Self {
            config,
            container_manager,
        }
    }

    /// Run the TLS proxy server until cancelled.
    pub async fn run(self, cancel_token: CancellationToken) -> Result<()> {
        // Certificates are resolved during config validation
        let cert_path = self.config.cert_path();
        let key_path = self.config.key_path();

        let addr: SocketAddr = format!("{}:{}", self.config.listen_addr, self.config.port)
            .parse()
            .context("Invalid TLS proxy listen address")?;

        // Load TLS certificate and key
        let tls_config = load_tls_config(cert_path, key_path)
            .await
            .context("Failed to load TLS configuration")?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        let listener = TcpListener::bind(&addr).await.context("Failed to bind TLS proxy")?;

        info!(
            %addr,
            proxy_domain = %self.config.proxy_domain,
            "TLS proxy server listening (TCP passthrough mode)"
        );

        let proxy_domain = Arc::new(self.config.proxy_domain.clone());
        let container_manager = self.container_manager.clone();

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("TLS proxy server shutting down");
                    return Ok(());
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = result.context("Failed to accept connection")?;
                    let tls_acceptor = tls_acceptor.clone();
                    let proxy_domain = proxy_domain.clone();
                    let container_manager = container_manager.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            stream,
                            peer_addr,
                            tls_acceptor,
                            &proxy_domain,
                            &container_manager,
                        ).await {
                            debug!(error = %e, %peer_addr, "Connection error");
                        }
                    });
                }
            }
        }
    }
}

/// Load TLS configuration from certificate and key files.
async fn load_tls_config(cert_path: &str, key_path: &str) -> Result<ServerConfig> {
    // Install the ring crypto provider (required for rustls 0.23+)
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_file = tokio::fs::read(cert_path)
        .await
        .context("Failed to read certificate file")?;
    let key_file = tokio::fs::read(key_path)
        .await
        .context("Failed to read key file")?;

    let certs: Vec<_> = certs(&mut cert_file.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to parse certificates")?;

    let key = private_key(&mut key_file.as_slice())
        .context("Failed to parse private key")?
        .context("No private key found")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("Failed to build TLS config")?;

    Ok(config)
}

/// Handle a TLS connection: terminate TLS, extract Host, TCP passthrough.
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    proxy_domain: &str,
    container_manager: &FunctionContainerManager,
) -> Result<()> {
    // Terminate TLS
    let tls_stream = tls_acceptor
        .accept(stream)
        .await
        .context("TLS handshake failed")?;

    let (read_half, write_half) = tokio::io::split(tls_stream);
    let mut reader = BufReader::new(read_half);

    // Read the first line (HTTP request line) and headers to find Host
    let mut initial_data = Vec::new();
    let mut host: Option<String> = None;

    // Read headers line by line until we find Host or hit end of headers
    loop {
        let mut line = String::new();
        let bytes_read = reader
            .read_line(&mut line)
            .await
            .context("Failed to read HTTP headers")?;

        if bytes_read == 0 {
            anyhow::bail!("Connection closed before headers complete");
        }

        initial_data.extend_from_slice(line.as_bytes());

        // Empty line signals end of headers
        if line == "\r\n" || line == "\n" {
            break;
        }

        // Check for Host header (case-insensitive)
        let line_lower = line.to_lowercase();
        if line_lower.starts_with("host:") {
            let value = line[5..].trim();
            host = Some(value.to_string());
        }
    }

    let host = host.context("No Host header found in request")?;

    // Parse hostname to get sandbox_id and port
    let (sandbox_id, port) = parse_tunnel_hostname(&host, proxy_domain)
        .context(format!("Invalid hostname format: {}", host))?;

    // Look up container address
    let target_addr = container_manager
        .get_sandbox_address(&sandbox_id, port)
        .await
        .context(format!(
            "Sandbox '{}' not found or port {} not available",
            sandbox_id, port
        ))?;

    debug!(
        %peer_addr,
        sandbox_id,
        port,
        %target_addr,
        "Proxying connection"
    );

    // Connect to container
    let mut container_stream = TcpStream::connect(&target_addr)
        .await
        .context(format!("Failed to connect to container at {}", target_addr))?;

    // Send the initial data we already read (HTTP request + headers)
    container_stream
        .write_all(&initial_data)
        .await
        .context("Failed to write initial data to container")?;

    // Now do bidirectional copy between TLS stream and container
    // Reassemble the TLS stream from reader and write_half
    let read_half = reader.into_inner();
    let mut tls_stream = read_half.unsplit(write_half);

    // Bidirectional copy
    match tokio::io::copy_bidirectional(&mut tls_stream, &mut container_stream).await {
        Ok((to_container, from_container)) => {
            debug!(
                sandbox_id,
                to_container,
                from_container,
                "Connection closed"
            );
        }
        Err(e) => {
            debug!(error = %e, sandbox_id, "Connection error during proxy");
        }
    }

    Ok(())
}

/// Parse tunnel hostname format.
///
/// - `{port}-{sandbox_id}.{proxy_domain}` → (sandbox_id, port)
/// - `{sandbox_id}.{proxy_domain}` → (sandbox_id, 9501)
fn parse_tunnel_hostname(hostname: &str, proxy_domain: &str) -> Option<(String, u16)> {
    // Strip port if present (e.g., "host:443" -> "host")
    let hostname = hostname.split(':').next().unwrap_or(hostname);

    // Strip the tunnel domain suffix
    let prefix = hostname.strip_suffix(&format!(".{}", proxy_domain))?;

    // Try to parse as {port}-{sandbox_id}
    if let Some((port_str, sandbox_id)) = prefix.split_once('-')
        && let Ok(port) = port_str.parse::<u16>()
    {
        return Some((sandbox_id.to_string(), port));
    }

    // No port prefix - use daemon API port
    Some((prefix.to_string(), DAEMON_API_PORT))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hostname_with_port() {
        let result =
            parse_tunnel_hostname("8080-sb-abc123.sandboxes.tensorlake.ai", "sandboxes.tensorlake.ai");
        assert_eq!(result, Some(("sb-abc123".to_string(), 8080)));
    }

    #[test]
    fn test_parse_hostname_without_port() {
        let result =
            parse_tunnel_hostname("sb-abc123.sandboxes.tensorlake.ai", "sandboxes.tensorlake.ai");
        assert_eq!(result, Some(("sb-abc123".to_string(), 9501)));
    }

    #[test]
    fn test_parse_hostname_with_port_suffix() {
        let result = parse_tunnel_hostname(
            "8080-sb-abc123.sandboxes.tensorlake.ai:443",
            "sandboxes.tensorlake.ai",
        );
        assert_eq!(result, Some(("sb-abc123".to_string(), 8080)));
    }

    #[test]
    fn test_parse_hostname_invalid_domain() {
        let result = parse_tunnel_hostname("sb-abc123.other.com", "sandboxes.tensorlake.ai");
        assert_eq!(result, None);
    }
}
