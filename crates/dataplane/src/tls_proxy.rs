//! TLS Proxy server with SNI-based routing.
//!
//! Terminates TLS connections, extracts the target from SNI (Server Name
//! Indication), and forwards traffic to sandbox containers over plaintext TCP.
//!
//! ## Hostname Format
//!
//! ```text
//! {port}-{sandbox_id}.sandboxes.tensorlake.ai  →  container:{port}
//! {sandbox_id}.sandboxes.tensorlake.ai         →  container:9501 (daemon API)
//! ```
//!
//! ## Flow
//!
//! 1. Client connects with TLS, SNI indicates target hostname
//! 2. Proxy terminates TLS, extracts sandbox_id and port from SNI
//! 3. Proxy connects to container via plaintext TCP
//! 4. Bidirectional copy between decrypted TLS stream and container

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    config::TlsProxyConfig,
    driver::DAEMON_HTTP_PORT,
    function_container_manager::FunctionContainerManager,
};

/// TLS proxy server with SNI-based routing.
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
        let cert_path = self.config.cert_path();
        let key_path = self.config.key_path();

        let addr: SocketAddr = format!("{}:{}", self.config.listen_addr, self.config.port)
            .parse()
            .context("Invalid TLS proxy listen address")?;

        let tls_config = load_tls_config(cert_path, key_path)
            .await
            .context("Failed to load TLS configuration")?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        let listener = TcpListener::bind(&addr)
            .await
            .context("Failed to bind TLS proxy")?;

        info!(
            %addr,
            proxy_domain = %self.config.proxy_domain,
            "TLS proxy server listening (SNI-based routing)"
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

/// Handle a TLS connection: terminate TLS, route by SNI, forward to container.
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    tls_acceptor: TlsAcceptor,
    proxy_domain: &str,
    container_manager: &FunctionContainerManager,
) -> Result<()> {
    // Terminate TLS
    let mut tls_stream = tls_acceptor
        .accept(stream)
        .await
        .context("TLS handshake failed")?;

    // Extract SNI hostname from TLS connection
    let sni = tls_stream
        .get_ref()
        .1
        .server_name()
        .context("No SNI in TLS connection")?;

    // Parse SNI to get sandbox_id and port
    let (sandbox_id, port) =
        parse_sni_hostname(sni, proxy_domain).with_context(|| "Invalid SNI hostname format")?;

    // Look up container address
    let target_addr = container_manager
        .get_sandbox_address(&sandbox_id, port)
        .await
        .with_context(|| "Sandbox not found or port not available")?;

    debug!(
        %peer_addr,
        %sni,
        sandbox_id,
        port,
        %target_addr,
        "Proxying connection"
    );

    // Connect to container (plaintext)
    let mut container_stream = TcpStream::connect(&target_addr)
        .await
        .with_context(|| "Failed to connect to container")?;

    // Bidirectional copy between TLS stream and container
    match tokio::io::copy_bidirectional(&mut tls_stream, &mut container_stream).await {
        Ok((to_container, from_container)) => {
            debug!(
                sandbox_id,
                to_container, from_container, "Connection closed"
            );
        }
        Err(e) => {
            debug!(error = %e, sandbox_id, "Connection error during proxy");
        }
    }

    Ok(())
}

/// Parse SNI hostname to extract sandbox_id and port.
///
/// - `{port}-{sandbox_id}.{proxy_domain}` → (sandbox_id, port)
/// - `{sandbox_id}.{proxy_domain}` → (sandbox_id, DAEMON_HTTP_PORT)
fn parse_sni_hostname(sni: &str, proxy_domain: &str) -> Option<(String, u16)> {
    // Strip the proxy domain suffix
    let prefix = sni.strip_suffix(&format!(".{}", proxy_domain))?;

    // Try to parse as {port}-{sandbox_id}
    if let Some((port_str, sandbox_id)) = prefix.split_once('-') &&
        let Ok(port) = port_str.parse::<u16>()
    {
        return Some((sandbox_id.to_string(), port));
    }

    // No port prefix - use daemon HTTP API port
    Some((prefix.to_string(), DAEMON_HTTP_PORT))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sni_with_port() {
        let result = parse_sni_hostname(
            "8080-sb-abc123.sandboxes.tensorlake.ai",
            "sandboxes.tensorlake.ai",
        );
        assert_eq!(result, Some(("sb-abc123".to_string(), 8080)));
    }

    #[test]
    fn test_parse_sni_without_port() {
        let result = parse_sni_hostname(
            "sb-abc123.sandboxes.tensorlake.ai",
            "sandboxes.tensorlake.ai",
        );
        assert_eq!(result, Some(("sb-abc123".to_string(), DAEMON_HTTP_PORT)));
    }

    #[test]
    fn test_parse_sni_invalid_domain() {
        let result = parse_sni_hostname("sb-abc123.other.com", "sandboxes.tensorlake.ai");
        assert_eq!(result, None);
    }
}
