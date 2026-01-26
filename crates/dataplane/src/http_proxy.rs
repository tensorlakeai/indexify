//! HTTP Proxy server with header-based routing.
//!
//! Accepts HTTP connections and routes to sandbox containers based on
//! the `X-Sandbox-Id` header. This proxy receives plaintext HTTP from
//! the external sandbox-proxy (which handles TLS termination and auth).
//!
//! ## Headers
//!
//! - `X-Sandbox-Id` (required): The sandbox ID to route to
//! - `X-Sandbox-Port` (optional): The container port (defaults to 9501)
//!
//! ## Flow
//!
//! 1. External request arrives via sandbox-proxy with routing headers
//! 2. Proxy extracts sandbox_id and port from headers
//! 3. Proxy looks up container address from container manager
//! 4. Proxy forwards request to container, stripping routing headers

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use pingora::{http::Method, prelude::*, protocols::TcpKeepalive, upstreams::peer::PeerOptions};
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{FailToProxy, ProxyHttp, Session, http_proxy_service};
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, error, info, warn};

use crate::{
    config::{HttpProxyConfig, UpstreamConfig},
    function_container_manager::{FunctionContainerManager, SandboxLookupResult},
};

const DEFAULT_SANDBOX_PORT: u16 = 9501;

fn create_peer_options(config: &UpstreamConfig) -> PeerOptions {
    let mut options = PeerOptions::new();

    // Close idle connections before the upstream does
    options.idle_timeout = Some(Duration::from_secs(config.idle_timeout_secs));

    // TCP keepalive to detect dead connections early
    options.tcp_keepalive = Some(TcpKeepalive {
        idle: Duration::from_secs(config.keepalive_idle_secs),
        interval: Duration::from_secs(config.keepalive_interval_secs),
        count: config.keepalive_count,
        #[cfg(target_os = "linux")]
        user_timeout: Duration::from_secs(0),
    });

    // Timeout for establishing TCP connection
    options.connection_timeout = Some(Duration::from_secs(config.connection_timeout_secs));

    // Timeout for each read operation from upstream
    options.read_timeout = Some(Duration::from_secs(config.read_timeout_secs));

    // Timeout for each write operation to upstream
    options.write_timeout = Some(Duration::from_secs(config.write_timeout_secs));

    options
}

/// Context passed through the request lifecycle.
pub struct ProxyContext {
    /// Tracing span for this request (contains common fields)
    span: Span,
    /// Resolved container address (IP:port)
    container_addr: Option<String>,
    /// Request start time for duration tracking
    request_start: Instant,
    /// Response status code (set after upstream response)
    status_code: Option<u16>,
}

/// HTTP proxy with header-based routing to sandbox containers.
pub struct HttpProxy {
    container_manager: Arc<FunctionContainerManager>,
    upstream_config: UpstreamConfig,
}

impl HttpProxy {
    pub fn new(
        container_manager: Arc<FunctionContainerManager>,
        upstream_config: UpstreamConfig,
    ) -> Self {
        Self {
            container_manager,
            upstream_config,
        }
    }
}

async fn send_error_response(
    session: &mut Session,
    status: u16,
    message: &str,
    origin: Option<&str>,
) -> Result<bool> {
    let body = message.as_bytes();
    let mut resp = ResponseHeader::build(status, Some(5))?;
    if let Some(origin) = origin {
        resp.insert_header("access-control-allow-origin", origin)?;
        resp.insert_header("access-control-allow-credentials", "true")?;
    }
    resp.insert_header("content-type", "text/plain")?;
    resp.insert_header("content-length", body.len().to_string())?;
    session.write_response_header(Box::new(resp), false).await?;
    session
        .write_response_body(Some(bytes::Bytes::from(message.to_owned())), true)
        .await?;
    Ok(true)
}

#[async_trait]
impl ProxyHttp for HttpProxy {
    type CTX = ProxyContext;

    fn new_ctx(&self) -> Self::CTX {
        ProxyContext {
            span: Span::none(),
            container_addr: None,
            request_start: Instant::now(),
            status_code: None,
        }
    }

    /// Extract sandbox routing info from headers and lookup container.
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool> {
        let req = session.req_header();
        let method = &req.method;
        let path = req.uri.path().to_string();

        // Get origin for CORS headers (used for all responses)
        let origin = session
            .req_header()
            .headers
            .get("origin")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // Handle CORS preflight requests (OPTIONS)
        if method == Method::OPTIONS {
            let mut resp = ResponseHeader::build(200, Some(4))?;
            if let Some(ref origin) = origin {
                resp.insert_header("access-control-allow-origin", origin)?;
            }
            resp.insert_header(
                "access-control-allow-methods",
                "GET, POST, PUT, DELETE, OPTIONS",
            )?;
            resp.insert_header(
                "access-control-allow-headers",
                "content-type, x-sandbox-id, x-sandbox-port",
            )?;
            resp.insert_header("access-control-allow-credentials", "true")?;
            resp.insert_header("access-control-max-age", "86400")?;
            session.write_response_header(Box::new(resp), true).await?;
            return Ok(true); // Request handled, don't proxy upstream
        }

        // Extract X-Sandbox-Id header (required)
        let sandbox_id = match session
            .req_header()
            .headers
            .get("x-sandbox-id")
            .and_then(|v| v.to_str().ok())
        {
            Some(id) => id,
            None => {
                warn!(%method, %path, "Missing X-Sandbox-Id header");
                return send_error_response(
                    session,
                    400,
                    "Missing X-Sandbox-Id header",
                    origin.as_deref(),
                )
                .await;
            }
        };

        // Extract X-Sandbox-Port header (optional, defaults to 9501)
        let port: u16 = session
            .req_header()
            .headers
            .get("x-sandbox-port")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SANDBOX_PORT);

        // Create span with all common fields for this request
        ctx.span = tracing::info_span!(
            "proxy_request",
            sandbox_id,
            port,
            %method,
            path,
            container_addr = tracing::field::Empty,
            status_code = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        );
        let _guard = ctx.span.enter();

        // Lookup container address with detailed status
        let container_addr = match self
            .container_manager
            .lookup_sandbox(sandbox_id, port)
            .await
        {
            SandboxLookupResult::Running(addr) => addr,
            SandboxLookupResult::NotFound => {
                warn!("Sandbox not found");
                return send_error_response(session, 404, "Sandbox not found", origin.as_deref())
                    .await;
            }
            SandboxLookupResult::NotRunning(state) => {
                warn!(state, "Sandbox not running");
                let msg = format!("Sandbox not running (state: {})", state);
                return send_error_response(session, 503, &msg, origin.as_deref()).await;
            }
        };

        ctx.span.record("container_addr", &container_addr);
        ctx.container_addr = Some(container_addr);
        debug!("Routing request to container");

        Ok(false) // Continue processing
    }

    /// Select container as upstream.
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let addr = ctx
            .container_addr
            .as_ref()
            .ok_or_else(|| Error::explain(ErrorType::HTTPStatus(500), "No container address"))?;

        // Connect to container over plaintext HTTP with configured connection options
        let mut peer = HttpPeer::new(addr, false, String::new());
        peer.options = create_peer_options(&self.upstream_config);
        Ok(Box::new(peer))
    }

    /// Remove routing headers before forwarding to container.
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Remove internal routing headers - container doesn't need these
        upstream_request.remove_header("x-sandbox-id");
        upstream_request.remove_header("x-sandbox-port");
        Ok(())
    }

    /// Called when upstream response headers are received.
    /// Records status code for logging and adds CORS headers.
    async fn upstream_response_filter(
        &self,
        session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        ctx.status_code = Some(upstream_response.status.as_u16());

        // Add CORS headers for browser access (local dev)
        if let Some(origin) = session
            .req_header()
            .headers
            .get("origin")
            .and_then(|v| v.to_str().ok())
        {
            upstream_response
                .insert_header("access-control-allow-origin", origin)
                .ok();
            upstream_response
                .insert_header(
                    "access-control-allow-methods",
                    "GET, POST, PUT, DELETE, OPTIONS",
                )
                .ok();
            upstream_response
                .insert_header(
                    "access-control-allow-headers",
                    "content-type, x-sandbox-id, x-sandbox-port",
                )
                .ok();
            upstream_response
                .insert_header("access-control-allow-credentials", "true")
                .ok();
        }

        Ok(())
    }

    /// Called at the end of each request for logging.
    async fn logging(&self, _session: &mut Session, e: Option<&Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let duration_ms = ctx.request_start.elapsed().as_millis() as u64;
        let status_code = ctx.status_code.unwrap_or(0);

        // Record final fields in span
        ctx.span.record("status_code", status_code);
        ctx.span.record("duration_ms", duration_ms);

        let _guard = ctx.span.enter();

        match (e, status_code) {
            (Some(err), _) => {
                error!(error = %err, error_type = err.etype().as_str(), "Request failed");
            }
            (None, code) if code >= 500 => {
                error!("Upstream server error");
            }
            (None, code) if code >= 400 => {
                warn!("Client error");
            }
            (None, code) if code > 0 => {
                info!("Request completed");
            }
            _ => {}
        }
    }

    /// Called when connection to upstream fails.
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        ctx: &mut Self::CTX,
        e: Box<Error>,
    ) -> Box<Error> {
        let duration_ms = ctx.request_start.elapsed().as_millis() as u64;
        ctx.span.record("duration_ms", duration_ms);

        let _guard = ctx.span.enter();
        error!(error = %e, error_type = e.etype().as_str(), "Failed to connect to container");

        e
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        ctx: &mut Self::CTX,
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        let error_type = e.etype();

        let status = match error_type {
            ErrorType::ConnectionClosed => 502,
            ErrorType::ConnectTimedout => 504,
            ErrorType::ReadTimedout => 504,
            ErrorType::WriteTimedout => 504,
            ErrorType::ConnectRefused => 502,
            _ => 502,
        };

        // Disable keepalive to ensure clean connection close
        session.set_keepalive(None);

        ctx.status_code = Some(status);

        FailToProxy {
            error_code: status,
            can_reuse_downstream: false,
        }
    }
}

/// Run the HTTP proxy server until cancelled.
pub async fn run_http_proxy(
    config: HttpProxyConfig,
    container_manager: Arc<FunctionContainerManager>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let addr = config.socket_addr();

    info!(
        listen_addr = %addr,
        advertise_addr = %config.get_advertise_address(),
        "Starting HTTP proxy server"
    );

    let proxy = HttpProxy::new(container_manager, config.upstream.clone());

    let mut server = Server::new(None)?;
    server.bootstrap();

    let mut proxy_service = http_proxy_service(&server.configuration, proxy);
    proxy_service.add_tcp(&addr);

    server.add_service(proxy_service);

    // Run Pingora in a separate OS thread since it creates its own runtime
    let server_handle = std::thread::spawn(move || {
        server.run_forever();
    });

    // Wait for cancellation
    cancel_token.cancelled().await;
    info!("HTTP proxy server shutting down");

    // Note: Pingora doesn't have a clean shutdown API
    // The thread will be terminated when the process exits
    drop(server_handle);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_port() {
        assert_eq!(DEFAULT_SANDBOX_PORT, 9501);
    }
}
