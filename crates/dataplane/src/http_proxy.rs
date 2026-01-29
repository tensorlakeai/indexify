//! HTTP Proxy server with header-based routing.
//!
//! Accepts HTTP connections and routes to sandbox containers based on
//! the `X-Tensorlake-Sandbox-Id` header. This proxy receives plaintext HTTP
//! from the external sandbox-proxy (which handles TLS termination and auth).
//!
//! ## Headers
//!
//! - `X-Tensorlake-Sandbox-Id` (required): The sandbox ID to route to
//! - `X-Tensorlake-Sandbox-Port` (optional): The container port (defaults to
//!   9501)
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
use pingora::{
    http::Method,
    prelude::*,
    protocols::TcpKeepalive,
    services::listening::Service,
    upstreams::peer::PeerOptions,
};
use pingora_core::apps::HttpServerOptions;
use pingora_http::{RequestHeader, ResponseHeader};
use pingora_proxy::{FailToProxy, HttpProxy as PingoraHttpProxy, ProxyHttp, Session};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, error, info, warn};

use crate::{
    config::{HttpProxyConfig, UpstreamConfig},
    function_container_manager::{FunctionContainerManager, SandboxLookupResult},
};

// Header names
const HEADER_SANDBOX_ID: &str = "x-tensorlake-sandbox-id";
const HEADER_SANDBOX_PORT: &str = "x-tensorlake-sandbox-port";
const HEADER_ORIGIN: &str = "origin";

// CORS headers
const CORS_ALLOW_METHODS: &str = "GET, POST, PUT, DELETE, OPTIONS";
const CORS_ALLOW_HEADERS: &str = "content-type, x-tensorlake-sandbox-id, x-tensorlake-sandbox-port";
const CORS_MAX_AGE: &str = "86400";

const DEFAULT_SANDBOX_PORT: u16 = 9501;

// Error codes for machine-readable error responses
mod error_code {
    pub const MISSING_SANDBOX_ID: &str = "MISSING_SANDBOX_ID";
    pub const SANDBOX_NOT_FOUND: &str = "SANDBOX_NOT_FOUND";
    pub const SANDBOX_NOT_RUNNING: &str = "SANDBOX_NOT_RUNNING";
    pub const CONNECTION_CLOSED: &str = "CONNECTION_CLOSED";
    pub const CONNECTION_TIMEOUT: &str = "CONNECTION_TIMEOUT";
    pub const READ_TIMEOUT: &str = "READ_TIMEOUT";
    pub const WRITE_TIMEOUT: &str = "WRITE_TIMEOUT";
    pub const CONNECTION_REFUSED: &str = "CONNECTION_REFUSED";
    pub const PROXY_ERROR: &str = "PROXY_ERROR";
}

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

/// JSON error response body.
#[derive(Serialize)]
struct ErrorResponse<'a> {
    error: &'a str,
    code: &'a str,
}

/// Add CORS headers to a response.
fn add_cors_headers(resp: &mut ResponseHeader, origin: Option<&str>) {
    if let Some(origin) = origin {
        resp.insert_header("access-control-allow-origin", origin)
            .ok();
        resp.insert_header("access-control-allow-methods", CORS_ALLOW_METHODS)
            .ok();
        resp.insert_header("access-control-allow-headers", CORS_ALLOW_HEADERS)
            .ok();
        resp.insert_header("access-control-allow-credentials", "true")
            .ok();
    }
}

/// Extract origin header from request for CORS.
fn get_origin(session: &Session) -> Option<String> {
    session
        .req_header()
        .headers
        .get(HEADER_ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

async fn send_error_response(
    session: &mut Session,
    status: u16,
    message: &str,
    code: &str,
    origin: Option<&str>,
) -> Result<bool> {
    let error = ErrorResponse {
        error: message,
        code,
    };
    let body = serde_json::to_vec(&error).unwrap_or_else(|_| message.as_bytes().to_vec());
    let mut resp = ResponseHeader::build(status, Some(5))?;
    add_cors_headers(&mut resp, origin);
    resp.insert_header("content-type", "application/json")?;
    resp.insert_header("content-length", body.len().to_string())?;
    session.write_response_header(Box::new(resp), false).await?;
    session
        .write_response_body(Some(bytes::Bytes::from(body)), true)
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
        let origin = get_origin(session);

        // Handle CORS preflight requests (OPTIONS)
        if method == Method::OPTIONS {
            let mut resp = ResponseHeader::build(200, Some(5))?;
            add_cors_headers(&mut resp, origin.as_deref());
            resp.insert_header("access-control-max-age", CORS_MAX_AGE)?;
            session.write_response_header(Box::new(resp), true).await?;
            return Ok(true); // Request handled, don't proxy upstream
        }

        // Extract X-Tensorlake-Sandbox-Id header (required)
        let sandbox_id = match session
            .req_header()
            .headers
            .get(HEADER_SANDBOX_ID)
            .and_then(|v| v.to_str().ok())
        {
            Some(id) => id,
            None => {
                warn!(%method, %path, "Missing X-Tensorlake-Sandbox-Id header");
                return send_error_response(
                    session,
                    400,
                    "Missing X-Tensorlake-Sandbox-Id header",
                    error_code::MISSING_SANDBOX_ID,
                    origin.as_deref(),
                )
                .await;
            }
        };

        // Extract X-Tensorlake-Sandbox-Port header (optional, defaults to 9501)
        let port: u16 = session
            .req_header()
            .headers
            .get(HEADER_SANDBOX_PORT)
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
                return send_error_response(
                    session,
                    404,
                    "Sandbox not found",
                    error_code::SANDBOX_NOT_FOUND,
                    origin.as_deref(),
                )
                .await;
            }
            SandboxLookupResult::NotRunning(state) => {
                warn!(state, "Sandbox not running");
                let msg = format!("Sandbox not running (state: {})", state);
                return send_error_response(
                    session,
                    503,
                    &msg,
                    error_code::SANDBOX_NOT_RUNNING,
                    origin.as_deref(),
                )
                .await;
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

        // Connect to container with HTTP/2 preferred, HTTP/1.1 fallback
        // This supports gRPC (requires HTTP/2), regular HTTP, and WebSockets (HTTP/1.1
        // upgrade)
        let mut peer = HttpPeer::new(addr, false, String::new());
        peer.options = create_peer_options(&self.upstream_config);
        // Prefer HTTP/2 (h2c) but allow HTTP/1.1 fallback for WebSockets and legacy
        // services
        peer.options.set_http_version(2, 1);
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
        upstream_request.remove_header(HEADER_SANDBOX_ID);
        upstream_request.remove_header(HEADER_SANDBOX_PORT);
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
        add_cors_headers(upstream_response, get_origin(session).as_deref());
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

        // Determine status, message, and code based on error type
        let (status, message, code) = match error_type {
            ErrorType::ConnectionClosed => (
                502,
                "Connection to sandbox closed unexpectedly. The sandbox may have terminated.",
                error_code::CONNECTION_CLOSED,
            ),
            ErrorType::ConnectTimedout => (
                504,
                "Connection to sandbox timed out. The sandbox may be overloaded or unresponsive.",
                error_code::CONNECTION_TIMEOUT,
            ),
            ErrorType::ReadTimedout => (
                504,
                "Reading from sandbox timed out. The operation is taking longer than expected.",
                error_code::READ_TIMEOUT,
            ),
            ErrorType::WriteTimedout => (
                504,
                "Writing to sandbox timed out. The sandbox may be overloaded.",
                error_code::WRITE_TIMEOUT,
            ),
            ErrorType::ConnectRefused => (
                502,
                "Connection to sandbox refused. The sandbox may not be running.",
                error_code::CONNECTION_REFUSED,
            ),
            _ => (
                502,
                "Failed to proxy request to sandbox.",
                error_code::PROXY_ERROR,
            ),
        };

        // Disable keepalive to ensure clean connection close
        session.set_keepalive(None);

        ctx.status_code = Some(status);

        let origin = get_origin(session);

        // Best effort - if this fails, Pingora will still return the status code
        let _ = send_error_response(session, status, message, code, origin.as_deref()).await;

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

    // Create HttpProxy with h2c support enabled for gRPC compatibility
    let mut http_proxy = PingoraHttpProxy::new(proxy, server.configuration.clone());

    // Enable HTTP/2 cleartext (h2c) for inbound connections from sandbox-proxy
    let mut http_server_options = HttpServerOptions::default();
    http_server_options.h2c = true;
    http_proxy.server_options = Some(http_server_options);

    http_proxy.handle_init_modules();
    let mut proxy_service = Service::new("Dataplane HTTP Proxy".to_string(), http_proxy);
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
