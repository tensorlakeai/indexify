//! HTTP Proxy server for routing requests to sandbox containers.
//!
//! This module provides a reverse proxy that routes incoming HTTP requests
//! to the appropriate sandbox container based on the sandbox ID and port
//! in the request path.
//!
//! ## URL Format
//!
//! The proxy expects requests in the format:
//! ```text
//! /{sandbox_id}/{port}/{path...}
//! ```
//!
//! For example:
//! ```text
//! GET /sb-abc123/8080/api/users
//! ```
//!
//! This would be proxied to the container's internal address on port 8080.

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header::HOST},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::TryStreamExt;
use reqwest::Client;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{config::ProxyConfig, function_container_manager::FunctionContainerManager};

/// Proxy server that routes requests to sandbox containers.
pub struct ProxyServer {
    config: ProxyConfig,
    container_manager: Arc<FunctionContainerManager>,
}

impl ProxyServer {
    pub fn new(config: ProxyConfig, container_manager: Arc<FunctionContainerManager>) -> Self {
        Self {
            config,
            container_manager,
        }
    }

    /// Run the proxy server until cancelled.
    pub async fn run(self, cancel_token: CancellationToken) -> Result<()> {
        if !self.config.enabled {
            info!("Proxy server disabled");
            return Ok(());
        }

        let addr: SocketAddr = format!("{}:{}", self.config.listen_addr, self.config.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid proxy listen address: {}", e))?;

        let state = ProxyState {
            container_manager: self.container_manager,
            http_client: Client::new(),
        };

        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/health/", get(health_handler))
            .route("/_internal/sandboxes", get(list_sandboxes_handler))
            .route("/_internal/sandboxes/", get(list_sandboxes_handler))
            // Catch-all route for proxy requests: /{sandbox_id}/{port}/{path...}
            .fallback(proxy_handler)
            .with_state(state);

        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "Proxy server listening");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                cancel_token.cancelled().await;
                info!("Proxy server shutting down");
            })
            .await
            .map_err(|e| anyhow::anyhow!("Proxy server error: {}", e))
    }
}

#[derive(Clone)]
struct ProxyState {
    container_manager: Arc<FunctionContainerManager>,
    http_client: Client,
}

/// Health check endpoint.
async fn health_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        r#"{"status":"ok"}"#,
    )
}

/// List all running sandboxes (internal endpoint).
async fn list_sandboxes_handler(State(state): State<ProxyState>) -> impl IntoResponse {
    let sandboxes = state.container_manager.list_sandboxes().await;
    let json = serde_json::to_string(&sandboxes).unwrap_or_else(|_| "[]".to_string());

    (StatusCode::OK, [("Content-Type", "application/json")], json)
}

/// Handle proxy requests.
async fn proxy_handler(State(state): State<ProxyState>, req: Request) -> Response {
    let path = req.uri().path();

    // Parse path: /{sandbox_id}/{port}/{rest...}
    let parts: Vec<&str> = path.trim_start_matches('/').splitn(3, '/').collect();

    if parts.len() < 2 {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_path",
            "Expected /{sandbox_id}/{port}/...",
        );
    }

    let sandbox_id = parts[0];
    let port: u16 = match parts[1].parse() {
        Ok(p) => p,
        Err(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_port",
                "Port must be a number",
            );
        }
    };

    let remaining_path = if parts.len() > 2 {
        format!("/{}", parts[2])
    } else {
        "/".to_string()
    };

    // Look up the sandbox's container address
    let target_addr = match state
        .container_manager
        .get_sandbox_address(sandbox_id, port)
        .await
    {
        Some(addr) => addr,
        None => {
            warn!(sandbox_id, port, "Sandbox not found or port not available");
            return error_response(
                StatusCode::NOT_FOUND,
                "sandbox_not_found",
                &format!(
                    "Sandbox '{}' not found or port {} not available",
                    sandbox_id, port
                ),
            );
        }
    };

    debug!(sandbox_id, port, %target_addr, path = %remaining_path, "Proxying request");

    // Build the target URI
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let target_url = format!("http://{}{}{}", target_addr, remaining_path, query);

    // Forward the request
    proxy_request(&state.http_client, req, &target_url).await
}

/// Forward a request to the target URL with streaming body support.
async fn proxy_request(client: &Client, req: Request, target_url: &str) -> Response {
    let method = req.method().clone();
    let headers = req.headers().clone();

    // Build the proxied request
    let mut builder = client.request(method, target_url);

    // Copy headers (except Host which we'll set to the target)
    for (name, value) in headers.iter() {
        if name != HOST && let Ok(value_str) = value.to_str() {
            builder = builder.header(name.as_str(), value_str);
        }
    }

    // Stream the request body directly without buffering
    let body_stream = req.into_body();
    let reqwest_body = reqwest::Body::wrap_stream(
        http_body_util::BodyStream::new(body_stream).map_ok(|frame| {
            frame
                .into_data()
                .unwrap_or_else(|_| bytes::Bytes::new())
        }),
    );
    builder = builder.body(reqwest_body);

    // Send the request
    match builder.send().await {
        Ok(response) => {
            // Convert reqwest response to axum response
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            let mut builder = Response::builder().status(status);

            // Copy response headers
            for (name, value) in response.headers().iter() {
                if let Ok(name) = axum::http::header::HeaderName::try_from(name.as_str())
                    && let Ok(value) = HeaderValue::from_bytes(value.as_bytes())
                {
                    builder = builder.header(name, value);
                }
            }

            // Stream response body back without buffering
            let response_stream = response.bytes_stream();
            let body = Body::from_stream(response_stream);

            builder.body(body).unwrap_or_else(|_| {
                error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "response_build_error",
                    "Failed to build response",
                )
            })
        }
        Err(e) => {
            error!(error = %e, "Failed to proxy request");
            error_response(
                StatusCode::BAD_GATEWAY,
                "proxy_error",
                &format!("Failed to connect to container: {}", e),
            )
        }
    }
}

/// Create a JSON error response.
fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    let body = format!(r#"{{"error":"{}","message":"{}"}}"#, error, message);
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_path() {
        let path = "/sb-abc123/8080/api/users";
        let parts: Vec<&str> = path.trim_start_matches('/').splitn(3, '/').collect();

        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "sb-abc123");
        assert_eq!(parts[1], "8080");
        assert_eq!(parts[2], "api/users");
    }

    #[test]
    fn test_parse_path_no_trailing() {
        let path = "/sb-abc123/8080";
        let parts: Vec<&str> = path.trim_start_matches('/').splitn(3, '/').collect();

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "sb-abc123");
        assert_eq!(parts[1], "8080");
    }
}
