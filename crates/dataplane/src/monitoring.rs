//! HTTP monitoring server for the dataplane.
//!
//! Provides startup/health probes and state inspection endpoints,
//! matching the Python executor's monitoring server behavior.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Shared state for the monitoring server.
pub struct MonitoringState {
    /// Whether the dataplane is ready (first heartbeat succeeded).
    pub ready: AtomicBool,
    /// Whether the heartbeat is currently healthy.
    pub heartbeat_healthy: Arc<AtomicBool>,
    /// Debug text of the last ReportExecutorStateRequest sent to server.
    pub last_reported_state: Mutex<Option<String>>,
    /// Debug text of the last desired state received from server.
    pub last_desired_state: Mutex<Option<String>>,
}

impl MonitoringState {
    pub fn new(heartbeat_healthy: Arc<AtomicBool>) -> Self {
        Self {
            ready: AtomicBool::new(false),
            heartbeat_healthy,
            last_reported_state: Mutex::new(None),
            last_desired_state: Mutex::new(None),
        }
    }
}

/// Run the HTTP monitoring server until the cancellation token fires.
pub async fn run_monitoring_server(
    addr: &str,
    state: Arc<MonitoringState>,
    cancel_token: CancellationToken,
) {
    let app = Router::new()
        .route("/monitoring/startup", get(startup_handler))
        .route("/monitoring/health", get(health_handler))
        .route("/state/reported", get(reported_state_handler))
        .route("/state/desired", get(desired_state_handler))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %addr, error = %e, "Failed to bind monitoring server");
            return;
        }
    };

    tracing::info!(addr = %addr, "Monitoring server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
        })
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "Monitoring server error");
        });
}

async fn startup_handler(State(state): State<Arc<MonitoringState>>) -> Response {
    if state.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "{\"status\":\"ok\"}").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "{\"status\":\"nok\"}").into_response()
    }
}

async fn health_handler(State(state): State<Arc<MonitoringState>>) -> Response {
    if state.heartbeat_healthy.load(Ordering::SeqCst) {
        (
            StatusCode::OK,
            "{\"status\":\"ok\",\"message\":\"Successful\",\"checker\":\"DataplaneHealthChecker\"}",
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"status\":\"nok\",\"message\":\"heartbeat unhealthy\",\"checker\":\"DataplaneHealthChecker\"}",
        )
            .into_response()
    }
}

async fn reported_state_handler(State(state): State<Arc<MonitoringState>>) -> Response {
    let guard = state.last_reported_state.lock().await;
    match &*guard {
        Some(text) => (StatusCode::OK, text.clone()).into_response(),
        None => (StatusCode::OK, "No state reported so far".to_string()).into_response(),
    }
}

async fn desired_state_handler(State(state): State<Arc<MonitoringState>>) -> Response {
    let guard = state.last_desired_state.lock().await;
    match &*guard {
        Some(text) => (StatusCode::OK, text.clone()).into_response(),
        None => (
            StatusCode::OK,
            "No desired state received from Server yet".to_string(),
        )
            .into_response(),
    }
}
