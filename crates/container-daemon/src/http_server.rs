//! HTTP server for the user-facing Sandbox API.
//!
//! Provides RESTful endpoints for:
//! - Process management (start, list, signal, kill)
//! - Streaming I/O (stdin, stdout, stderr via SSE)
//! - File operations (read, write, delete, list)

use std::{convert::Infallible, net::SocketAddr, time::Instant};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        IntoResponse,
        Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post, put},
};
use bytes::Bytes;
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

use crate::{
    file_manager::FileManager,
    http_models::{
        DaemonInfo,
        ErrorResponse,
        FilePathQuery,
        HealthResponse,
        ListDirectoryResponse,
        ListProcessesResponse,
        OutputEvent,
        OutputResponse,
        SendSignalRequest,
        SendSignalResponse,
        StartProcessRequest,
        StartProcessResponse,
    },
    process_manager::ProcessManager,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub process_manager: ProcessManager,
    pub file_manager: FileManager,
    pub start_time: Instant,
}

/// Run the HTTP server for the sandbox API.
pub async fn run_http_server(
    port: u16,
    process_manager: ProcessManager,
    file_manager: FileManager,
    cancel_token: CancellationToken,
) -> Result<()> {
    let state = AppState {
        process_manager,
        file_manager,
        start_time: Instant::now(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        // Health and info
        .route("/api/v1/health", get(health))
        .route("/api/v1/info", get(info))
        // Process management
        .route("/api/v1/processes", post(start_process))
        .route("/api/v1/processes", get(list_processes))
        .route("/api/v1/processes/{pid}", get(get_process))
        .route("/api/v1/processes/{pid}", delete(kill_process))
        .route("/api/v1/processes/{pid}/signal", post(send_signal))
        // Streaming I/O
        .route("/api/v1/processes/{pid}/stdin", post(write_stdin))
        .route("/api/v1/processes/{pid}/stdin/close", post(close_stdin))
        // Output endpoints - get all output from log file
        .route("/api/v1/processes/{pid}/stdout", get(get_stdout))
        .route("/api/v1/processes/{pid}/stderr", get(get_stderr))
        .route("/api/v1/processes/{pid}/output", get(get_output))
        // Follow endpoints - replay from log file then follow live via SSE
        .route("/api/v1/processes/{pid}/stdout/follow", get(follow_stdout))
        .route("/api/v1/processes/{pid}/stderr/follow", get(follow_stderr))
        .route("/api/v1/processes/{pid}/output/follow", get(follow_output))
        // File operations
        .route("/api/v1/files", get(read_file))
        .route("/api/v1/files", put(write_file))
        .route("/api/v1/files", delete(delete_file))
        .route("/api/v1/files/list", get(list_directory))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    info!(port = port, addr = %addr, "HTTP server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("Failed to bind HTTP server")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            info!("HTTP server shutting down");
        })
        .await
        .context("HTTP server error")?;

    Ok(())
}

// ============================================================================
// Health and Info Endpoints
// ============================================================================

async fn health() -> impl IntoResponse {
    axum::Json(HealthResponse { healthy: true })
}

async fn info(State(state): State<AppState>) -> impl IntoResponse {
    let (running, total) = state.process_manager.get_counts().await;
    axum::Json(DaemonInfo {
        version: VERSION.to_string(),
        uptime_secs: state.start_time.elapsed().as_secs(),
        running_processes: running,
        total_processes: total,
    })
}

// ============================================================================
// Process Management Endpoints
// ============================================================================

async fn start_process(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<StartProcessRequest>,
) -> impl IntoResponse {
    match state.process_manager.start_process(req).await {
        Ok(process_info) => (
            StatusCode::CREATED,
            axum::Json(StartProcessResponse {
                process: process_info,
            }),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "Failed to start process");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ErrorResponse::new(e.to_string())),
            )
                .into_response()
        }
    }
}

async fn list_processes(State(state): State<AppState>) -> impl IntoResponse {
    let processes = state.process_manager.list_processes().await;
    axum::Json(ListProcessesResponse { processes })
}

async fn get_process(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.get_process(pid).await {
        Some(process) => (StatusCode::OK, axum::Json(process)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(ErrorResponse::with_code(
                format!("Process {} not found", pid),
                "NOT_FOUND",
            )),
        )
            .into_response(),
    }
}

async fn kill_process(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.kill_process(pid).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("Process {} not found", pid),
                        "NOT_FOUND",
                    )),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(ErrorResponse::new(e.to_string())),
                )
                    .into_response()
            }
        }
    }
}

async fn send_signal(
    State(state): State<AppState>,
    Path(pid): Path<u32>,
    axum::Json(req): axum::Json<SendSignalRequest>,
) -> impl IntoResponse {
    match state.process_manager.send_signal(pid, req.signal).await {
        Ok(()) => axum::Json(SendSignalResponse { success: true }).into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("Process {} not found", pid),
                        "NOT_FOUND",
                    )),
                )
                    .into_response()
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(ErrorResponse::new(e.to_string())),
                )
                    .into_response()
            }
        }
    }
}

// ============================================================================
// Streaming I/O Endpoints
// ============================================================================

async fn write_stdin(
    State(state): State<AppState>,
    Path(pid): Path<u32>,
    body: Bytes,
) -> impl IntoResponse {
    match state.process_manager.write_stdin(pid, body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("Process {} not found", pid),
                        "NOT_FOUND",
                    )),
                )
                    .into_response()
            } else if e.to_string().contains("stdin") {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(ErrorResponse::with_code(
                        e.to_string(),
                        "STDIN_NOT_WRITABLE",
                    )),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(ErrorResponse::new(e.to_string())),
                )
                    .into_response()
            }
        }
    }
}

async fn close_stdin(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.close_stdin(pid).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("Process {} not found", pid),
                        "NOT_FOUND",
                    )),
                )
                    .into_response()
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(ErrorResponse::new(e.to_string())),
                )
                    .into_response()
            }
        }
    }
}

// ============================================================================
// Output Endpoints (read from log files)
// ============================================================================

async fn get_stdout(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.get_stdout(pid).await {
        Ok(lines) => {
            let response = OutputResponse {
                pid,
                line_count: lines.len(),
                lines,
            };
            (StatusCode::OK, axum::Json(response)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_stderr(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.get_stderr(pid).await {
        Ok(lines) => {
            let response = OutputResponse {
                pid,
                line_count: lines.len(),
                lines,
            };
            (StatusCode::OK, axum::Json(response)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

async fn get_output(State(state): State<AppState>, Path(pid): Path<u32>) -> impl IntoResponse {
    match state.process_manager.get_output(pid).await {
        Ok(events) => {
            let lines: Vec<String> = events.iter().map(|e| e.line.clone()).collect();
            let response = OutputResponse {
                pid,
                line_count: lines.len(),
                lines,
            };
            (StatusCode::OK, axum::Json(response)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

// ============================================================================
// Follow Endpoints (replay log file then stream live via SSE)
// ============================================================================

async fn follow_stdout(State(state): State<AppState>, Path(pid): Path<u32>) -> Response {
    match state.process_manager.follow_stdout(pid).await {
        Ok((existing_lines, rx)) => {
            let stream = create_follow_stream(existing_lines, None, rx);
            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

async fn follow_stderr(State(state): State<AppState>, Path(pid): Path<u32>) -> Response {
    match state.process_manager.follow_stderr(pid).await {
        Ok((existing_lines, rx)) => {
            let stream = create_follow_stream(existing_lines, None, rx);
            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

async fn follow_output(State(state): State<AppState>, Path(pid): Path<u32>) -> Response {
    match state.process_manager.follow_output(pid).await {
        Ok((existing_events, rx)) => {
            let stream = create_follow_stream_with_events(existing_events, rx);
            Sse::new(stream)
                .keep_alive(KeepAlive::default())
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

/// Create an SSE stream that first replays existing lines, then follows live
/// updates.
fn create_follow_stream(
    existing_lines: Vec<String>,
    stream_name: Option<&'static str>,
    rx: Option<tokio::sync::broadcast::Receiver<OutputEvent>>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    async_stream::stream! {
        // First, replay all existing lines from the log file
        for line in existing_lines {
            let event = OutputEvent {
                line,
                timestamp: 0,
                stream: stream_name.map(|s| s.to_string()),
            };
            let json = serde_json::to_string(&event).unwrap_or_default();
            yield Ok(Event::default().event("output").data(json));
        }

        // Then follow live updates if receiver is available
        if let Some(mut rx) = rx {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let json = serde_json::to_string(&event).unwrap_or_default();
                        yield Ok(Event::default().event("output").data(json));
                    }
                    Err(RecvError::Closed) => {
                        yield Ok(Event::default().event("eof").data("{}"));
                        break;
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, "Output stream lagged, some messages were missed");
                        continue;
                    }
                }
            }
        } else {
            // No live receiver (process doesn't exist or already exited), just send EOF
            yield Ok(Event::default().event("eof").data("{}"));
        }
    }
}

/// Create an SSE stream for combined output with pre-tagged events.
fn create_follow_stream_with_events(
    existing_events: Vec<OutputEvent>,
    rx: Option<tokio::sync::broadcast::Receiver<OutputEvent>>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    async_stream::stream! {
        // First, replay all existing events from the log files
        for event in existing_events {
            let json = serde_json::to_string(&event).unwrap_or_default();
            yield Ok(Event::default().event("output").data(json));
        }

        // Then follow live updates if receiver is available
        if let Some(mut rx) = rx {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let json = serde_json::to_string(&event).unwrap_or_default();
                        yield Ok(Event::default().event("output").data(json));
                    }
                    Err(RecvError::Closed) => {
                        yield Ok(Event::default().event("eof").data("{}"));
                        break;
                    }
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, "Output stream lagged, some messages were missed");
                        continue;
                    }
                }
            }
        } else {
            // No live receiver, just send EOF
            yield Ok(Event::default().event("eof").data("{}"));
        }
    }
}

// ============================================================================
// File Operations Endpoints
// ============================================================================

async fn read_file(State(state): State<AppState>, Query(query): Query<FilePathQuery>) -> Response {
    match state.file_manager.read_file(&query.path).await {
        Ok(content) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/octet-stream")
            .body(Body::from(content))
            .unwrap(),
        Err(e) => {
            let err_msg = e.to_string();
            let (status, code) =
                if err_msg.contains("not found") || err_msg.contains("No such file") {
                    (StatusCode::NOT_FOUND, "NOT_FOUND")
                } else if err_msg.contains("traversal") {
                    (StatusCode::FORBIDDEN, "PATH_TRAVERSAL")
                } else if err_msg.contains("is a directory") {
                    (StatusCode::BAD_REQUEST, "IS_DIRECTORY")
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
                };
            (
                status,
                axum::Json(ErrorResponse::with_code(e.to_string(), code)),
            )
                .into_response()
        }
    }
}

async fn write_file(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
    body: Bytes,
) -> impl IntoResponse {
    match state
        .file_manager
        .write_file(&query.path, body.to_vec())
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (status, code) = if e.to_string().contains("traversal") {
                (StatusCode::FORBIDDEN, "PATH_TRAVERSAL")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
            };
            (
                status,
                axum::Json(ErrorResponse::with_code(e.to_string(), code)),
            )
                .into_response()
        }
    }
}

async fn delete_file(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
) -> impl IntoResponse {
    match state.file_manager.delete_file(&query.path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (status, code) =
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    (StatusCode::NOT_FOUND, "NOT_FOUND")
                } else if e.to_string().contains("traversal") {
                    (StatusCode::FORBIDDEN, "PATH_TRAVERSAL")
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
                };
            (
                status,
                axum::Json(ErrorResponse::with_code(e.to_string(), code)),
            )
                .into_response()
        }
    }
}

async fn list_directory(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
) -> impl IntoResponse {
    match state.file_manager.list_directory(&query.path).await {
        Ok(entries) => (
            StatusCode::OK,
            axum::Json(ListDirectoryResponse {
                path: query.path,
                entries,
            }),
        )
            .into_response(),
        Err(e) => {
            let (status, code) =
                if e.to_string().contains("not found") || e.to_string().contains("No such file") {
                    (StatusCode::NOT_FOUND, "NOT_FOUND")
                } else if e.to_string().contains("traversal") {
                    (StatusCode::FORBIDDEN, "PATH_TRAVERSAL")
                } else if e.to_string().contains("not a directory") {
                    (StatusCode::BAD_REQUEST, "NOT_A_DIRECTORY")
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
                };
            (
                status,
                axum::Json(ErrorResponse::with_code(e.to_string(), code)),
            )
                .into_response()
        }
    }
}
