//! HTTP server for the user-facing Sandbox API.
//!
//! Provides RESTful endpoints for:
//! - Process management (start, list, signal, kill)
//! - Streaming I/O (stdin, stdout, stderr via SSE)
//! - File operations (read, write, delete, list)

use std::{
    convert::Infallible,
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{
        Path,
        Query,
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
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
use tracing::{error, info, warn};

use crate::{
    file_manager::{FileError, FileManager},
    http_models::{
        CreatePtySessionRequest,
        CreatePtySessionResponse,
        DaemonInfo,
        ErrorResponse,
        FilePathQuery,
        HealthResponse,
        ListDirectoryResponse,
        ListProcessesResponse,
        ListPtySessionsResponse,
        OutputEvent,
        OutputResponse,
        ResizePtyRequest,
        SendSignalRequest,
        SendSignalResponse,
        StartProcessRequest,
        StartProcessResponse,
        WsTokenQuery,
    },
    process_manager::ProcessManager,
    pty_manager::PtyManager,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub process_manager: ProcessManager,
    pub file_manager: FileManager,
    pub pty_manager: PtyManager,
    pub start_time: Instant,
}

/// Build the HTTP router with all routes configured.
pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
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
        // PTY sessions
        .route("/api/v1/pty", post(create_pty_session))
        .route("/api/v1/pty", get(list_pty_sessions))
        .route("/api/v1/pty/{session_id}", get(get_pty_session))
        .route("/api/v1/pty/{session_id}", delete(kill_pty_session))
        .route("/api/v1/pty/{session_id}/resize", post(resize_pty))
        .route("/api/v1/pty/{session_id}/ws", get(pty_websocket_handler))
        .layer(cors)
        .with_state(state)
}

/// Run the HTTP server for the sandbox API.
pub async fn run_http_server(
    port: u16,
    process_manager: ProcessManager,
    file_manager: FileManager,
    pty_manager: PtyManager,
    cancel_token: CancellationToken,
) -> Result<()> {
    let state = AppState {
        process_manager,
        file_manager,
        pty_manager,
        start_time: Instant::now(),
    };

    let app = build_router(state);

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
            let (status, code) = match &e {
                FileError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
                FileError::PathTraversal => (StatusCode::FORBIDDEN, "PATH_TRAVERSAL"),
                FileError::IsDirectory(_) => (StatusCode::BAD_REQUEST, "IS_DIRECTORY"),
                FileError::NotADirectory(_) | FileError::Other(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
                }
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
            let (status, code) = match &e {
                FileError::PathTraversal => (StatusCode::FORBIDDEN, "PATH_TRAVERSAL"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
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
            let (status, code) = match &e {
                FileError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
                FileError::PathTraversal => (StatusCode::FORBIDDEN, "PATH_TRAVERSAL"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
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
            let (status, code) = match &e {
                FileError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
                FileError::PathTraversal => (StatusCode::FORBIDDEN, "PATH_TRAVERSAL"),
                FileError::NotADirectory(_) => (StatusCode::BAD_REQUEST, "NOT_A_DIRECTORY"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
            };
            (
                status,
                axum::Json(ErrorResponse::with_code(e.to_string(), code)),
            )
                .into_response()
        }
    }
}

// ============================================================================
// PTY Session Endpoints
// ============================================================================

/// Maximum size of buffered output held before client sends READY signal.
const MAX_PRE_READY_BUFFER_BYTES: usize = 1_048_576; // 1MB

/// Interval between WebSocket ping frames for connection liveness detection.
const WS_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum WebSocket message/frame size (64KB). Terminal messages should never
/// exceed this. Prevents malicious clients from sending huge frames.
const WS_MAX_MESSAGE_SIZE: usize = 65_536;

/// Maximum input bytes per second from a WebSocket client (1MB/sec). Generous
/// enough for pastes but prevents flooding.
const MAX_INPUT_BYTES_PER_SEC: usize = 1_048_576;

async fn create_pty_session(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<CreatePtySessionRequest>,
) -> impl IntoResponse {
    match state.pty_manager.create_session(req).await {
        Ok(info) => {
            let resp = CreatePtySessionResponse {
                session_id: info.session_id,
                token: info.token.unwrap_or_default(),
            };
            (StatusCode::CREATED, axum::Json(resp)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to create PTY session");
            if e.to_string().contains("Maximum number") {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    axum::Json(ErrorResponse::with_code(e.to_string(), "TOO_MANY_SESSIONS")),
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

async fn list_pty_sessions(State(state): State<AppState>) -> impl IntoResponse {
    let sessions = state.pty_manager.list_sessions().await;
    axum::Json(ListPtySessionsResponse { sessions })
}

async fn get_pty_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.pty_manager.get_session(&session_id).await {
        Some(info) => (StatusCode::OK, axum::Json(info)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(ErrorResponse::with_code(
                format!("PTY session {} not found", session_id),
                "NOT_FOUND",
            )),
        )
            .into_response(),
    }
}

async fn kill_pty_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.pty_manager.kill_session(&session_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("PTY session {} not found", session_id),
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

async fn resize_pty(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    axum::Json(req): axum::Json<ResizePtyRequest>,
) -> impl IntoResponse {
    match state
        .pty_manager
        .resize(&session_id, req.rows, req.cols)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(ErrorResponse::with_code(
                        format!("PTY session {} not found", session_id),
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

async fn pty_websocket_handler(
    ws: WebSocketUpgrade,
    Path(session_id): Path<String>,
    Query(query): Query<WsTokenQuery>,
    State(state): State<AppState>,
) -> Response {
    // Validate token
    if !state
        .pty_manager
        .validate_token(&session_id, &query.token)
        .await
    {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(ErrorResponse::with_code("Invalid token", "INVALID_TOKEN")),
        )
            .into_response();
    }

    ws.max_message_size(WS_MAX_MESSAGE_SIZE)
        .max_frame_size(WS_MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_pty_websocket(socket, session_id, state))
}

async fn handle_pty_websocket(mut socket: WebSocket, session_id: String, state: AppState) {
    // Subscribe to output (get history + optional live receiver)
    let (history, output_rx) = match state.pty_manager.subscribe_output(&session_id).await {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "Failed to subscribe to PTY output");
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    // Get session cancel token for instant disconnect on kill/shutdown
    let session_cancel = match state.pty_manager.get_session_cancel(&session_id).await {
        Some(t) => t,
        None => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    // Track client connection
    if let Err(e) = state.pty_manager.client_connected(&session_id).await {
        error!(error = %e, "Failed to register client connection");
        let _ = socket.send(Message::Close(None)).await;
        return;
    }

    let mut ready = false;
    let mut pre_ready_buffer: Vec<Bytes> = history;
    let mut pre_ready_bytes: usize = pre_ready_buffer.iter().map(|b| b.len()).sum();
    let mut output_rx = output_rx;

    // Input rate limiting
    let mut input_bytes_this_second: usize = 0;
    let mut rate_limit_reset = tokio::time::Instant::now();

    // Ping/pong for connection liveness detection
    let mut ping_interval = tokio::time::interval(WS_PING_INTERVAL);
    ping_interval.tick().await; // consume the immediate first tick
    let mut awaiting_pong = false;

    loop {
        tokio::select! {
            result = async {
                match output_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match result {
                    Ok(data) => {
                        if ready {
                            let mut frame = Vec::with_capacity(1 + data.len());
                            frame.push(0x00);
                            frame.extend_from_slice(&data);
                            if socket.send(Message::Binary(Bytes::from(frame))).await.is_err() {
                                break;
                            }
                        } else {
                            pre_ready_bytes += data.len();
                            pre_ready_buffer.push(data);
                            if pre_ready_bytes > MAX_PRE_READY_BUFFER_BYTES {
                                warn!(
                                    session_id = %session_id,
                                    bytes = pre_ready_bytes,
                                    "Pre-ready buffer exceeded limit, disconnecting client"
                                );
                                let _ = socket
                                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                        code: 1008,
                                        reason: "pre-ready buffer overflow".into(),
                                    })))
                                    .await;
                                break;
                            }
                        }
                    }
                    Err(RecvError::Closed) => {
                        // Output stream closed. If ready, send close frame
                        // with exit code. Otherwise wait for READY to flush
                        // buffer first.
                        output_rx = None;
                        if ready {
                            send_exit_close_frame(&mut socket, &session_id, &state).await;
                            break;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!(
                            lagged = n,
                            session_id = %session_id,
                            "PTY output stream lagged, disconnecting client"
                        );
                        let _ = socket
                            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                code: 1008,
                                reason: format!("output lagged by {} messages", n).into(),
                            })))
                            .await;
                        break;
                    }
                }
            }

            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Binary(data))) if !data.is_empty() => {
                        awaiting_pong = false;
                        match data[0] {
                            // DATA: forward to PTY (with rate limiting)
                            0x00 => {
                                let now = tokio::time::Instant::now();
                                if now.duration_since(rate_limit_reset) >= Duration::from_secs(1) {
                                    input_bytes_this_second = 0;
                                    rate_limit_reset = now;
                                }
                                input_bytes_this_second += data.len() - 1;
                                if input_bytes_this_second > MAX_INPUT_BYTES_PER_SEC {
                                    warn!(
                                        session_id = %session_id,
                                        "Input rate limit exceeded, disconnecting client"
                                    );
                                    let _ = socket
                                        .send(Message::Close(Some(
                                            axum::extract::ws::CloseFrame {
                                                code: 1008,
                                                reason: "input rate limit exceeded".into(),
                                            },
                                        )))
                                        .await;
                                    break;
                                }
                                if let Err(e) = state
                                    .pty_manager
                                    .write_input(&session_id, &data[1..])
                                    .await
                                {
                                    warn!(error = %e, "Failed to write to PTY");
                                }
                            }
                            // RESIZE: cols(u16BE) + rows(u16BE)
                            0x01 if data.len() >= 5 => {
                                let cols = u16::from_be_bytes([data[1], data[2]]);
                                let rows = u16::from_be_bytes([data[3], data[4]]);
                                if let Err(e) = state
                                    .pty_manager
                                    .resize(&session_id, rows, cols)
                                    .await
                                {
                                    warn!(error = %e, "Failed to resize PTY");
                                }
                            }
                            // READY: flush buffered output
                            0x02 => {
                                ready = true;
                                let mut flush_failed = false;
                                for buffered in pre_ready_buffer.drain(..) {
                                    let mut frame = Vec::with_capacity(1 + buffered.len());
                                    frame.push(0x00);
                                    frame.extend_from_slice(&buffered);
                                    if socket
                                        .send(Message::Binary(Bytes::from(frame)))
                                        .await
                                        .is_err()
                                    {
                                        flush_failed = true;
                                        break;
                                    }
                                }
                                pre_ready_bytes = 0;
                                if flush_failed {
                                    break;
                                }
                                // If output already closed, send close frame now
                                if output_rx.is_none() {
                                    send_exit_close_frame(&mut socket, &session_id, &state).await;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        awaiting_pong = false;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        warn!(error = %e, "WebSocket error");
                        break;
                    }
                    _ => {
                        // Text, Ping, or empty Binary — any client activity
                        // resets the pong deadline.
                        awaiting_pong = false;
                    }
                }
            }

            _ = ping_interval.tick() => {
                if awaiting_pong {
                    warn!(
                        session_id = %session_id,
                        "WebSocket client not responding to pings, disconnecting"
                    );
                    break;
                }
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
                awaiting_pong = true;
            }

            _ = session_cancel.cancelled() => {
                info!(
                    session_id = %session_id,
                    "Session cancelled, disconnecting WebSocket client"
                );
                let _ = socket
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1001,
                        reason: "session terminated".into(),
                    })))
                    .await;
                break;
            }
        }
    }

    state.pty_manager.client_disconnected(&session_id).await;
}

/// Send a WebSocket close frame with the session's exit code.
async fn send_exit_close_frame(socket: &mut WebSocket, session_id: &str, state: &AppState) {
    let exit_code = state
        .pty_manager
        .get_session(session_id)
        .await
        .and_then(|info| info.exit_code)
        .unwrap_or(-1);
    let reason = format!("exit:{}", exit_code);
    let _ = socket
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code: 1000,
            reason: reason.into(),
        })))
        .await;
}
