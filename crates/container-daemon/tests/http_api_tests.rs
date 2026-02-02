//! Integration tests for the HTTP Sandbox API.

use std::{collections::HashMap, path::PathBuf, process::Stdio, sync::Arc, time::Instant};

use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
};
use bytes::Bytes;
use chrono::Utc;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, broadcast, mpsc},
};
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};

// ============================================================================
// Test Types and Helpers
// ============================================================================

const OUTPUT_CHANNEL_SIZE: usize = 1024;
const STDIN_CHANNEL_SIZE: usize = 256;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StartProcessRequest {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub stdin_mode: StdinMode,
    #[serde(default)]
    pub stdout_mode: OutputMode,
    #[serde(default)]
    pub stderr_mode: OutputMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StdinMode {
    #[default]
    Closed,
    Pipe,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    #[default]
    Capture,
    Discard,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessInfo {
    pub process_id: String,
    pub pid: Option<u32>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub stdin_writable: bool,
    pub command: String,
    pub args: Vec<String>,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputEvent {
    pub line: String,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SendSignalRequest {
    pub signal: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: Option<u64>,
    pub modified_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    Running,
    Exited(i32),
    Signaled(i32),
}

struct ManagedProcess {
    process_id: String,
    pid: Option<u32>,
    status: ProcessStatus,
    command: String,
    args: Vec<String>,
    started_at: i64,
    ended_at: Option<i64>,
    stdin_mode: StdinMode,
    stdin_tx: Option<mpsc::Sender<Bytes>>,
    #[allow(dead_code)]
    stdout_tx: broadcast::Sender<OutputEvent>,
    #[allow(dead_code)]
    stderr_tx: broadcast::Sender<OutputEvent>,
    #[allow(dead_code)]
    combined_tx: broadcast::Sender<OutputEvent>,
}

impl ManagedProcess {
    fn to_info(&self) -> ProcessInfo {
        ProcessInfo {
            process_id: self.process_id.clone(),
            pid: self.pid,
            status: match self.status {
                ProcessStatus::Running => "running".to_string(),
                ProcessStatus::Exited(_) => "exited".to_string(),
                ProcessStatus::Signaled(_) => "signaled".to_string(),
            },
            exit_code: match self.status {
                ProcessStatus::Exited(code) => Some(code),
                _ => None,
            },
            signal: match self.status {
                ProcessStatus::Signaled(sig) => Some(sig),
                _ => None,
            },
            stdin_writable: self.stdin_tx.is_some() &&
                matches!(self.status, ProcessStatus::Running),
            command: self.command.clone(),
            args: self.args.clone(),
            started_at: self.started_at,
            ended_at: self.ended_at,
        }
    }
}

#[derive(Clone)]
pub struct ProcessManager {
    processes: Arc<Mutex<HashMap<String, ManagedProcess>>>,
    #[allow(dead_code)]
    log_dir: PathBuf,
}

impl ProcessManager {
    pub fn new(log_dir: PathBuf) -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
            log_dir,
        }
    }

    pub async fn start_process(&self, req: StartProcessRequest) -> anyhow::Result<ProcessInfo> {
        let process_id = nanoid::nanoid!(12);
        let started_at = Utc::now().timestamp_millis();

        let (stdout_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
        let (stderr_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
        let (combined_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);

        let (stdin_tx, stdin_rx) = if req.stdin_mode == StdinMode::Pipe {
            let (tx, rx) = mpsc::channel::<Bytes>(STDIN_CHANNEL_SIZE);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let mut cmd = Command::new(&req.command);
        cmd.args(&req.args);

        for (key, value) in &req.env {
            cmd.env(key, value);
        }

        if let Some(dir) = &req.working_dir {
            cmd.current_dir(dir);
        }

        cmd.stdout(
            if req.stdout_mode == OutputMode::Capture {
                Stdio::piped()
            } else {
                Stdio::null()
            },
        );

        cmd.stderr(
            if req.stderr_mode == OutputMode::Capture {
                Stdio::piped()
            } else {
                Stdio::null()
            },
        );

        cmd.stdin(
            if req.stdin_mode == StdinMode::Pipe {
                Stdio::piped()
            } else {
                Stdio::null()
            },
        );

        let mut child = cmd.spawn()?;
        let pid = child.id().ok_or_else(|| anyhow::anyhow!("No PID"))?;

        if let Some(stdout) = child.stdout.take() {
            let stdout_tx = stdout_tx.clone();
            let combined_tx = combined_tx.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let timestamp = Utc::now().timestamp_millis();
                    let _ = stdout_tx.send(OutputEvent {
                        line: line.clone(),
                        timestamp,
                        stream: None,
                    });
                    let _ = combined_tx.send(OutputEvent {
                        line,
                        timestamp,
                        stream: Some("stdout".to_string()),
                    });
                }
            });
        }

        if let Some(stderr) = child.stderr.take() {
            let stderr_tx = stderr_tx.clone();
            let combined_tx = combined_tx.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let timestamp = Utc::now().timestamp_millis();
                    let _ = stderr_tx.send(OutputEvent {
                        line: line.clone(),
                        timestamp,
                        stream: None,
                    });
                    let _ = combined_tx.send(OutputEvent {
                        line,
                        timestamp,
                        stream: Some("stderr".to_string()),
                    });
                }
            });
        }

        if let Some(mut stdin) = child.stdin.take() &&
            let Some(mut rx) = stdin_rx
        {
            tokio::spawn(async move {
                while let Some(data) = rx.recv().await {
                    if stdin.write_all(&data).await.is_err() {
                        break;
                    }
                    let _ = stdin.flush().await;
                }
            });
        }

        let managed = ManagedProcess {
            process_id: process_id.clone(),
            pid: Some(pid),
            status: ProcessStatus::Running,
            command: req.command,
            args: req.args,
            started_at,
            ended_at: None,
            stdin_mode: req.stdin_mode,
            stdin_tx,
            stdout_tx,
            stderr_tx,
            combined_tx,
        };

        let process_info = managed.to_info();

        {
            let mut processes = self.processes.lock().await;
            processes.insert(process_id.clone(), managed);
        }

        let processes_ref = self.processes.clone();
        tokio::spawn(async move {
            if let Ok(status) = child.wait().await {
                let mut processes = processes_ref.lock().await;
                if let Some(process) = processes.get_mut(&process_id) {
                    process.ended_at = Some(Utc::now().timestamp_millis());
                    process.stdin_tx = None;
                    if let Some(code) = status.code() {
                        process.status = ProcessStatus::Exited(code);
                    } else {
                        #[cfg(unix)]
                        {
                            use std::os::unix::process::ExitStatusExt;
                            if let Some(signal) = status.signal() {
                                process.status = ProcessStatus::Signaled(signal);
                            } else {
                                process.status = ProcessStatus::Exited(-1);
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            process.status = ProcessStatus::Exited(-1);
                        }
                    }
                }
            }
        });

        Ok(process_info)
    }

    pub async fn get_process(&self, id: &str) -> Option<ProcessInfo> {
        let processes = self.processes.lock().await;
        processes.get(id).map(|p| p.to_info())
    }

    pub async fn list_processes(&self) -> Vec<ProcessInfo> {
        let processes = self.processes.lock().await;
        processes.values().map(|p| p.to_info()).collect()
    }

    pub async fn get_counts(&self) -> (usize, usize) {
        let processes = self.processes.lock().await;
        let total = processes.len();
        let running = processes
            .values()
            .filter(|p| matches!(p.status, ProcessStatus::Running))
            .count();
        (running, total)
    }

    pub async fn send_signal(&self, id: &str, signal: i32) -> anyhow::Result<()> {
        let processes = self.processes.lock().await;
        let process = processes
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;
        let pid = process.pid.ok_or_else(|| anyhow::anyhow!("No PID"))?;

        if !matches!(process.status, ProcessStatus::Running) {
            anyhow::bail!("Process is not running");
        }

        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };
            let nix_signal = Signal::try_from(signal)?;
            kill(Pid::from_raw(pid as i32), nix_signal)?;
        }

        #[cfg(not(unix))]
        {
            let _ = (pid, signal);
            anyhow::bail!("Signals not supported on this platform");
        }

        Ok(())
    }

    pub async fn kill_process(&self, id: &str) -> anyhow::Result<()> {
        self.send_signal(id, 9).await
    }

    pub async fn write_stdin(&self, id: &str, data: Bytes) -> anyhow::Result<()> {
        let processes = self.processes.lock().await;
        let process = processes
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;

        if process.stdin_mode != StdinMode::Pipe {
            anyhow::bail!("Process stdin is not writable");
        }

        let tx = process
            .stdin_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("stdin closed"))?;
        tx.send(data)
            .await
            .map_err(|_| anyhow::anyhow!("stdin channel closed"))
    }

    pub async fn close_stdin(&self, id: &str) -> anyhow::Result<()> {
        let mut processes = self.processes.lock().await;
        let process = processes
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;
        process.stdin_tx = None;
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct FileManager;

impl FileManager {
    pub fn new() -> Self {
        Self
    }

    fn validate_path(&self, path: &str) -> anyhow::Result<PathBuf> {
        let path = std::path::Path::new(path);
        for component in path.components() {
            if let std::path::Component::ParentDir = component {
                anyhow::bail!("Path traversal detected");
            }
        }
        let canonical = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        Ok(canonical)
    }

    pub async fn read_file(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.validate_path(path)?;
        Ok(tokio::fs::read(&path).await?)
    }

    pub async fn write_file(&self, path: &str, content: Vec<u8>) -> anyhow::Result<()> {
        let path = self.validate_path(path)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, content).await?;
        Ok(())
    }

    pub async fn delete_file(&self, path: &str) -> anyhow::Result<()> {
        let path = self.validate_path(path)?;
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    pub async fn list_directory(&self, path: &str) -> anyhow::Result<Vec<DirectoryEntry>> {
        let path = self.validate_path(path)?;
        let metadata = tokio::fs::metadata(&path).await?;
        if !metadata.is_dir() {
            anyhow::bail!("Path is not a directory");
        }

        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&path).await?;

        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = entry.metadata().await?;
            let is_dir = metadata.is_dir();
            let size = if is_dir { None } else { Some(metadata.len()) };
            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64);

            entries.push(DirectoryEntry {
                name,
                is_dir,
                size,
                modified_at,
            });
        }

        entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        });

        Ok(entries)
    }
}

#[derive(Clone)]
pub struct AppState {
    pub process_manager: ProcessManager,
    pub file_manager: FileManager,
    pub start_time: Instant,
}

#[derive(Deserialize)]
struct FilePathQuery {
    path: String,
}

async fn health() -> impl IntoResponse {
    axum::Json(json!({"healthy": true}))
}

async fn info(State(state): State<AppState>) -> impl IntoResponse {
    let (running, total) = state.process_manager.get_counts().await;
    axum::Json(json!({
        "version": "0.1.0",
        "uptime_secs": state.start_time.elapsed().as_secs(),
        "running_processes": running,
        "total_processes": total,
    }))
}

async fn start_process(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<StartProcessRequest>,
) -> impl IntoResponse {
    match state.process_manager.start_process(req).await {
        Ok(info) => (
            StatusCode::CREATED,
            axum::Json(serde_json::to_value(info).unwrap()),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_processes(State(state): State<AppState>) -> impl IntoResponse {
    let processes = state.process_manager.list_processes().await;
    axum::Json(json!({"processes": processes}))
}

async fn get_process(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.process_manager.get_process(&id).await {
        Some(info) => (
            StatusCode::OK,
            axum::Json(serde_json::to_value(info).unwrap()),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": "Process not found", "code": "NOT_FOUND"})),
        )
            .into_response(),
    }
}

async fn kill_process(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.process_manager.kill_process(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            if e.to_string().contains("not found") {
                (
                    StatusCode::NOT_FOUND,
                    axum::Json(json!({"error": "Process not found"})),
                )
                    .into_response()
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": e.to_string()})),
                )
                    .into_response()
            }
        }
    }
}

async fn send_signal(
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::Json(req): axum::Json<SendSignalRequest>,
) -> impl IntoResponse {
    match state.process_manager.send_signal(&id, req.signal).await {
        Ok(()) => axum::Json(json!({"success": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn write_stdin(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    match state.process_manager.write_stdin(&id, body).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn close_stdin(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    match state.process_manager.close_stdin(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn read_file(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
) -> impl IntoResponse {
    match state.file_manager.read_file(&query.path).await {
        Ok(content) => (StatusCode::OK, content).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
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
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_file(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
) -> impl IntoResponse {
    match state.file_manager.delete_file(&query.path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_directory(
    State(state): State<AppState>,
    Query(query): Query<FilePathQuery>,
) -> impl IntoResponse {
    match state.file_manager.list_directory(&query.path).await {
        Ok(entries) => (
            StatusCode::OK,
            axum::Json(json!({"path": query.path, "entries": entries})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

fn create_test_app(temp_dir: &std::path::Path) -> Router {
    let state = AppState {
        process_manager: ProcessManager::new(temp_dir.to_path_buf()),
        file_manager: FileManager::new(),
        start_time: Instant::now(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/v1/processes", post(start_process))
        .route("/api/v1/processes", get(list_processes))
        .route("/api/v1/processes/{id}", get(get_process))
        .route("/api/v1/processes/{id}", delete(kill_process))
        .route("/api/v1/processes/{id}/signal", post(send_signal))
        .route("/api/v1/processes/{id}/stdin", post(write_stdin))
        .route("/api/v1/processes/{id}/stdin/close", post(close_stdin))
        .route("/api/v1/files", get(read_file))
        .route("/api/v1/files", put(write_file))
        .route("/api/v1/files", delete(delete_file))
        .route("/api/v1/files/list", get(list_directory))
        .layer(cors)
        .with_state(state)
}

// ============================================================================
// Health and Info Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_health_endpoint() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["healthy"], true);
}

#[tokio::test]
async fn test_info_endpoint() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["version"].is_string());
    assert!(json["uptime_secs"].is_number());
    assert_eq!(json["running_processes"], 0);
    assert_eq!(json["total_processes"], 0);
}

// ============================================================================
// Process Management Tests
// ============================================================================

#[tokio::test]
async fn test_start_process() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&json!({
                        "command": "echo",
                        "args": ["hello", "world"]
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["process_id"].is_string());
    assert!(json["pid"].is_number());
    assert_eq!(json["command"], "echo");
}

#[tokio::test]
async fn test_list_processes() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a process first
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "sleep", "args": ["10"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    // List processes
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/processes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["processes"].is_array());
    assert_eq!(json["processes"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_get_process() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a process first
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "sleep", "args": ["10"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();

    // Get process
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["process_id"], process_id);
    assert_eq!(json["command"], "sleep");
}

#[tokio::test]
async fn test_get_nonexistent_process() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/processes/nonexistent123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_kill_process() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a long-running process
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "sleep", "args": ["60"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();

    // Kill process
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Wait a bit for the process to be killed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify process is terminated
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "signaled");
}

#[tokio::test]
async fn test_process_exit_code() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a process that exits with code 0
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "true"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();

    // Wait for process to exit
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check status
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "exited");
    assert_eq!(json["exit_code"], 0);
}

// ============================================================================
// File Operation Tests
// ============================================================================

#[tokio::test]
async fn test_write_and_read_file() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let file_path = temp_dir.path().join("test.txt");
    let file_content = "Hello, World!";

    // Write file
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/files?path={}", file_path.display()))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(file_content))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Read file
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/files?path={}", file_path.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8_lossy(&body), file_content);
}

#[tokio::test]
async fn test_delete_file() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let file_path = temp_dir.path().join("to_delete.txt");

    // Create file first
    std::fs::write(&file_path, "delete me").unwrap();
    assert!(file_path.exists());

    // Delete file
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/files?path={}", file_path.display()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(!file_path.exists());
}

#[tokio::test]
async fn test_list_directory() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Create some files and directories
    std::fs::write(temp_dir.path().join("file1.txt"), "content1").unwrap();
    std::fs::write(temp_dir.path().join("file2.txt"), "content2").unwrap();
    std::fs::create_dir(temp_dir.path().join("subdir")).unwrap();

    // List directory
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/api/v1/files/list?path={}",
                    temp_dir.path().display()
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);

    // Directories come first
    assert!(entries[0]["is_dir"].as_bool().unwrap());
    assert_eq!(entries[0]["name"], "subdir");
}

#[tokio::test]
async fn test_read_nonexistent_file() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/files?path=/nonexistent/file.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// stdin Tests
// ============================================================================

#[tokio::test]
async fn test_stdin_write_and_close() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a cat process with stdin piped
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "cat", "stdin_mode": "pipe"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();
    assert!(json["stdin_writable"].as_bool().unwrap());

    // Write to stdin
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/processes/{}/stdin", process_id))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from("Hello from stdin\n"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Close stdin
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/processes/{}/stdin/close", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Wait for process to exit (cat exits when stdin is closed)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify process exited
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "exited");
    assert_eq!(json["exit_code"], 0);
}

#[tokio::test]
async fn test_stdin_write_to_process_without_pipe() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a process without stdin pipe
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "sleep", "args": ["10"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();
    assert!(!json["stdin_writable"].as_bool().unwrap());

    // Try to write to stdin (should fail)
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/processes/{}/stdin", process_id))
                .header("Content-Type", "application/octet-stream")
                .body(Body::from("test"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Signal Tests
// ============================================================================

#[tokio::test]
#[cfg(unix)]
async fn test_send_signal() {
    let temp_dir = TempDir::new().unwrap();
    let app = create_test_app(temp_dir.path());

    // Start a long-running process
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/processes")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"command": "sleep", "args": ["60"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let process_id = json["process_id"].as_str().unwrap();

    // Send SIGTERM (15)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/processes/{}/signal", process_id))
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"signal": 15}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["success"].as_bool().unwrap());

    // Wait for process to be killed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify process is terminated
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/processes/{}", process_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "signaled");
    assert_eq!(json["signal"], 15);
}
