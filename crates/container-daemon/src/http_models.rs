//! HTTP API request and response models for the Sandbox API.
//!
//! These types are used by the HTTP server for the user-facing sandbox
//! operations.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Request to start a new process.
#[derive(Debug, Clone, Deserialize)]
pub struct StartProcessRequest {
    /// Command to execute.
    pub command: String,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables as key-value pairs.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Working directory for the process.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Whether to capture stdin for writing later.
    #[serde(default)]
    pub stdin_mode: StdinMode,
    /// How to handle stdout.
    #[serde(default)]
    pub stdout_mode: OutputMode,
    /// How to handle stderr.
    #[serde(default)]
    pub stderr_mode: OutputMode,
}

/// Mode for stdin handling.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StdinMode {
    /// Stdin is closed immediately (null).
    #[default]
    Closed,
    /// Stdin can be written to via the API.
    Pipe,
}

/// Mode for stdout/stderr handling.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    /// Output is captured and can be streamed via API.
    #[default]
    Capture,
    /// Output is discarded (null).
    Discard,
}

/// Status of a process.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStatusType {
    /// Process is currently running.
    Running,
    /// Process has exited with a code.
    Exited,
    /// Process was terminated by a signal.
    Signaled,
}

/// Information about a process.
#[derive(Debug, Clone, Serialize)]
pub struct ProcessInfo {
    /// Operating system PID.
    pub pid: u32,
    /// Current status of the process.
    pub status: ProcessStatusType,
    /// Exit code if the process has exited.
    pub exit_code: Option<i32>,
    /// Signal number if the process was signaled.
    pub signal: Option<i32>,
    /// Whether stdin is writable.
    pub stdin_writable: bool,
    /// Command that was executed.
    pub command: String,
    /// Arguments passed to the command.
    pub args: Vec<String>,
    /// Timestamp when the process started (milliseconds since epoch).
    pub started_at: i64,
    /// Timestamp when the process ended (milliseconds since epoch).
    pub ended_at: Option<i64>,
}

/// Response after starting a process.
#[derive(Debug, Clone, Serialize)]
pub struct StartProcessResponse {
    /// The created process info.
    #[serde(flatten)]
    pub process: ProcessInfo,
}

/// Response for listing processes.
#[derive(Debug, Clone, Serialize)]
pub struct ListProcessesResponse {
    /// List of processes.
    pub processes: Vec<ProcessInfo>,
}

/// Request to send a signal to a process.
#[derive(Debug, Clone, Deserialize)]
pub struct SendSignalRequest {
    /// Signal number to send (e.g., 15 for SIGTERM, 9 for SIGKILL).
    pub signal: i32,
}

/// Response after sending a signal.
#[derive(Debug, Clone, Serialize)]
pub struct SendSignalResponse {
    /// Whether the signal was sent successfully.
    pub success: bool,
}

/// Output event for streaming stdout/stderr.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputEvent {
    /// The output line.
    pub line: String,
    /// Timestamp in milliseconds since epoch.
    pub timestamp: i64,
    /// Stream identifier (stdout or stderr) for combined output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

/// Daemon information response.
#[derive(Debug, Clone, Serialize)]
pub struct DaemonInfo {
    /// Daemon version.
    pub version: String,
    /// Uptime in seconds.
    pub uptime_secs: u64,
    /// Number of running processes.
    pub running_processes: usize,
    /// Total processes (including exited).
    pub total_processes: usize,
}

/// Health check response.
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    /// Whether the daemon is healthy.
    pub healthy: bool,
}

/// Query parameters for file operations.
#[derive(Debug, Clone, Deserialize)]
pub struct FilePathQuery {
    /// Path to the file.
    pub path: String,
}

/// Response for directory listing.
#[derive(Debug, Clone, Serialize)]
pub struct DirectoryEntry {
    /// Name of the file or directory.
    pub name: String,
    /// Whether this is a directory.
    pub is_dir: bool,
    /// Size in bytes (for files).
    pub size: Option<u64>,
    /// Last modified timestamp in milliseconds since epoch.
    pub modified_at: Option<i64>,
}

/// Response for listing directory contents.
#[derive(Debug, Clone, Serialize)]
pub struct ListDirectoryResponse {
    /// Path that was listed.
    pub path: String,
    /// Entries in the directory.
    pub entries: Vec<DirectoryEntry>,
}

/// Response for getting captured output.
#[derive(Debug, Clone, Serialize)]
pub struct OutputResponse {
    /// Process PID.
    pub pid: u32,
    /// Output lines.
    pub lines: Vec<String>,
    /// Total number of lines.
    pub line_count: usize,
}

// ============================================================================
// PTY Session Models
// ============================================================================

/// Request to create a PTY session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePtySessionRequest {
    /// Command to execute.
    pub command: String,
    /// Arguments to pass to the command.
    #[serde(default)]
    pub args: Option<Vec<String>>,
    /// Environment variables as key-value pairs.
    #[serde(default)]
    pub env: Option<HashMap<String, String>>,
    /// Working directory for the process.
    #[serde(default)]
    pub working_dir: Option<String>,
    /// Number of rows for the terminal.
    pub rows: Option<u16>,
    /// Number of columns for the terminal.
    pub cols: Option<u16>,
}

/// Response after creating a PTY session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePtySessionResponse {
    /// Session identifier.
    pub session_id: String,
    /// Authentication token for WebSocket connection.
    pub token: String,
}

/// Information about a PTY session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PtySessionInfo {
    /// Session identifier.
    pub session_id: String,
    /// Authentication token (only included in create response).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Operating system PID of the PTY process.
    pub pid: u32,
    /// Command that was executed.
    pub command: String,
    /// Arguments passed to the command.
    pub args: Vec<String>,
    /// Number of terminal rows.
    pub rows: u16,
    /// Number of terminal columns.
    pub cols: u16,
    /// Timestamp when the session was created (milliseconds since epoch).
    pub created_at: i64,
    /// Timestamp when the session ended (milliseconds since epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<i64>,
    /// Exit code of the process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Whether the PTY process is still alive.
    pub is_alive: bool,
}

/// Response for listing PTY sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPtySessionsResponse {
    /// List of PTY sessions.
    pub sessions: Vec<PtySessionInfo>,
}

/// Request to resize a PTY session.
#[derive(Debug, Clone, Deserialize)]
pub struct ResizePtyRequest {
    /// Number of rows.
    pub rows: u16,
    /// Number of columns.
    pub cols: u16,
}

/// Query parameters for PTY WebSocket connection.
#[derive(Debug, Clone, Deserialize)]
pub struct WsTokenQuery {
    /// Authentication token.
    pub token: String,
}

/// Error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error message.
    pub error: String,
    /// Error code (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
        }
    }

    pub fn with_code(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: Some(code.into()),
        }
    }
}
