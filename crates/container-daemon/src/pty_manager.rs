//! PTY session manager for interactive terminal sessions.
//!
//! Provides pseudo-terminal (PTY) allocation and management for interactive
//! programs like shells, vim, htop, etc. Sessions are created via REST API
//! and attached to via WebSocket.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use chrono::Utc;
use pty_process::Size;
use rand::RngExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, RwLock, broadcast},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::http_models::{CreatePtySessionRequest, PtySessionInfo};

/// Inactivity timeout: kill PTY if no client for this many seconds.
const INACTIVITY_TIMEOUT_SECS: u64 = 300;
/// How often to check for inactivity.
const INACTIVITY_CHECK_INTERVAL_SECS: u64 = 5;
/// Broadcast channel capacity for PTY output.
const OUTPUT_CHANNEL_CAPACITY: usize = 4096;
/// Read buffer size for PTY output.
const PTY_READ_BUF_SIZE: usize = 4096;
/// Seconds to wait after SIGHUP before SIGKILL.
const GRACEFUL_SHUTDOWN_SECS: u64 = 5;
/// Maximum size of the output buffer in bytes. Oldest chunks are evicted when
/// this limit is exceeded.
const MAX_OUTPUT_BUFFER_BYTES: usize = 1_048_576; // 1MB
/// Seconds to keep a dead session in the map before cleanup, allowing clients
/// to query exit codes.
const SESSION_CLEANUP_DELAY_SECS: u64 = 60;
/// EIO errno value (standard on POSIX systems). Expected when the PTY slave
/// closes.
const EIO_ERRNO: i32 = 5;
/// Maximum number of concurrent PTY sessions.
const MAX_CONCURRENT_SESSIONS: usize = 64;
/// Minimum allowed terminal dimension (rows or cols).
const MIN_TERMINAL_DIM: u16 = 1;
/// Maximum allowed terminal rows.
const MAX_TERMINAL_ROWS: u16 = 500;
/// Maximum allowed terminal columns.
const MAX_TERMINAL_COLS: u16 = 1000;

/// Manages PTY sessions for interactive terminal access.
#[derive(Clone)]
pub struct PtyManager {
    sessions: Arc<RwLock<HashMap<String, Arc<PtySessionInner>>>>,
    cancel: CancellationToken,
}

/// Ring buffer for PTY output that evicts oldest chunks when the total size
/// exceeds [`MAX_OUTPUT_BUFFER_BYTES`].
struct OutputBuffer {
    chunks: VecDeque<Bytes>,
    total_bytes: usize,
}

impl OutputBuffer {
    fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            total_bytes: 0,
        }
    }

    fn push(&mut self, data: Bytes) {
        self.total_bytes += data.len();
        self.chunks.push_back(data);
        while self.total_bytes > MAX_OUTPUT_BUFFER_BYTES {
            if let Some(evicted) = self.chunks.pop_front() {
                self.total_bytes -= evicted.len();
            } else {
                break;
            }
        }
    }

    fn clone_chunks(&self) -> Vec<Bytes> {
        self.chunks.iter().cloned().collect()
    }
}

/// Consolidated mutable session state, updated atomically under a single lock.
struct SessionStatus {
    is_alive: bool,
    exit_code: Option<i32>,
    ended_at: Option<i64>,
}

struct PtySessionInner {
    session_id: String,
    token: String,
    pid: u32,
    command: String,
    args: Vec<String>,
    size: Mutex<(u16, u16)>,
    created_at: i64,
    status: Mutex<SessionStatus>,
    pty_write: Mutex<pty_process::OwnedWritePty>,
    output_buffer: Mutex<OutputBuffer>,
    /// Broadcast sender for live PTY output. Set to `None` when the output
    /// reader exits, which closes the broadcast channel and notifies all
    /// receivers.
    output_tx: Mutex<Option<broadcast::Sender<Bytes>>>,
    client_count: AtomicUsize,
    session_cancel: CancellationToken,
}

impl PtyManager {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            cancel,
        }
    }

    /// Create a new PTY session, spawn the command, and start background tasks.
    pub async fn create_session(&self, req: CreatePtySessionRequest) -> Result<PtySessionInfo> {
        let session_id = nanoid::nanoid!();
        let token = generate_token();
        let rows = clamp_rows(req.rows.unwrap_or(24));
        let cols = clamp_cols(req.cols.unwrap_or(80));

        // Create PTY
        let (pty, pts) = pty_process::open().context("Failed to allocate PTY")?;
        pty.resize(Size::new(rows, cols))
            .context("Failed to set initial PTY size")?;

        // Build and spawn command
        let args = req.args.clone().unwrap_or_default();
        let mut cmd = pty_process::Command::new(&req.command).args(&args);
        if let Some(ref env) = req.env {
            for (key, value) in env {
                cmd = cmd.env(key, value);
            }
        }
        if let Some(ref cwd) = req.working_dir {
            cmd = cmd.current_dir(cwd);
        }

        let child = cmd.spawn(pts).context("Failed to spawn command on PTY")?;
        let pid = child.id().context("Failed to get PID of spawned process")?;

        // Split PTY into read/write halves
        let (read_pty, write_pty) = pty.into_split();

        // Broadcast channel for output
        let (output_tx, _) = broadcast::channel(OUTPUT_CHANNEL_CAPACITY);
        let created_at = Utc::now().timestamp_millis();
        let session_cancel = self.cancel.child_token();

        let session = Arc::new(PtySessionInner {
            session_id: session_id.clone(),
            token: token.clone(),
            pid,
            command: req.command.clone(),
            args: args.clone(),
            size: Mutex::new((rows, cols)),
            created_at,
            status: Mutex::new(SessionStatus {
                is_alive: true,
                exit_code: None,
                ended_at: None,
            }),
            pty_write: Mutex::new(write_pty),
            output_buffer: Mutex::new(OutputBuffer::new()),
            output_tx: Mutex::new(Some(output_tx)),
            client_count: AtomicUsize::new(0),
            session_cancel: session_cancel.clone(),
        });

        // Start background tasks
        Self::spawn_output_reader(session.clone(), read_pty);
        Self::spawn_child_waiter(session.clone(), child, self.sessions.clone());
        Self::spawn_inactivity_monitor(session.clone());

        // Check session limit and store atomically under a single write lock
        // to prevent TOCTOU races where concurrent creates both pass the check.
        {
            let mut sessions = self.sessions.write().await;
            if sessions.len() >= MAX_CONCURRENT_SESSIONS {
                // Clean up the just-spawned session before returning error.
                session.session_cancel.cancel();
                bail!(
                    "Maximum number of concurrent PTY sessions ({}) reached",
                    MAX_CONCURRENT_SESSIONS
                );
            }
            sessions.insert(session_id.clone(), session);
        }

        info!(session_id = %session_id, pid = pid, command = %req.command, "PTY session created");

        Ok(PtySessionInfo {
            session_id,
            token: Some(token),
            pid,
            command: req.command,
            args,
            rows,
            cols,
            created_at,
            ended_at: None,
            exit_code: None,
            is_alive: true,
        })
    }

    /// Validate a session token using constant-time comparison.
    pub async fn validate_token(&self, session_id: &str, token: &str) -> bool {
        let sessions = self.sessions.read().await;
        match sessions.get(session_id) {
            Some(session) => constant_time_eq(&session.token, token),
            None => false,
        }
    }

    /// Get session info (token is excluded).
    pub async fn get_session(&self, session_id: &str) -> Option<PtySessionInfo> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id)?;
        Some(session_to_info(session).await)
    }

    /// List all sessions (tokens are excluded).
    pub async fn list_sessions(&self) -> Vec<PtySessionInfo> {
        let sessions = self.sessions.read().await;
        let mut result = Vec::with_capacity(sessions.len());
        for session in sessions.values() {
            result.push(session_to_info(session).await);
        }
        result
    }

    /// Write input data to the PTY master.
    pub async fn write_input(&self, session_id: &str, data: &[u8]) -> Result<()> {
        let session = {
            let sessions = self.sessions.read().await;
            Arc::clone(sessions.get(session_id).context("Session not found")?)
        };
        if !session.status.lock().await.is_alive {
            bail!("Session is no longer alive");
        }
        session
            .pty_write
            .lock()
            .await
            .write_all(data)
            .await
            .context("Failed to write to PTY")?;
        Ok(())
    }

    /// Resize the PTY terminal. Values are clamped to valid bounds.
    pub async fn resize(&self, session_id: &str, rows: u16, cols: u16) -> Result<()> {
        let rows = clamp_rows(rows);
        let cols = clamp_cols(cols);
        let session = {
            let sessions = self.sessions.read().await;
            Arc::clone(sessions.get(session_id).context("Session not found")?)
        };
        session
            .pty_write
            .lock()
            .await
            .resize(Size::new(rows, cols))
            .context("Failed to resize PTY")?;
        *session.size.lock().await = (rows, cols);
        Ok(())
    }

    /// Subscribe to session output. Returns all buffered output and optionally
    /// a broadcast receiver for live output. Returns `None` for the receiver
    /// if the output stream has already closed.
    ///
    /// The buffer and output_tx locks are held together to prevent a race where
    /// output could be lost between cloning the buffer and subscribing.
    pub async fn subscribe_output(
        &self,
        session_id: &str,
    ) -> Result<(Vec<Bytes>, Option<broadcast::Receiver<Bytes>>)> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id).context("Session not found")?;
        let buffer_guard = session.output_buffer.lock().await;
        let tx_guard = session.output_tx.lock().await;
        let rx = tx_guard.as_ref().map(|tx| tx.subscribe());
        let buffer = buffer_guard.clone_chunks();
        drop(tx_guard);
        drop(buffer_guard);
        Ok((buffer, rx))
    }

    /// Get the cancellation token for a session. Used by the WebSocket handler
    /// to detect kill/shutdown promptly.
    pub async fn get_session_cancel(&self, session_id: &str) -> Option<CancellationToken> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).map(|s| s.session_cancel.clone())
    }

    /// Record that a WebSocket client has connected.
    pub async fn client_connected(&self, session_id: &str) -> Result<()> {
        let sessions = self.sessions.read().await;
        let session = sessions.get(session_id).context("Session not found")?;
        let count = session.client_count.fetch_add(1, Ordering::Release) + 1;
        info!(session_id = %session_id, clients = count, "Client connected to PTY session");
        Ok(())
    }

    /// Record that a WebSocket client has disconnected. Uses a CAS loop to
    /// prevent underflow if called spuriously.
    pub async fn client_disconnected(&self, session_id: &str) {
        let sessions = self.sessions.read().await;
        if let Some(session) = sessions.get(session_id) {
            let result = session.client_count.fetch_update(
                Ordering::Release,
                Ordering::Acquire,
                |current| current.checked_sub(1),
            );
            let count = match result {
                Ok(prev) => prev - 1,
                Err(_) => {
                    warn!(
                        session_id = %session_id,
                        "client_disconnected called with count already at 0"
                    );
                    0
                }
            };
            info!(session_id = %session_id, clients = count, "Client disconnected from PTY session");
        }
    }

    /// Kill a PTY session: SIGHUP immediately, then SIGKILL after a grace
    /// period in the background.
    pub async fn kill_session(&self, session_id: &str) -> Result<()> {
        let session = {
            let sessions = self.sessions.read().await;
            Arc::clone(sessions.get(session_id).context("Session not found")?)
        };

        if !session.status.lock().await.is_alive {
            return Ok(());
        }

        let pid = session.pid;
        info!(session_id = %session_id, pid = pid, "Killing PTY session");

        // Send SIGHUP first
        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGHUP);
        }

        // Background task: wait grace period then SIGKILL if still alive
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_SECS)).await;
            if session.status.lock().await.is_alive {
                warn!(
                    session_id = %session.session_id,
                    pid = pid,
                    "PTY session did not exit after SIGHUP, sending SIGKILL"
                );
                #[cfg(unix)]
                {
                    use nix::{
                        sys::signal::{Signal, kill},
                        unistd::Pid,
                    };
                    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
                }
            }
            session.session_cancel.cancel();
        });

        Ok(())
    }

    /// Stop all PTY sessions (called during daemon shutdown).
    /// Sends SIGHUP, waits a grace period, then SIGKILL any survivors.
    pub async fn stop_all_sessions(&self) {
        let alive_sessions: Vec<Arc<PtySessionInner>> = {
            let sessions = self.sessions.read().await;
            let mut alive = Vec::new();
            for session in sessions.values() {
                if session.status.lock().await.is_alive {
                    alive.push(Arc::clone(session));
                }
                session.session_cancel.cancel();
            }
            alive
        };

        if alive_sessions.is_empty() {
            return;
        }

        // Send SIGHUP to all alive sessions
        for session in &alive_sessions {
            info!(session_id = %session.session_id, "Stopping PTY session");
            #[cfg(unix)]
            {
                use nix::{
                    sys::signal::{Signal, kill},
                    unistd::Pid,
                };
                let _ = kill(Pid::from_raw(session.pid as i32), Signal::SIGHUP);
            }
        }

        // Wait for graceful shutdown
        tokio::time::sleep(std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_SECS)).await;

        // SIGKILL any that didn't exit
        for session in &alive_sessions {
            if session.status.lock().await.is_alive {
                warn!(
                    session_id = %session.session_id,
                    "PTY session did not exit gracefully, sending SIGKILL"
                );
                #[cfg(unix)]
                {
                    use nix::{
                        sys::signal::{Signal, kill},
                        unistd::Pid,
                    };
                    let _ = kill(Pid::from_raw(session.pid as i32), Signal::SIGKILL);
                }
            }
        }
    }

    /// Background task: read PTY output and broadcast to subscribers.
    fn spawn_output_reader(session: Arc<PtySessionInner>, mut read_pty: pty_process::OwnedReadPty) {
        tokio::spawn(async move {
            let mut buf = vec![0u8; PTY_READ_BUF_SIZE];
            loop {
                tokio::select! {
                    _ = session.session_cancel.cancelled() => break,
                    result = read_pty.read(&mut buf) => {
                        match result {
                            Ok(0) => break,
                            Ok(n) => {
                                let data = Bytes::copy_from_slice(&buf[..n]);
                                session.output_buffer.lock().await.push(data.clone());
                                if let Some(tx) = session.output_tx.lock().await.as_ref() {
                                    let _ = tx.send(data);
                                }
                            }
                            Err(e) => {
                                if e.raw_os_error() != Some(EIO_ERRNO) {
                                    warn!(
                                        session_id = %session.session_id,
                                        error = %e,
                                        "PTY read error"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
            // Close the broadcast channel so receivers get RecvError::Closed
            session.output_tx.lock().await.take();
            info!(session_id = %session.session_id, "PTY output reader exited");
        });
    }

    /// Background task: wait for child process to exit, update session state,
    /// then remove the session from the map after a grace period.
    fn spawn_child_waiter(
        session: Arc<PtySessionInner>,
        mut child: tokio::process::Child,
        sessions: Arc<RwLock<HashMap<String, Arc<PtySessionInner>>>>,
    ) {
        tokio::spawn(async move {
            let exit_code = tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(s) => s.code(),
                        Err(e) => {
                            error!(
                                session_id = %session.session_id,
                                error = %e,
                                "Error waiting for PTY child"
                            );
                            None
                        }
                    }
                }
                _ = session.session_cancel.cancelled() => {
                    // Cancelled (kill/shutdown). Give child a brief grace period.
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_SECS),
                        child.wait(),
                    ).await {
                        Ok(Ok(s)) => s.code(),
                        _ => None,
                    }
                }
            };

            {
                let mut status = session.status.lock().await;
                status.exit_code = exit_code;
                status.is_alive = false;
                status.ended_at = Some(Utc::now().timestamp_millis());
            }

            info!(
                session_id = %session.session_id,
                exit_code = ?exit_code,
                "PTY child process exited"
            );

            // Keep the session in the map for a grace period so clients can
            // query the exit code, then remove it. During shutdown or kill,
            // session_cancel is cancelled, skipping the delay.
            tokio::select! {
                _ = session.session_cancel.cancelled() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(SESSION_CLEANUP_DELAY_SECS)) => {}
            }
            sessions.write().await.remove(&session.session_id);
            info!(session_id = %session.session_id, "Dead PTY session cleaned up");
        });
    }

    /// Background task: kill session if no clients connect for too long.
    fn spawn_inactivity_monitor(session: Arc<PtySessionInner>) {
        tokio::spawn(async move {
            let mut no_client_since: Option<tokio::time::Instant> = None;

            loop {
                tokio::select! {
                    _ = session.session_cancel.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(INACTIVITY_CHECK_INTERVAL_SECS)) => {
                        if !session.status.lock().await.is_alive {
                            break;
                        }

                        let clients = session.client_count.load(Ordering::Acquire);
                        if clients == 0 {
                            match no_client_since {
                                None => {
                                    no_client_since = Some(tokio::time::Instant::now());
                                }
                                Some(since) => {
                                    if since.elapsed().as_secs() >= INACTIVITY_TIMEOUT_SECS {
                                        warn!(
                                            session_id = %session.session_id,
                                            timeout_secs = INACTIVITY_TIMEOUT_SECS,
                                            "PTY session inactive, killing"
                                        );
                                        #[cfg(unix)]
                                        {
                                            use nix::sys::signal::{Signal, kill};
                                            use nix::unistd::Pid;
                                            let _ = kill(
                                                Pid::from_raw(session.pid as i32),
                                                Signal::SIGKILL,
                                            );
                                        }
                                        session.session_cancel.cancel();
                                        break;
                                    }
                                }
                            }
                        } else {
                            no_client_since = None;
                        }
                    }
                }
            }
        });
    }
}

/// Convert a session to its public info representation (no token).
async fn session_to_info(session: &PtySessionInner) -> PtySessionInfo {
    let (rows, cols) = *session.size.lock().await;
    let status = session.status.lock().await;
    PtySessionInfo {
        session_id: session.session_id.clone(),
        token: None,
        pid: session.pid,
        command: session.command.clone(),
        args: session.args.clone(),
        rows,
        cols,
        created_at: session.created_at,
        ended_at: status.ended_at,
        exit_code: status.exit_code,
        is_alive: status.is_alive,
    }
}

/// Clamp a row count to valid terminal bounds.
fn clamp_rows(rows: u16) -> u16 {
    rows.clamp(MIN_TERMINAL_DIM, MAX_TERMINAL_ROWS)
}

/// Clamp a column count to valid terminal bounds.
fn clamp_cols(cols: u16) -> u16 {
    cols.clamp(MIN_TERMINAL_DIM, MAX_TERMINAL_COLS)
}

/// Generate a 64-character hex token from 32 random bytes.
fn generate_token() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 32] = rng.random();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Constant-time string comparison to prevent timing attacks on tokens.
pub(crate) fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y)) ==
        0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_buffer_push_stores_data() {
        let mut buf = OutputBuffer::new();
        let data = Bytes::from_static(b"hello");
        buf.push(data.clone());
        assert_eq!(buf.chunks.len(), 1);
        assert_eq!(buf.total_bytes, 5);
        assert_eq!(&buf.chunks[0][..], b"hello");
    }

    #[test]
    fn output_buffer_push_tracks_total_bytes() {
        let mut buf = OutputBuffer::new();
        buf.push(Bytes::from_static(b"aaa"));
        buf.push(Bytes::from_static(b"bb"));
        assert_eq!(buf.total_bytes, 5);
        assert_eq!(buf.chunks.len(), 2);
    }

    #[test]
    fn output_buffer_push_evicts_oldest_when_over_limit() {
        let mut buf = OutputBuffer::new();
        // Push slightly more than MAX_OUTPUT_BUFFER_BYTES
        let chunk_size = 100_000;
        let num_chunks = (MAX_OUTPUT_BUFFER_BYTES / chunk_size) + 2; // e.g. 12 chunks
        for _ in 0..num_chunks {
            buf.push(Bytes::from(vec![0u8; chunk_size]));
        }
        // total_bytes should be <= MAX_OUTPUT_BUFFER_BYTES
        assert!(buf.total_bytes <= MAX_OUTPUT_BUFFER_BYTES);
        // At least one chunk should have been evicted
        assert!(buf.chunks.len() < num_chunks);
    }

    #[test]
    fn output_buffer_clone_chunks_returns_all_in_order() {
        let mut buf = OutputBuffer::new();
        buf.push(Bytes::from_static(b"first"));
        buf.push(Bytes::from_static(b"second"));
        buf.push(Bytes::from_static(b"third"));
        let chunks = buf.clone_chunks();
        assert_eq!(chunks.len(), 3);
        assert_eq!(&chunks[0][..], b"first");
        assert_eq!(&chunks[1][..], b"second");
        assert_eq!(&chunks[2][..], b"third");
    }

    #[test]
    fn generate_token_produces_64_hex_chars() {
        let token = generate_token();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generate_token_produces_unique_values() {
        let t1 = generate_token();
        let t2 = generate_token();
        assert_ne!(t1, t2);
    }

    #[test]
    fn constant_time_eq_equal_strings() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn constant_time_eq_unequal_strings() {
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "xyz"));
    }

    #[test]
    fn constant_time_eq_different_lengths() {
        assert!(!constant_time_eq("abc", "ab"));
        assert!(!constant_time_eq("a", "ab"));
    }

    // Note: PTY allocation tests require Linux (pty-process resize uses
    // TIOCSWINSZ on the master fd, which returns ENOTTY on macOS).
    // Actual PTY integration tests are in tests/pty_tests.rs and gated
    // on target_os = "linux".
}
