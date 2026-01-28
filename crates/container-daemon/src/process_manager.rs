//! Multi-process manager for the sandbox API.
//!
//! Handles spawning, tracking, and managing multiple concurrent processes
//! with streaming stdin/stdout/stderr support.

use std::{collections::HashMap, path::PathBuf, process::Stdio, sync::Arc};

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::Utc;
#[cfg(unix)]
use nix::sys::signal::{Signal, kill};
#[cfg(unix)]
use nix::unistd::Pid;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, broadcast, mpsc},
};
use tracing::{debug, error, info, warn};

use crate::http_models::{
    OutputEvent,
    OutputMode,
    ProcessInfo,
    ProcessStatusType,
    StartProcessRequest,
    StdinMode,
};

/// Size of the broadcast channel for output events.
const OUTPUT_CHANNEL_SIZE: usize = 1024;

/// Size of the stdin channel.
const STDIN_CHANNEL_SIZE: usize = 256;

/// Status of a managed process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatus {
    /// Process is currently running
    Running,
    /// Process has exited with a code
    Exited(i32),
    /// Process was terminated by a signal
    Signaled(i32),
}

/// Internal state for a single managed process.
struct ManagedProcess {
    pid: u32,
    status: ProcessStatus,
    command: String,
    args: Vec<String>,
    started_at: i64,
    ended_at: Option<i64>,
    stdin_mode: StdinMode,
    stdin_tx: Option<mpsc::Sender<Bytes>>,
    stdout_tx: broadcast::Sender<OutputEvent>,
    stderr_tx: broadcast::Sender<OutputEvent>,
    combined_tx: broadcast::Sender<OutputEvent>,
}

impl ManagedProcess {
    fn to_info(&self) -> ProcessInfo {
        ProcessInfo {
            pid: self.pid,
            status: match self.status {
                ProcessStatus::Running => ProcessStatusType::Running,
                ProcessStatus::Exited(_) => ProcessStatusType::Exited,
                ProcessStatus::Signaled(_) => ProcessStatusType::Signaled,
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

/// Manages multiple concurrent processes for the sandbox API.
#[derive(Clone)]
pub struct ProcessManager {
    processes: Arc<Mutex<HashMap<u32, ManagedProcess>>>,
    log_dir: PathBuf,
}

impl ProcessManager {
    /// Create a new process manager.
    pub fn new(log_dir: PathBuf) -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
            log_dir,
        }
    }

    /// Start a new process with the given configuration.
    pub async fn start_process(&self, req: StartProcessRequest) -> Result<ProcessInfo> {
        let started_at = Utc::now().timestamp_millis();

        // Create broadcast channels for output
        let (stdout_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
        let (stderr_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);
        let (combined_tx, _) = broadcast::channel(OUTPUT_CHANNEL_SIZE);

        // Create stdin channel if needed
        let (stdin_tx, stdin_rx) = if req.stdin_mode == StdinMode::Pipe {
            let (tx, rx) = mpsc::channel(STDIN_CHANNEL_SIZE);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Build the command
        let mut cmd = Command::new(&req.command);
        cmd.args(&req.args);

        // Set environment variables
        for (key, value) in &req.env {
            cmd.env(key, value);
        }

        // Set working directory if specified
        if let Some(dir) = &req.working_dir {
            cmd.current_dir(dir);
        }

        // Configure stdio
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

        // Spawn the process
        let mut child = cmd.spawn().context("Failed to spawn process")?;

        let pid = child.id().context("Failed to get child PID")?;

        info!(
            pid = pid,
            command = %req.command,
            args = ?req.args,
            "Starting process"
        );

        // Create process log directory
        let process_log_dir = self.log_dir.join(pid.to_string());
        if let Err(e) = std::fs::create_dir_all(&process_log_dir) {
            warn!(error = %e, "Failed to create process log directory");
        }

        // Set up stdout forwarding
        if let Some(stdout) = child.stdout.take() {
            let stdout_tx = stdout_tx.clone();
            let combined_tx = combined_tx.clone();
            let log_path = process_log_dir.join("stdout.log");
            tokio::spawn(async move {
                if let Err(e) =
                    forward_output_stream(stdout, stdout_tx, combined_tx, "stdout", log_path).await
                {
                    debug!(error = %e, "stdout forwarding ended");
                }
            });
        }

        // Set up stderr forwarding
        if let Some(stderr) = child.stderr.take() {
            let stderr_tx = stderr_tx.clone();
            let combined_tx = combined_tx.clone();
            let log_path = process_log_dir.join("stderr.log");
            tokio::spawn(async move {
                if let Err(e) =
                    forward_output_stream(stderr, stderr_tx, combined_tx, "stderr", log_path).await
                {
                    debug!(error = %e, "stderr forwarding ended");
                }
            });
        }

        // Set up stdin forwarding
        if let Some(stdin) = child.stdin.take() &&
            let Some(rx) = stdin_rx
        {
            tokio::spawn(async move {
                forward_stdin(stdin, rx).await;
            });
        }

        // Create the managed process entry
        let managed = ManagedProcess {
            pid,
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

        // Store the process
        {
            let mut processes = self.processes.lock().await;
            processes.insert(pid, managed);
        }

        // Spawn a task to wait for the process to exit
        let processes_ref = self.processes.clone();
        tokio::spawn(async move {
            wait_for_process_exit(child, pid, processes_ref).await;
        });

        Ok(process_info)
    }

    /// Get information about a specific process.
    pub async fn get_process(&self, pid: u32) -> Option<ProcessInfo> {
        let processes = self.processes.lock().await;
        processes.get(&pid).map(|p| p.to_info())
    }

    /// List all processes.
    pub async fn list_processes(&self) -> Vec<ProcessInfo> {
        let processes = self.processes.lock().await;
        processes.values().map(|p| p.to_info()).collect()
    }

    /// Get counts of running and total processes.
    pub async fn get_counts(&self) -> (usize, usize) {
        let processes = self.processes.lock().await;
        let total = processes.len();
        let running = processes
            .values()
            .filter(|p| matches!(p.status, ProcessStatus::Running))
            .count();
        (running, total)
    }

    /// Send a signal to a process.
    pub async fn send_signal(&self, pid: u32, signal: i32) -> Result<()> {
        let processes = self.processes.lock().await;
        let process = processes.get(&pid).context("Process not found")?;

        if !matches!(process.status, ProcessStatus::Running) {
            anyhow::bail!("Process {} is not running", pid);
        }

        info!(pid = pid, signal = signal, "Sending signal to process");

        #[cfg(unix)]
        {
            let nix_signal = Signal::try_from(signal).context("Invalid signal number")?;
            kill(Pid::from_raw(pid as i32), nix_signal)
                .with_context(|| format!("Failed to send signal to pid {}", pid))?;
        }

        #[cfg(not(unix))]
        {
            anyhow::bail!("Signal sending not supported on this platform");
        }

        Ok(())
    }

    /// Kill a process (SIGKILL).
    pub async fn kill_process(&self, pid: u32) -> Result<()> {
        self.send_signal(pid, 9).await
    }

    /// Write data to a process's stdin.
    pub async fn write_stdin(&self, pid: u32, data: Bytes) -> Result<()> {
        let processes = self.processes.lock().await;
        let process = processes.get(&pid).context("Process not found")?;

        if process.stdin_mode != StdinMode::Pipe {
            anyhow::bail!("Process stdin is not writable");
        }

        let tx = process
            .stdin_tx
            .as_ref()
            .context("Process stdin is closed")?;

        tx.send(data)
            .await
            .map_err(|_| anyhow::anyhow!("Process stdin channel closed"))
    }

    /// Close a process's stdin (send EOF).
    pub async fn close_stdin(&self, pid: u32) -> Result<()> {
        let mut processes = self.processes.lock().await;
        let process = processes.get_mut(&pid).context("Process not found")?;

        if process.stdin_mode != StdinMode::Pipe {
            anyhow::bail!("Process stdin is not writable");
        }

        // Drop the sender to close stdin
        process.stdin_tx = None;
        Ok(())
    }

    /// Subscribe to stdout events for a process.
    pub async fn subscribe_stdout(&self, pid: u32) -> Option<broadcast::Receiver<OutputEvent>> {
        let processes = self.processes.lock().await;
        processes.get(&pid).map(|p| p.stdout_tx.subscribe())
    }

    /// Subscribe to stderr events for a process.
    pub async fn subscribe_stderr(&self, pid: u32) -> Option<broadcast::Receiver<OutputEvent>> {
        let processes = self.processes.lock().await;
        processes.get(&pid).map(|p| p.stderr_tx.subscribe())
    }

    /// Subscribe to combined stdout+stderr events for a process.
    pub async fn subscribe_combined(&self, pid: u32) -> Option<broadcast::Receiver<OutputEvent>> {
        let processes = self.processes.lock().await;
        processes.get(&pid).map(|p| p.combined_tx.subscribe())
    }

    /// Get all captured stdout output for a process (from log file).
    pub async fn get_stdout(&self, pid: u32) -> Result<Vec<String>> {
        let log_path = self.log_dir.join(pid.to_string()).join("stdout.log");
        self.read_log_file(&log_path).await
    }

    /// Get all captured stderr output for a process (from log file).
    pub async fn get_stderr(&self, pid: u32) -> Result<Vec<String>> {
        let log_path = self.log_dir.join(pid.to_string()).join("stderr.log");
        self.read_log_file(&log_path).await
    }

    /// Get all captured output (stdout + stderr combined) for a process.
    /// Returns lines tagged with their stream source.
    pub async fn get_output(&self, pid: u32) -> Result<Vec<OutputEvent>> {
        let stdout_path = self.log_dir.join(pid.to_string()).join("stdout.log");
        let stderr_path = self.log_dir.join(pid.to_string()).join("stderr.log");

        let stdout_lines = self.read_log_file(&stdout_path).await?;
        let stderr_lines = self.read_log_file(&stderr_path).await?;

        // Combine and tag with stream source
        // Note: We don't have timestamps in log files, so we interleave them
        let mut events: Vec<OutputEvent> = Vec::new();

        for line in stdout_lines {
            events.push(OutputEvent {
                line,
                timestamp: 0, // No timestamp available from log file
                stream: Some("stdout".to_string()),
            });
        }

        for line in stderr_lines {
            events.push(OutputEvent {
                line,
                timestamp: 0,
                stream: Some("stderr".to_string()),
            });
        }

        Ok(events)
    }

    /// Follow stdout: get existing lines from log file and optionally a
    /// receiver for live updates. Returns (existing_lines,
    /// Option<receiver>) - receiver is None if process doesn't exist.
    pub async fn follow_stdout(
        &self,
        pid: u32,
    ) -> Result<(Vec<String>, Option<broadcast::Receiver<OutputEvent>>)> {
        let log_path = self.log_dir.join(pid.to_string()).join("stdout.log");
        let lines = self.read_log_file(&log_path).await?;
        let rx = self.subscribe_stdout(pid).await;
        Ok((lines, rx))
    }

    /// Follow stderr: get existing lines from log file and optionally a
    /// receiver for live updates.
    pub async fn follow_stderr(
        &self,
        pid: u32,
    ) -> Result<(Vec<String>, Option<broadcast::Receiver<OutputEvent>>)> {
        let log_path = self.log_dir.join(pid.to_string()).join("stderr.log");
        let lines = self.read_log_file(&log_path).await?;
        let rx = self.subscribe_stderr(pid).await;
        Ok((lines, rx))
    }

    /// Follow combined output: get existing lines from log files and optionally
    /// a receiver for live updates.
    pub async fn follow_output(
        &self,
        pid: u32,
    ) -> Result<(Vec<OutputEvent>, Option<broadcast::Receiver<OutputEvent>>)> {
        let events = self.get_output(pid).await?;
        let rx = self.subscribe_combined(pid).await;
        Ok((events, rx))
    }

    /// Read a log file and return lines.
    async fn read_log_file(&self, path: &std::path::Path) -> Result<Vec<String>> {
        match tokio::fs::read_to_string(path).await {
            Ok(content) => Ok(content.lines().map(|s| s.to_string()).collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(anyhow::anyhow!("Failed to read log file: {}", e)),
        }
    }

    /// Stop all running processes gracefully with a timeout.
    /// This is used during daemon shutdown.
    pub async fn stop_all_processes(&self, timeout_secs: u64) -> Result<()> {
        let processes = self.processes.lock().await;
        let running_pids: Vec<u32> = processes
            .iter()
            .filter(|(_, p)| matches!(p.status, ProcessStatus::Running))
            .map(|(pid, _)| *pid)
            .collect();
        drop(processes);

        if running_pids.is_empty() {
            debug!("No running processes to stop");
            return Ok(());
        }

        info!(count = running_pids.len(), "Stopping all running processes");

        // Send SIGTERM to all
        for pid in &running_pids {
            let _ = self.send_signal(*pid, 15).await; // SIGTERM
        }

        // Wait for processes to exit or timeout
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(timeout_secs);

        loop {
            let processes = self.processes.lock().await;
            let still_running = processes
                .iter()
                .filter(|(pid, p)| {
                    running_pids.contains(pid) && matches!(p.status, ProcessStatus::Running)
                })
                .count();
            drop(processes);

            if still_running == 0 {
                info!("All processes stopped gracefully");
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Timeout - send SIGKILL to remaining
        warn!("Some processes did not stop gracefully, sending SIGKILL");
        for pid in &running_pids {
            let _ = self.kill_process(*pid).await;
        }

        // Wait a bit for SIGKILL to take effect
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Ok(())
    }

    // Legacy single-process API for backward compatibility with gRPC server
    // These methods operate on the "primary" process (first started, or most
    // recent)

    /// Send a signal to the first running process (legacy API for gRPC).
    pub async fn send_signal_legacy(&self, signal: i32) -> Result<()> {
        let processes = self.processes.lock().await;

        // Find the first running process
        let running_id = processes
            .iter()
            .find(|(_, p)| matches!(p.status, ProcessStatus::Running))
            .map(|(id, _)| *id);

        let total_processes = processes.len();
        drop(processes);

        if let Some(id) = running_id {
            self.send_signal(id, signal).await
        } else if total_processes == 0 {
            anyhow::bail!("No processes spawned")
        } else {
            anyhow::bail!(
                "No running process found ({} processes exited)",
                total_processes
            )
        }
    }

    /// Stop the managed process gracefully with a timeout (legacy API).
    pub async fn stop_process(&self, timeout_secs: u64) -> Result<()> {
        self.stop_all_processes(timeout_secs).await
    }
}

/// Wait for a process to exit and update its status.
async fn wait_for_process_exit(
    mut child: Child,
    pid: u32,
    processes: Arc<Mutex<HashMap<u32, ManagedProcess>>>,
) {
    match child.wait().await {
        Ok(exit_status) => {
            let mut processes = processes.lock().await;
            if let Some(process) = processes.get_mut(&pid) {
                let ended_at = Utc::now().timestamp_millis();
                process.ended_at = Some(ended_at);
                process.stdin_tx = None; // Close stdin

                if let Some(code) = exit_status.code() {
                    info!(pid = pid, exit_code = code, "Process exited");
                    process.status = ProcessStatus::Exited(code);
                } else {
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::ExitStatusExt;
                        if let Some(signal) = exit_status.signal() {
                            info!(pid = pid, signal = signal, "Process killed by signal");
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
        Err(e) => {
            error!(pid = pid, error = %e, "Failed to wait for process");
            let mut processes = processes.lock().await;
            if let Some(process) = processes.get_mut(&pid) {
                process.status = ProcessStatus::Exited(-1);
                process.ended_at = Some(Utc::now().timestamp_millis());
                process.stdin_tx = None;
            }
        }
    }
}

/// Forward output from a stream to broadcast channels and log file.
async fn forward_output_stream<R: tokio::io::AsyncRead + Unpin>(
    reader: R,
    stream_tx: broadcast::Sender<OutputEvent>,
    combined_tx: broadcast::Sender<OutputEvent>,
    stream_name: &str,
    log_path: PathBuf,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .ok(); // Log file is optional

    let reader = BufReader::new(reader);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let timestamp = Utc::now().timestamp_millis();

        let event = OutputEvent {
            line: line.clone(),
            timestamp,
            stream: None,
        };

        let combined_event = OutputEvent {
            line: line.clone(),
            timestamp,
            stream: Some(stream_name.to_string()),
        };

        // Write to log file
        if let Some(ref mut f) = file {
            let _ = f.write_all(line.as_bytes()).await;
            let _ = f.write_all(b"\n").await;
        }

        // Send to stream-specific channel (ignore errors - no receivers is fine)
        let _ = stream_tx.send(event);

        // Send to combined channel
        let _ = combined_tx.send(combined_event);

        // Log via tracing (truncate long lines)
        let display_line = if line.len() > 200 {
            format!("{}...", &line[..200])
        } else {
            line
        };
        debug!(stream = stream_name, "{}", display_line);
    }

    Ok(())
}

/// Forward stdin data from a channel to a process.
async fn forward_stdin(mut stdin: ChildStdin, mut rx: mpsc::Receiver<Bytes>) {
    while let Some(data) = rx.recv().await {
        if let Err(e) = stdin.write_all(&data).await {
            debug!(error = %e, "stdin write failed");
            break;
        }
        if let Err(e) = stdin.flush().await {
            debug!(error = %e, "stdin flush failed");
            break;
        }
    }
    debug!("stdin forwarding ended");
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn create_start_request(command: &str, args: &[&str]) -> StartProcessRequest {
        StartProcessRequest {
            command: command.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            working_dir: None,
            stdin_mode: StdinMode::Closed,
            stdout_mode: OutputMode::Capture,
            stderr_mode: OutputMode::Capture,
        }
    }

    #[tokio::test]
    async fn test_start_simple_process() {
        let temp_dir = TempDir::new().unwrap();
        let manager = ProcessManager::new(temp_dir.path().to_path_buf());

        let req = create_start_request("echo", &["hello"]);
        let result = manager.start_process(req).await;

        assert!(result.is_ok());
        let info = result.unwrap();
        assert!(info.pid > 0);
        assert_eq!(info.status, ProcessStatusType::Running);

        // Wait for process to exit
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let info = manager.get_process(info.pid).await.unwrap();
        assert_eq!(info.status, ProcessStatusType::Exited);
        assert_eq!(info.exit_code, Some(0));
    }

    #[tokio::test]
    async fn test_multiple_processes() {
        let temp_dir = TempDir::new().unwrap();
        let manager = ProcessManager::new(temp_dir.path().to_path_buf());

        // Start multiple processes
        let req1 = create_start_request("echo", &["one"]);
        let req2 = create_start_request("echo", &["two"]);
        let req3 = create_start_request("echo", &["three"]);

        let info1 = manager.start_process(req1).await.unwrap();
        let info2 = manager.start_process(req2).await.unwrap();
        let info3 = manager.start_process(req3).await.unwrap();

        // All should have unique PIDs
        assert_ne!(info1.pid, info2.pid);
        assert_ne!(info2.pid, info3.pid);
        assert_ne!(info1.pid, info3.pid);

        // List should return all
        let processes = manager.list_processes().await;
        assert_eq!(processes.len(), 3);
    }

    #[tokio::test]
    async fn test_process_counts() {
        let temp_dir = TempDir::new().unwrap();
        let manager = ProcessManager::new(temp_dir.path().to_path_buf());

        let req = create_start_request("sleep", &["10"]);
        let info = manager.start_process(req).await.unwrap();

        let (running, total) = manager.get_counts().await;
        assert_eq!(running, 1);
        assert_eq!(total, 1);

        // Kill the process
        manager.kill_process(info.pid).await.unwrap();

        // Wait a bit
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let (running, total) = manager.get_counts().await;
        assert_eq!(running, 0);
        assert_eq!(total, 1);
    }
}
