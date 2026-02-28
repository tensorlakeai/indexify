//! Log streaming for Firecracker VM log files.
//!
//! Tails both the VMM log (`fc-{vm_id}.log`) and the guest serial console
//! log (`fc-{vm_id}-serial.log`), emitting each new line via `tracing` with
//! structured labels for observability.
//!
//! All file I/O is performed inside `spawn_blocking` to avoid blocking the
//! Tokio runtime.

use std::{
    collections::HashMap,
    io::{self, BufRead, Seek, SeekFrom},
    path::PathBuf,
};

use tokio_util::sync::CancellationToken;

/// Poll interval for checking new log content.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Number of consecutive open failures before logging a warning.
const OPEN_WARN_THRESHOLD: u32 = 20; // ~10 seconds at 500ms

/// Spawn a task that tails both Firecracker log files and emits each
/// new line via tracing with the provided labels.
///
/// Returns a `CancellationToken` that stops the task when cancelled.
pub fn spawn_log_streamer(
    vm_id: String,
    log_dir: PathBuf,
    labels: HashMap<String, String>,
) -> CancellationToken {
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    let vm_id_for_panic = vm_id.clone();
    tokio::spawn(async move {
        let result = tokio::spawn(run_log_streamer(vm_id, log_dir, labels, cancel_clone)).await;
        if let Err(e) = result {
            tracing::error!(
                vm_id = %vm_id_for_panic,
                error = %e,
                "Log streamer task panicked"
            );
        }
    });

    cancel
}

/// State for tailing a single log file.
struct TailState {
    reader: io::BufReader<std::fs::File>,
    /// Last known file position (used for truncation detection).
    position: u64,
}

async fn run_log_streamer(
    vm_id: String,
    log_dir: PathBuf,
    labels: HashMap<String, String>,
    cancel: CancellationToken,
) {
    let vmm_path = log_dir.join(format!("fc-{}.log", vm_id));
    let serial_path = log_dir.join(format!("fc-{}-serial.log", vm_id));

    let container_id = labels.get("container_id").cloned().unwrap_or_default();
    let sandbox_id = labels.get("sandbox_id").cloned().unwrap_or_default();
    let pool_id = labels.get("pool_id").cloned().unwrap_or_default();
    let namespace = labels.get("namespace").cloned().unwrap_or_default();
    let function = labels.get("fn").cloned().unwrap_or_default();

    let mut vmm_state: Option<TailState> = None;
    let mut serial_state: Option<TailState> = None;
    let mut vmm_open_failures: u32 = 0;
    let mut serial_open_failures: u32 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!(
                    vm_id = %vm_id,
                    "Log streamer cancelled, stopping"
                );
                return;
            }
            _ = tokio::time::sleep(POLL_INTERVAL) => {
                // --- VMM log ---
                let vmm_lines = poll_file(
                    &mut vmm_state,
                    &vmm_path,
                    &mut vmm_open_failures,
                ).await;
                for line in &vmm_lines {
                    tracing::info!(
                        target: "firecracker::vmm",
                        vm_id = %vm_id,
                        container_id = %container_id,
                        sandbox_id = %sandbox_id,
                        pool_id = %pool_id,
                        namespace = %namespace,
                        function = %function,
                        "firecracker vmm: {line}"
                    );
                }

                // --- Serial log ---
                let serial_lines = poll_file(
                    &mut serial_state,
                    &serial_path,
                    &mut serial_open_failures,
                ).await;
                for line in &serial_lines {
                    tracing::info!(
                        target: "firecracker::serial",
                        vm_id = %vm_id,
                        container_id = %container_id,
                        sandbox_id = %sandbox_id,
                        pool_id = %pool_id,
                        namespace = %namespace,
                        function = %function,
                        "firecracker serial: {line}"
                    );
                }
            }
        }
    }
}

/// Poll a single log file for new lines. Handles:
/// - Initial open + seek to end
/// - Truncation detection (re-opens from beginning)
/// - I/O error recovery (resets reader so it re-opens next poll)
/// - Warning after repeated open failures
///
/// All blocking I/O runs inside `spawn_blocking`.
async fn poll_file(
    state: &mut Option<TailState>,
    path: &PathBuf,
    open_failures: &mut u32,
) -> Vec<String> {
    // Take ownership of state for the blocking closure. We'll put it back after.
    let taken_state = state.take();

    let path_for_blocking = path.clone();
    let failure_count = *open_failures;

    let result = tokio::task::spawn_blocking(move || {
        poll_file_blocking(taken_state, &path_for_blocking, failure_count)
    })
    .await;

    match result {
        Ok((new_state, lines, opened_fresh)) => {
            if opened_fresh {
                // Reset failure counter on successful open
                *open_failures = 0;
            } else if new_state.is_none() {
                // File still not available
                *open_failures += 1;
                if *open_failures == OPEN_WARN_THRESHOLD {
                    tracing::warn!(
                        path = %path.display(),
                        "Log file not available after {} attempts, will keep retrying",
                        OPEN_WARN_THRESHOLD,
                    );
                }
            }
            *state = new_state;
            lines
        }
        Err(e) => {
            // spawn_blocking panicked
            tracing::error!(error = %e, "Log file poll task panicked");
            *state = None;
            Vec::new()
        }
    }
}

/// Blocking implementation of a single poll cycle for one file.
/// Returns (updated state, new lines, whether a fresh open happened).
fn poll_file_blocking(
    state: Option<TailState>,
    path: &PathBuf,
    _failure_count: u32,
) -> (Option<TailState>, Vec<String>, bool) {
    match state {
        Some(mut ts) => {
            // Check for truncation: if the file is now smaller than our position,
            // re-open from the beginning.
            if let Ok(meta) = std::fs::metadata(path) {
                if meta.len() < ts.position {
                    tracing::info!(
                        path = %path.display(),
                        old_position = ts.position,
                        new_size = meta.len(),
                        "Log file was truncated, re-opening from beginning"
                    );
                    match open_at_position(path, 0) {
                        Some(new_ts) => {
                            let mut ts = new_ts;
                            let lines = read_new_lines(&mut ts);
                            return (Some(ts), lines, true);
                        }
                        None => return (None, Vec::new(), false),
                    }
                }
            }

            let lines = read_new_lines(&mut ts);
            (Some(ts), lines, false)
        }
        None => {
            // Try to open the file and seek to end
            match open_at_position(path, -1) {
                Some(ts) => (Some(ts), Vec::new(), true),
                None => (None, Vec::new(), false),
            }
        }
    }
}

/// Open a file at a given position. If `offset` is -1, seeks to end.
/// Returns `None` if the file doesn't exist or can't be opened.
fn open_at_position(path: &PathBuf, offset: i64) -> Option<TailState> {
    let mut file = std::fs::File::open(path).ok()?;
    let position = if offset < 0 {
        file.seek(SeekFrom::End(0)).ok()?
    } else {
        file.seek(SeekFrom::Start(offset as u64)).ok()?
    };
    Some(TailState {
        reader: io::BufReader::new(file),
        position,
    })
}

/// Read all new complete lines from the reader, returning them.
/// Updates the TailState position. On I/O error, logs a warning.
fn read_new_lines(ts: &mut TailState) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        match ts.reader.read_line(&mut line) {
            Ok(0) => break, // No more data
            Ok(n) => {
                ts.position += n as u64;
                let trimmed = line.trim_end();
                if !trimmed.is_empty() {
                    lines.push(trimmed.to_string());
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "I/O error reading log file, will re-open on next poll"
                );
                break;
            }
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_at_position_nonexistent() {
        let path = PathBuf::from("/tmp/nonexistent-fc-log-test-file.log");
        assert!(open_at_position(&path, -1).is_none());
    }

    #[test]
    fn test_read_new_lines_and_seek_to_end() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");

        // Write initial content
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "line1").unwrap();
            writeln!(f, "line2").unwrap();
        }

        // Open and seek to end — should not read existing lines
        let mut ts = open_at_position(&path, -1).unwrap();
        let lines = read_new_lines(&mut ts);
        assert!(lines.is_empty(), "Should not read pre-existing lines");

        // Append new content
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "line3").unwrap();
            writeln!(f, "line4").unwrap();
        }

        // Now should read the new lines
        let lines = read_new_lines(&mut ts);
        assert_eq!(lines, vec!["line3", "line4"]);
    }

    #[test]
    fn test_truncation_detection() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");

        // Write initial content and open at end
        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "original line 1").unwrap();
            writeln!(f, "original line 2").unwrap();
        }
        let ts = open_at_position(&path, -1).unwrap();
        assert!(ts.position > 0);

        // Truncate and write shorter content
        {
            let mut f = std::fs::File::create(&path).unwrap(); // truncates
            writeln!(f, "new").unwrap();
        }

        // File is now smaller than our position — poll_file_blocking should detect
        let (new_state, lines, opened_fresh) = poll_file_blocking(Some(ts), &path, 0);
        assert!(new_state.is_some());
        assert!(opened_fresh);
        assert_eq!(lines, vec!["new"]);
    }

    #[test]
    fn test_line_buffer_reuse() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.log");

        {
            let mut f = std::fs::File::create(&path).unwrap();
            for i in 0..100 {
                writeln!(f, "line {}", i).unwrap();
            }
        }

        let mut ts = open_at_position(&path, 0).unwrap();
        let lines = read_new_lines(&mut ts);
        assert_eq!(lines.len(), 100);
        assert_eq!(lines[0], "line 0");
        assert_eq!(lines[99], "line 99");
    }

    #[tokio::test]
    async fn test_spawn_log_streamer_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let labels = HashMap::new();

        let cancel = spawn_log_streamer("test-vm".to_string(), dir.path().to_path_buf(), labels);

        // Let it run briefly then cancel
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel.cancel();

        // Give the task time to stop
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // If we get here without hanging, the test passes
    }
}
