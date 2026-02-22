//! Per-VM state tracking and metadata serialization for recovery.
//!
//! Each running VM has a `VmState` stored in memory and a corresponding
//! JSON metadata file on disk. On driver startup, metadata files are scanned
//! to recover state for VMs that survived a dataplane restart.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tokio::process::Child;

/// Process handle for a Firecracker VM — either owned (we spawned it) or
/// recovered (found a running process after restart).
pub enum VmProcess {
    /// We spawned this Firecracker process and own the `Child` handle.
    Owned(Child),
    /// We found this process running after a restart. We only know the PID.
    Recovered { pid: u32 },
}

impl VmProcess {
    /// Get the PID of the Firecracker process.
    pub fn pid(&self) -> Option<u32> {
        match self {
            VmProcess::Owned(child) => child.id(),
            VmProcess::Recovered { pid } => Some(*pid),
        }
    }

    /// Check if the process is still running.
    pub fn is_alive(&mut self) -> bool {
        match self {
            VmProcess::Owned(child) => {
                // try_wait returns Ok(None) if the process is still running
                matches!(child.try_wait(), Ok(None))
            }
            VmProcess::Recovered { pid } => {
                // Check via /proc/{pid} — more reliable than signals
                // since we may not own the process.
                Path::new(&format!("/proc/{}", pid)).exists()
            }
        }
    }

    /// Try to get the exit status without blocking.
    pub fn try_exit_status(&mut self) -> Option<std::process::ExitStatus> {
        match self {
            VmProcess::Owned(child) => child.try_wait().ok().flatten(),
            VmProcess::Recovered { .. } => {
                // Can't get exit status for recovered processes
                None
            }
        }
    }

    /// Kill the process.
    pub fn kill(&mut self) -> Result<()> {
        match self {
            VmProcess::Owned(child) => {
                // Use start_kill which doesn't require &mut self to be async
                child
                    .start_kill()
                    .context("Failed to kill owned Firecracker process")?;
                Ok(())
            }
            VmProcess::Recovered { pid } => {
                nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(*pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                )
                .context("Failed to kill recovered Firecracker process")?;
                Ok(())
            }
        }
    }

    /// Send a signal to the process.
    pub fn send_signal(&self, signal: i32) -> Result<()> {
        let pid = self
            .pid()
            .context("Cannot send signal: process has no PID")?;
        let sig = nix::sys::signal::Signal::try_from(signal)
            .context("Invalid signal number")?;
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), sig)
            .with_context(|| format!("Failed to send signal {} to PID {}", signal, pid))?;
        Ok(())
    }
}

/// In-memory state for a running VM.
pub struct VmState {
    /// The Firecracker VMM process.
    pub process: VmProcess,
    /// VM metadata (also persisted to disk).
    pub metadata: VmMetadata,
    /// Cancellation token for the log streaming task.
    /// None for VMs that haven't started streaming yet.
    pub log_cancel: Option<CancellationToken>,
}

/// Serializable metadata for a VM, persisted to `{state_dir}/fc-{vm_id}.json`
/// for recovery after dataplane restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmMetadata {
    /// The handle ID returned to the caller (e.g., "fc-abc123").
    pub handle_id: String,
    /// The unique VM identifier.
    pub vm_id: String,
    /// PID of the Firecracker process.
    pub pid: u32,
    /// Thin device ID used for this VM's rootfs.
    pub thin_id: u32,
    /// Name of the network namespace.
    pub netns_name: String,
    /// Guest IP address.
    pub guest_ip: String,
    /// Daemon gRPC address (ip:port).
    pub daemon_addr: String,
    /// Daemon HTTP address (ip:port).
    pub http_addr: String,
    /// Path to the Firecracker API socket.
    pub socket_path: String,
    /// Labels from the application layer for log attribution.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl VmMetadata {
    /// Write this metadata to a JSON file for recovery.
    pub fn save(&self, state_dir: &Path) -> Result<()> {
        let path = self.metadata_path(state_dir);
        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize VM metadata")?;
        std::fs::write(&path, json)
            .with_context(|| format!("Failed to write VM metadata to {}", path.display()))?;
        Ok(())
    }

    /// Load metadata from a JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read VM metadata from {}", path.display()))?;
        let metadata: Self = serde_json::from_str(&json)
            .with_context(|| format!("Failed to parse VM metadata from {}", path.display()))?;
        Ok(metadata)
    }

    /// Remove the metadata file.
    pub fn remove(&self, state_dir: &Path) {
        let path = self.metadata_path(state_dir);
        let _ = std::fs::remove_file(path);
    }

    /// Get the path to the metadata file.
    fn metadata_path(&self, state_dir: &Path) -> PathBuf {
        state_dir.join(format!("fc-{}.json", self.vm_id))
    }
}

/// Scan a state directory for VM metadata files.
pub fn scan_metadata_files(state_dir: &Path) -> Result<Vec<VmMetadata>> {
    let mut results = Vec::new();

    if !state_dir.exists() {
        return Ok(results);
    }

    let entries = std::fs::read_dir(state_dir)
        .with_context(|| format!("Failed to read state directory {}", state_dir.display()))?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("fc-") && name.ends_with(".json") {
                match VmMetadata::load(&path) {
                    Ok(metadata) => results.push(metadata),
                    Err(e) => {
                        tracing::warn!(
                            path = %path.display(),
                            error = ?e,
                            "Failed to load VM metadata file, skipping"
                        );
                    }
                }
            }
        }
    }

    Ok(results)
}

/// Metadata for the origin thin volume (the base rootfs image).
///
/// Tracks which base image was used to populate the origin so we can
/// detect when the image changes and rebuild the origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OriginMetadata {
    /// SHA-256 hex digest of the base rootfs image used to populate the origin.
    pub base_image_hash: String,
    /// Thin device ID of the origin volume (always 0).
    pub thin_id: u32,
}

impl OriginMetadata {
    /// Write this metadata to `{state_dir}/fc-origin.json`.
    pub fn save(&self, state_dir: &Path) -> Result<()> {
        let path = state_dir.join("fc-origin.json");
        let json =
            serde_json::to_string_pretty(self).context("Failed to serialize origin metadata")?;
        std::fs::write(&path, json)
            .with_context(|| format!("Failed to write origin metadata to {}", path.display()))?;
        Ok(())
    }

    /// Load origin metadata from `{state_dir}/fc-origin.json`.
    pub fn load(state_dir: &Path) -> Result<Option<Self>> {
        let path = state_dir.join("fc-origin.json");
        if !path.exists() {
            return Ok(None);
        }
        let json = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read origin metadata from {}", path.display()))?;
        let metadata: Self = serde_json::from_str(&json)
            .with_context(|| format!("Failed to parse origin metadata from {}", path.display()))?;
        Ok(Some(metadata))
    }

    /// Remove the origin metadata file.
    pub fn remove(state_dir: &Path) {
        let path = state_dir.join("fc-origin.json");
        let _ = std::fs::remove_file(path);
    }
}

/// Compute the SHA-256 hex digest of a file.
pub fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open {} for hashing", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("Failed to read {} during hashing", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Check if a PID belongs to a Firecracker process by inspecting
/// `/proc/{pid}/comm`.
pub fn is_firecracker_process(pid: u32) -> bool {
    let comm_path = format!("/proc/{}/comm", pid);
    std::fs::read_to_string(comm_path)
        .map(|comm| comm.trim() == "firecracker")
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> VmMetadata {
        VmMetadata {
            handle_id: "fc-test-vm-1".to_string(),
            vm_id: "test-vm-1".to_string(),
            pid: 12345,
            thin_id: 1,
            netns_name: "indexify-vm-test-vm-1".to_string(),
            guest_ip: "192.168.30.2".to_string(),
            daemon_addr: "192.168.30.2:9500".to_string(),
            http_addr: "192.168.30.2:9501".to_string(),
            socket_path: "/tmp/fc-test-vm-1.sock".to_string(),
            labels: HashMap::new(),
        }
    }

    #[test]
    fn test_metadata_serialize_roundtrip() {
        let metadata = sample_metadata();
        let json = serde_json::to_string(&metadata).unwrap();
        let loaded: VmMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.handle_id, metadata.handle_id);
        assert_eq!(loaded.vm_id, metadata.vm_id);
        assert_eq!(loaded.pid, metadata.pid);
        assert_eq!(loaded.thin_id, metadata.thin_id);
        assert_eq!(loaded.netns_name, metadata.netns_name);
        assert_eq!(loaded.guest_ip, metadata.guest_ip);
        assert_eq!(loaded.daemon_addr, metadata.daemon_addr);
        assert_eq!(loaded.http_addr, metadata.http_addr);
        assert_eq!(loaded.socket_path, metadata.socket_path);
    }

    #[test]
    fn test_metadata_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = sample_metadata();

        metadata.save(dir.path()).unwrap();

        let expected_path = dir.path().join("fc-test-vm-1.json");
        assert!(expected_path.exists(), "Metadata file should be created");

        let loaded = VmMetadata::load(&expected_path).unwrap();
        assert_eq!(loaded.handle_id, "fc-test-vm-1");
        assert_eq!(loaded.vm_id, "test-vm-1");
        assert_eq!(loaded.pid, 12345);
        assert_eq!(loaded.thin_id, 1);
    }

    #[test]
    fn test_metadata_remove() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = sample_metadata();

        metadata.save(dir.path()).unwrap();
        let path = dir.path().join("fc-test-vm-1.json");
        assert!(path.exists());

        metadata.remove(dir.path());
        assert!(!path.exists(), "Metadata file should be removed");
    }

    #[test]
    fn test_scan_metadata_files_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let results = scan_metadata_files(dir.path()).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_metadata_files_nonexistent_dir() {
        let results = scan_metadata_files(Path::new("/nonexistent/path")).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_scan_metadata_files_finds_fc_json() {
        let dir = tempfile::tempdir().unwrap();

        // Save two metadata files
        let mut m1 = sample_metadata();
        m1.vm_id = "vm-1".to_string();
        m1.handle_id = "fc-vm-1".to_string();
        m1.thin_id = 1;
        m1.save(dir.path()).unwrap();

        let mut m2 = sample_metadata();
        m2.vm_id = "vm-2".to_string();
        m2.handle_id = "fc-vm-2".to_string();
        m2.thin_id = 2;
        m2.save(dir.path()).unwrap();

        // Write a non-matching file (should be ignored)
        std::fs::write(dir.path().join("other.json"), "{}").unwrap();

        let results = scan_metadata_files(dir.path()).unwrap();
        assert_eq!(results.len(), 2, "Should find both fc-*.json files");

        let ids: std::collections::HashSet<String> =
            results.iter().map(|m| m.vm_id.clone()).collect();
        assert!(ids.contains("vm-1"));
        assert!(ids.contains("vm-2"));
    }

    #[test]
    fn test_scan_metadata_files_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();

        // Save one valid file
        sample_metadata().save(dir.path()).unwrap();

        // Write a corrupt fc-*.json file
        std::fs::write(dir.path().join("fc-corrupt.json"), "not valid json").unwrap();

        let results = scan_metadata_files(dir.path()).unwrap();
        assert_eq!(results.len(), 1, "Should skip corrupt file");
        assert_eq!(results[0].vm_id, "test-vm-1");
    }

    #[test]
    fn test_is_firecracker_process_nonexistent_pid() {
        // PID 99999999 almost certainly doesn't exist
        assert!(!is_firecracker_process(99999999));
    }

    #[test]
    fn test_vm_process_recovered_pid() {
        let process = VmProcess::Recovered { pid: 42 };
        assert_eq!(process.pid(), Some(42));
    }

    #[test]
    fn test_vm_process_recovered_not_alive() {
        // PID 99999999 doesn't exist, so should not be alive
        let mut process = VmProcess::Recovered { pid: 99999999 };
        assert!(!process.is_alive());
    }

    #[test]
    fn test_origin_metadata_serialize_roundtrip() {
        let metadata = OriginMetadata {
            base_image_hash: "abc123def456".to_string(),
            thin_id: 0,
        };
        let json = serde_json::to_string(&metadata).unwrap();
        let loaded: OriginMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.base_image_hash, "abc123def456");
        assert_eq!(loaded.thin_id, 0);
    }

    #[test]
    fn test_origin_metadata_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = OriginMetadata {
            base_image_hash: "deadbeef".to_string(),
            thin_id: 0,
        };

        metadata.save(dir.path()).unwrap();

        let expected_path = dir.path().join("fc-origin.json");
        assert!(expected_path.exists(), "Origin metadata file should be created");

        let loaded = OriginMetadata::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.base_image_hash, "deadbeef");
        assert_eq!(loaded.thin_id, 0);
    }

    #[test]
    fn test_origin_metadata_load_missing() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = OriginMetadata::load(dir.path()).unwrap();
        assert!(loaded.is_none(), "Should return None for missing file");
    }

    #[test]
    fn test_origin_metadata_remove() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = OriginMetadata {
            base_image_hash: "test".to_string(),
            thin_id: 0,
        };
        metadata.save(dir.path()).unwrap();
        let path = dir.path().join("fc-origin.json");
        assert!(path.exists());

        OriginMetadata::remove(dir.path());
        assert!(!path.exists(), "Origin metadata file should be removed");
    }

    #[test]
    fn test_sha256_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.bin");
        std::fs::write(&file_path, b"hello world").unwrap();

        let hash = sha256_file(&file_path).unwrap();
        // SHA-256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_sha256_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("empty.bin");
        std::fs::write(&file_path, b"").unwrap();

        let hash = sha256_file(&file_path).unwrap();
        // SHA-256 of empty input
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
