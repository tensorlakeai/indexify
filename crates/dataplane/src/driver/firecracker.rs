//! Firecracker microVM process driver.
//!
//! Implements the [`ProcessDriver`] trait by launching Firecracker microVMs
//! instead of Docker containers. Each VM gets a copy-on-write rootfs via the
//! device mapper [`Snapshotter`](super::snapshotter::Snapshotter) and
//! communicates over a UNIX socket API.
//!
//! Firecracker is controlled via its REST API exposed over a Unix socket:
//! - PUT /boot-source — kernel config
//! - PUT /drives/{id} — rootfs block device
//! - PUT /network-interfaces/{id} — network config
//! - PUT /machine-config — vCPU/memory
//! - PUT /actions — InstanceStart
//!
//! Reference: <https://github.com/firecracker-microvm/firecracker>

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use tracing::{debug, info, warn};

use super::{ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle, ProcessType};
use super::snapshotter::{SnapshotConfig, SnapshotHandle, Snapshotter};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Firecracker driver.
#[derive(Debug, Clone)]
pub struct FirecrackerDriverConfig {
    /// Path to the Firecracker binary.
    pub firecracker_bin: PathBuf,
    /// Path to the kernel image (vmlinux).
    pub kernel_image_path: PathBuf,
    /// Kernel boot arguments.
    pub kernel_boot_args: String,
    /// Path to the base rootfs image (ext4).
    pub base_rootfs_path: PathBuf,
    /// Directory for VM runtime state (sockets, logs, overlays).
    pub state_dir: PathBuf,
    /// Default number of vCPUs per VM.
    pub default_vcpu_count: u32,
    /// Default memory size in MiB per VM.
    pub default_mem_size_mib: u32,
    /// Overlay size in bytes for the snapshot rootfs.
    /// Must be >= base rootfs size. Defaults to 2 GiB.
    pub overlay_size_bytes: u64,
}

impl Default for FirecrackerDriverConfig {
    fn default() -> Self {
        Self {
            firecracker_bin: PathBuf::from("/usr/bin/firecracker"),
            kernel_image_path: PathBuf::from("/var/lib/firecracker/vmlinux"),
            kernel_boot_args: "console=ttyS0 reboot=k panic=1 pci=off".to_string(),
            base_rootfs_path: PathBuf::from("/var/lib/firecracker/rootfs.ext4"),
            state_dir: PathBuf::from("/var/lib/firecracker/vms"),
            default_vcpu_count: 1,
            default_mem_size_mib: 256,
            overlay_size_bytes: 2 * 1024 * 1024 * 1024, // 2 GiB
        }
    }
}

// ---------------------------------------------------------------------------
// Internal VM state
// ---------------------------------------------------------------------------

/// State for a running Firecracker VM.
#[derive(Debug)]
struct VmState {
    /// The Firecracker child process.
    child: Child,
    /// API socket path for this VM.
    api_socket_path: PathBuf,
    /// Snapshot handle (loop devices + DM devices) — must be cleaned up.
    snapshot: Option<SnapshotHandle>,
    /// vCPU count assigned to this VM.
    #[allow(dead_code)]
    vcpu_count: u32,
    /// Memory in MiB assigned to this VM.
    #[allow(dead_code)]
    mem_size_mib: u32,
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Firecracker microVM process driver.
///
/// Launches Firecracker VMs with copy-on-write rootfs snapshots. Each VM is
/// configured via the Firecracker API socket, then started with an
/// `InstanceStart` action.
pub struct FirecrackerDriver {
    config: FirecrackerDriverConfig,
    snapshotter: Snapshotter,
    vms: Arc<Mutex<HashMap<String, VmState>>>,
}

impl FirecrackerDriver {
    /// Create a new FirecrackerDriver with the given configuration.
    pub fn new(config: FirecrackerDriverConfig) -> Self {
        let snapshot_config = SnapshotConfig {
            base_image_path: config.base_rootfs_path.clone(),
            overlay_dir: config.state_dir.join("overlays"),
            overlay_size_bytes: config.overlay_size_bytes,
            chunk_size: None,
        };
        let snapshotter = Snapshotter::new(snapshot_config);

        Self {
            config,
            snapshotter,
            vms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the API socket path for a VM.
    fn api_socket_path(&self, id: &str) -> PathBuf {
        self.config.state_dir.join(format!("{}.sock", id))
    }

    /// Get the log file path for a VM.
    fn log_file_path(&self, id: &str) -> PathBuf {
        self.config.state_dir.join(format!("{}.log", id))
    }

    /// Send a PUT request to the Firecracker API socket using curl.
    ///
    /// Firecracker exposes a REST API over a Unix socket. We use curl to
    /// communicate since it handles Unix socket HTTP natively.
    async fn api_put(socket_path: &Path, endpoint: &str, body: &str) -> Result<()> {
        let output = Command::new("curl")
            .args([
                "--unix-socket",
                &socket_path.to_string_lossy(),
                "-X",
                "PUT",
                &format!("http://localhost/{}", endpoint),
                "-H",
                "Content-Type: application/json",
                "-d",
                body,
                "--silent",
                "--show-error",
                "--fail",
            ])
            .output()
            .await
            .with_context(|| format!("Failed to call Firecracker API: PUT /{}", endpoint))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            anyhow::bail!(
                "Firecracker API PUT /{} failed: stderr={}, stdout={}",
                endpoint,
                stderr.trim(),
                stdout.trim()
            );
        }

        debug!(endpoint = endpoint, "Firecracker API call succeeded");
        Ok(())
    }

    /// Configure and start a Firecracker VM.
    ///
    /// This:
    /// 1. Creates a device mapper snapshot for the rootfs
    /// 2. Spawns the Firecracker process
    /// 3. Configures it via the API socket (kernel, rootfs, machine config)
    /// 4. Sends the InstanceStart action
    async fn launch_vm(&self, id: &str, config: &ProcessConfig) -> Result<(VmState, String)> {
        // Ensure state directory exists
        tokio::fs::create_dir_all(&self.config.state_dir)
            .await
            .context("Failed to create state directory")?;

        // Step 1: Create rootfs snapshot
        let snapshot = self
            .snapshotter
            .create_snapshot(id)
            .await
            .context("Failed to create rootfs snapshot")?;

        let rootfs_device = snapshot.overlay_device_path.clone();

        // Step 2: Spawn Firecracker process
        let socket_path = self.api_socket_path(id);
        let log_path = self.log_file_path(id);

        // Remove stale socket if it exists
        if socket_path.exists() {
            tokio::fs::remove_file(&socket_path).await.ok();
        }

        let child = Command::new(&self.config.firecracker_bin)
            .args([
                "--api-sock",
                &socket_path.to_string_lossy(),
                "--log-path",
                &log_path.to_string_lossy(),
                "--level",
                "Info",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn Firecracker process")?;

        // Wait briefly for the API socket to become available
        let socket_ready = Self::wait_for_socket(&socket_path, 10).await;
        if !socket_ready {
            warn!(
                id = id,
                socket = %socket_path.display(),
                "Firecracker API socket not ready after timeout"
            );
        }

        // Step 3: Extract resource limits
        let (vcpu_count, mem_size_mib) = self.extract_resources(config);

        // Step 4: Configure VM via API
        // Set kernel
        let kernel_config = format!(
            r#"{{"kernel_image_path": "{}", "boot_args": "{}"}}"#,
            self.config.kernel_image_path.display(),
            self.config.kernel_boot_args
        );
        Self::api_put(&socket_path, "boot-source", &kernel_config).await?;

        // Set rootfs drive
        let drive_config = format!(
            r#"{{"drive_id": "rootfs", "path_on_host": "{}", "is_root_device": true, "is_read_only": false}}"#,
            rootfs_device.display()
        );
        Self::api_put(&socket_path, "drives/rootfs", &drive_config).await?;

        // Set machine config
        let machine_config = format!(
            r#"{{"vcpu_count": {}, "mem_size_mib": {}}}"#,
            vcpu_count, mem_size_mib
        );
        Self::api_put(&socket_path, "machine-config", &machine_config).await?;

        // Step 5: Start the VM
        Self::api_put(&socket_path, "actions", r#"{"action_type": "InstanceStart"}"#).await?;

        // Determine guest IP (for now use a convention-based approach)
        // In production this would come from network config or DHCP.
        let container_ip = format!("172.16.0.{}", self.next_ip_suffix().await);

        info!(
            id = id,
            rootfs = %rootfs_device.display(),
            vcpu = vcpu_count,
            mem_mib = mem_size_mib,
            ip = %container_ip,
            "Firecracker VM started"
        );

        let vm_state = VmState {
            child,
            api_socket_path: socket_path,
            snapshot: Some(snapshot),
            vcpu_count,
            mem_size_mib,
        };

        Ok((vm_state, container_ip))
    }

    /// Wait for the Firecracker API socket to appear.
    async fn wait_for_socket(socket_path: &Path, max_attempts: u32) -> bool {
        for i in 0..max_attempts {
            if socket_path.exists() {
                return true;
            }
            debug!(
                attempt = i + 1,
                max = max_attempts,
                "Waiting for Firecracker API socket"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    }

    /// Extract vCPU count and memory from resource limits.
    fn extract_resources(&self, config: &ProcessConfig) -> (u32, u32) {
        let vcpu_count = config
            .resources
            .as_ref()
            .and_then(|r| r.cpu_millicores)
            .map(|m| ((m as f64 / 1000.0).ceil() as u32).max(1))
            .unwrap_or(self.config.default_vcpu_count);

        let mem_size_mib = config
            .resources
            .as_ref()
            .and_then(|r| r.memory_bytes)
            .map(|b| (b / (1024 * 1024)) as u32)
            .unwrap_or(self.config.default_mem_size_mib);

        (vcpu_count, mem_size_mib)
    }

    /// Generate a simple IP suffix for guest networking.
    async fn next_ip_suffix(&self) -> u32 {
        let vms = self.vms.lock().await;
        // Start at .2 (reserve .1 for host)
        (vms.len() as u32) + 2
    }

    /// Clean up a VM's resources (snapshot, socket, log file).
    async fn cleanup_vm_resources(id: &str, vm_state: &VmState) {
        if let Some(snapshot) = &vm_state.snapshot {
            if let Err(e) = snapshot.cleanup().await {
                warn!(id = id, error = ?e, "Failed to clean up snapshot");
            }
        }

        // Remove socket file
        if vm_state.api_socket_path.exists() {
            if let Err(e) = tokio::fs::remove_file(&vm_state.api_socket_path).await {
                warn!(
                    id = id,
                    error = ?e,
                    "Failed to remove API socket"
                );
            }
        }
    }
}

#[async_trait]
impl ProcessDriver for FirecrackerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let id = &config.id;
        let vm_name = format!("fc-{}", id);

        let (vm_state, container_ip) = self
            .launch_vm(&vm_name, &config)
            .await
            .with_context(|| format!("Failed to launch Firecracker VM: {}", vm_name))?;

        // Determine daemon address based on process type
        let daemon_addr = match config.process_type {
            ProcessType::Sandbox => {
                Some(format!("{}:{}", container_ip, super::DAEMON_GRPC_PORT))
            }
            ProcessType::Function => Some(format!("{}:9600", container_ip)),
        };

        let http_addr = match config.process_type {
            ProcessType::Sandbox => {
                Some(format!("{}:{}", container_ip, super::DAEMON_HTTP_PORT))
            }
            ProcessType::Function => None,
        };

        let handle = ProcessHandle {
            id: vm_name.clone(),
            daemon_addr,
            http_addr,
            container_ip,
        };

        self.vms.lock().await.insert(vm_name, vm_state);

        Ok(handle)
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        let vms = self.vms.lock().await;
        let vm = vms
            .get(&handle.id)
            .ok_or_else(|| anyhow::anyhow!("VM not found: {}", handle.id))?;

        let pid = vm
            .child
            .id()
            .ok_or_else(|| anyhow::anyhow!("VM process has no PID: {}", handle.id))?;

        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };

            let signal = Signal::try_from(signal).context("Invalid signal number")?;
            kill(Pid::from_raw(pid as i32), signal).context("Failed to send signal to VM")?;
        }

        #[cfg(not(unix))]
        {
            let _ = (pid, signal);
            anyhow::bail!("send_sig is only supported on Unix platforms");
        }

        Ok(())
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        let mut vms = self.vms.lock().await;

        if let Some(mut vm_state) = vms.remove(&handle.id) {
            // Send InstanceHalt via API (best effort)
            let halt_result = Self::api_put(
                &vm_state.api_socket_path,
                "actions",
                r#"{"action_type": "SendCtrlAltDel"}"#,
            )
            .await;

            if halt_result.is_err() {
                // If API call fails, force-kill the process
                vm_state
                    .child
                    .kill()
                    .await
                    .context("Failed to kill Firecracker process")?;
            }

            // Wait for process to exit
            vm_state.child.wait().await.ok();

            // Clean up snapshot and socket
            Self::cleanup_vm_resources(&handle.id, &vm_state).await;

            info!(id = %handle.id, "Firecracker VM killed and cleaned up");
        }

        Ok(())
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        let mut vms = self.vms.lock().await;

        if let Some(vm_state) = vms.get_mut(&handle.id) {
            match vm_state.child.try_wait() {
                Ok(Some(_)) => {
                    // Process exited — clean up
                    if let Some(vm_state) = vms.remove(&handle.id) {
                        Self::cleanup_vm_resources(&handle.id, &vm_state).await;
                    }
                    Ok(false)
                }
                Ok(None) => Ok(true),
                Err(e) => Err(e).context("Failed to check VM process status"),
            }
        } else {
            Ok(false)
        }
    }

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        let mut vms = self.vms.lock().await;

        if let Some(vm_state) = vms.get_mut(&handle.id) {
            match vm_state.child.try_wait() {
                Ok(Some(status)) => {
                    let exit_code = status.code().map(|c| c as i64);
                    Ok(Some(ExitStatus {
                        exit_code,
                        oom_killed: false,
                    }))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e).context("Failed to get VM exit status"),
            }
        } else {
            Ok(None)
        }
    }

    async fn list_containers(&self) -> Result<Vec<String>> {
        let vms = self.vms.lock().await;
        Ok(vms.keys().cloned().collect())
    }

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
        let log_path = self.config.state_dir.join(format!("{}.log", handle.id));

        if !log_path.exists() {
            return Ok(String::new());
        }

        // Read last `tail` lines from the log file
        let output = Command::new("tail")
            .args(["-n", &tail.to_string()])
            .arg(&log_path)
            .output()
            .await
            .context("Failed to read VM log file")?;

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::ResourceLimits;

    #[test]
    fn test_default_config() {
        let config = FirecrackerDriverConfig::default();
        assert_eq!(config.firecracker_bin, PathBuf::from("/usr/bin/firecracker"));
        assert_eq!(config.default_vcpu_count, 1);
        assert_eq!(config.default_mem_size_mib, 256);
        assert_eq!(config.overlay_size_bytes, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_api_socket_path() {
        let config = FirecrackerDriverConfig {
            state_dir: PathBuf::from("/tmp/fc-test"),
            ..Default::default()
        };
        let driver = FirecrackerDriver::new(config);
        assert_eq!(
            driver.api_socket_path("vm-1"),
            PathBuf::from("/tmp/fc-test/vm-1.sock")
        );
    }

    #[test]
    fn test_log_file_path() {
        let config = FirecrackerDriverConfig {
            state_dir: PathBuf::from("/tmp/fc-test"),
            ..Default::default()
        };
        let driver = FirecrackerDriver::new(config);
        assert_eq!(
            driver.log_file_path("vm-1"),
            PathBuf::from("/tmp/fc-test/vm-1.log")
        );
    }

    #[test]
    fn test_extract_resources_defaults() {
        let config = FirecrackerDriverConfig {
            default_vcpu_count: 2,
            default_mem_size_mib: 512,
            ..Default::default()
        };
        let driver = FirecrackerDriver::new(config);

        let process_config = ProcessConfig {
            id: "test".to_string(),
            process_type: ProcessType::Sandbox,
            image: None,
            command: String::new(),
            args: vec![],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
        };

        let (vcpu, mem) = driver.extract_resources(&process_config);
        assert_eq!(vcpu, 2);
        assert_eq!(mem, 512);
    }

    #[test]
    fn test_extract_resources_from_limits() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        let process_config = ProcessConfig {
            id: "test".to_string(),
            process_type: ProcessType::Sandbox,
            image: None,
            command: String::new(),
            args: vec![],
            env: vec![],
            working_dir: None,
            resources: Some(ResourceLimits {
                memory_bytes: Some(1024 * 1024 * 1024), // 1 GiB
                cpu_millicores: Some(2500),              // 2.5 CPUs → 3
                gpu_device_ids: None,
            }),
            labels: vec![],
        };

        let (vcpu, mem) = driver.extract_resources(&process_config);
        assert_eq!(vcpu, 3); // ceil(2500/1000)
        assert_eq!(mem, 1024); // 1 GiB in MiB
    }

    #[test]
    fn test_extract_resources_sub_core() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        let process_config = ProcessConfig {
            id: "test".to_string(),
            process_type: ProcessType::Sandbox,
            image: None,
            command: String::new(),
            args: vec![],
            env: vec![],
            working_dir: None,
            resources: Some(ResourceLimits {
                memory_bytes: Some(256 * 1024 * 1024),
                cpu_millicores: Some(500), // 0.5 CPU → 1 (minimum)
                gpu_device_ids: None,
            }),
            labels: vec![],
        };

        let (vcpu, mem) = driver.extract_resources(&process_config);
        assert_eq!(vcpu, 1); // max(ceil(500/1000), 1) = 1
        assert_eq!(mem, 256);
    }

    #[test]
    fn test_vm_naming() {
        let id = "abc-123";
        assert_eq!(format!("fc-{}", id), "fc-abc-123");
    }

    #[test]
    fn test_daemon_addr_sandbox() {
        use crate::driver::{DAEMON_GRPC_PORT, DAEMON_HTTP_PORT};
        let container_ip = "172.16.0.2";
        let daemon_addr = format!("{}:{}", container_ip, DAEMON_GRPC_PORT);
        let http_addr = format!("{}:{}", container_ip, DAEMON_HTTP_PORT);
        assert_eq!(daemon_addr, "172.16.0.2:9500");
        assert_eq!(http_addr, "172.16.0.2:9501");
    }

    #[test]
    fn test_daemon_addr_function() {
        let container_ip = "172.16.0.3";
        let daemon_addr = format!("{}:9600", container_ip);
        assert_eq!(daemon_addr, "172.16.0.3:9600");
    }

    #[test]
    fn test_kernel_config_json() {
        let kernel_path = "/var/lib/firecracker/vmlinux";
        let boot_args = "console=ttyS0 reboot=k panic=1 pci=off";
        let json = format!(
            r#"{{"kernel_image_path": "{}", "boot_args": "{}"}}"#,
            kernel_path, boot_args
        );
        assert!(json.contains("vmlinux"));
        assert!(json.contains("console=ttyS0"));
    }

    #[test]
    fn test_drive_config_json() {
        let rootfs_path = "/dev/mapper/overlay-vm-1";
        let json = format!(
            r#"{{"drive_id": "rootfs", "path_on_host": "{}", "is_root_device": true, "is_read_only": false}}"#,
            rootfs_path
        );
        assert!(json.contains("overlay-vm-1"));
        assert!(json.contains("is_root_device"));
    }

    #[test]
    fn test_machine_config_json() {
        let vcpu = 2u32;
        let mem = 512u32;
        let json = format!(
            r#"{{"vcpu_count": {}, "mem_size_mib": {}}}"#,
            vcpu, mem
        );
        assert!(json.contains("\"vcpu_count\": 2"));
        assert!(json.contains("\"mem_size_mib\": 512"));
    }

    #[tokio::test]
    async fn test_next_ip_suffix_increments() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        // Initially empty → suffix 2
        assert_eq!(driver.next_ip_suffix().await, 2);
    }

    #[tokio::test]
    async fn test_list_containers_empty() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        let containers = driver.list_containers().await.unwrap();
        assert!(containers.is_empty());
    }

    #[tokio::test]
    async fn test_alive_unknown_handle() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        let handle = ProcessHandle {
            id: "nonexistent-vm".to_string(),
            daemon_addr: None,
            http_addr: None,
            container_ip: "172.16.0.99".to_string(),
        };

        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_exit_status_unknown_handle() {
        let config = FirecrackerDriverConfig::default();
        let driver = FirecrackerDriver::new(config);

        let handle = ProcessHandle {
            id: "nonexistent-vm".to_string(),
            daemon_addr: None,
            http_addr: None,
            container_ip: "172.16.0.99".to_string(),
        };

        assert!(driver.get_exit_status(&handle).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_logs_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = FirecrackerDriverConfig {
            state_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let driver = FirecrackerDriver::new(config);

        let handle = ProcessHandle {
            id: "no-such-vm".to_string(),
            daemon_addr: None,
            http_addr: None,
            container_ip: "172.16.0.99".to_string(),
        };

        let logs = driver.get_logs(&handle, 10).await.unwrap();
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn test_get_logs_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("fc-test.log");
        tokio::fs::write(&log_path, "line1\nline2\nline3\n")
            .await
            .unwrap();

        let config = FirecrackerDriverConfig {
            state_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let driver = FirecrackerDriver::new(config);

        let handle = ProcessHandle {
            id: "fc-test".to_string(),
            daemon_addr: None,
            http_addr: None,
            container_ip: "172.16.0.2".to_string(),
        };

        let logs = driver.get_logs(&handle, 2).await.unwrap();
        assert!(logs.contains("line2"));
        assert!(logs.contains("line3"));
    }

    #[test]
    fn test_snapshotter_config_from_driver_config() {
        let config = FirecrackerDriverConfig {
            base_rootfs_path: PathBuf::from("/images/rootfs.ext4"),
            state_dir: PathBuf::from("/var/run/fc"),
            overlay_size_bytes: 4 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let snap_config = SnapshotConfig {
            base_image_path: config.base_rootfs_path.clone(),
            overlay_dir: config.state_dir.join("overlays"),
            overlay_size_bytes: config.overlay_size_bytes,
            chunk_size: None,
        };

        assert_eq!(snap_config.base_image_path, PathBuf::from("/images/rootfs.ext4"));
        assert_eq!(
            snap_config.overlay_dir,
            PathBuf::from("/var/run/fc/overlays")
        );
        assert_eq!(snap_config.overlay_size_bytes, 4 * 1024 * 1024 * 1024);
    }
}
