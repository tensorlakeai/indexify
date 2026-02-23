mod docker;
#[cfg(feature = "firecracker")]
pub(crate) mod firecracker;
mod fork_exec;

use anyhow::Result;
use async_trait::async_trait;
pub use docker::{DockerDriver, ImageError};
#[cfg(feature = "firecracker")]
pub use firecracker::FirecrackerDriver;
pub use fork_exec::ForkExecDriver;

/// Container port for the daemon gRPC server (internal API).
pub const DAEMON_GRPC_PORT: u16 = 9500;

/// Container port for the daemon HTTP server (user-facing Sandbox API).
pub const DAEMON_HTTP_PORT: u16 = 9501;

/// Determines the binary launched by the driver.
#[derive(Debug, Clone, Default)]
pub enum ProcessType {
    /// Sandbox: launches container-daemon binary as PID 1 (current behavior).
    #[default]
    Sandbox,
    /// Function executor: launches function-executor binary as a subprocess.
    Function,
}

/// Resource limits for a process/container.
#[derive(Debug, Clone, Default)]
pub struct ResourceLimits {
    /// Memory limit in bytes.
    pub memory_bytes: Option<u64>,
    /// CPU limit in millicores (1000 = 1 CPU core), equivalent to
    /// `cpu_ms_per_sec` from the server proto.
    pub cpu_millicores: Option<u64>,
    /// Specific GPU UUIDs to pass to the container via Docker DeviceRequest.
    pub gpu_device_ids: Option<Vec<String>>,
}

/// Configuration for starting a process.
pub struct ProcessConfig {
    /// Unique identifier for this container (used as Docker container name
    /// suffix).
    pub id: String,
    /// Type of process to launch (Sandbox or Function).
    pub process_type: ProcessType,
    /// Container image (for Docker driver).
    pub image: Option<String>,
    /// Command to execute.
    pub command: String,
    /// Arguments to pass to the command.
    pub args: Vec<String>,
    /// Environment variables.
    pub env: Vec<(String, String)>,
    /// Working directory.
    pub working_dir: Option<String>,
    /// Resource limits (CPU, memory).
    pub resources: Option<ResourceLimits>,
    /// Labels to attach to the container (for Docker driver).
    pub labels: Vec<(String, String)>,
    /// Path to a local tar file containing the rootfs overlay (upper layer).
    /// When set, the Docker driver applies this as a gVisor annotation
    /// (`dev.gvisor.tar.rootfs.upper`) during container creation.
    pub rootfs_overlay: Option<String>,
}

/// Handle to a running process.
#[derive(Debug, Clone)]
pub struct ProcessHandle {
    /// Unique identifier for the process (container name or PID).
    pub id: String,
    /// Address for daemon gRPC communication (e.g., "127.0.0.1:32768" for
    /// Docker).
    pub daemon_addr: Option<String>,
    /// Address for daemon HTTP API (Sandbox API) (e.g., "127.0.0.1:32769" for
    /// Docker). This is exposed externally as `sandbox_http_address`.
    pub http_addr: Option<String>,
    /// Container's internal IP address.
    /// For Docker: the container's network IP.
    /// For ForkExec: "127.0.0.1" (localhost).
    pub container_ip: String,
}

/// Exit status information for a terminated process.
#[derive(Debug, Clone, Default)]
pub struct ExitStatus {
    /// Exit code of the process (0 = success).
    pub exit_code: Option<i64>,
    /// Whether the process was killed due to out-of-memory.
    pub oom_killed: bool,
}

/// Trait for process drivers that can start and manage processes.
#[async_trait]
pub trait ProcessDriver: Send + Sync {
    /// Start a new process with the given configuration.
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle>;

    /// Send a signal to a process.
    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()>;

    /// Gracefully stop a process (SIGTERM + wait + SIGKILL).
    /// Unlike `kill`, this waits for the process to exit cleanly, which
    /// ensures gVisor flushes filesystem writes to the overlay before the
    /// container is removed.
    async fn stop(&self, handle: &ProcessHandle, _timeout_secs: u64) -> Result<()> {
        // Default implementation: just kill
        self.kill(handle).await
    }

    /// Kill a process.
    async fn kill(&self, handle: &ProcessHandle) -> Result<()>;

    /// Check if a process is still alive.
    async fn alive(&self, handle: &ProcessHandle) -> Result<bool>;

    /// Get exit status for a terminated process.
    /// Returns None if the process is still running or status cannot be
    /// determined.
    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>>;

    /// List all container IDs managed by this driver.
    /// Used for cleanup of orphaned containers.
    async fn list_containers(&self) -> Result<Vec<String>>;

    /// Get the last `tail` lines of stdout/stderr from a container.
    /// Returns empty string for drivers that don't support log retrieval.
    async fn get_logs(&self, _handle: &ProcessHandle, _tail: u32) -> Result<String> {
        Ok(String::new())
    }
}
