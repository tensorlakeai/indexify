mod docker;
mod fork_exec;

use anyhow::Result;
use async_trait::async_trait;
pub use docker::DockerDriver;
pub use fork_exec::ForkExecDriver;

/// Resource limits for a process/container.
#[derive(Debug, Clone, Default)]
pub struct ResourceLimits {
    /// Memory limit in megabytes.
    pub memory_mb: Option<u64>,
    /// CPU limit in millicores (1000 = 1 CPU core).
    pub cpu_millicores: Option<u64>,
}

/// Configuration for starting a process.
pub struct ProcessConfig {
    /// Unique identifier for this container (used as Docker container name suffix).
    pub id: String,
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
    /// Docker).
    #[allow(dead_code)]
    pub http_addr: Option<String>,
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
}
