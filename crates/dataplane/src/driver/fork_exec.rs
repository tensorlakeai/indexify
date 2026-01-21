use std::{collections::HashMap, net::TcpListener, process::Stdio, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use tracing::info;

use super::{ProcessConfig, ProcessDriver, ProcessHandle};
use crate::daemon_binary;

pub struct ForkExecDriver {
    processes: Arc<Mutex<HashMap<String, Child>>>,
}

impl ForkExecDriver {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for ForkExecDriver {
    fn default() -> Self {
        Self::new()
    }
}

/// Allocate an ephemeral port by binding to port 0 and getting the assigned
/// port.
fn allocate_ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("Failed to bind to ephemeral port")?;
    let port = listener
        .local_addr()
        .context("Failed to get local address")?
        .port();
    // Drop the listener to release the port - there's a small race window here
    // but it's acceptable for local testing
    drop(listener);
    Ok(port)
}

#[async_trait]
impl ProcessDriver for ForkExecDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        // If image is specified, we're in sandbox mode - start the daemon binary
        // If no image, we're in direct mode - run the command directly (for testing)
        let (cmd, daemon_addr, http_addr) = if config.image.is_some() {
            // Sandbox mode: start daemon binary with ephemeral ports
            let daemon_path = daemon_binary::get_daemon_path()
                .context("Daemon binary not available for fork_exec driver")?;

            let grpc_port = allocate_ephemeral_port().context("Failed to allocate gRPC port")?;
            let http_port = allocate_ephemeral_port().context("Failed to allocate HTTP port")?;

            info!(
                daemon_path = %daemon_path.display(),
                grpc_port = grpc_port,
                http_port = http_port,
                "Starting daemon via fork_exec (ignoring image: {:?})",
                config.image
            );

            let mut cmd = Command::new(daemon_path);
            cmd.arg("--port").arg(grpc_port.to_string());
            cmd.arg("--http-port").arg(http_port.to_string());
            cmd.arg("--log-dir").arg("/tmp/indexify-daemon-logs");

            // Pass original command after -- if provided
            if !config.command.is_empty() {
                cmd.arg("--");
                cmd.arg(&config.command);
                cmd.args(&config.args);
            }

            (
                cmd,
                Some(format!("127.0.0.1:{}", grpc_port)),
                Some(format!("127.0.0.1:{}", http_port)),
            )
        } else {
            // Direct mode: run the command directly (for testing)
            let mut cmd = Command::new(&config.command);
            cmd.args(&config.args);
            (cmd, None, None)
        };

        let mut cmd = cmd;
        cmd.envs(config.env);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        if let Some(dir) = &config.working_dir {
            cmd.current_dir(dir);
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn process: {:?}", config.command))?;

        let pid = child
            .id()
            .ok_or_else(|| anyhow::anyhow!("Failed to get process ID"))?;

        let id = pid.to_string();
        let handle = ProcessHandle {
            id: id.clone(),
            daemon_addr,
            http_addr,
        };

        self.processes.lock().await.insert(id, child);

        Ok(handle)
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        let pid: i32 = handle.id.parse().context("Invalid process ID")?;

        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };

            let signal = Signal::try_from(signal).context("Invalid signal number")?;
            kill(Pid::from_raw(pid), signal).context("Failed to send signal")?;
        }

        #[cfg(not(unix))]
        {
            let _ = (pid, signal);
            anyhow::bail!("send_sig is only supported on Unix platforms");
        }

        Ok(())
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        let mut processes = self.processes.lock().await;

        if let Some(child) = processes.get_mut(&handle.id) {
            child.kill().await.context("Failed to kill process")?;
            processes.remove(&handle.id);
        }

        Ok(())
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        let mut processes = self.processes.lock().await;

        if let Some(child) = processes.get_mut(&handle.id) {
            match child.try_wait() {
                Ok(Some(_)) => {
                    processes.remove(&handle.id);
                    Ok(false)
                }
                Ok(None) => Ok(true),
                Err(e) => Err(e).context("Failed to check process status"),
            }
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_and_check_alive() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
        };

        let handle = driver.start(config).await.unwrap();
        assert!(!handle.id.is_empty());
        assert!(handle.daemon_addr.is_none());

        // Process should be alive
        assert!(driver.alive(&handle).await.unwrap());

        // Clean up
        driver.kill(&handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_start_short_lived_process() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
        };

        let handle = driver.start(config).await.unwrap();

        // Wait for process to exit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Process should no longer be alive
        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn test_kill_process() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "sleep".to_string(),
            args: vec!["60".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
        };

        let handle = driver.start(config).await.unwrap();

        // Process should be alive
        assert!(driver.alive(&handle).await.unwrap());

        // Kill it
        driver.kill(&handle).await.unwrap();

        // Process should no longer be alive
        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn test_start_with_env() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "env".to_string(),
            args: vec![],
            env: vec![("TEST_VAR".to_string(), "test_value".to_string())],
            working_dir: None,
            resources: None,
        };

        let handle = driver.start(config).await.unwrap();

        // Wait for process to exit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Process should have exited
        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn test_start_nonexistent_command() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "nonexistent_command_12345".to_string(),
            args: vec![],
            env: vec![],
            working_dir: None,
            resources: None,
        };

        let result = driver.start(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_send_signal() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            image: None,
            command: "sleep".to_string(),
            args: vec!["60".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
        };

        let handle = driver.start(config).await.unwrap();

        // Send SIGTERM (15)
        driver.send_sig(&handle, 15).await.unwrap();

        // Wait for process to die
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Process should no longer be alive
        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn test_alive_unknown_handle() {
        let driver = ForkExecDriver::new();

        let handle = ProcessHandle {
            id: "99999999".to_string(),
            daemon_addr: None,
            http_addr: None,
        };

        // Unknown handle should return false (not alive)
        assert!(!driver.alive(&handle).await.unwrap());
    }
}
