use std::{
    collections::{HashMap, HashSet},
    net::TcpListener,
    path::Path,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
    time::{Instant, sleep},
};
use tracing::{info, warn};

use super::{ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle, ProcessType};
use crate::daemon_binary;

const FE_DYNAMIC_LISTEN_ADDR: &str = "127.0.0.1:0";
const FE_DYNAMIC_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(1);
const FE_ANNOUNCE_POLL_INTERVAL: Duration = Duration::from_millis(25);
const FE_LEGACY_START_RETRIES: usize = 4;
const FE_EARLY_EXIT_GRACE: Duration = Duration::from_millis(250);

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
pub fn allocate_ephemeral_port() -> Result<u16> {
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

/// Build a command for sandbox mode with ephemeral daemon ports.
fn build_sandbox_command(
    config: &ProcessConfig,
) -> Result<(Command, Option<String>, Option<String>)> {
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

    Ok((
        cmd,
        Some(format!("127.0.0.1:{}", grpc_port)),
        Some(format!("127.0.0.1:{}", http_port)),
    ))
}

/// Build a command for function executor mode with an explicit gRPC port.
fn build_function_command_with_port(config: &ProcessConfig, grpc_port: u16) -> Command {
    let mut cmd = Command::new(&config.command);
    cmd.args(&config.args);
    cmd.arg("--address").arg(format!("127.0.0.1:{}", grpc_port));
    cmd
}

/// Build a function command that binds to an OS-assigned port.
fn build_function_command_dynamic(config: &ProcessConfig) -> Command {
    let mut cmd = Command::new(&config.command);
    cmd.args(&config.args);
    cmd.arg("--address").arg(FE_DYNAMIC_LISTEN_ADDR);
    cmd
}

fn configure_command(cmd: &mut Command, config: &ProcessConfig) {
    cmd.envs(config.env.iter().cloned());
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());
    if let Some(dir) = &config.working_dir {
        cmd.current_dir(dir);
    }
}

async fn wait_for_early_exit(
    child: &mut Child,
    timeout: Duration,
) -> Result<Option<std::process::ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().context("Failed to check process status")? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        sleep(FE_ANNOUNCE_POLL_INTERVAL).await;
    }
}

fn parse_socket_inode(link_target: &Path) -> Option<u64> {
    let text = link_target.to_string_lossy();
    let rest = text.strip_prefix("socket:[")?;
    let inode = rest.strip_suffix(']')?;
    inode.parse::<u64>().ok()
}

fn socket_inodes_for_pid(pid: u32) -> Result<HashSet<u64>> {
    let fd_dir = format!("/proc/{pid}/fd");
    let mut inodes = HashSet::new();

    let dir_iter = match std::fs::read_dir(&fd_dir) {
        Ok(iter) => iter,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(inodes),
        Err(e) => {
            return Err(e).with_context(|| format!("Failed to read process FD directory {fd_dir}"));
        }
    };

    for entry in dir_iter {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).context("Failed to read process FD entry"),
        };

        let link_target = match std::fs::read_link(entry.path()) {
            Ok(link_target) => link_target,
            // FD sets can churn while the process is bootstrapping.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).context("Failed to read process FD link"),
        };

        if let Some(inode) = parse_socket_inode(&link_target) {
            inodes.insert(inode);
        }
    }
    Ok(inodes)
}

fn filter_new_socket_inodes(
    current_socket_inodes: HashSet<u64>,
    inherited_socket_inodes: &HashSet<u64>,
) -> HashSet<u64> {
    current_socket_inodes
        .into_iter()
        .filter(|inode| !inherited_socket_inodes.contains(inode))
        .collect()
}

fn parse_listening_port_from_table(path: &str, inodes: &HashSet<u64>) -> Result<Option<u16>> {
    // /proc/net/tcp* fields:
    // sl  local_address rem_address st ... uid timeout inode ...
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read {path} for dynamic FE port discovery"))?;
    for line in contents.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 {
            continue;
        }

        let local = fields[1];
        let state = fields[3];
        let inode = fields[9];

        // LISTEN only.
        if state != "0A" {
            continue;
        }

        let Ok(inode_num) = inode.parse::<u64>() else {
            continue;
        };
        if !inodes.contains(&inode_num) {
            continue;
        }

        let Some((ip_hex, port_hex)) = local.split_once(':') else {
            continue;
        };

        let _ = ip_hex;
        if let Ok(port) = u16::from_str_radix(port_hex, 16) &&
            port != 0
        {
            return Ok(Some(port));
        }
    }
    Ok(None)
}

fn parse_listening_port(inodes: &HashSet<u64>) -> Result<Option<u16>> {
    // Prefer IPv4 so `127.0.0.1:<port>` stays valid for tonic connect.
    if let Some(port) = parse_listening_port_from_table("/proc/net/tcp", inodes)? {
        return Ok(Some(port));
    }
    // Fall back to IPv6 listeners (typically dual-stack).
    parse_listening_port_from_table("/proc/net/tcp6", inodes)
}

async fn wait_for_dynamic_address(
    child: &mut Child,
    timeout: Duration,
    inherited_socket_inodes: &HashSet<u64>,
) -> Result<String> {
    let pid = child
        .id()
        .ok_or_else(|| anyhow!("Failed to get function executor PID for port discovery"))?;

    let deadline = Instant::now() + timeout;
    loop {
        let socket_inodes =
            filter_new_socket_inodes(socket_inodes_for_pid(pid)?, inherited_socket_inodes);
        if !socket_inodes.is_empty() &&
            let Some(port) = parse_listening_port(&socket_inodes)?
        {
            return Ok(format!("127.0.0.1:{port}"));
        }

        if let Some(status) = child
            .try_wait()
            .context("Failed to check process status while waiting for FE dynamic bind")?
        {
            return Err(anyhow!(
                "Function executor exited before exposing a listening port (status={status})"
            ));
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "Timed out discovering FE dynamic listening address"
            ));
        }
        sleep(FE_ANNOUNCE_POLL_INTERVAL).await;
    }
}

fn handle_from_child(
    child: &Child,
    daemon_addr: Option<String>,
    http_addr: Option<String>,
) -> Result<ProcessHandle> {
    let pid = child
        .id()
        .ok_or_else(|| anyhow!("Failed to get process ID"))?;

    Ok(ProcessHandle {
        id: pid.to_string(),
        daemon_addr,
        http_addr,
        container_ip: "127.0.0.1".to_string(),
    })
}

impl ForkExecDriver {
    async fn start_function_process(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let inherited_socket_inodes = match socket_inodes_for_pid(std::process::id()) {
            Ok(inodes) => inodes,
            Err(err) => {
                warn!(
                    error = ?err,
                    "Failed to snapshot parent socket inodes; dynamic FE discovery may match inherited sockets"
                );
                HashSet::new()
            }
        };

        info!(
            command = %config.command,
            address = FE_DYNAMIC_LISTEN_ADDR,
            "Starting function-executor via fork_exec (dynamic bind)"
        );

        let mut dynamic_cmd = build_function_command_dynamic(&config);
        configure_command(&mut dynamic_cmd, &config);

        let mut dynamic_child = dynamic_cmd
            .spawn()
            .with_context(|| format!("Failed to spawn process: {:?}", config.command))?;

        let dynamic_result = wait_for_dynamic_address(
            &mut dynamic_child,
            FE_DYNAMIC_DISCOVERY_TIMEOUT,
            &inherited_socket_inodes,
        )
        .await;

        match dynamic_result {
            Ok(daemon_addr) => {
                let handle = handle_from_child(&dynamic_child, Some(daemon_addr), None)?;
                self.processes
                    .lock()
                    .await
                    .insert(handle.id.clone(), dynamic_child);
                return Ok(handle);
            }
            Err(err) => {
                warn!(
                    error = ?err,
                    "Function executor dynamic bind discovery failed; falling back to legacy startup"
                );
                let _ = dynamic_child.kill().await;
                let _ = dynamic_child.wait().await;
            }
        }

        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=FE_LEGACY_START_RETRIES {
            let grpc_port = match allocate_ephemeral_port() {
                Ok(port) => port,
                Err(e) => {
                    last_err = Some(e.context("Failed to allocate FE gRPC port"));
                    continue;
                }
            };

            info!(
                command = %config.command,
                grpc_port,
                attempt,
                retries = FE_LEGACY_START_RETRIES,
                "Starting function-executor via fork_exec (legacy bind)"
            );

            let mut cmd = build_function_command_with_port(&config, grpc_port);
            configure_command(&mut cmd, &config);

            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(e) => {
                    last_err = Some(
                        anyhow!(e)
                            .context(format!("Failed to spawn process: {:?}", config.command)),
                    );
                    continue;
                }
            };

            match wait_for_early_exit(&mut child, FE_EARLY_EXIT_GRACE).await {
                Ok(Some(status)) => {
                    let err = anyhow!(
                        "Function executor exited during startup attempt {attempt}/{FE_LEGACY_START_RETRIES} (status={status})"
                    );
                    warn!(error = ?err, "Retrying function executor startup");
                    last_err = Some(err);
                    continue;
                }
                Ok(None) => {
                    let handle =
                        handle_from_child(&child, Some(format!("127.0.0.1:{grpc_port}")), None)?;
                    self.processes.lock().await.insert(handle.id.clone(), child);
                    return Ok(handle);
                }
                Err(err) => {
                    last_err = Some(err);
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    continue;
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("Failed to start function executor process")))
    }
}

#[async_trait]
impl ProcessDriver for ForkExecDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        if matches!(config.process_type, ProcessType::Function) {
            return self.start_function_process(config).await;
        }

        let (mut cmd, daemon_addr, http_addr) = match config.process_type {
            ProcessType::Sandbox if config.image.is_some() => build_sandbox_command(&config)?,
            _ => {
                // Direct mode: run the command directly (for testing)
                let mut cmd = Command::new(&config.command);
                cmd.args(&config.args);
                (cmd, None, None)
            }
        };

        configure_command(&mut cmd, &config);

        let child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn process: {:?}", config.command))?;

        let handle = handle_from_child(&child, daemon_addr, http_addr)?;
        let id = handle.id.clone();

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

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        let mut processes = self.processes.lock().await;

        if let Some(child) = processes.get_mut(&handle.id) {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let exit_code = status.code().map(|c| c as i64);
                    Ok(Some(ExitStatus {
                        exit_code,
                        oom_killed: false, // Can't detect OOM for local processes easily
                    }))
                }
                Ok(None) => Ok(None), // Still running
                Err(e) => Err(e).context("Failed to get exit status"),
            }
        } else {
            Ok(None)
        }
    }

    async fn list_containers(&self) -> Result<Vec<String>> {
        // Fork-exec processes don't persist across restarts, so nothing to list
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_and_check_alive() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
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
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "echo".to_string(),
            args: vec!["hello".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
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
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "sleep".to_string(),
            args: vec!["60".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
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
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "env".to_string(),
            args: vec![],
            env: vec![("TEST_VAR".to_string(), "test_value".to_string())],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
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
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "nonexistent_command_12345".to_string(),
            args: vec![],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
        };

        let result = driver.start(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn test_send_signal() {
        let driver = ForkExecDriver::new();

        let config = ProcessConfig {
            id: "test".to_string(),
            process_type: ProcessType::default(),
            image: None,
            command: "sleep".to_string(),
            args: vec!["60".to_string()],
            env: vec![],
            working_dir: None,
            resources: None,
            labels: vec![],
            rootfs_overlay: None,
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
            container_ip: "127.0.0.1".to_string(),
        };

        // Unknown handle should return false (not alive)
        assert!(!driver.alive(&handle).await.unwrap());
    }

    #[test]
    fn test_filter_new_socket_inodes_excludes_inherited() {
        let current_socket_inodes: HashSet<u64> = [11_u64, 12, 13].into_iter().collect();
        let inherited_socket_inodes: HashSet<u64> = [10_u64, 12].into_iter().collect();

        let filtered = filter_new_socket_inodes(current_socket_inodes, &inherited_socket_inodes);
        let expected: HashSet<u64> = [11_u64, 13].into_iter().collect();

        assert_eq!(filtered, expected);
    }
}
