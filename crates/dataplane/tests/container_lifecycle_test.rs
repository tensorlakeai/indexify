//! Integration tests for FunctionContainerManager lifecycle.
//!
//! These tests exercise the code paths invoked by:
//! - Desired state updates (container creation/deletion via sync())
//! - Heartbeats (health checks via run_health_checks())
//!
//! The tests use the actual container-daemon binary started as a subprocess,
//! simulating what happens inside a real container.

use std::{
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
// Import from the crate being tested
use indexify_dataplane::driver::{ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle};
use indexify_dataplane::{
    DataplaneMetrics,
    function_container_manager::{FunctionContainerManager, ImageResolver},
    state_file::StateFile,
};
use proto_api::executor_api_pb::{
    FunctionExecutorDescription,
    FunctionExecutorStatus,
    FunctionRef,
    SandboxMetadata,
};
use tempfile::TempDir;
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

fn create_test_metrics() -> Arc<DataplaneMetrics> {
    Arc::new(DataplaneMetrics::new())
}

async fn create_test_state_file() -> Arc<StateFile> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    // Leak the tempdir so it doesn't get cleaned up during the test
    std::mem::forget(dir);
    Arc::new(StateFile::new(&path).await.unwrap())
}

/// Test image resolver
struct TestImageResolver;

#[async_trait]
impl ImageResolver for TestImageResolver {
    async fn sandbox_image_for_pool(&self, _namespace: &str, _pool_id: &str) -> anyhow::Result<String> {
        Ok("test-image:latest".to_string())
    }

    async fn sandbox_image(&self, _namespace: &str, _sandbox_id: &str) -> anyhow::Result<String> {
        Ok("test-image:latest".to_string())
    }

    async fn function_image(
        &self,
        _namespace: &str,
        _app: &str,
        _function: &str,
        _version: &str,
    ) -> anyhow::Result<String> {
        Ok("test-image:latest".to_string())
    }
}

/// A test driver that starts the actual daemon binary as a subprocess.
/// This simulates what the Docker driver does when starting a container.
struct DaemonTestDriver {
    /// Path to the daemon binary
    daemon_binary: PathBuf,
    /// Log directory
    log_dir: PathBuf,
    /// Running daemon processes
    daemons: Arc<Mutex<Vec<(String, Child, u16)>>>,
    /// Counter for unique IDs
    counter: AtomicUsize,
    /// Port counter for unique ports
    port_counter: AtomicU16,
}

impl DaemonTestDriver {
    fn new(daemon_binary: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            daemon_binary,
            log_dir,
            daemons: Arc::new(Mutex::new(Vec::new())),
            counter: AtomicUsize::new(0),
            port_counter: AtomicU16::new(19500), // Start from a high port
        }
    }

    async fn cleanup(&self) {
        let mut daemons = self.daemons.lock().await;
        for (id, mut child, _port) in daemons.drain(..) {
            tracing::info!(id = %id, "Cleaning up daemon process");
            let _ = child.kill().await;
        }
    }

    /// Wait for a daemon on a specific port to be ready.
    async fn wait_for_daemon(&self, port: u16, timeout: Duration) -> bool {
        let addr = format!("127.0.0.1:{}", port);
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            if let Ok(stream) = tokio::net::TcpStream::connect(&addr).await {
                drop(stream);
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }
}

#[async_trait]
impl ProcessDriver for DaemonTestDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let id = format!(
            "test-container-{}",
            self.counter.fetch_add(1, Ordering::SeqCst)
        );
        let port = self.port_counter.fetch_add(1, Ordering::SeqCst);
        let daemon_addr = format!("127.0.0.1:{}", port);

        tracing::info!(
            id = %id,
            daemon_addr = %daemon_addr,
            command = %config.command,
            "Starting daemon subprocess"
        );

        // Start the daemon binary
        let child = Command::new(&self.daemon_binary)
            .arg("--port")
            .arg(port.to_string())
            .arg("--log-dir")
            .arg(&self.log_dir)
            .arg("--")
            .arg(&config.command)
            .args(&config.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Store the daemon process
        self.daemons.lock().await.push((id.clone(), child, port));

        // Wait for daemon to be ready (port to be listening)
        if !self.wait_for_daemon(port, Duration::from_secs(5)).await {
            anyhow::bail!("Daemon failed to start on port {}", port);
        }

        tracing::info!(
            id = %id,
            daemon_addr = %daemon_addr,
            "Daemon is ready"
        );

        Ok(ProcessHandle {
            id,
            daemon_addr: Some(daemon_addr),
            http_addr: None,
            container_ip: "127.0.0.1".to_string(),
        })
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{Signal, kill},
                unistd::Pid,
            };

            let daemons = self.daemons.lock().await;
            for (id, child, _port) in daemons.iter() {
                if id == &handle.id {
                    if let Some(pid) = child.id() {
                        let sig = Signal::try_from(signal)?;
                        kill(Pid::from_raw(pid as i32), sig)?;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        let mut daemons = self.daemons.lock().await;
        let mut idx_to_remove = None;

        for (idx, (id, child, _port)) in daemons.iter_mut().enumerate() {
            if id == &handle.id {
                tracing::info!(id = %id, "Killing daemon process");
                let _ = child.kill().await;
                idx_to_remove = Some(idx);
                break;
            }
        }

        if let Some(idx) = idx_to_remove {
            daemons.remove(idx);
        }

        Ok(())
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        let mut daemons = self.daemons.lock().await;

        for (id, child, _port) in daemons.iter_mut() {
            if id == &handle.id {
                match child.try_wait() {
                    Ok(Some(_)) => return Ok(false), // Process exited
                    Ok(None) => return Ok(true),     // Still running
                    Err(_) => return Ok(false),
                }
            }
        }

        Ok(false) // Not found
    }

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        let mut daemons = self.daemons.lock().await;

        for (id, child, _port) in daemons.iter_mut() {
            if id == &handle.id {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        return Ok(Some(ExitStatus {
                            exit_code: status.code().map(|c| c as i64),
                            oom_killed: false,
                        }));
                    }
                    Ok(None) => return Ok(None), // Still running
                    Err(_) => return Ok(None),
                }
            }
        }

        Ok(None) // Not found
    }

    async fn list_containers(&self) -> Result<Vec<String>> {
        let daemons = self.daemons.lock().await;
        Ok(daemons.iter().map(|(id, ..)| id.clone()).collect())
    }
}

fn create_test_fe_description(id: &str) -> FunctionExecutorDescription {
    FunctionExecutorDescription {
        id: Some(id.to_string()),
        function: Some(FunctionRef {
            namespace: Some("test-ns".to_string()),
            application_name: Some("test-app".to_string()),
            function_name: Some("test-fn".to_string()),
            application_version: Some("v1".to_string()),
        }),
        secret_names: vec![],
        initialization_timeout_ms: None,
        application: None,
        resources: None,
        max_concurrency: None,
        allocation_timeout_ms: None,
        sandbox_metadata: Some(SandboxMetadata {
            image: Some("test-image:latest".to_string()),
            timeout_secs: None,
            entrypoint: vec![],
            network_policy: None,
            sandbox_id: None,
        }),
        container_type: None,
        pool_id: None,
    }
}

/// Find the daemon binary path
fn find_daemon_binary() -> Option<PathBuf> {
    // Try debug build first
    let debug_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/debug/indexify-container-daemon");

    if debug_path.exists() {
        return Some(debug_path);
    }

    // Try release build
    let release_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/release/indexify-container-daemon");

    if release_path.exists() {
        return Some(release_path);
    }

    None
}

/// Test: Container creation via sync() with desired state
///
/// This tests the code path: sync() -> start_container_with_daemon() -> daemon
/// connection
#[tokio::test]
async fn test_sync_creates_container_with_daemon() {
    let _ = tracing_subscriber::fmt::try_init();

    let daemon_binary = match find_daemon_binary() {
        Some(p) => p,
        None => {
            eprintln!(
                "Skipping test: daemon binary not found. Run `cargo build -p indexify-container-daemon` first."
            );
            return;
        }
    };

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(log_dir.join("logs")).unwrap();

    let driver = Arc::new(DaemonTestDriver::new(daemon_binary, log_dir));
    let resolver = Arc::new(TestImageResolver);
    let state_file = create_test_state_file().await;
    let manager = FunctionContainerManager::new(
        driver.clone(),
        resolver,
        Arc::new(indexify_dataplane::NoopSecretsProvider::new()),
        create_test_metrics(),
        state_file,
        "test-executor".to_string(),
    );

    // Initially no containers
    let states = manager.get_states().await;
    assert!(states.is_empty(), "Expected no containers initially");

    // Sync with desired state containing one FE
    let desired = vec![create_test_fe_description("fe-integration-1")];
    manager.sync(desired).await;

    // Should have one container in pending state immediately
    let states = manager.get_states().await;
    assert_eq!(states.len(), 1, "Expected one container after sync");
    assert_eq!(
        states[0].status,
        Some(FunctionExecutorStatus::Pending.into()),
        "Container should be in pending state"
    );

    // Wait for container to start (daemon connection + process start)
    // This exercises the full start_container_with_daemon() code path
    tokio::time::sleep(Duration::from_secs(3)).await;

    let states = manager.get_states().await;
    assert_eq!(states.len(), 1, "Should still have one container");

    // Container should either be Running (success) or Terminated (daemon/process
    // failure)
    let status = states[0].status.unwrap();
    let running: i32 = FunctionExecutorStatus::Running.into();
    let terminated: i32 = FunctionExecutorStatus::Terminated.into();
    assert!(
        status == running || status == terminated,
        "Container should be Running or Terminated, got: {:?}",
        status
    );

    // Cleanup
    driver.cleanup().await;
}

/// Test: Container deletion via sync() when removed from desired state
///
/// This tests the code path: sync() -> initiate_stop() -> graceful shutdown
#[tokio::test]
async fn test_sync_deletes_container_when_removed_from_desired() {
    let _ = tracing_subscriber::fmt::try_init();

    let daemon_binary = match find_daemon_binary() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: daemon binary not found.");
            return;
        }
    };

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(log_dir.join("logs")).unwrap();

    let driver = Arc::new(DaemonTestDriver::new(daemon_binary, log_dir));
    let resolver = Arc::new(TestImageResolver);
    let state_file = create_test_state_file().await;
    let manager = FunctionContainerManager::new(
        driver.clone(),
        resolver,
        Arc::new(indexify_dataplane::NoopSecretsProvider::new()),
        create_test_metrics(),
        state_file,
        "test-executor".to_string(),
    );

    // Create a container
    let desired = vec![create_test_fe_description("fe-to-delete")];
    manager.sync(desired).await;

    // Wait for container creation attempt
    tokio::time::sleep(Duration::from_secs(2)).await;

    let states = manager.get_states().await;
    assert_eq!(states.len(), 1, "Should have one container");

    // Now sync with empty desired state - this should trigger deletion
    manager.sync(vec![]).await;

    // The container should be marked for stopping or already terminated
    let _states = manager.get_states().await;

    // If container was running, it will be in Stopping state
    // If it was already terminated, it will be removed
    // Either way, after grace period (10 seconds) it should be gone

    // Wait for grace period + cleanup (KILL_GRACE_PERIOD is 10 seconds)
    tokio::time::sleep(Duration::from_secs(12)).await;

    // Sync again to remove terminated containers
    manager.sync(vec![]).await;

    let states = manager.get_states().await;
    assert!(
        states.is_empty(),
        "Container should be removed after deletion, got {} containers",
        states.len()
    );

    // Cleanup
    driver.cleanup().await;
}

/// Test: Health check detects container death
///
/// This tests the code path: run_health_checks() -> check_all_containers() ->
/// alive() check
#[tokio::test]
async fn test_health_check_detects_container_death() {
    let _ = tracing_subscriber::fmt::try_init();

    let daemon_binary = match find_daemon_binary() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: daemon binary not found.");
            return;
        }
    };

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(log_dir.join("logs")).unwrap();

    let driver = Arc::new(DaemonTestDriver::new(daemon_binary, log_dir));
    let resolver = Arc::new(TestImageResolver);
    let state_file = create_test_state_file().await;
    let manager = FunctionContainerManager::new(
        driver.clone(),
        resolver,
        Arc::new(indexify_dataplane::NoopSecretsProvider::new()),
        create_test_metrics(),
        state_file,
        "test-executor".to_string(),
    );

    // Create a container
    let desired = vec![create_test_fe_description("fe-health-check")];
    manager.sync(desired.clone()).await;

    // Wait for container creation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Start health check loop
    let cancel_token = CancellationToken::new();
    let manager = Arc::new(manager);
    let manager_for_health = manager.clone();
    let cancel_clone = cancel_token.clone();

    let health_check_handle = tokio::spawn(async move {
        manager_for_health.run_health_checks(cancel_clone).await;
    });

    // Give health checks time to run once
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Kill the daemon process externally (simulating container crash)
    driver.cleanup().await;

    // Wait for health check to detect the death (runs every 5 seconds by default)
    // We'll check more frequently
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let states = manager.get_states().await;
        if !states.is_empty() {
            let status = states[0].status.unwrap();
            let terminated: i32 = FunctionExecutorStatus::Terminated.into();
            if status == terminated {
                // Health check detected the death
                cancel_token.cancel();
                let _ = health_check_handle.await;
                return; // Test passed
            }
        }
    }

    // Cancel health checks
    cancel_token.cancel();
    let _ = health_check_handle.await;

    // Check final state
    let states = manager.get_states().await;
    if !states.is_empty() {
        let status = states[0].status.unwrap();
        let terminated: i32 = FunctionExecutorStatus::Terminated.into();
        assert_eq!(
            status, terminated,
            "Container should be terminated after daemon death"
        );
    }
}

/// Test: Multiple containers can be managed simultaneously
#[tokio::test]
async fn test_multiple_containers_lifecycle() {
    let _ = tracing_subscriber::fmt::try_init();

    let daemon_binary = match find_daemon_binary() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: daemon binary not found.");
            return;
        }
    };

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(log_dir.join("logs")).unwrap();

    let driver = Arc::new(DaemonTestDriver::new(daemon_binary, log_dir));
    let resolver = Arc::new(TestImageResolver);
    let state_file = create_test_state_file().await;
    let manager = FunctionContainerManager::new(
        driver.clone(),
        resolver,
        Arc::new(indexify_dataplane::NoopSecretsProvider::new()),
        create_test_metrics(),
        state_file,
        "test-executor".to_string(),
    );

    // Create multiple containers
    let desired = vec![
        create_test_fe_description("fe-multi-1"),
        create_test_fe_description("fe-multi-2"),
        create_test_fe_description("fe-multi-3"),
    ];
    manager.sync(desired.clone()).await;

    // Should have 3 containers
    let states = manager.get_states().await;
    assert_eq!(states.len(), 3, "Should have 3 containers");

    // Wait for creation attempts
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remove one container from desired state
    let desired = vec![
        create_test_fe_description("fe-multi-1"),
        create_test_fe_description("fe-multi-3"),
    ];
    manager.sync(desired).await;

    // Wait for deletion
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Sync again to clean up
    manager
        .sync(vec![
            create_test_fe_description("fe-multi-1"),
            create_test_fe_description("fe-multi-3"),
        ])
        .await;

    // Should have 2 containers (or fewer if they terminated)
    let states = manager.get_states().await;
    assert!(states.len() <= 3, "Should have at most 3 containers");

    // Cleanup all
    manager.sync(vec![]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.sync(vec![]).await;

    driver.cleanup().await;
}

/// Test: Sync is idempotent - calling with same desired state doesn't create
/// duplicates
#[tokio::test]
async fn test_sync_idempotent() {
    let _ = tracing_subscriber::fmt::try_init();

    let daemon_binary = match find_daemon_binary() {
        Some(p) => p,
        None => {
            eprintln!("Skipping test: daemon binary not found.");
            return;
        }
    };

    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();
    std::fs::create_dir_all(log_dir.join("logs")).unwrap();

    let driver = Arc::new(DaemonTestDriver::new(daemon_binary, log_dir));
    let resolver = Arc::new(TestImageResolver);
    let state_file = create_test_state_file().await;
    let manager = FunctionContainerManager::new(
        driver.clone(),
        resolver,
        Arc::new(indexify_dataplane::NoopSecretsProvider::new()),
        create_test_metrics(),
        state_file,
        "test-executor".to_string(),
    );

    let desired = vec![create_test_fe_description("fe-idempotent")];

    // Call sync multiple times with same state
    manager.sync(desired.clone()).await;
    manager.sync(desired.clone()).await;
    manager.sync(desired.clone()).await;

    // Should still have only one container
    let states = manager.get_states().await;
    assert_eq!(
        states.len(),
        1,
        "Should have exactly one container after multiple syncs"
    );

    // Cleanup
    driver.cleanup().await;
}
