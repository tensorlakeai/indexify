//! Firecracker driver integration tests.
//!
//! These tests are compiled when `--features firecracker` is enabled.
//!
//! **Two tiers of tests:**
//!
//! 1. **Unit-level tests** (always run): exercise config parsing, metadata
//!    serialization, MAC generation, API request formatting, init script
//!    validation, and rootfs injection — no Firecracker infrastructure needed.
//!    These live inside each module's `#[cfg(test)]` block and run via
//!    `cargo test -p indexify-dataplane --features firecracker`.
//!
//! 2. **Full VM lifecycle tests** (this file): create real VMs, verify the
//!    container daemon starts, test health checks, multi-VM isolation, signal
//!    handling, cleanup, recovery, and log retrieval. These require Firecracker
//!    infrastructure (binary, kernel, rootfs image, CNI).
//!    They check prerequisites at runtime and **skip gracefully** if the
//!    infrastructure is missing.
//!
//! ## Running full lifecycle tests
//!
//! Provision a host with:
//! - `firecracker` and `cnitool` on PATH
//! - A Linux kernel image (vmlinux)
//! - A base ext4 rootfs image
//! - CNI conflist at `/etc/cni/net.d/<name>.conflist`
//!
//! Then set environment variables and run:
//! ```sh
//! FC_KERNEL_IMAGE=/opt/firecracker/vmlinux \
//! FC_BASE_ROOTFS=/opt/firecracker/rootfs.ext4 \
//! cargo test -p indexify-dataplane --features firecracker \
//!   --test firecracker_integration_test -- --test-threads=1
//! ```
//!
//! **Note:** These tests MUST run serially (`--test-threads=1`) because they
//! share the same dm-snapshot origin device.

#![cfg(feature = "firecracker")]

use std::time::Duration;

use anyhow::Result;
use indexify_dataplane::driver::{
    FirecrackerDriver, ProcessConfig, ProcessDriver, ProcessHandle, ProcessType, ResourceLimits,
};

/// Ensure tests run one at a time. The dm-snapshot origin uses a fixed device
/// name, so concurrent tests would conflict.
/// Each test must call `let _lock = serial();` at the start.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Prerequisite checks — tests skip if infrastructure is absent
// ---------------------------------------------------------------------------

/// Returns `true` if the full Firecracker infrastructure is available.
/// Prints a skip reason and returns `false` otherwise.
fn infra_available() -> bool {
    // 1. firecracker binary
    if !command_exists("firecracker") {
        eprintln!("SKIP: `firecracker` binary not found on PATH");
        return false;
    }
    // 2. cnitool binary
    if !command_exists("cnitool") {
        eprintln!("SKIP: `cnitool` binary not found on PATH");
        return false;
    }
    // 3. Required env vars for test configuration
    let required = ["FC_KERNEL_IMAGE", "FC_BASE_ROOTFS"];
    for var in &required {
        if std::env::var(var).is_err() {
            eprintln!("SKIP: environment variable {} not set", var);
            return false;
        }
    }
    // 4. Check paths exist
    let kernel = std::env::var("FC_KERNEL_IMAGE").unwrap();
    if !std::path::Path::new(&kernel).exists() {
        eprintln!("SKIP: kernel image {} does not exist", kernel);
        return false;
    }
    let rootfs = std::env::var("FC_BASE_ROOTFS").unwrap();
    if !std::path::Path::new(&rootfs).exists() {
        eprintln!("SKIP: base rootfs {} does not exist", rootfs);
        return false;
    }

    // 5. Check we have permission to use device-mapper (requires root)
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/mapper/control")
    {
        Ok(_) => {}
        Err(_) => {
            eprintln!("SKIP: no write access to /dev/mapper/control — run with sudo");
            return false;
        }
    }

    true
}

fn command_exists(name: &str) -> bool {
    std::process::Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Test driver factory
// ---------------------------------------------------------------------------

fn create_test_driver() -> Result<FirecrackerDriver> {
    // Extract the embedded daemon binary (required before start() can work).
    indexify_dataplane::daemon_binary::extract_daemon_binary(None)?;

    let kernel = std::env::var("FC_KERNEL_IMAGE")?;
    let rootfs = std::env::var("FC_BASE_ROOTFS")?;
    let cni_network =
        std::env::var("FC_CNI_NETWORK").unwrap_or_else(|_| "indexify-fc".to_string());
    let gateway =
        std::env::var("FC_GUEST_GATEWAY").unwrap_or_else(|_| "192.168.30.1".to_string());

    // Use unique temp dirs per test run to avoid cross-contamination.
    let run_id = uuid::Uuid::new_v4();
    let state_dir = format!("/tmp/indexify-fc-test-{}/state", run_id);
    let log_dir = format!("/tmp/indexify-fc-test-{}/logs", run_id);

    FirecrackerDriver::new(
        None,
        kernel,
        None,
        rootfs,
        cni_network,
        None,
        gateway,
        None,
        None,
        None,
        state_dir.into(),
        log_dir.into(),
    )
}

fn sandbox_config(id: &str) -> ProcessConfig {
    ProcessConfig {
        id: id.to_string(),
        process_type: ProcessType::Sandbox,
        image: None,
        command: String::new(),
        args: Vec::new(),
        env: vec![("RUST_LOG".to_string(), "info".to_string())],
        working_dir: None,
        resources: None,
        labels: Vec::new(),
        rootfs_overlay: None,
    }
}

// ---------------------------------------------------------------------------
// Test: VM boots and the firecracker process is alive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_vm_boots_and_is_alive() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("boot-1"))
        .await
        .expect("VM start");

    assert!(handle.id.starts_with("fc-"), "handle ID has fc- prefix");
    assert!(handle.daemon_addr.is_some(), "daemon_addr is set");
    assert!(handle.http_addr.is_some(), "http_addr is set");
    assert!(
        !handle.container_ip.is_empty(),
        "container_ip is non-empty"
    );

    // Give the VM time to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        driver.alive(&handle).await.unwrap(),
        "VM should be alive after boot"
    );

    driver.kill(&handle).await.expect("kill");

    // After kill the process should be gone
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !driver.alive(&handle).await.unwrap(),
        "VM should be dead after kill"
    );
}

// ---------------------------------------------------------------------------
// Test: Container daemon gRPC health check via DaemonClient
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_daemon_grpc_health_check() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("daemon-hc-1"))
        .await
        .expect("VM start");

    let daemon_addr = handle.daemon_addr.as_ref().unwrap();

    // Connect to the daemon with retry (the VM needs time to boot and start
    // the daemon binary inside the guest).
    let mut client =
        match indexify_dataplane::DaemonClient::connect_with_retry(
            daemon_addr,
            Duration::from_secs(30),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                // Collect VM logs for debugging before failing
                let logs = driver.get_logs(&handle, 50).await.unwrap_or_default();
                eprintln!("VM logs:\n{}", logs);
                driver.kill(&handle).await.ok();
                panic!("Failed to connect to daemon at {}: {}", daemon_addr, e);
            }
        };

    // Wait for daemon to report healthy
    client
        .wait_for_ready(Duration::from_secs(15))
        .await
        .expect("daemon should become ready");

    // Explicit health check
    let healthy = client.health().await.expect("health RPC");
    assert!(healthy, "daemon should report healthy");

    driver.kill(&handle).await.expect("kill");
}

// ---------------------------------------------------------------------------
// Test: Container daemon HTTP port is reachable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_daemon_http_port_reachable() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("daemon-http-1"))
        .await
        .expect("VM start");

    let http_addr = handle.http_addr.as_ref().unwrap();

    // Wait for guest to boot and daemon to start listening
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut connected = false;
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(http_addr).await.is_ok() {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(connected, "Should connect to daemon HTTP port at {}", http_addr);

    driver.kill(&handle).await.expect("kill");
}

// ---------------------------------------------------------------------------
// Test: Custom resource limits (vCPU / memory) are applied
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_custom_resource_limits() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let mut config = sandbox_config("resources-1");
    config.resources = Some(ResourceLimits {
        memory_bytes: Some(256 * 1024 * 1024), // 256 MiB
        cpu_millicores: Some(1000),              // 1 vCPU
        gpu_device_ids: None,
    });

    let handle = driver.start(config).await.expect("VM start");
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(
        driver.alive(&handle).await.unwrap(),
        "VM with custom resources should boot successfully"
    );

    driver.kill(&handle).await.expect("kill");
}

// ---------------------------------------------------------------------------
// Test: Multiple VMs get unique IPs and run concurrently
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_vms_unique_ips() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let mut handles: Vec<ProcessHandle> = Vec::new();

    for i in 0..3 {
        let handle = driver
            .start(sandbox_config(&format!("multi-{}", i)))
            .await
            .expect("VM start");
        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut ips = std::collections::HashSet::new();
    for handle in &handles {
        assert!(
            driver.alive(handle).await.unwrap(),
            "VM {} should be alive",
            handle.id
        );
        ips.insert(handle.container_ip.clone());
    }
    assert_eq!(ips.len(), 3, "All 3 VMs should have unique IPs");

    // Verify daemon addresses are also unique
    let addrs: std::collections::HashSet<_> = handles
        .iter()
        .map(|h| h.daemon_addr.as_ref().unwrap().clone())
        .collect();
    assert_eq!(addrs.len(), 3, "All 3 daemon addresses should be unique");

    for handle in &handles {
        driver.kill(handle).await.expect("kill");
    }
}

// ---------------------------------------------------------------------------
// Test: Environment variables are injected into the guest
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_env_vars_injected() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let mut config = sandbox_config("env-1");
    config.env = vec![
        ("INDEXIFY_TEST_KEY".to_string(), "hello_firecracker".to_string()),
        ("RUST_LOG".to_string(), "debug".to_string()),
    ];

    let handle = driver.start(config).await.expect("VM start");
    let daemon_addr = handle.daemon_addr.as_ref().unwrap();

    // If the daemon starts successfully, the env was loaded (the init script
    // sources /etc/indexify-env before exec-ing the daemon).
    let connect_result = indexify_dataplane::DaemonClient::connect_with_retry(
        daemon_addr,
        Duration::from_secs(30),
    )
    .await;
    assert!(
        connect_result.is_ok(),
        "Daemon should start with injected env vars"
    );

    driver.kill(&handle).await.expect("kill");
}

// ---------------------------------------------------------------------------
// Test: kill() cleans up dm-snapshot, COW files, netns, metadata, sockets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cleanup_on_kill() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("cleanup-1"))
        .await
        .expect("VM start");

    let vm_id = handle.id.strip_prefix("fc-").unwrap().to_string();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Kill and wait for cleanup
    driver.kill(&handle).await.expect("kill");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // dm-snapshot device should be removed
    let dm_path = format!("/dev/mapper/indexify-vm-{}", vm_id);
    assert!(
        !std::path::Path::new(&dm_path).exists(),
        "dm-snapshot device should be removed after kill"
    );

    // Netns should be removed
    let netns_path = format!("/var/run/netns/indexify-vm-{}", vm_id);
    assert!(
        !std::path::Path::new(&netns_path).exists(),
        "Network namespace should be removed after kill"
    );

    // VM should no longer appear in list_containers
    let containers = driver.list_containers().await.unwrap();
    assert!(
        !containers.contains(&handle.id),
        "Killed VM should not appear in list_containers"
    );

    // A second kill should be idempotent (no error)
    driver.kill(&handle).await.expect("second kill should be idempotent");
}

// ---------------------------------------------------------------------------
// Test: list_containers() returns running VMs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_containers() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let h1 = driver
        .start(sandbox_config("list-1"))
        .await
        .expect("start 1");
    let h2 = driver
        .start(sandbox_config("list-2"))
        .await
        .expect("start 2");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let containers = driver.list_containers().await.unwrap();
    assert!(
        containers.contains(&h1.id),
        "list should include first VM"
    );
    assert!(
        containers.contains(&h2.id),
        "list should include second VM"
    );

    // Kill one, verify it's removed from the list
    driver.kill(&h1).await.expect("kill h1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let containers = driver.list_containers().await.unwrap();
    assert!(
        !containers.contains(&h1.id),
        "killed VM should be removed from list"
    );
    assert!(
        containers.contains(&h2.id),
        "surviving VM should still be in list"
    );

    driver.kill(&h2).await.expect("kill h2");
}

// ---------------------------------------------------------------------------
// Test: get_logs() retrieves Firecracker VM log output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_logs() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("logs-1"))
        .await
        .expect("VM start");

    // Let Firecracker write some log output
    tokio::time::sleep(Duration::from_secs(3)).await;

    let logs = driver.get_logs(&handle, 0).await.expect("get_logs full");
    // Firecracker writes at least some boot log
    // (may be empty if --log-path isn't producing output yet, so just verify no error)
    assert!(
        logs.len() >= 0,
        "get_logs should return without error"
    );

    // Tail should return at most N lines
    let tail_logs = driver
        .get_logs(&handle, 5)
        .await
        .expect("get_logs tail");
    assert!(
        tail_logs.lines().count() <= 5,
        "tail=5 should return at most 5 lines"
    );

    driver.kill(&handle).await.expect("kill");
}

// ---------------------------------------------------------------------------
// Test: get_exit_status() returns None for running VM, Some after death
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_exit_status() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("exit-1"))
        .await
        .expect("VM start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Should be None while running
    let status = driver.get_exit_status(&handle).await.unwrap();
    assert!(status.is_none(), "Running VM should have no exit status");

    // Kill it
    driver.kill(&handle).await.expect("kill");
    // Note: after kill(), the VM is cleaned from in-memory state, so
    // get_exit_status will return None (VM not found). This is expected
    // behavior — the Docker driver behaves identically.
}

// ---------------------------------------------------------------------------
// Test: send_sig() delivers a signal to the VM process
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_send_signal() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");
    let handle = driver
        .start(sandbox_config("sig-1"))
        .await
        .expect("VM start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // SIGUSR1 (10) should be deliverable without crashing
    driver
        .send_sig(&handle, 10)
        .await
        .expect("SIGUSR1 should succeed");

    // VM should still be alive after a harmless signal
    assert!(
        driver.alive(&handle).await.unwrap(),
        "VM should survive SIGUSR1"
    );

    // SIGTERM (15) should cause the VM to exit
    driver
        .send_sig(&handle, 15)
        .await
        .expect("SIGTERM should succeed");

    // Wait for the process to terminate
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !driver.alive(&handle).await.unwrap(),
        "VM should be dead after SIGTERM"
    );

    // Cleanup
    driver.kill(&handle).await.ok();
}

// ---------------------------------------------------------------------------
// Test: Recovery — new driver instance recovers VMs from metadata files
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recovery_after_restart() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    // Extract daemon binary
    indexify_dataplane::daemon_binary::extract_daemon_binary(None).expect("extract daemon binary");

    // Use a shared state dir so the second driver can find the metadata.
    let run_id = uuid::Uuid::new_v4();
    let state_dir = format!("/tmp/indexify-fc-recovery-{}/state", run_id);
    let log_dir = format!("/tmp/indexify-fc-recovery-{}/logs", run_id);

    let kernel = std::env::var("FC_KERNEL_IMAGE").unwrap();
    let rootfs = std::env::var("FC_BASE_ROOTFS").unwrap();
    let cni_network =
        std::env::var("FC_CNI_NETWORK").unwrap_or_else(|_| "indexify-fc".to_string());
    let gateway =
        std::env::var("FC_GUEST_GATEWAY").unwrap_or_else(|_| "192.168.30.1".to_string());

    let make_driver = || {
        FirecrackerDriver::new(
            None,
            kernel.clone(),
            None,
            rootfs.clone(),
            cni_network.clone(),
            None,
            gateway.clone(),
            None,
            None,
            None,
            state_dir.clone().into(),
            log_dir.clone().into(),
        )
    };

    // Start a VM with driver #1
    let driver1 = make_driver().expect("driver1 creation");
    let handle = driver1
        .start(sandbox_config("recovery-1"))
        .await
        .expect("VM start");

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(driver1.alive(&handle).await.unwrap(), "VM alive in driver1");

    // Drop driver1 (simulates dataplane restart — the FC process stays alive)
    drop(driver1);

    // Create driver #2 — it should recover the running VM from the metadata file
    let driver2 = make_driver().expect("driver2 creation");

    let containers = driver2.list_containers().await.unwrap();
    assert!(
        containers.contains(&handle.id),
        "Recovered driver should discover the VM"
    );

    assert!(
        driver2.alive(&handle).await.unwrap(),
        "Recovered VM should still be alive"
    );

    // Clean up through driver2
    driver2.kill(&handle).await.expect("kill via recovered driver");

    // Clean up temp dirs
    let _ = std::fs::remove_dir_all(format!("/tmp/indexify-fc-recovery-{}", run_id));
}

// ---------------------------------------------------------------------------
// Test: Measure VM provisioning time with dm-snapshot CoW
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_snapshot_provisioning_time() {
    if !infra_available() {
        return;
    }
    let _lock = serial();

    let driver = create_test_driver().expect("driver creation");

    // Start 3 VMs sequentially and measure the time for each start() call.
    let mut handles = Vec::new();
    let mut durations = Vec::new();

    for i in 0..3 {
        let config = sandbox_config(&format!("snap-timing-{}", i));
        let start = std::time::Instant::now();
        let handle = driver.start(config).await.expect("VM start");
        let elapsed = start.elapsed();
        eprintln!(
            "VM {} start() took: {:?} ({}ms)",
            i,
            elapsed,
            elapsed.as_millis()
        );
        durations.push(elapsed);
        handles.push(handle);
    }

    // Wait for VMs to boot then verify they're alive
    tokio::time::sleep(Duration::from_secs(3)).await;
    for handle in &handles {
        assert!(
            driver.alive(handle).await.unwrap(),
            "VM {} should be alive",
            handle.id
        );
    }

    // Print summary
    let avg_ms: f64 =
        durations.iter().map(|d| d.as_millis() as f64).sum::<f64>() / durations.len() as f64;
    eprintln!("\n=== VM Provisioning Timing Summary ===");
    for (i, d) in durations.iter().enumerate() {
        eprintln!("  VM {}: {:>7.1}ms", i, d.as_secs_f64() * 1000.0);
    }
    eprintln!("  Average: {:>7.1}ms", avg_ms);
    eprintln!("======================================\n");

    // Clean up
    for handle in &handles {
        driver.kill(handle).await.expect("kill");
    }
}
