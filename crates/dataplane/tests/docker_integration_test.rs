//! Docker integration tests for the container daemon injection.
//!
//! These tests verify that:
//! - The daemon binary is correctly mounted into containers
//! - The daemon runs as PID 1 inside the container
//! - The gRPC communication works over TCP
//!
//! These tests require Docker to be running and are SKIPPED by default.
//! To run them, set the environment variable: RUN_DOCKER_TESTS=1
//!
//! **IMPORTANT**: These tests require the daemon binary to be compiled for
//! Linux. When building on macOS, set RUN_DOCKER_TESTS=1 to automatically
//! cross-compile the daemon for Linux using Docker.
//!
//! Example:
//!   RUN_DOCKER_TESTS=1 cargo test -p indexify-dataplane --test
//! docker_integration_test

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use indexify_dataplane::{
    daemon_client::DaemonClient,
    driver::{DockerDriver, ProcessConfig, ProcessDriver, ProcessType},
};

/// Check if Docker tests should run
fn should_run_docker_tests() -> bool {
    std::env::var("RUN_DOCKER_TESTS")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Skip message for when Docker tests are disabled
fn skip_message() -> &'static str {
    "Skipping Docker test. Set RUN_DOCKER_TESTS=1 to run."
}

/// Check if daemon binary can run in Docker containers.
/// Returns true if the binary was built for Linux (x86_64-unknown-linux-musl).
fn is_daemon_binary_compatible() -> bool {
    let is_linux = indexify_dataplane::daemon_binary::is_linux_binary();
    if !is_linux {
        eprintln!(
            "Daemon binary was built for {} (not Linux). \
            Rebuild with RUN_DOCKER_TESTS=1 to cross-compile for Linux.",
            indexify_dataplane::daemon_binary::DAEMON_BINARY_TARGET
        );
    }
    is_linux
}

/// Check if Docker is available
async fn is_docker_available() -> bool {
    tokio::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Extract the daemon binary for testing
fn extract_daemon_binary() -> Result<PathBuf> {
    indexify_dataplane::daemon_binary::extract_daemon_binary(None).map(|p| p.to_path_buf())
}

/// Test: Docker container starts with daemon binary mounted
#[tokio::test]
#[cfg(test)]
async fn test_docker_daemon_binary_mounted() {
    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    // Extract daemon binary first
    let daemon_path = match extract_daemon_binary() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Skipping: Failed to extract daemon binary: {}", e);
            return;
        }
    };
    assert!(daemon_path.exists(), "Daemon binary should exist");

    let driver = DockerDriver::new().expect("Failed to create DockerDriver");

    // Start a container with the daemon
    let config = ProcessConfig {
        id: format!("test-{}", uuid::Uuid::new_v4()),
        process_type: ProcessType::default(),
        image: Some("alpine:latest".to_string()),
        command: "sleep".to_string(),
        args: vec!["30".to_string()],
        env: vec![],
        working_dir: None,
        resources: None,
        labels: vec![],
    };

    let handle = match driver.start(config).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Failed to start container: {}", e);
            panic!("Container start failed: {}", e);
        }
    };

    // Verify container is running
    assert!(
        driver.alive(&handle).await.unwrap(),
        "Container should be running"
    );

    // Verify daemon binary is mounted by checking if it exists in the container
    let check_daemon = tokio::process::Command::new("docker")
        .args(["exec", &handle.id, "ls", "-la", "/indexify-daemon"])
        .output()
        .await
        .expect("Failed to exec into container");

    assert!(
        check_daemon.status.success(),
        "Daemon binary should be mounted at /indexify-daemon. stderr: {}",
        String::from_utf8_lossy(&check_daemon.stderr)
    );

    // Verify daemon is PID 1
    let check_pid1 = tokio::process::Command::new("docker")
        .args(["exec", &handle.id, "cat", "/proc/1/comm"])
        .output()
        .await
        .expect("Failed to check PID 1");

    let pid1_name = String::from_utf8_lossy(&check_pid1.stdout);
    assert!(
        pid1_name.trim() == "indexify-conta" || pid1_name.contains("indexify"),
        "PID 1 should be the daemon, got: {}",
        pid1_name.trim()
    );

    // Cleanup
    driver
        .kill(&handle)
        .await
        .expect("Failed to kill container");
}

/// Test: Daemon is accessible via TCP
#[tokio::test]
async fn test_docker_daemon_accessible() {
    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    let _ = extract_daemon_binary();
    let driver = DockerDriver::new().expect("Failed to create DockerDriver");

    let config = ProcessConfig {
        id: format!("test-{}", uuid::Uuid::new_v4()),
        process_type: ProcessType::default(),
        image: Some("alpine:latest".to_string()),
        command: "sleep".to_string(),
        args: vec!["30".to_string()],
        env: vec![],
        working_dir: None,
        resources: None,
        labels: vec![],
    };

    let handle = driver
        .start(config)
        .await
        .expect("Failed to start container");

    // Check that daemon_addr is set
    assert!(
        handle.daemon_addr.is_some(),
        "Daemon address should be set in handle"
    );

    let daemon_addr = handle.daemon_addr.as_ref().unwrap();
    eprintln!("Daemon address: {}", daemon_addr);

    // Verify container is running
    assert!(
        driver.alive(&handle).await.unwrap(),
        "Container should be running"
    );

    // Get container logs for debugging
    tokio::time::sleep(Duration::from_secs(2)).await;
    let logs = tokio::process::Command::new("docker")
        .args(["logs", &handle.id])
        .output()
        .await;

    if let Ok(output) = logs {
        eprintln!(
            "Container stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "Container stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Cleanup
    driver
        .kill(&handle)
        .await
        .expect("Failed to kill container");
}

/// Test: gRPC health check works over TCP
#[tokio::test]
async fn test_docker_grpc_health_check() {
    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    let _ = extract_daemon_binary();
    let driver = DockerDriver::new().expect("Failed to create DockerDriver");

    let config = ProcessConfig {
        id: format!("test-{}", uuid::Uuid::new_v4()),
        process_type: ProcessType::default(),
        image: Some("alpine:latest".to_string()),
        command: "sleep".to_string(),
        args: vec!["60".to_string()],
        env: vec![],
        working_dir: None,
        resources: None,
        labels: vec![],
    };

    let handle = driver
        .start(config)
        .await
        .expect("Failed to start container");
    let daemon_addr = handle
        .daemon_addr
        .as_ref()
        .expect("Daemon address should be set");

    // Connect to daemon with retry
    let mut client =
        match DaemonClient::connect_with_retry(daemon_addr, Duration::from_secs(15)).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to connect to daemon: {}", e);
                // Get container logs for debugging
                let logs = tokio::process::Command::new("docker")
                    .args(["logs", &handle.id])
                    .output()
                    .await;
                if let Ok(output) = logs {
                    eprintln!(
                        "Container logs: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                driver.kill(&handle).await.ok();
                panic!("Connection failed: {}", e);
            }
        };

    // Wait for daemon to be ready
    client
        .wait_for_ready(Duration::from_secs(10))
        .await
        .expect("Daemon not ready");

    // Health check should succeed
    let health_result = client.health().await;
    assert!(
        health_result.is_ok(),
        "Health check should succeed: {:?}",
        health_result.err()
    );
    assert!(health_result.unwrap(), "Daemon should report healthy");

    // Cleanup
    driver
        .kill(&handle)
        .await
        .expect("Failed to kill container");
}

/// Test: Multiple containers can run simultaneously
#[tokio::test]
async fn test_docker_multiple_containers() {
    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    let _ = extract_daemon_binary();
    let driver = Arc::new(DockerDriver::new().expect("Failed to create DockerDriver"));

    // Start 3 containers
    let mut handles = Vec::new();
    for i in 0..3 {
        let config = ProcessConfig {
            id: format!("test-multi-{}", i),
            process_type: ProcessType::default(),
            image: Some("alpine:latest".to_string()),
            command: "sleep".to_string(),
            args: vec!["30".to_string()],
            env: vec![("CONTAINER_NUM".to_string(), i.to_string())],
            working_dir: None,
            resources: None,
            labels: vec![],
        };

        let handle = driver
            .start(config)
            .await
            .expect("Failed to start container");
        handles.push(handle);
    }

    // Verify all are running
    for handle in &handles {
        assert!(
            driver.alive(handle).await.unwrap(),
            "Container {} should be alive",
            handle.id
        );
    }

    // Verify each has a unique daemon address
    let daemon_addrs: Vec<_> = handles
        .iter()
        .map(|h| h.daemon_addr.as_ref().unwrap().clone())
        .collect();

    for i in 0..daemon_addrs.len() {
        for j in (i + 1)..daemon_addrs.len() {
            assert_ne!(
                daemon_addrs[i], daemon_addrs[j],
                "Daemon addresses should be unique"
            );
        }
    }

    // Cleanup
    for handle in handles {
        driver
            .kill(&handle)
            .await
            .expect("Failed to kill container");
    }
}

/// Test: Environment variables are passed to daemon
#[tokio::test]
async fn test_docker_env_vars_passed() {
    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    let _ = extract_daemon_binary();
    let driver = DockerDriver::new().expect("Failed to create DockerDriver");

    let config = ProcessConfig {
        id: format!("test-{}", uuid::Uuid::new_v4()),
        process_type: ProcessType::default(),
        image: Some("alpine:latest".to_string()),
        command: "sleep".to_string(),
        args: vec!["30".to_string()],
        env: vec![
            ("TEST_VAR".to_string(), "test_value".to_string()),
            ("ANOTHER_VAR".to_string(), "another_value".to_string()),
        ],
        working_dir: None,
        resources: None,
        labels: vec![],
    };

    let handle = driver
        .start(config)
        .await
        .expect("Failed to start container");

    // Check environment variables inside container
    let check_env = tokio::process::Command::new("docker")
        .args(["exec", &handle.id, "printenv", "TEST_VAR"])
        .output()
        .await
        .expect("Failed to exec into container");

    let env_value = String::from_utf8_lossy(&check_env.stdout);
    assert_eq!(
        env_value.trim(),
        "test_value",
        "Environment variable should be set"
    );

    // Cleanup
    driver
        .kill(&handle)
        .await
        .expect("Failed to kill container");
}

/// Test: Resource limits are applied to container
#[tokio::test]
async fn test_docker_resource_limits() {
    use indexify_dataplane::driver::ResourceLimits;

    if !should_run_docker_tests() {
        eprintln!("{}", skip_message());
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    if !is_daemon_binary_compatible() {
        eprintln!("Skipping: Daemon binary not compatible with Docker container architecture");
        return;
    }

    let _ = extract_daemon_binary();
    let driver = DockerDriver::new().expect("Failed to create DockerDriver");

    let config = ProcessConfig {
        id: format!("test-{}", uuid::Uuid::new_v4()),
        process_type: ProcessType::default(),
        image: Some("alpine:latest".to_string()),
        command: "sleep".to_string(),
        args: vec!["30".to_string()],
        env: vec![],
        working_dir: None,
        resources: Some(ResourceLimits {
            memory_mb: Some(256),      // 256 MB
            cpu_millicores: Some(500), // 0.5 CPU cores
        }),
        labels: vec![],
    };

    let handle = driver
        .start(config)
        .await
        .expect("Failed to start container");

    // Check memory limit using docker inspect
    let check_memory = tokio::process::Command::new("docker")
        .args(["inspect", "-f", "{{.HostConfig.Memory}}", &handle.id])
        .output()
        .await
        .expect("Failed to inspect container");

    let memory_bytes: u64 = String::from_utf8_lossy(&check_memory.stdout)
        .trim()
        .parse()
        .expect("Failed to parse memory value");

    // 256 MB = 256 * 1024 * 1024 = 268435456 bytes
    assert_eq!(
        memory_bytes,
        256 * 1024 * 1024,
        "Memory limit should be 256MB"
    );

    // Check CPU limit using docker inspect (NanoCpus = cpus * 1e9)
    let check_cpu = tokio::process::Command::new("docker")
        .args(["inspect", "-f", "{{.HostConfig.NanoCpus}}", &handle.id])
        .output()
        .await
        .expect("Failed to inspect container");

    let nano_cpus: u64 = String::from_utf8_lossy(&check_cpu.stdout)
        .trim()
        .parse()
        .expect("Failed to parse CPU value");

    // 500 millicores = 0.5 CPUs = 0.5 * 1e9 = 500000000 NanoCpus
    assert_eq!(
        nano_cpus, 500_000_000,
        "CPU limit should be 0.5 cores (500 millicores)"
    );

    // Cleanup
    driver
        .kill(&handle)
        .await
        .expect("Failed to kill container");
}
