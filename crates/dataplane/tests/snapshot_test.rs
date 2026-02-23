//! Snapshot integration tests for the Docker snapshotter.
//!
//! Tests the full snapshot lifecycle with a local filesystem blob store:
//!   1. Create a sandbox container
//!   2. Modify state inside it (write a marker file)
//!   3. Snapshot (docker export → zstd compress → local file)
//!   4. Terminate the original container
//!   5. Restore (local file → zstd decompress → docker import)
//!   6. Start a new container from the restored image
//!   7. Verify the marker file survived the snapshot/restore cycle
//!
//! Requires Docker. Set RUN_DOCKER_TESTS=1 to run.
//!
//! Example:
//!   RUN_DOCKER_TESTS=1 cargo test -p indexify-dataplane --test snapshot_test

use std::{sync::Arc, time::Duration};

use bollard::{
    Docker,
    models::ContainerCreateBody,
    query_parameters::{
        CreateContainerOptions,
        KillContainerOptions,
        RemoveContainerOptions,
        RemoveImageOptions,
        StartContainerOptions,
    },
};
use indexify_dataplane::{
    blob_ops::LazyBlobStore,
    metrics::DataplaneMetrics,
    snapshotter::{Snapshotter, docker_snapshotter::DockerSnapshotter},
};

fn should_run_docker_tests() -> bool {
    std::env::var("RUN_DOCKER_TESTS")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false)
}

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

/// Helper to exec a command inside a Docker container and return stdout.
async fn docker_exec(container: &str, cmd: &[&str]) -> (bool, String, String) {
    let output = tokio::process::Command::new("docker")
        .arg("exec")
        .arg(container)
        .args(cmd)
        .output()
        .await
        .expect("Failed to exec into container");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Guard that removes a Docker container on drop.
struct ContainerGuard {
    docker: Docker,
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let docker = self.docker.clone();
        let name = self.name.clone();
        // Best-effort cleanup — fire and forget.
        tokio::task::spawn(async move {
            let _ = docker
                .kill_container(
                    &name,
                    Some(KillContainerOptions {
                        signal: "SIGKILL".to_string(),
                    }),
                )
                .await;
            let _ = docker
                .remove_container(
                    &name,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;
        });
    }
}

/// Full snapshot lifecycle: create → snapshot → terminate → restore → verify.
#[tokio::test]
async fn test_snapshot_create_and_restore() {
    if !should_run_docker_tests() {
        eprintln!("Skipping snapshot test. Set RUN_DOCKER_TESTS=1 to run.");
        return;
    }

    if !is_docker_available().await {
        eprintln!("Skipping: Docker is not available");
        return;
    }

    let _ = tracing_subscriber::fmt::try_init();

    let docker = Docker::connect_with_local_defaults().expect("Failed to connect to Docker daemon");

    let temp_dir = tempfile::tempdir().unwrap();
    let snapshot_path = temp_dir
        .path()
        .join("snapshots")
        .join("test-ns")
        .join("snap-001.tar.zst");
    let snapshot_uri = format!("file://{}", snapshot_path.display());

    let metrics = Arc::new(DataplaneMetrics::new());
    let blob_store = LazyBlobStore::new(metrics.clone());
    let snapshotter = DockerSnapshotter::new(docker.clone(), blob_store, metrics, None, None, None);

    // ── 1. Create and start a sandbox container ────────────────────────
    let container_name = format!("snapshot-test-{}", uuid::Uuid::new_v4());
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: Some(container_name.clone()),
                platform: String::new(),
            }),
            ContainerCreateBody {
                image: Some("alpine:latest".to_string()),
                cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create container");

    docker
        .start_container(&container_name, None::<StartContainerOptions>)
        .await
        .expect("Failed to start container");

    let _guard = ContainerGuard {
        docker: docker.clone(),
        name: container_name.clone(),
    };

    // Give the container a moment to initialise.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── 2. Write a marker file inside the container ────────────────────
    let (ok, _, stderr) = docker_exec(
        &container_name,
        &["sh", "-c", "echo 'SNAPSHOT_MARKER_12345' > /tmp/marker.txt"],
    )
    .await;
    assert!(ok, "Failed to write marker file: {}", stderr);

    // Verify it's there before snapshotting.
    let (ok, stdout, _) = docker_exec(&container_name, &["cat", "/tmp/marker.txt"]).await;
    assert!(ok, "Marker file should be readable before snapshot");
    assert!(
        stdout.contains("SNAPSHOT_MARKER_12345"),
        "Marker content mismatch"
    );

    // ── 3. Snapshot the container ──────────────────────────────────────
    let snapshot_result = snapshotter
        .create_snapshot(&container_name, "snap-001", &snapshot_uri)
        .await
        .expect("Failed to create snapshot");

    assert!(!snapshot_result.snapshot_uri.is_empty());
    assert!(
        snapshot_result.size_bytes > 0,
        "Snapshot should have non-zero size"
    );
    assert!(
        snapshot_path.exists(),
        "Snapshot file should exist on disk at {}",
        snapshot_path.display()
    );
    eprintln!(
        "Snapshot created: {} bytes at {}",
        snapshot_result.size_bytes,
        snapshot_path.display()
    );

    // ── 4. Kill and remove the original container ──────────────────────
    let _ = docker
        .kill_container(
            &container_name,
            Some(KillContainerOptions {
                signal: "SIGKILL".to_string(),
            }),
        )
        .await;
    docker
        .remove_container(
            &container_name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .expect("Failed to remove original container");
    // Disarm the guard — container is already removed.
    std::mem::forget(_guard);

    // ── 5. Restore from snapshot ───────────────────────────────────────
    let restore_result = snapshotter
        .restore_snapshot(&snapshot_uri)
        .await
        .expect("Failed to restore snapshot");

    let restored_image = &restore_result.image;
    eprintln!("Restored as Docker image: {}", restored_image);

    // ── 6. Start a new container from the restored image ───────────────
    let restored_name = format!("restored-test-{}", uuid::Uuid::new_v4());
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: Some(restored_name.clone()),
                platform: String::new(),
            }),
            ContainerCreateBody {
                image: Some(restored_image.clone()),
                cmd: Some(vec!["sleep".to_string(), "300".to_string()]),
                ..Default::default()
            },
        )
        .await
        .expect("Failed to create restored container");

    docker
        .start_container(&restored_name, None::<StartContainerOptions>)
        .await
        .expect("Failed to start restored container");

    let _restored_guard = ContainerGuard {
        docker: docker.clone(),
        name: restored_name.clone(),
    };

    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── 7. Verify marker file in the restored container ────────────────
    let (ok, stdout, stderr) = docker_exec(&restored_name, &["cat", "/tmp/marker.txt"]).await;
    assert!(
        ok,
        "Marker file should exist in restored container. stderr: {}",
        stderr
    );
    assert!(
        stdout.contains("SNAPSHOT_MARKER_12345"),
        "Marker file should contain expected content, got: {}",
        stdout
    );

    eprintln!("SUCCESS: Marker file preserved through snapshot/restore cycle");

    // ── 8. Cleanup ─────────────────────────────────────────────────────
    let _ = docker
        .kill_container(
            &restored_name,
            Some(KillContainerOptions {
                signal: "SIGKILL".to_string(),
            }),
        )
        .await;
    let _ = docker
        .remove_container(
            &restored_name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await;
    std::mem::forget(_restored_guard);

    // Remove the snapshot Docker image.
    let _ = docker
        .remove_image(restored_image, None::<RemoveImageOptions>, None)
        .await;
}
