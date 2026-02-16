//! Build script for dataplane that embeds the container daemon binary.
//!
//! The daemon binary is built separately and embedded using include_bytes!().
//! At runtime, it's extracted to a temporary location and bind-mounted into
//! containers.
//!
//! On macOS, when RUN_DOCKER_TESTS=1 is set, the build script will attempt to
//! cross-compile the daemon for Linux using Docker. This enables running Docker
//! integration tests on macOS.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    // Tell Cargo to rerun if the daemon source changes
    println!("cargo:rerun-if-changed=../container-daemon/src/");
    println!("cargo:rerun-if-changed=../container-daemon/Cargo.toml");
    println!("cargo:rerun-if-env-changed=RUN_DOCKER_TESTS");
    println!("cargo:rerun-if-env-changed=DAEMON_STATIC");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let target_dir = out_dir.join("daemon-binary");

    // Create target directory for daemon binary
    std::fs::create_dir_all(&target_dir).expect("Failed to create daemon binary directory");

    // Determine the target triple for cross-compilation support
    let host_target = env::var("TARGET").unwrap();

    // Check if we need to build for Linux (Docker tests on macOS)
    let run_docker_tests = env::var("RUN_DOCKER_TESTS")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    // Build statically with musl so the daemon works in any container
    // regardless of the base image's libc (e.g., Alpine uses musl, not glibc).
    // Defaults to true on Linux; set DAEMON_STATIC=0 to disable.
    let daemon_static = env::var("DAEMON_STATIC")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(host_target.contains("linux"));

    let host_is_macos = host_target.contains("apple") || host_target.contains("darwin");
    let need_linux_binary = run_docker_tests && host_is_macos;

    let (target, daemon_binary_path) = if need_linux_binary {
        // macOS host with RUN_DOCKER_TESTS=1: cross-compile via Docker
        let linux_target = "x86_64-unknown-linux-musl";

        match build_with_docker(&target_dir, linux_target) {
            Ok(path) => (linux_target.to_string(), path),
            Err(e) => {
                println!(
                    "cargo:warning=Failed to build Linux daemon with Docker: {}",
                    e
                );
                println!("cargo:warning=Falling back to host build. Docker tests will be skipped.");
                build_for_host(&target_dir, &host_target)
            }
        }
    } else if daemon_static {
        // Static build requested (e.g. Docker image build) — use musl
        let musl_target = if host_target.contains("aarch64") {
            "aarch64-unknown-linux-musl"
        } else {
            "x86_64-unknown-linux-musl"
        };
        build_for_host(&target_dir, musl_target)
    } else {
        build_for_host(&target_dir, &host_target)
    };

    println!(
        "cargo:rustc-env=DAEMON_BINARY_PATH={}",
        daemon_binary_path.display()
    );
    println!("cargo:rustc-env=DAEMON_BINARY_TARGET={}", target);

    // Also emit the path as a warning for debugging
    println!(
        "cargo:warning=Daemon binary path: {}",
        daemon_binary_path.display()
    );
    println!("cargo:warning=Built for target: {}", target);
}

/// Build the daemon for the host target using cargo
fn build_for_host(target_dir: &Path, target: &str) -> (String, PathBuf) {
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "-p",
            "indexify-container-daemon",
            "--target-dir",
            target_dir.to_str().unwrap(),
            "--target",
            target,
        ])
        .status()
        .expect("Failed to execute cargo build for container-daemon");

    if !status.success() {
        panic!("Failed to build container-daemon");
    }

    let daemon_binary_path = target_dir
        .join(target)
        .join("release")
        .join("indexify-container-daemon");

    (target.to_string(), daemon_binary_path)
}

/// Build the daemon for Linux using Docker (cross-compilation from macOS)
fn build_with_docker(target_dir: &Path, linux_target: &str) -> Result<PathBuf, String> {
    // Get the workspace root (go up from crates/dataplane/build.rs)
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").map_err(|e| e.to_string())?;
    let workspace_root = PathBuf::from(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .ok_or("Cannot find workspace root")?
        .to_path_buf();

    println!("cargo:warning=Building Linux daemon using Docker...");

    // Use the rust musl image for static linking (always x86_64)
    let docker_image = "messense/rust-musl-cross:x86_64-musl";

    // Build using Docker
    let output_dir = target_dir.join(linux_target).join("release");
    std::fs::create_dir_all(&output_dir).map_err(|e| e.to_string())?;

    // Build script that installs protoc and builds the daemon
    let build_script = format!(
        "apt-get update && apt-get install -y protobuf-compiler && \
         cargo build --release -p indexify-container-daemon --target {} && \
         cp target/{}/release/indexify-container-daemon /output/",
        linux_target, linux_target
    );

    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/workspace", workspace_root.display()),
            "-v",
            &format!("{}:/output", output_dir.display()),
            "-w",
            "/workspace",
            docker_image,
            "sh",
            "-c",
            &build_script,
        ])
        .status()
        .map_err(|e| format!("Failed to run Docker: {}", e))?;

    if !status.success() {
        return Err("Docker build failed".to_string());
    }

    let daemon_binary_path = output_dir.join("indexify-container-daemon");
    if !daemon_binary_path.exists() {
        return Err("Daemon binary not found after Docker build".to_string());
    }

    Ok(daemon_binary_path)
}
