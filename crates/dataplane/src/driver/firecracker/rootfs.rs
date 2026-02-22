//! Rootfs injection for Firecracker VMs.
//!
//! Each per-VM volume is a CoW snapshot of the origin volume (which holds
//! the base OS image). This module mounts the snapshot and injects the
//! container-daemon binary, an init script, and environment variables so
//! the VM boots directly into the Indexify daemon.

use std::path::Path;
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

/// The init script injected at `/sbin/indexify-init` in the guest rootfs.
///
/// This script runs as PID 1 via the kernel `init=` parameter. It mounts
/// essential pseudo-filesystems, configures networking from the kernel `ip=`
/// boot parameter, loads environment variables, then `exec`s the
/// container-daemon which takes over as the real PID 1 (with zombie reaping
/// and signal handling from pid1.rs).
const INIT_SCRIPT: &str = r#"#!/bin/sh
# /sbin/indexify-init — Firecracker VM init for Indexify
set -e

# 1. Mount essential pseudo-filesystems (tolerate already-mounted)
mount -t proc     proc     /proc     2>/dev/null || true
mount -t sysfs    sysfs    /sys      2>/dev/null || true
mount -t devtmpfs devtmpfs /dev      2>/dev/null || true
mkdir -p /dev/pts /dev/shm
mount -t devpts   devpts   /dev/pts  2>/dev/null || true
mount -t tmpfs    tmpfs    /dev/shm  2>/dev/null || true
mount -t tmpfs    tmpfs    /tmp      2>/dev/null || true
mount -t tmpfs    tmpfs    /run      2>/dev/null || true

# 2. Configure networking from kernel ip= boot param
ip_param=$(cat /proc/cmdline | tr ' ' '\n' | grep '^ip=' | head -1)

if [ -n "$ip_param" ]; then
    client_ip=$(echo "$ip_param" | cut -d= -f2 | cut -d: -f1)
    gateway=$(echo "$ip_param" | cut -d: -f3)
    netmask=$(echo "$ip_param" | cut -d: -f4)
    device=$(echo "$ip_param" | cut -d: -f6)
    [ -z "$device" ] && device="eth0"

    ip addr add "${client_ip}/${netmask}" dev "$device" 2>/dev/null || true
    ip link set "$device" up 2>/dev/null || true
    [ -n "$gateway" ] && ip route add default via "$gateway" dev "$device" 2>/dev/null || true

    : > /etc/resolv.conf
    [ -n "$gateway" ] && echo "nameserver $gateway" >> /etc/resolv.conf
    echo "nameserver 8.8.8.8" >> /etc/resolv.conf
fi

# 3. Set hostname
hostname indexify-vm

# 4. Create required directories
mkdir -p /var/log/indexify

# 5. Load environment variables
if [ -f /etc/indexify-env ]; then
    set -a
    . /etc/indexify-env
    set +a
fi

# 6. Exec the container-daemon as PID 1
exec /indexify-daemon --port 9500 --http-port 9501 --log-dir /var/log/indexify
"#;

/// Mount a thin volume, inject daemon + init + env, unmount.
///
/// Each per-VM volume is a CoW snapshot of the origin — the base image
/// data is already present via block sharing. This function only writes
/// the per-VM files (daemon binary, init script, env vars).
pub async fn inject_rootfs(
    thin_dev_path: &Path,
    daemon_binary: &Path,
    env_vars: &[(String, String)],
    vm_id: &str,
) -> Result<()> {
    let mount_point = format!("/tmp/indexify-mount-{}", vm_id);
    std::fs::create_dir_all(&mount_point)
        .with_context(|| format!("Failed to create mount point {}", mount_point))?;
    mount_device(thin_dev_path, &mount_point).await?;

    let result = inject_files(&mount_point, daemon_binary, env_vars).await;

    if let Err(e) = unmount(&mount_point).await {
        tracing::warn!(mount_point, error = ?e, "Failed to unmount rootfs");
    }
    let _ = std::fs::remove_dir(&mount_point);

    result
}

/// Mount a block device at the given mount point.
async fn mount_device(device: &Path, mount_point: &str) -> Result<()> {
    let output = Command::new("mount")
        .args([device.to_str().unwrap(), mount_point])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("Failed to execute mount")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Failed to mount {} at {}: {}",
            device.display(),
            mount_point,
            stderr
        );
    }
    Ok(())
}

/// Unmount a filesystem.
async fn unmount(mount_point: &str) -> Result<()> {
    let output = Command::new("umount")
        .arg(mount_point)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("Failed to execute umount")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Failed to unmount {}: {}", mount_point, stderr);
    }
    Ok(())
}

/// Inject daemon binary, init script, and environment variables into the
/// mounted rootfs.
async fn inject_files(
    mount_point: &str,
    daemon_binary: &Path,
    env_vars: &[(String, String)],
) -> Result<()> {
    let root = Path::new(mount_point);

    // Copy daemon binary
    let daemon_dest = root.join("indexify-daemon");
    std::fs::copy(daemon_binary, &daemon_dest)
        .with_context(|| format!("Failed to copy daemon binary to {}", daemon_dest.display()))?;
    set_executable(&daemon_dest)?;

    // Write init script
    let init_dest = root.join("sbin/indexify-init");
    if let Some(parent) = init_dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&init_dest, INIT_SCRIPT)
        .with_context(|| format!("Failed to write init script to {}", init_dest.display()))?;
    set_executable(&init_dest)?;

    // Write environment variables
    let env_dest = root.join("etc/indexify-env");
    if let Some(parent) = env_dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let env_content: String = env_vars
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&env_dest, &env_content)
        .with_context(|| format!("Failed to write env file to {}", env_dest.display()))?;

    // Create log directory
    let log_dir = root.join("var/log/indexify");
    std::fs::create_dir_all(&log_dir)?;

    Ok(())
}

/// Set a file as executable (0o755).
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn test_init_script_is_valid_shell() {
        assert!(
            INIT_SCRIPT.starts_with("#!/bin/sh"),
            "Init script must start with shebang"
        );
    }

    #[test]
    fn test_init_script_mounts_proc() {
        assert!(
            INIT_SCRIPT.contains("mount -t proc"),
            "Init script must mount /proc"
        );
    }

    #[test]
    fn test_init_script_mounts_sysfs() {
        assert!(
            INIT_SCRIPT.contains("mount -t sysfs"),
            "Init script must mount /sys"
        );
    }

    #[test]
    fn test_init_script_mounts_devtmpfs() {
        assert!(
            INIT_SCRIPT.contains("mount -t devtmpfs"),
            "Init script must mount /dev"
        );
    }

    #[test]
    fn test_init_script_parses_ip_boot_param() {
        assert!(
            INIT_SCRIPT.contains("ip_param=") && INIT_SCRIPT.contains("/proc/cmdline"),
            "Init script must parse ip= from /proc/cmdline"
        );
    }

    #[test]
    fn test_init_script_loads_env_file() {
        assert!(
            INIT_SCRIPT.contains("/etc/indexify-env"),
            "Init script must source /etc/indexify-env"
        );
    }

    #[test]
    fn test_init_script_execs_daemon() {
        assert!(
            INIT_SCRIPT.contains("exec /indexify-daemon"),
            "Init script must exec the daemon"
        );
        assert!(
            INIT_SCRIPT.contains("--port 9500"),
            "Init script must set gRPC port 9500"
        );
        assert!(
            INIT_SCRIPT.contains("--http-port 9501"),
            "Init script must set HTTP port 9501"
        );
    }

    #[tokio::test]
    async fn test_inject_files_creates_all_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let mount_point = dir.path().to_str().unwrap();

        // Create a fake daemon binary
        let daemon_path = dir.path().join("fake-daemon");
        std::fs::write(&daemon_path, b"#!/bin/sh\necho daemon").unwrap();

        let env_vars = vec![
            ("KEY1".to_string(), "value1".to_string()),
            ("KEY2".to_string(), "value2".to_string()),
        ];

        inject_files(mount_point, &daemon_path, &env_vars)
            .await
            .unwrap();

        // Check daemon binary was copied
        let daemon_dest = dir.path().join("indexify-daemon");
        assert!(daemon_dest.exists(), "Daemon binary should be copied");
        let perms = std::fs::metadata(&daemon_dest).unwrap().permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o755,
            "Daemon should be executable"
        );

        // Check init script was written
        let init_dest = dir.path().join("sbin/indexify-init");
        assert!(init_dest.exists(), "Init script should be written");
        let init_content = std::fs::read_to_string(&init_dest).unwrap();
        assert!(init_content.starts_with("#!/bin/sh"));
        let perms = std::fs::metadata(&init_dest).unwrap().permissions();
        assert_eq!(
            perms.mode() & 0o777,
            0o755,
            "Init script should be executable"
        );

        // Check env file was written
        let env_dest = dir.path().join("etc/indexify-env");
        assert!(env_dest.exists(), "Env file should be written");
        let env_content = std::fs::read_to_string(&env_dest).unwrap();
        assert!(env_content.contains("KEY1=value1"));
        assert!(env_content.contains("KEY2=value2"));

        // Check log directory was created
        let log_dir = dir.path().join("var/log/indexify");
        assert!(log_dir.exists(), "Log directory should be created");
    }

    #[tokio::test]
    async fn test_inject_files_empty_env_vars() {
        let dir = tempfile::tempdir().unwrap();
        let mount_point = dir.path().to_str().unwrap();

        let daemon_path = dir.path().join("fake-daemon");
        std::fs::write(&daemon_path, b"binary").unwrap();

        inject_files(mount_point, &daemon_path, &[]).await.unwrap();

        let env_dest = dir.path().join("etc/indexify-env");
        assert!(env_dest.exists(), "Env file should exist even when empty");
        let content = std::fs::read_to_string(&env_dest).unwrap();
        assert!(content.is_empty(), "Env file should be empty");
    }
}
