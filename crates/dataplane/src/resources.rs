//! Host resource probing for containerized environments.
//!
//! This module is used by the service binary but not exported from the library,
//! hence the allow(dead_code) annotation.

#![allow(dead_code)]

use std::{fs, path::Path};

use proto_api::executor_api_pb::HostResources;
use sysinfo::{Disks, System};

use crate::metrics::ResourceAvailability;

/// Path prefix for host-mounted proc filesystem when running in a container.
/// Mount with: -v /proc:/host/proc:ro
const HOST_PROC_PATH: &str = "/host/proc";

/// Check if we're running in a container with host filesystems mounted.
fn is_host_mounted() -> bool {
    Path::new(HOST_PROC_PATH).exists()
}

/// Parse memory info from /proc/meminfo format.
/// Returns total memory in bytes.
fn parse_meminfo(path: &str) -> Option<u64> {
    let content = fs::read_to_string(path).ok()?;
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            // Format: "MemTotal:       16384000 kB"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse().ok()?;
                return Some(kb * 1024); // Convert to bytes
            }
        }
    }
    None
}

/// Parse CPU count from /proc/cpuinfo format.
/// Counts the number of "processor" entries.
fn parse_cpuinfo(path: &str) -> Option<u32> {
    let content = fs::read_to_string(path).ok()?;
    let count = content
        .lines()
        .filter(|line| line.starts_with("processor"))
        .count();
    if count > 0 { Some(count as u32) } else { None }
}

/// Parse available memory from /proc/meminfo.
/// Returns available memory in bytes.
fn parse_meminfo_available(path: &str) -> Option<u64> {
    let content = fs::read_to_string(path).ok()?;
    for line in content.lines() {
        if line.starts_with("MemAvailable:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: u64 = parts[1].parse().ok()?;
                return Some(kb * 1024);
            }
        }
    }
    None
}

/// Probe total host resources (used for heartbeat reporting).
///
/// When running in a container, this function reads from host-mounted
/// filesystems at `/host/proc` and `/host/sys` to get actual host resources.
/// When running directly on the host, it uses the sysinfo crate.
///
/// To enable host resource detection in a container, mount:
/// ```text
/// -v /proc:/host/proc:ro -v /sys:/host/sys:ro
/// ```
pub fn probe_host_resources() -> HostResources {
    if is_host_mounted() {
        probe_host_resources_from_mount()
    } else {
        probe_host_resources_from_sysinfo()
    }
}

/// Probe host resources from mounted /host/proc filesystem.
fn probe_host_resources_from_mount() -> HostResources {
    let meminfo_path = format!("{}/meminfo", HOST_PROC_PATH);
    let cpuinfo_path = format!("{}/cpuinfo", HOST_PROC_PATH);

    let memory_bytes = parse_meminfo(&meminfo_path).unwrap_or_else(|| {
        tracing::warn!("Failed to parse {}, falling back to sysinfo", meminfo_path);
        let mut sys = System::new();
        sys.refresh_memory();
        sys.total_memory()
    });

    let cpu_count = parse_cpuinfo(&cpuinfo_path).unwrap_or_else(|| {
        tracing::warn!("Failed to parse {}, falling back to sysinfo", cpuinfo_path);
        let mut sys = System::new();
        sys.refresh_cpu_all();
        sys.cpus().len() as u32
    });

    // For disk, we still use sysinfo as disk detection is more complex
    // and typically the container's view of disks is acceptable
    let disks = Disks::new_with_refreshed_list();
    let disk_bytes: u64 = disks.iter().map(|d| d.total_space()).sum();

    tracing::info!(
        cpu_count,
        memory_bytes,
        disk_bytes,
        source = "host_mount",
        "Probed host resources from mounted /host/proc"
    );

    HostResources {
        cpu_count: Some(cpu_count),
        memory_bytes: Some(memory_bytes),
        disk_bytes: Some(disk_bytes),
        gpu: None,
    }
}

/// Probe host resources using sysinfo (when running directly on host).
fn probe_host_resources_from_sysinfo() -> HostResources {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let cpu_count = sys.cpus().len() as u32;
    let memory_bytes = sys.total_memory();

    let disks = Disks::new_with_refreshed_list();
    let disk_bytes: u64 = disks.iter().map(|d| d.total_space()).sum();

    tracing::info!(
        cpu_count,
        memory_bytes,
        disk_bytes,
        source = "sysinfo",
        "Probed host resources"
    );

    HostResources {
        cpu_count: Some(cpu_count),
        memory_bytes: Some(memory_bytes),
        disk_bytes: Some(disk_bytes),
        gpu: None,
    }
}

/// Probe free/available resources for metrics.
///
/// Similar to probe_host_resources, this uses host-mounted filesystems
/// when available.
pub fn probe_free_resources() -> ResourceAvailability {
    if is_host_mounted() {
        probe_free_resources_from_mount()
    } else {
        probe_free_resources_from_sysinfo()
    }
}

/// Probe free resources from mounted /host/proc filesystem.
fn probe_free_resources_from_mount() -> ResourceAvailability {
    let meminfo_path = format!("{}/meminfo", HOST_PROC_PATH);

    // CPU usage is trickier to get from /proc, use sysinfo for now
    let mut sys = System::new();
    sys.refresh_cpu_all();
    let cpu_usage: f32 =
        sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
    let free_cpu_percent = (100.0 - cpu_usage) as f64;

    let free_memory_bytes = parse_meminfo_available(&meminfo_path).unwrap_or_else(|| {
        tracing::warn!(
            "Failed to parse MemAvailable from {}, falling back to sysinfo",
            meminfo_path
        );
        sys.refresh_memory();
        sys.available_memory()
    });

    let disks = Disks::new_with_refreshed_list();
    let free_disk_bytes: u64 = disks.iter().map(|d| d.available_space()).sum();

    ResourceAvailability {
        free_cpu_percent,
        free_memory_bytes,
        free_disk_bytes,
    }
}

/// Probe free resources using sysinfo.
fn probe_free_resources_from_sysinfo() -> ResourceAvailability {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let cpu_usage: f32 =
        sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
    let free_cpu_percent = (100.0 - cpu_usage) as f64;

    let free_memory_bytes = sys.available_memory();

    let disks = Disks::new_with_refreshed_list();
    let free_disk_bytes: u64 = disks.iter().map(|d| d.available_space()).sum();

    ResourceAvailability {
        free_cpu_percent,
        free_memory_bytes,
        free_disk_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meminfo() {
        let content = r#"MemTotal:       16384000 kB
MemFree:         1234567 kB
MemAvailable:    8000000 kB
Buffers:          123456 kB
"#;
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("meminfo");
        fs::write(&path, content).unwrap();

        let total = parse_meminfo(path.to_str().unwrap());
        assert_eq!(total, Some(16384000 * 1024));

        let available = parse_meminfo_available(path.to_str().unwrap());
        assert_eq!(available, Some(8000000 * 1024));
    }

    #[test]
    fn test_parse_cpuinfo() {
        let content = r#"processor	: 0
vendor_id	: GenuineIntel
model name	: Intel(R) Core(TM) i7

processor	: 1
vendor_id	: GenuineIntel
model name	: Intel(R) Core(TM) i7

processor	: 2
vendor_id	: GenuineIntel
model name	: Intel(R) Core(TM) i7

processor	: 3
vendor_id	: GenuineIntel
model name	: Intel(R) Core(TM) i7
"#;
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("cpuinfo");
        fs::write(&path, content).unwrap();

        let count = parse_cpuinfo(path.to_str().unwrap());
        assert_eq!(count, Some(4));
    }

    #[test]
    fn test_probe_host_resources_runs() {
        // Just verify it doesn't panic
        let resources = probe_host_resources();
        assert!(resources.cpu_count.is_some());
        assert!(resources.memory_bytes.is_some());
    }
}
