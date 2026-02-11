//! Host resource probing for containerized environments.
//!
//! This module is used by the service binary but not exported from the library,
//! hence the allow(dead_code) annotation.

#![allow(dead_code)]

use std::{fs, path::Path, process::Command};

use proto_api::executor_api_pb::{GpuModel, GpuResources, HostResources};
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

/// Check if nvidia-smi is available on the system.
fn nvidia_smi_available() -> bool {
    Command::new("nvidia-smi")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Detect NVIDIA GPUs using nvidia-smi.
///
/// Runs `nvidia-smi --query-gpu=index,name,uuid --format=csv,noheader` and
/// parses the output to determine GPU count and model. Matches the Python
/// executor's nvidia_gpu.py detection logic.
fn detect_nvidia_gpus() -> Option<GpuResources> {
    if !nvidia_smi_available() {
        return None;
    }

    let output = Command::new("nvidia-smi")
        .args(["--query-gpu=index,name,uuid", "--format=csv,noheader"])
        .output()
        .ok()?;

    if !output.status.success() {
        tracing::warn!("nvidia-smi query failed with status {}", output.status);
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();

    if lines.is_empty() {
        return None;
    }

    let count = lines.len() as u32;

    // Detect model from the first GPU's product name.
    // All GPUs on a host are typically the same model.
    let model = lines
        .first()
        .and_then(|line| {
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            // Format: "index, product_name, uuid"
            if parts.len() >= 2 {
                Some(product_name_to_gpu_model(parts[1]))
            } else {
                None
            }
        })
        .unwrap_or(GpuModel::Unknown);

    tracing::info!(
        gpu_count = count,
        gpu_model = ?model,
        "Detected NVIDIA GPUs"
    );

    Some(GpuResources {
        count: Some(count),
        model: Some(model.into()),
    })
}

/// Map nvidia-smi product name to GPUModel enum.
/// Matches Python executor's nvidia_gpu.py _product_name_to_model().
fn product_name_to_gpu_model(product_name: &str) -> GpuModel {
    if product_name.starts_with("NVIDIA A100") && product_name.contains("80GB") {
        GpuModel::NvidiaA10080gb
    } else if product_name.starts_with("NVIDIA A100") && product_name.contains("40GB") {
        GpuModel::NvidiaA10040gb
    } else if product_name.starts_with("NVIDIA H100") && product_name.contains("80GB") {
        GpuModel::NvidiaH10080gb
    } else if product_name.starts_with("Tesla T4") {
        GpuModel::NvidiaTeslaT4
    } else if product_name.starts_with("NVIDIA RTX A6000") {
        GpuModel::NvidiaA6000
    } else if product_name.starts_with("NVIDIA A10") {
        GpuModel::NvidiaA10
    } else {
        tracing::warn!(product_name = %product_name, "Unknown GPU model");
        GpuModel::Unknown
    }
}

/// Probe total host resources (used for heartbeat reporting).
///
/// When running in a container, reads from host-mounted filesystems at
/// `/host/proc` to get actual host resources. Falls back to sysinfo.
///
/// To enable host resource detection in a container, mount:
/// ```text
/// -v /proc:/host/proc:ro -v /sys:/host/sys:ro
/// ```
pub fn probe_host_resources() -> HostResources {
    let gpu = detect_nvidia_gpus();
    let mut sys = System::new();

    let (cpu_count, memory_bytes) = if is_host_mounted() {
        let meminfo_path = format!("{}/meminfo", HOST_PROC_PATH);
        let cpuinfo_path = format!("{}/cpuinfo", HOST_PROC_PATH);

        let memory = parse_meminfo(&meminfo_path).unwrap_or_else(|| {
            tracing::warn!("Failed to parse {}, falling back to sysinfo", meminfo_path);
            sys.refresh_memory();
            sys.total_memory()
        });
        let cpu = parse_cpuinfo(&cpuinfo_path).unwrap_or_else(|| {
            tracing::warn!("Failed to parse {}, falling back to sysinfo", cpuinfo_path);
            sys.refresh_cpu_all();
            sys.cpus().len() as u32
        });
        (cpu, memory)
    } else {
        sys.refresh_cpu_all();
        sys.refresh_memory();
        (sys.cpus().len() as u32, sys.total_memory())
    };

    let disks = Disks::new_with_refreshed_list();
    let disk_bytes: u64 = disks.iter().map(|d| d.total_space()).sum();

    let source = if is_host_mounted() {
        "host_mount"
    } else {
        "sysinfo"
    };
    tracing::info!(
        cpu_count,
        memory_bytes,
        disk_bytes,
        source,
        "Probed host resources"
    );

    HostResources {
        cpu_count: Some(cpu_count),
        memory_bytes: Some(memory_bytes),
        disk_bytes: Some(disk_bytes),
        gpu,
    }
}

/// Probe free/available resources for metrics.
///
/// Uses host-mounted filesystems for memory when available,
/// sysinfo for CPU and disk.
pub fn probe_free_resources() -> ResourceAvailability {
    let mut sys = System::new();
    sys.refresh_cpu_all();

    let cpu_usage: f32 =
        sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
    let free_cpu_percent = (100.0 - cpu_usage) as f64;

    let free_memory_bytes = if is_host_mounted() {
        let meminfo_path = format!("{}/meminfo", HOST_PROC_PATH);
        parse_meminfo_available(&meminfo_path).unwrap_or_else(|| {
            tracing::warn!(
                "Failed to parse MemAvailable from {}, falling back to sysinfo",
                meminfo_path
            );
            sys.refresh_memory();
            sys.available_memory()
        })
    } else {
        sys.refresh_memory();
        sys.available_memory()
    };

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

    #[test]
    fn test_product_name_to_gpu_model() {
        assert_eq!(
            product_name_to_gpu_model("NVIDIA A100-SXM4-80GB"),
            GpuModel::NvidiaA10080gb
        );
        assert_eq!(
            product_name_to_gpu_model("NVIDIA A100-PCIE-40GB"),
            GpuModel::NvidiaA10040gb
        );
        assert_eq!(
            product_name_to_gpu_model("NVIDIA H100 80GB HBM3"),
            GpuModel::NvidiaH10080gb
        );
        assert_eq!(
            product_name_to_gpu_model("Tesla T4"),
            GpuModel::NvidiaTeslaT4
        );
        assert_eq!(
            product_name_to_gpu_model("NVIDIA RTX A6000"),
            GpuModel::NvidiaA6000
        );
        assert_eq!(product_name_to_gpu_model("NVIDIA A10"), GpuModel::NvidiaA10);
        assert_eq!(
            product_name_to_gpu_model("Some Unknown GPU"),
            GpuModel::Unknown
        );
    }
}
