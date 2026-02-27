//! Host resource probing for containerized environments.

use std::{fs, path::Path};

use proto_api::executor_api_pb::{GpuModel, GpuResources, HostResources};
use sysinfo::{Disks, System};

use crate::{gpu_allocator::GpuInfo, metrics::ResourceAvailability};

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

/// Convert pre-discovered GPU info into the proto `GpuResources` for heartbeat
/// reporting.
fn gpu_resources_from_info(gpus: &[GpuInfo]) -> Option<GpuResources> {
    if gpus.is_empty() {
        return None;
    }

    let count = gpus.len() as u32;
    // All GPUs on a host are typically the same model.
    let model = product_name_to_gpu_model(&gpus[0].product_name);

    Some(GpuResources {
        count: Some(count),
        model: Some(model.into()),
    })
}

/// Map nvidia-smi product name to GPUModel enum.
/// Matches Python executor's nvidia_gpu.py _product_name_to_model().
pub fn product_name_to_gpu_model(product_name: &str) -> GpuModel {
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

/// Probe an LVM volume group for total and free space.
///
/// Runs `vgs --noheadings --nosuffix --units b -o vg_size,vg_free <vg>`.
/// Returns `(total_bytes, free_bytes)` or `None` if the command fails.
fn probe_lvm_disk(vg_name: &str) -> Option<(u64, u64)> {
    let output = std::process::Command::new("vgs")
        .args([
            "--noheadings",
            "--nosuffix",
            "--units",
            "b",
            "-o",
            "vg_size,vg_free",
            vg_name,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let line = String::from_utf8_lossy(&output.stdout);
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        let total: u64 = parts[0].parse().ok()?;
        let free: u64 = parts[1].parse().ok()?;
        Some((total, free))
    } else {
        None
    }
}

/// Probe total host resources (used for heartbeat reporting).
///
/// `gpus` should be discovered once at startup via
/// [`gpu_allocator::discover_gpus`] and shared with the GPU allocator.
///
/// When `lvm_vg` is provided, uses the LVM volume group size for disk
/// reporting instead of sysinfo (which can't see LVM thin pools).
///
/// When running in a container, reads from host-mounted filesystems at
/// `/host/proc` to get actual host resources. Falls back to sysinfo.
///
/// To enable host resource detection in a container, mount:
/// ```text
/// -v /proc:/host/proc:ro -v /sys:/host/sys:ro
/// ```
pub fn probe_host_resources(gpus: &[GpuInfo], lvm_vg: Option<&str>) -> HostResources {
    let gpu = gpu_resources_from_info(gpus);
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

    let (disk_bytes, disk_source) = if let Some(vg) = lvm_vg {
        match probe_lvm_disk(vg) {
            Some((total, _free)) => {
                tracing::info!(
                    vg_name = vg,
                    total_bytes = total,
                    "Using LVM VG for disk reporting"
                );
                (total, "lvm")
            }
            None => {
                tracing::warn!(
                    vg_name = vg,
                    "Failed to probe LVM VG, falling back to sysinfo for disk"
                );
                let disks = Disks::new_with_refreshed_list();
                (disks.iter().map(|d| d.total_space()).sum(), "sysinfo")
            }
        }
    } else {
        let disks = Disks::new_with_refreshed_list();
        (disks.iter().map(|d| d.total_space()).sum(), "sysinfo")
    };

    let source = if is_host_mounted() {
        "host_mount"
    } else {
        "sysinfo"
    };
    tracing::info!(
        cpu_count,
        memory_bytes,
        disk_bytes,
        cpu_memory_source = source,
        disk_source,
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
/// `sys` must be a long-lived `System` instance that is reused across calls.
/// `sysinfo` computes `cpu_usage()` as a delta between the previous and current
/// refresh, so a freshly-created `System` would always return 0% usage (no
/// baseline), which would make `free_cpu_percent` always 100%.
///
/// Uses host-mounted filesystems for memory when available,
/// sysinfo for CPU and disk. When `lvm_vg` is provided, uses LVM VG free
/// space for disk instead of sysinfo.
pub fn probe_free_resources(sys: &mut System, lvm_vg: Option<&str>) -> ResourceAvailability {
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

    let free_disk_bytes = if let Some(vg) = lvm_vg {
        probe_lvm_disk(vg)
            .map(|(_total, free)| free)
            .unwrap_or_else(|| {
                let disks = Disks::new_with_refreshed_list();
                disks.iter().map(|d| d.available_space()).sum()
            })
    } else {
        let disks = Disks::new_with_refreshed_list();
        disks.iter().map(|d| d.available_space()).sum()
    };

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
        // Just verify it doesn't panic (pass empty GPUs for test portability)
        let resources = probe_host_resources(&[], None);
        assert!(resources.cpu_count.is_some());
        assert!(resources.memory_bytes.is_some());
        assert!(resources.gpu.is_none());
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
