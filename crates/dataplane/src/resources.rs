use proto_api::executor_api_pb::HostResources;
use sysinfo::{Disks, System};

use crate::metrics::ResourceAvailability;

/// Probe total host resources (used for heartbeat reporting).
pub fn probe_host_resources() -> HostResources {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let cpu_count = sys.cpus().len() as u32;
    let memory_bytes = sys.total_memory();

    let disks = Disks::new_with_refreshed_list();
    let disk_bytes: u64 = disks.iter().map(|d| d.total_space()).sum();

    tracing::info!(cpu_count, memory_bytes, disk_bytes, "Probed host resources");

    HostResources {
        cpu_count: Some(cpu_count),
        memory_bytes: Some(memory_bytes),
        disk_bytes: Some(disk_bytes),
        gpu: None,
    }
}

pub fn probe_free_resources() -> ResourceAvailability {
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
