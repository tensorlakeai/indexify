use proto_api::executor_api_pb::HostResources;
use sysinfo::{Disks, System};

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
