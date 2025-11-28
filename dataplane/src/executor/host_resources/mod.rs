use std::path::Path;

use sysinfo::{Disks, System};

use crate::executor::{
    executor_api::executor_api_pb::{GpuResources, HostResources as HostResourcesProto},
    host_resources::nvidia_gpu::{NvidiaGpuAllocator, NvidiaGpuInfo},
};

pub mod nvidia_gpu;

#[derive(Debug, Clone)]
pub struct HostResources {
    cpu_count: u32,
    memory_mb: u64,
    disk_mb: u64,
    gpus: Vec<NvidiaGpuInfo>,
}

impl HostResources {
    pub fn to_proto(&self) -> HostResourcesProto {
        let gpu = if self.gpus.len() > 0 {
            let model = Some(self.gpus[0].model as i32);
            Some(GpuResources {
                count: Some(self.gpus.len() as u32),
                model,
            })
        } else {
            None
        };
        HostResourcesProto {
            cpu_count: Some(self.cpu_count),
            memory_bytes: Some(self.memory_mb * 1024 * 1024),
            disk_bytes: Some(self.disk_mb * 1024 * 1024),
            gpu,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostResourcesProvider {
    gpu_allocator: NvidiaGpuAllocator,
    function_executors_ephemeral_disk_path: String,
    host_overhead_cpus: u32,
    host_overhead_memory_gb: u32,
    host_overhead_function_executors_ephemeral_disks_gb: u32,
}

impl HostResourcesProvider {
    pub fn new(
        gpu_allocator: NvidiaGpuAllocator,
        function_executors_ephemeral_disk_path: String,
        host_overhead_cpus: u32,
        host_overhead_memory_gb: u32,
        host_overhead_function_executors_ephemeral_disks_gb: u32,
    ) -> Self {
        Self {
            gpu_allocator,
            function_executors_ephemeral_disk_path,
            host_overhead_cpus,
            host_overhead_memory_gb,
            host_overhead_function_executors_ephemeral_disks_gb,
        }
    }

    pub fn total_host_resources(&self) -> HostResources {
        let mut sys = System::new_all();
        sys.refresh_all();
        let gpus = self.gpu_allocator.list_all();
        let cpu_count = sys.cpus().len() as u32;
        let memory_mb = (self.host_overhead_memory_gb * 1024) as u64;
        let mut disk_mb: u64 = 0;
        let disks = Disks::new_with_refreshed_list();
        for disk in &disks {
            if disk.mount_point().to_str()
                == Path::new(&self.function_executors_ephemeral_disk_path).to_str()
            {
                disk_mb += disk.available_space() as u64 / 1024 / 1024;
            }
        }
        HostResources {
            cpu_count,
            memory_mb,
            disk_mb,
            gpus,
        }
    }

    pub fn total_function_executor_resources(&self) -> HostResources {
        let total_resources = self.total_host_resources();
        HostResources {
            cpu_count: (total_resources.cpu_count - self.host_overhead_cpus as u32).max(0),
            memory_mb: (total_resources.memory_mb - (self.host_overhead_memory_gb * 1024) as u64)
                .max(0),
            disk_mb: (total_resources.disk_mb
                - (self.host_overhead_function_executors_ephemeral_disks_gb * 1024) as u64)
                .max(0),
            gpus: total_resources.gpus.clone(),
        }
    }
}
