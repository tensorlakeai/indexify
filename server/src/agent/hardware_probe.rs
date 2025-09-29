use std::{process::Command, sync::Mutex};

use sysinfo::{Disks, System};
use tracing::warn;

use crate::executor_api::executor_api_pb;

pub struct HardwareProbe {
    sys: Mutex<System>,
}

impl HardwareProbe {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        Self {
            sys: Mutex::new(sys),
        }
    }

    pub fn cpu_cores(&self) -> u32 {
        let mut sys = self.sys.lock().unwrap();
        sys.refresh_cpu_usage();
        sys.cpus().len() as u32
    }

    pub fn memory_bytes(&self) -> u64 {
        let mut sys = self.sys.lock().unwrap();
        sys.refresh_memory();
        sys.total_memory()
    }

    pub fn disk_bytes(&self) -> u64 {
        let disks = Disks::new_with_refreshed_list();
        disks.iter().map(|d| d.total_space()).sum()
    }

    fn map_gpu_model(product_name: &str) -> executor_api_pb::GpuModel {
        match product_name.trim() {
            "NVIDIA A100-SXM4-80GB" => executor_api_pb::GpuModel::NvidiaA10080gb,
            "NVIDIA A100-PCIE-40GB" => executor_api_pb::GpuModel::NvidiaA10040gb,
            "NVIDIA H100 80GB HBM3" => executor_api_pb::GpuModel::NvidiaH10080gb,
            "Tesla T4" => executor_api_pb::GpuModel::NvidiaTeslaT4,
            "NVIDIA RTX A6000" => executor_api_pb::GpuModel::NvidiaA6000,
            "NVIDIA A10" => executor_api_pb::GpuModel::NvidiaA10,
            other => {
                let n = other.to_ascii_uppercase();
                let model = if n.contains("H100") && n.contains("80") {
                    executor_api_pb::GpuModel::NvidiaH10080gb
                } else if n.contains("A100") && n.contains("80") {
                    executor_api_pb::GpuModel::NvidiaA10080gb
                } else if n.contains("A100") && n.contains("40") {
                    executor_api_pb::GpuModel::NvidiaA10040gb
                } else if n.contains("T4") {
                    executor_api_pb::GpuModel::NvidiaTeslaT4
                } else if n.contains("A6000") {
                    executor_api_pb::GpuModel::NvidiaA6000
                } else if n.contains("A10") {
                    executor_api_pb::GpuModel::NvidiaA10
                } else {
                    executor_api_pb::GpuModel::Unknown
                };
                if matches!(model, executor_api_pb::GpuModel::Unknown) {
                    warn!(nvidia_smi_product_name = %other, "Unknown GPU model detected; defaulting to UNKNOWN");
                }
                model
            }
        }
    }

    fn probe_nvidia_gpus(&self) -> Option<executor_api_pb::GpuResources> {
        let output = Command::new("nvidia-smi")
            .args(["--query-gpu=index,name,uuid", "--format=csv,noheader"])
            .output()
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut count: u32 = 0;
        let mut first_name: Option<String> = None;
        for line in stdout.lines().map(|s| s.trim()).filter(|s| !s.is_empty()) {
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 3 {
                continue;
            }
            if first_name.is_none() {
                first_name = Some(parts[1].to_string());
            }
            count += 1;
        }
        let first_name = first_name?;
        Some(executor_api_pb::GpuResources {
            count: Some(count),
            model: Some(Self::map_gpu_model(&first_name) as i32),
        })
    }

    pub fn total_resources(&self) -> executor_api_pb::HostResources {
        executor_api_pb::HostResources {
            cpu_count: Some(self.cpu_cores()),
            memory_bytes: Some(self.memory_bytes()),
            disk_bytes: Some(self.disk_bytes()),
            gpu: self.probe_nvidia_gpus(),
        }
    }
}
