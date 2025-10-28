use nvml_wrapper::Nvml;
use sysinfo::{Disks, System};
use tracing::debug;

use crate::executor_client::executor_api_pb::{GpuModel, GpuResources, HostResources};

/// Hardware resource probe that caches static system information
pub struct HardwareProbe {
    /// Number of CPU cores (static)
    cpu_count: u32,
    /// Total system memory in bytes (static)
    memory_bytes: u64,
    /// Total disk space in bytes (static)
    disk_bytes: u64,
    /// GPU resources if available (static)
    gpu: Option<GpuResources>,
}

impl HardwareProbe {
    /// Creates a new HardwareProbe and detects all system resources
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();

        let cpu_count = sys.cpus().len() as u32;
        let memory_bytes = sys.total_memory();

        // Get total disk space across all mounted disks
        let disks = Disks::new_with_refreshed_list();
        let disk_bytes: u64 = disks.iter().map(|disk| disk.total_space()).sum();

        // Detect GPU resources (only done once at startup)
        let gpu = Self::detect_gpu_resources();

        debug!(
            cpu_count = cpu_count,
            memory_bytes = memory_bytes,
            disk_bytes = disk_bytes,
            has_gpu = gpu.is_some(),
            "Hardware resources detected"
        );

        Self {
            cpu_count,
            memory_bytes,
            disk_bytes,
            gpu,
        }
    }

    /// Returns the current system resources
    /// This is a cheap operation as it returns cached static values
    pub fn get_resources(&self) -> HostResources {
        HostResources {
            cpu_count: Some(self.cpu_count),
            memory_bytes: Some(self.memory_bytes),
            disk_bytes: Some(self.disk_bytes),
            gpu: self.gpu.clone(),
        }
    }

    /// Detects the GPU model based on the device name string
    fn detect_gpu_model(name: &str) -> GpuModel {
        let name_lower = name.to_lowercase();

        if name_lower.contains("a100") {
            if name_lower.contains("80gb") || name_lower.contains("80g") {
                GpuModel::NvidiaA10080gb
            } else {
                GpuModel::NvidiaA10040gb
            }
        } else if name_lower.contains("h100") {
            GpuModel::NvidiaH10080gb
        } else if name_lower.contains("t4") || name_lower.contains("tesla t4") {
            GpuModel::NvidiaTeslaT4
        } else if name_lower.contains("a6000") {
            GpuModel::NvidiaA6000
        } else if name_lower.contains("a10") {
            GpuModel::NvidiaA10
        } else {
            GpuModel::Unknown
        }
    }

    /// Detects NVIDIA GPU resources using NVML (NVIDIA Management Library)
    /// Returns None if NVML is not available or no GPUs are detected
    fn detect_gpu_resources() -> Option<GpuResources> {
        // Try to initialize NVML
        let nvml = match Nvml::init() {
            Ok(nvml) => nvml,
            Err(_) => {
                // NVML not available (no NVIDIA drivers or GPUs)
                debug!("NVML not available, no GPU resources detected");
                return None;
            }
        };

        // Get device count
        let device_count = match nvml.device_count() {
            Ok(count) => count,
            Err(_) => {
                debug!("Failed to get GPU device count");
                return None;
            }
        };

        if device_count == 0 {
            debug!("No GPUs detected");
            return None;
        }

        // Get the first GPU's information (assume homogeneous GPUs for now)
        let device = match nvml.device_by_index(0) {
            Ok(dev) => dev,
            Err(_) => {
                debug!("Failed to get GPU device by index");
                return None;
            }
        };

        let gpu_name = device.name().unwrap_or_default();
        let gpu_model = Self::detect_gpu_model(&gpu_name);

        debug!(
            gpu_count = device_count,
            gpu_name = %gpu_name,
            gpu_model = ?gpu_model,
            "GPU resources detected"
        );

        Some(GpuResources {
            count: Some(device_count),
            model: Some(gpu_model as i32),
        })
    }
}

impl Default for HardwareProbe {
    fn default() -> Self {
        Self::new()
    }
}
