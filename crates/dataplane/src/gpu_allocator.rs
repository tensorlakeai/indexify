//! GPU allocator for pinning specific GPUs to containers.
//!
//! Discovers NVIDIA GPUs on the host via nvidia-smi and tracks which GPUs
//! are free vs allocated. Containers receive specific GPU UUIDs via Docker
//! DeviceRequest.device_ids rather than a count, which avoids contention
//! when multiple containers need GPUs simultaneously.

use std::{process::Command, sync::Mutex};

use anyhow::{Result, bail};
use tracing::{info, warn};

/// Information about a single NVIDIA GPU on the host.
#[derive(Debug, Clone)]
pub struct GpuInfo {
    pub uuid: String,
    pub product_name: String,
}

/// Thread-safe allocator that tracks GPU assignment by UUID.
///
/// On construction, discovers all GPUs via nvidia-smi. Containers call
/// `allocate(count)` to reserve specific GPUs and `deallocate(uuids)` to
/// return them.
pub struct GpuAllocator {
    all_gpus: Vec<GpuInfo>,
    free_gpus: Mutex<Vec<GpuInfo>>,
}

impl GpuAllocator {
    /// Create a new allocator, discovering GPUs via nvidia-smi.
    ///
    /// Returns an allocator with zero GPUs if nvidia-smi is not available
    /// or fails.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let gpus = discover_gpus().unwrap_or_default();
        if !gpus.is_empty() {
            info!(
                gpu_count = gpus.len(),
                gpus = ?gpus.iter().map(|g| format!("{} ({})", g.product_name, g.uuid)).collect::<Vec<_>>(),
                "GPU allocator initialized"
            );
        }
        Self {
            free_gpus: Mutex::new(gpus.clone()),
            all_gpus: gpus,
        }
    }

    /// Allocate `count` GPUs, returning their UUIDs.
    ///
    /// Returns an error if not enough free GPUs are available.
    pub fn allocate(&self, count: u32) -> Result<Vec<String>> {
        let mut free = self.free_gpus.lock().unwrap();
        if (count as usize) > free.len() {
            bail!(
                "Not enough free GPUs: requested={}, available={}",
                count,
                free.len()
            );
        }

        let mut uuids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let gpu = free.pop().unwrap();
            info!(uuid = %gpu.uuid, product_name = %gpu.product_name, "Allocated GPU");
            uuids.push(gpu.uuid);
        }
        Ok(uuids)
    }

    /// Return GPUs to the free pool by their UUIDs.
    pub fn deallocate(&self, uuids: &[String]) {
        if uuids.is_empty() {
            return;
        }
        let mut free = self.free_gpus.lock().unwrap();
        for uuid in uuids {
            if let Some(gpu) = self.all_gpus.iter().find(|g| &g.uuid == uuid) {
                info!(uuid = %gpu.uuid, product_name = %gpu.product_name, "Deallocated GPU");
                free.push(gpu.clone());
            } else {
                warn!(uuid = %uuid, "Attempted to deallocate unknown GPU UUID");
            }
        }
    }
}

/// Discover NVIDIA GPUs by running nvidia-smi.
fn discover_gpus() -> Result<Vec<GpuInfo>> {
    let output = Command::new("nvidia-smi")
        .args(["--query-gpu=index,name,uuid", "--format=csv,noheader"])
        .output()?;

    if !output.status.success() {
        bail!("nvidia-smi failed with status {}", output.status);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut gpus = Vec::new();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 3 {
            warn!(line = %line, "Unexpected nvidia-smi output format");
            continue;
        }
        gpus.push(GpuInfo {
            uuid: parts[2].to_string(),
            product_name: parts[1].to_string(),
        });
    }

    Ok(gpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocator_no_gpus() {
        // Simulates a host without GPUs
        let alloc = GpuAllocator {
            all_gpus: vec![],
            free_gpus: Mutex::new(vec![]),
        };
        assert!(alloc.allocate(1).is_err());
    }

    #[test]
    fn test_allocate_and_deallocate() {
        let gpu = GpuInfo {
            uuid: "GPU-abc-123".to_string(),
            product_name: "Tesla T4".to_string(),
        };
        let alloc = GpuAllocator {
            all_gpus: vec![gpu.clone()],
            free_gpus: Mutex::new(vec![gpu]),
        };

        // Allocate
        let uuids = alloc.allocate(1).unwrap();
        assert_eq!(uuids, vec!["GPU-abc-123"]);

        // No more free
        assert!(alloc.allocate(1).is_err());

        // Deallocate
        alloc.deallocate(&uuids);

        // Can allocate again
        let uuids2 = alloc.allocate(1).unwrap();
        assert_eq!(uuids2, vec!["GPU-abc-123"]);
    }

    #[test]
    fn test_allocate_multiple() {
        let gpus = vec![
            GpuInfo {
                uuid: "GPU-aaa".to_string(),
                product_name: "Tesla T4".to_string(),
            },
            GpuInfo {
                uuid: "GPU-bbb".to_string(),
                product_name: "Tesla T4".to_string(),
            },
        ];
        let alloc = GpuAllocator {
            all_gpus: gpus.clone(),
            free_gpus: Mutex::new(gpus),
        };

        let uuids = alloc.allocate(2).unwrap();
        assert_eq!(uuids.len(), 2);
        assert!(alloc.allocate(1).is_err());

        alloc.deallocate(&uuids);
        assert!(alloc.allocate(2).is_ok());
    }
}
