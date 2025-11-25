use tokio::process::Command;

use crate::executor::executor_api::executor_api_pb::GpuModel;

pub fn product_name_to_model(product_name: &str) -> GpuModel {
    if product_name.starts_with("NVIDIA A100") && product_name.ends_with("40GB") {
        GpuModel::NvidiaA10040gb
    } else if product_name.starts_with("NVIDIA A100") && product_name.ends_with("80GB") {
        GpuModel::NvidiaA10080gb
    } else if product_name.starts_with("NVIDIA H100") && product_name.contains("80GB") {
        GpuModel::NvidiaH10080gb
    } else if product_name.starts_with("NVIDIA Tesla T4") {
        GpuModel::NvidiaTeslaT4
    } else if product_name.starts_with("NVIDIA A6000") {
        GpuModel::NvidiaA6000
    } else if product_name.starts_with("NVIDIA A10") {
        GpuModel::NvidiaA10
    } else {
        GpuModel::Unknown
    }
}

#[derive(Debug, Clone)]
pub struct NvidiaGpuInfo {
    pub index: String,
    pub uuid: String,
    pub product_name: String,
    pub model: GpuModel,
}

pub async fn nvidia_gpus_are_available() -> bool {
    Command::new("nvidia-smi").output().await.is_ok()
}

pub async fn fetch_nvidia_gpu_info() -> Result<Vec<NvidiaGpuInfo>, Box<dyn std::error::Error>> {
    let output = Command::new("nvidia-smi")
        .args(&["--query-gpu=index,name,uuid", "--format=csv,noheader"])
        .output()
        .await?;
    String::from_utf8(output.stdout)?
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split(',').collect();
            let model = product_name_to_model(parts[1].trim());
            if model == GpuModel::Unknown {
                return None;
            }
            Some(Ok(NvidiaGpuInfo {
                index: parts[0].trim().to_string(),
                uuid: parts[2].trim().to_string(),
                product_name: parts[1].trim().to_string(),
                model,
            }))
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct NvidiaGpuAllocator {
    all_gpus: Vec<NvidiaGpuInfo>,
    free_gpus: Vec<NvidiaGpuInfo>,
}

impl NvidiaGpuAllocator {
    pub fn new(gpus: Vec<NvidiaGpuInfo>) -> Self {
        let free_gpus = gpus.clone();
        Self {
            all_gpus: gpus,
            free_gpus,
        }
    }

    // Use Error over None here
    pub fn allocate(&mut self, count: usize) -> Result<Vec<NvidiaGpuInfo>, Error> {
        if count > self.free_gpus.len() {
            Err(Error::Other(format!(
                "Not enough free GPUs available, requested {count}, avalaible={}",
                self.free_gpus.len()
            )))
        } else {
            let allocated_gpus = self.free_gpus.drain(..count).collect();
            Ok(allocated_gpus)
        }
    }

    pub fn deallocate(&mut self, gpus: Vec<NvidiaGpuInfo>) {
        self.free_gpus.extend(gpus);
    }

    pub fn list_all(&self) -> Vec<NvidiaGpuInfo> {
        self.all_gpus.clone()
    }

    pub fn list_free(&self) -> Vec<NvidiaGpuInfo> {
        self.free_gpus.clone()
    }
}

#[derive(Debug)]
pub enum Error {
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {}
