use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

use super::BlobStorage;

pub struct DiskStorage {
    base_dir: String,
}

impl DiskStorage {
    pub fn new(base_dir: String) -> Result<Self, anyhow::Error> {
        std::fs::create_dir_all(base_dir.clone())?;
        Ok(Self { base_dir })
    }
}

#[async_trait]
impl BlobStorage for DiskStorage {
    async fn get(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error> {
        let path = format!("{}/{}", self.base_dir, key);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        Ok(path)
    }

    fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let path = format!("{}/{}", self.base_dir, key);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
