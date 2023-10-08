use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::BlobStorage;

#[derive(Debug)]
pub struct DiskStorage {
    base_dir: String,
}

impl DiskStorage {
    #[tracing::instrument]
    pub fn new(base_dir: String) -> Result<Self, anyhow::Error> {
        std::fs::create_dir_all(base_dir.clone())?;
        Ok(Self { base_dir })
    }
}

#[async_trait]
impl BlobStorage for DiskStorage {
    #[tracing::instrument]
    async fn get(&self, path: &str) -> Result<Vec<u8>, anyhow::Error> {
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        Ok(buffer)
    }

    #[tracing::instrument]
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error> {
        let path = format!("{}/{}", self.base_dir, key);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        Ok(path)
    }

    #[tracing::instrument]
    fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let path = format!("{}/{}", self.base_dir, key);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
