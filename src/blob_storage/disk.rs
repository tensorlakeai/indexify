use anyhow::Error;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::{BlobStorageReader, BlobStorageWriter, DiskStorageConfig};

#[derive(Debug)]
pub struct DiskStorage {
    config: DiskStorageConfig,
}

impl DiskStorage {
    #[tracing::instrument]
    pub fn new(config: DiskStorageConfig) -> Result<Self, anyhow::Error> {
        std::fs::create_dir_all(config.path.clone())?;
        Ok(Self { config })
    }
}

#[async_trait]
impl BlobStorageWriter for DiskStorage {
    #[tracing::instrument(skip(self))]
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error> {
        let path = format!("{}/{}", self.config.path, key);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        let path = format!("file://{}", path);
        Ok(path)
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let path = format!("{}/{}", self.config.path, key);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[async_trait]
impl BlobStorageReader for DiskStorage {
    #[tracing::instrument(skip(self))]
    async fn get(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        let mut readers = FuturesOrdered::new();
        for &key in keys {
            readers.push_back(async {
                let path = key.trim_start_matches("file://");
                let mut file = File::open(path).await?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await?;
                Ok::<_, Error>(buffer)
            });
        }

        let mut buffers = Vec::with_capacity(keys.len());

        while let Some(blob) = readers.next().await {
            buffers.push(blob?);
        }

        Ok(buffers)
    }
}
