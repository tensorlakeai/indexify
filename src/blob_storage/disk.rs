use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

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

pub struct DiskFileReader {}
impl DiskFileReader {
    pub fn new() -> Self {
        Self {}
    }
}

impl BlobStorageReader for DiskFileReader {
    fn get(&self, file_path: &str) -> BoxStream<Result<Bytes>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let file_path = file_path.trim_start_matches("file://").to_string();
        tokio::spawn(async move {
            let client = LocalFileSystem::new();
            let mut stream = client.get(&file_path.into()).await.unwrap().into_stream();
            while let Some(chunk) = stream.next().await {
                if let Ok(chunk) = chunk {
                    let _ = tx.send(Ok(chunk));
                } else {
                    let _ = tx.send(Err(anyhow::anyhow!(
                        "Error reading file: {:?}",
                        chunk.err()
                    )));
                    break;
                }
            }
        });
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}
