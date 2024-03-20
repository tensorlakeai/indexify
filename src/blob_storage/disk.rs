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
        file.shutdown().await?;
        let path = format!("file://{}", path);
        Ok(path)
    }

    async fn put_stream(
        &self,
        key: &str,
        data: BoxStream<'static, Result<Bytes>>,
    ) -> Result<String, anyhow::Error> {
        let path = format!("{}/{}", self.config.path, key);
        let mut file = File::create(&path).await?;
        let mut stream = data;
        // TODO: need to handle partially successful writes
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }
        file.shutdown().await?;
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
                }
            }
        });
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use futures::stream;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_put() -> Result<(), anyhow::Error> {
        let dir = tempdir()?;
        let config = DiskStorageConfig {
            path: dir.path().to_str().unwrap().to_string(),
        };
        let storage = DiskStorage::new(config)?;

        let key = "testfile";
        let data = Bytes::from_static(b"testdata");

        let path = storage.put(key, data.clone()).await?;
        assert_eq!(
            path,
            format!("file://{}/{}", dir.path().to_str().unwrap(), key)
        );

        let mut file = File::open(format!("{}/{}", dir.path().to_str().unwrap(), key))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        assert_eq!(contents, "testdata");

        dir.close()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_put_stream() -> Result<(), anyhow::Error> {
        let dir = tempdir()?;
        let config = DiskStorageConfig {
            path: dir.path().to_str().unwrap().to_string(),
        };
        let storage = DiskStorage::new(config)?;

        let key = "testfile";
        let data = stream::iter(vec![
            Ok(Bytes::from_static(b"testdata")),
            Ok(Bytes::from_static(b"testdata1")),
            Ok(Bytes::from_static(b"testdata2")),
        ]);

        let path = storage.put_stream(key, Box::pin(data)).await?;
        assert_eq!(
            path,
            format!("file://{}/{}", dir.path().to_str().unwrap(), key)
        );

        let mut file = File::open(format!("{}/{}", dir.path().to_str().unwrap(), key))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        assert_eq!(contents, "testdatatestdata1testdata2");

        dir.close()?;

        Ok(())
    }
}
