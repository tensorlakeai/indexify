use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    BlobStoragePartWriter,
    BlobStorageReader,
    BlobStorageWriter,
    DiskStorageConfig,
    StoragePartWriter,
};
use crate::blob_storage::PutResult;

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
    async fn put(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        let path = format!("{}/{}", self.config.path, key);
        let mut file = File::create(&path).await?;
        let mut stream = data;
        // TODO: need to handle partially successful writes
        let mut size_bytes: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            size_bytes += chunk.len() as u64;
        }
        file.shutdown().await?;
        let path = format!("file://{}", path);
        Ok(PutResult {
            url: path,
            size_bytes,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let path = key
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("Invalid key format"))?;
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[async_trait]
impl BlobStoragePartWriter for DiskStorage {
    async fn writer(&self, key: &str) -> Result<StoragePartWriter> {
        let path = format!("{}/{}", self.config.path, key);
        let file = File::create(&path).await?;
        Ok(StoragePartWriter {
            writer: Box::new(file),
            url: format!("file://{}", path),
        })
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
        let data = stream::iter(vec![
            Ok(Bytes::from_static(b"testdata")),
            Ok(Bytes::from_static(b"testdata1")),
            Ok(Bytes::from_static(b"testdata2")),
        ]);

        let res = storage.put(key, Box::pin(data)).await?;
        assert_eq!(
            res.url,
            format!("file://{}/{}", dir.path().to_str().unwrap(), key)
        );
        assert_eq!(res.size_bytes, 26);

        let mut file = File::open(format!("{}/{}", dir.path().to_str().unwrap(), key))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        assert_eq!(contents, "testdatatestdata1testdata2");

        dir.close()?;

        Ok(())
    }
}
