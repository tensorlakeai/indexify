use std::path::{Path, PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, ready, stream::BoxStream, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{
    BlobStoragePartWriter,
    BlobStorageReader,
    BlobStorageWriter,
    DiskStorageConfig,
    StoragePartWriter,
};
use crate::blob_storage::PutResult;

const BUFFER_SIZE: usize = 1024 * 1024 * 2;

// Creates a file in a temporary dir and moves it to the final location on
// shutdown().
struct DeleteOnFailureFile {
    inner: File,
    tmp_path: PathBuf,
    committed: bool,
    rename_future: BoxFuture<'static, std::io::Result<()>>,
}

impl DeleteOnFailureFile {
    pub async fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file_name = path.file_name().ok_or(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid file path",
        ))?;
        let tmp_path = match path.parent() {
            Some(parent) => parent.join("tmp").join(file_name),
            None => PathBuf::from("tmp").join(file_name),
        };
        let file = File::create(&tmp_path).await?;
        let rename_future = Box::pin(tokio::fs::rename(tmp_path.clone(), path));

        Ok(Self {
            inner: file,
            tmp_path,
            committed: false,
            rename_future: Box::pin(rename_future),
        })
    }
}

impl Drop for DeleteOnFailureFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

impl AsyncWrite for DeleteOnFailureFile {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<std::io::Result<()>> {
        // std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        let this = self.get_mut();

        if !this.committed {
            ready!(std::pin::Pin::new(&mut this.inner).poll_shutdown(cx))?;
            this.committed = true;
        }
        let poll = this.rename_future.as_mut().poll(cx);
        match poll {
            std::task::Poll::Ready(result) => match result {
                Ok(_) => std::task::Poll::Ready(Ok(())),
                Err(e) => std::task::Poll::Ready(Err(e)),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct DiskStorage {
    config: DiskStorageConfig,
}

impl DiskStorage {
    #[tracing::instrument]
    pub fn new(config: DiskStorageConfig) -> Result<Self, anyhow::Error> {
        let tmp_path = format!("{}/tmp", config.path);
        std::fs::create_dir_all(tmp_path)?;
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
        let file = DeleteOnFailureFile::create(&path).await?;
        let mut file = tokio::io::BufWriter::with_capacity(BUFFER_SIZE, file);
        let mut stream = data;
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
        let file = DeleteOnFailureFile::create(&path).await?;
        let file = tokio::io::BufWriter::with_capacity(BUFFER_SIZE, file);
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

#[async_trait]
impl BlobStorageReader for DiskFileReader {
    async fn get(&self, file_path: &str) -> Result<BoxStream<'static, Result<Bytes>>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let file_path = file_path.trim_start_matches("file://").to_string();
        let client = LocalFileSystem::new();
        let get_result = client
            .get(&file_path.clone().into())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read file: {:?}, error: {}", file_path, e))?;
        tokio::spawn(async move {
            let mut stream = get_result.into_stream();
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
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use anyhow::anyhow;
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

        let entries = std::fs::read_dir(dir.path().join("tmp"))?;
        assert!(entries.count() == 0, "temp directory is not empty");

        dir.close()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_put_stream_error() -> Result<(), anyhow::Error> {
        let dir = tempdir()?;
        let config = DiskStorageConfig {
            path: dir.path().to_str().unwrap().to_string(),
        };
        let storage = DiskStorage::new(config)?;

        let key = "testfile";
        let data = stream::iter(vec![
            Ok(Bytes::from_static(b"testdata")),
            Err(anyhow!("test error")),
        ]);

        let result = storage.put(key, Box::pin(data)).await;
        assert!(result.is_err());

        let entries = std::fs::read_dir(dir.path().join("tmp"))?;
        assert!(entries.count() == 0, "temp directory is not empty");

        dir.close()?;

        Ok(())
    }
}
