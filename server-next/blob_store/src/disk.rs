use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{local::LocalFileSystem, ObjectStore};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::BlobStorageReader;

pub struct DiskFileReader {
    file_path: String,
}
impl DiskFileReader {
    pub fn new(fil_path: &str) -> Self {
        Self {
            file_path: fil_path.to_string(),
        }
    }
}

#[async_trait]
impl BlobStorageReader for DiskFileReader {
    async fn get(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let file_path = &self.file_path.trim_start_matches("file://").to_string();
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
