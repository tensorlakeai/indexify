use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use std::path::PathBuf;

use crate::executor::blob_store::{BlobMetadata, BlobStoreImpl};

pub struct LocalFsBlobStore;

impl LocalFsBlobStore {
    pub async fn new() -> Self {
        LocalFsBlobStore
    }

    fn strip_prefix(uri: &str) -> &str {
        uri.strip_prefix("file://").unwrap_or(uri)
    }
}

impl BlobStoreImpl for LocalFsBlobStore {
    async fn get(&self, uri: &str) -> anyhow::Result<bytes::Bytes> {
        let uri = Self::strip_prefix(uri);
        let path = PathBuf::from(uri);
        let mut file = File::open(&path).await?;
        let metadata = file.metadata().await?;
        let size = metadata.len();
        let mut buffer = vec![0; size as usize];
        file.read_exact(&mut buffer).await?;
        Ok(buffer.into())
    }

    async fn get_metadata(&self, uri: &str) -> anyhow::Result<super::BlobMetadata> {
        let uri = Self::strip_prefix(uri);
        let path = PathBuf::from(uri);
        let file = File::open(&path).await?;
        let metadata = file.metadata().await?;
        let size_bytes = metadata.len();

        Ok(BlobMetadata { size_bytes })
    }

    async fn presign_get_uri(&self, uri: &str, _expires_in_sec: u64) -> anyhow::Result<String> {
        Ok(uri.to_string())
    }

    async fn upload(&self, uri: &str, data: bytes::Bytes) -> anyhow::Result<()> {
        let uri = Self::strip_prefix(uri);
        let path = PathBuf::from(uri);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        Ok(())
    }

    async fn create_multipart_upload(&self, _uri: &str) -> anyhow::Result<String> {
        Ok("no-id-for-local-file".to_string())
    }

    async fn complete_multipart_upload(
        &self,
        _uri: &str,
        _upload_id: &str,
        _parts: Vec<String>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn abort_multipart_upload(&self, _uri: &str, _upload_id: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        _part_number: i32,
        _upload_id: &str,
        _expires_in_sec: u64,
    ) -> anyhow::Result<String> {
        Ok(uri.to_string())
    }
}
