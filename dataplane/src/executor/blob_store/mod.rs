use anyhow::Result;
use bytes::Bytes;

use crate::executor::blob_store::{local_fs::LocalFsBlobStore, s3::S3BlobStore};

pub mod local_fs;
pub mod s3;

pub struct BlobMetadata {
    pub size_bytes: u64,
}

pub trait BlobStoreImpl {
    async fn get(&self, uri: &str) -> Result<Bytes>;
    async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata>;
    async fn presign_get_uri(&self, uri: &str, expires_in_sec: u64) -> Result<String>;
    async fn upload(&self, uri: &str, data: Bytes) -> Result<()>;
    async fn create_multipart_upload(&self, uri: &str) -> Result<String>;
    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts: Vec<String>,
    ) -> Result<()>;
    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()>;
    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
        expires_in_sec: u64,
    ) -> Result<String>;
}

pub struct BlobStore {
    local: LocalFsBlobStore,
    s3: S3BlobStore,
}

impl BlobStore {
    pub async fn new() -> Self {
        let local = LocalFsBlobStore::new().await;
        let s3 = S3BlobStore::new();
        Self { local, s3 }
    }
}

impl BlobStoreImpl for BlobStore {
    async fn get(&self, uri: &str) -> Result<Bytes> {
        if uri.starts_with("file://") {
            return self.local.get(uri).await;
        }
        self.s3.get(uri).await
    }

    async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata> {
        if uri.starts_with("file://") {
            return self.local.get_metadata(uri).await;
        }
        self.s3.get_metadata(uri).await
    }

    async fn presign_get_uri(&self, uri: &str, expires_in_sec: u64) -> Result<String> {
        if uri.starts_with("file://") {
            return self.local.presign_get_uri(uri, expires_in_sec).await;
        }
        self.s3.presign_get_uri(uri, expires_in_sec).await
    }

    async fn upload(&self, uri: &str, data: Bytes) -> Result<()> {
        if uri.starts_with("file://") {
            return self.local.upload(uri, data).await;
        }
        self.s3.upload(uri, data).await
    }

    async fn create_multipart_upload(&self, uri: &str) -> Result<String> {
        if uri.starts_with("file://") {
            return self.local.create_multipart_upload(uri).await;
        }
        self.s3.create_multipart_upload(uri).await
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts: Vec<String>,
    ) -> Result<()> {
        if uri.starts_with("file://") {
            return self
                .local
                .complete_multipart_upload(uri, upload_id, parts)
                .await;
        }
        self.s3
            .complete_multipart_upload(uri, upload_id, parts)
            .await
    }

    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
        if uri.starts_with("file://") {
            return self.local.abort_multipart_upload(uri, upload_id).await;
        }
        self.s3.abort_multipart_upload(uri, upload_id).await
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
        expires_in_sec: u64,
    ) -> Result<String> {
        if uri.starts_with("file://") {
            return self
                .local
                .presign_upload_part_uri(uri, part_number, upload_id, expires_in_sec)
                .await;
        }
        self.s3
            .presign_upload_part_uri(uri, part_number, upload_id, expires_in_sec)
            .await
    }
}
