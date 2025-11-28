use anyhow::Result;
use bytes::Bytes;

pub mod local_fs;
pub mod s3;

pub struct BlobMetadata {
    pub size_bytes: u64,
}

pub trait BlobStoreImpl {
    async fn get(&self, uri: &str) -> Result<Bytes>;
    async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata>;
    async fn presign_get_uri(
        &self,
        uri: &str,
        expires_in_sec: u64,
    ) -> Result<(String, Vec<(String, String)>)>;
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
    ) -> Result<(String, Vec<(String, String)>)>;
}
