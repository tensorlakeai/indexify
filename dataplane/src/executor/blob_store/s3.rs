use std::time::Duration;

use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client,
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};

use crate::executor::blob_store::{BlobMetadata, BlobStoreImpl};

const MAX_RETRIES: u32 = 3;

pub struct S3BlobStore {
    client: Option<Client>,
}

impl S3BlobStore {
    pub fn new() -> Self {
        S3BlobStore { client: None }
    }

    pub async fn lazy_create_client(&mut self) -> Result<()> {
        if self.client.is_none() {
            let client = Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);
            self.client = Some(client);
        }
        Ok(())
    }

    fn parse_uri(uri: &str) -> Result<(String, String)> {
        let uri = uri.strip_prefix("s3://").unwrap_or(uri);
        let parts: Vec<&str> = uri.splitn(2, '/').collect();
        if parts.len() != 2 {
            Err(anyhow!(format!(
                "Failed parsing bucket name from S3 URI '{uri}'"
            )))
        } else {
            Ok((parts[0].to_string(), parts[1].to_string()))
        }
    }
}

impl BlobStoreImpl for S3BlobStore {
    async fn get(&self, uri: &str) -> Result<bytes::Bytes> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let response = client.get_object().bucket(&bucket).key(&key).send().await?;
        let bytes = response.body.collect().await?.into_bytes();
        Ok(bytes)
    }

    async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let response = client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let size_bytes = response.content_length().unwrap_or(0) as u64;
        Ok(BlobMetadata { size_bytes })
    }

    async fn presign_get_uri(
        &self,
        uri: &str,
        expires_in_sec: u64,
    ) -> Result<(String, Vec<(String, String)>)> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let presigned_request = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(
                expires_in_sec,
            ))?)
            .await?;
        Ok((
            presigned_request.uri().to_string(),
            presigned_request
                .headers()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ))
    }

    async fn upload(&self, uri: &str, data: bytes::Bytes) -> Result<()> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await?;
        Ok(())
    }

    async fn create_multipart_upload(&self, uri: &str) -> Result<String> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let upload_id = match client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?
            .upload_id()
        {
            Some(id) => id.to_string(),
            None => "".to_string(),
        };
        Ok(upload_id)
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts: Vec<String>,
    ) -> Result<()> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let completed_parts: Vec<CompletedPart> = parts
            .into_iter()
            .enumerate()
            .map(|(i, etag)| {
                CompletedPart::builder()
                    .e_tag(etag)
                    .part_number((i + 1) as i32)
                    .build()
            })
            .collect();
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await?;
        Ok(())
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
        expires_in_sec: u64,
    ) -> Result<(String, Vec<(String, String)>)> {
        let (bucket, key) = Self::parse_uri(uri)?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("Client not initialized"))?;
        let presigned_request = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .part_number(part_number)
            .upload_id(upload_id)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(
                expires_in_sec,
            ))?)
            .await?;
        Ok((
            presigned_request.uri().to_string(),
            presigned_request
                .headers()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ))
    }
}
