//! S3 blob store backend using object_store + aws-sdk-s3 for presigning.

use std::{ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::Region,
    presigning::PresigningConfig,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::Bytes;
use object_store::{
    GetOptions,
    GetRange,
    ObjectStore,
    ObjectStoreExt,
    aws::{AmazonS3, AmazonS3Builder},
    path::Path as ObjectPath,
};
use tracing::debug;

use crate::{BlobError, BlobMetadata, BlobResult, BlobStore, presign};

/// S3 blob store backend.
pub struct S3BlobStore {
    /// object_store client for data I/O.
    object_store: Arc<AmazonS3>,

    /// AWS SDK S3 client for presigning.
    s3_client: S3Client,

    /// Bucket name extracted from base path.
    bucket: String,

    /// Optional prefix for all keys.
    prefix: String,
}

impl S3BlobStore {
    /// Create a new S3 blob store from a base URL.
    ///
    /// # Arguments
    /// * `url` - S3 URL (e.g., `s3://bucket/prefix`)
    /// * `region` - Optional AWS region override
    pub async fn new(url: &str, region: Option<String>) -> BlobResult<Self> {
        if !url.starts_with("s3://") {
            return Err(BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "URI must start with s3://".to_string(),
            });
        }

        // Parse bucket and prefix from URL
        let (bucket, prefix) = Self::parse_s3_url(url)?;

        // Build object_store client
        let mut builder = AmazonS3Builder::from_env().with_url(url);
        if let Some(ref r) = region {
            builder = builder.with_region(r);
        }
        let object_store = builder.build().map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        // Build AWS SDK S3 client for presigning
        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = region {
            config_loader = config_loader.region(Region::new(r));
        }
        let aws_config = config_loader.load().await;
        let s3_client = S3Client::new(&aws_config);

        debug!(
            bucket = %bucket,
            prefix = %prefix,
            "Created S3 blob store"
        );

        Ok(Self {
            object_store: Arc::new(object_store),
            s3_client,
            bucket,
            prefix,
        })
    }

    /// Parse S3 URL into bucket and prefix.
    fn parse_s3_url(url: &str) -> BlobResult<(String, String)> {
        let without_scheme = url
            .strip_prefix("s3://")
            .ok_or_else(|| BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "Must start with s3://".to_string(),
            })?;

        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
        let bucket = parts[0].to_string();
        let prefix = if parts.len() > 1 {
            parts[1].trim_end_matches('/').to_string()
        } else {
            String::new()
        };

        Ok((bucket, prefix))
    }

    /// Extract bucket and key from full S3 URI.
    fn parse_uri(&self, uri: &str) -> BlobResult<(String, String)> {
        Self::parse_s3_url(uri)
    }

    /// Convert a key to an ObjectPath for object_store.
    fn key_to_path(&self, key: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, key))
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await.map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        Ok(bytes.to_vec())
    }

    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let options = GetOptions {
            range: Some(GetRange::Bounded(range.clone())),
            ..Default::default()
        };

        let result = self.object_store.get_opts(&path, options).await?;
        let bytes = result.bytes().await.map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        Ok(bytes.to_vec())
    }

    async fn get_metadata(&self, uri: &str) -> BlobResult<BlobMetadata> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let metadata = self.object_store.head(&path).await?;

        Ok(BlobMetadata {
            size_bytes: metadata.size as u64,
            sha256_hash: None,
            etag: metadata.e_tag.clone(),
            content_type: None,
        })
    }

    async fn upload(&self, uri: &str, data: Vec<u8>) -> BlobResult<()> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        self.object_store
            .put(&path, Bytes::from(data).into())
            .await?;

        Ok(())
    }

    async fn presign_get_uri(&self, uri: &str, expires_in: Duration) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (bucket, key) = self.parse_uri(uri)?;

        let presigning_config =
            PresigningConfig::expires_in(expires_in).map_err(|e| BlobError::PresignError {
                reason: format!("Failed to create presigning config: {}", e),
            })?;

        let presigned = self
            .s3_client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .presigned(presigning_config)
            .await
            .map_err(|e| BlobError::PresignError {
                reason: format!("Failed to generate presigned GET URL: {}", e),
            })?;

        Ok(presigned.uri().to_string())
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        upload_id: &str,
        expires_in: Duration,
    ) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (bucket, key) = self.parse_uri(uri)?;

        let presigning_config =
            PresigningConfig::expires_in(expires_in).map_err(|e| BlobError::PresignError {
                reason: format!("Failed to create presigning config: {}", e),
            })?;

        let presigned = self
            .s3_client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .presigned(presigning_config)
            .await
            .map_err(|e| BlobError::PresignError {
                reason: format!("Failed to generate presigned upload part URL: {}", e),
            })?;

        Ok(presigned.uri().to_string())
    }

    async fn create_multipart_upload(&self, uri: &str) -> BlobResult<String> {
        let (bucket, key) = self.parse_uri(uri)?;

        let output = self
            .s3_client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| BlobError::MultipartError {
                reason: format!("Failed to create multipart upload: {}", e),
            })?;

        let upload_id = output
            .upload_id()
            .ok_or_else(|| BlobError::MultipartError {
                reason: "No upload ID returned from S3".to_string(),
            })?;

        Ok(upload_id.to_string())
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()> {
        let (bucket, key) = self.parse_uri(uri)?;

        // Build completed parts
        let parts: Vec<CompletedPart> = parts_etags
            .into_iter()
            .enumerate()
            .map(|(i, etag)| {
                CompletedPart::builder()
                    .e_tag(etag)
                    .part_number((i + 1) as i32)
                    .build()
            })
            .collect();

        let multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.s3_client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(multipart_upload)
            .send()
            .await
            .map_err(|e| BlobError::MultipartError {
                reason: format!("Failed to complete multipart upload: {}", e),
            })?;

        Ok(())
    }

    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> BlobResult<()> {
        let (bucket, key) = self.parse_uri(uri)?;

        self.s3_client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| BlobError::MultipartError {
                reason: format!("Failed to abort multipart upload: {}", e),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let (bucket, prefix) = S3BlobStore::parse_s3_url("s3://my-bucket/path/to/prefix").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/prefix");

        let (bucket, prefix) = S3BlobStore::parse_s3_url("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }
}
