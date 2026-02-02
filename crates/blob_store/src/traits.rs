//! Core blob store trait.

use std::{ops::Range, time::Duration};

use async_trait::async_trait;

use crate::{BlobMetadata, BlobResult};

/// Core blob store operations.
///
/// This trait provides low-level blob storage operations used by executors
/// and dataplanes. For high-level streaming operations used by the server,
/// see [`BlobStorage`](crate::BlobStorage).
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Get entire blob data.
    ///
    /// Returns `BlobError::NotFound` if the blob doesn't exist.
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>>;

    /// Get a specific byte range from a blob.
    ///
    /// This is critical for Python executor's parallel chunk downloads.
    /// Range is inclusive: `range.start` to `range.end - 1`.
    ///
    /// Returns `BlobError::NotFound` if the blob doesn't exist.
    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>>;

    /// Get blob metadata without downloading content.
    ///
    /// Returns `BlobError::NotFound` if the blob doesn't exist.
    async fn get_metadata(&self, uri: &str) -> BlobResult<BlobMetadata>;

    /// Upload blob data directly.
    ///
    /// This is a simple PUT operation for small blobs. For large uploads,
    /// use the multipart upload methods.
    async fn upload(&self, uri: &str, data: Vec<u8>) -> BlobResult<()>;

    // --- Presigned URL Operations ---

    /// Generate a presigned GET URL.
    ///
    /// The URL allows downloading the blob without authentication for the
    /// specified duration.
    ///
    /// # Arguments
    /// * `uri` - Blob URI (e.g., `s3://bucket/key` or `file:///path`)
    /// * `expires_in` - How long the URL is valid (max 7 days for S3)
    ///
    /// # Returns
    /// A presigned URL string. For local files, this may be the original URI.
    async fn presign_get_uri(&self, uri: &str, expires_in: Duration) -> BlobResult<String>;

    /// Generate a presigned PUT URL for a single part in a multipart upload.
    ///
    /// # Arguments
    /// * `uri` - Blob URI
    /// * `part_number` - Part number (starts from 1)
    /// * `upload_id` - Upload ID from `create_multipart_upload`
    /// * `expires_in` - How long the URL is valid
    ///
    /// # Returns
    /// A presigned URL that can be used to upload the specified part.
    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        upload_id: &str,
        expires_in: Duration,
    ) -> BlobResult<String>;

    // --- Multipart Upload Operations ---

    /// Create a multipart upload session.
    ///
    /// Returns an upload ID used for subsequent operations.
    async fn create_multipart_upload(&self, uri: &str) -> BlobResult<String>;

    /// Complete a multipart upload.
    ///
    /// Finalizes the upload using the ETags from uploaded parts.
    ///
    /// # Arguments
    /// * `uri` - Blob URI
    /// * `upload_id` - Upload ID from `create_multipart_upload`
    /// * `parts_etags` - ETags from uploaded parts, ordered by part number (1,
    ///   2, 3...)
    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()>;

    /// Abort a multipart upload.
    ///
    /// Cancels the upload and cleans up partial data.
    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> BlobResult<()>;
}
