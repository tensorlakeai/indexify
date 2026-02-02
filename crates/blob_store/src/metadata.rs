//! Blob metadata structures.

use serde::{Deserialize, Serialize};

/// Metadata about a blob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobMetadata {
    /// Size in bytes.
    pub size_bytes: u64,

    /// SHA256 hash (optional, may not be available for all backends).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256_hash: Option<String>,

    /// ETag from object store (S3/GCS/Azure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,

    /// Content type / MIME type (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

impl BlobMetadata {
    /// Create metadata with just size.
    pub fn with_size(size_bytes: u64) -> Self {
        Self {
            size_bytes,
            sha256_hash: None,
            etag: None,
            content_type: None,
        }
    }

    /// Create metadata with size and hash.
    pub fn with_hash(size_bytes: u64, sha256_hash: String) -> Self {
        Self {
            size_bytes,
            sha256_hash: Some(sha256_hash),
            etag: None,
            content_type: None,
        }
    }
}
