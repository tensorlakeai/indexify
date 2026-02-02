//! Shared blob store abstraction for Indexify.
//!
//! This crate provides a unified interface for blob storage operations across
//! Indexify components (server, dataplane, executors). It supports:
//!
//! - Multiple backends: S3, GCS, Azure, local filesystem
//! - Presigned URL generation (for executor → FE communication)
//! - Multipart uploads
//! - Range requests
//! - SHA256 hash verification
//!
//! # Architecture
//!
//! The crate provides a low-level [`BlobStore`] trait that all backends
//! implement. The [`BlobStoreDispatcher`] routes operations to the appropriate
//! backend based on URI scheme (s3://, file://, etc.).
//!
//! # Usage
//!
//! ## Dataplane: Presigned URLs for direct FE access
//!
//! ```rust,no_run
//! use blob_store::{BlobStore, BlobStoreDispatcher, BlobStorageConfig};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), blob_store::BlobError> {
//! // Create dispatcher with S3 backend
//! let config = BlobStorageConfig {
//!     path: "s3://my-bucket/prefix".to_string(),
//!     region: Some("us-west-2".to_string()),
//!     ..Default::default()
//! };
//! let store = BlobStoreDispatcher::new(config).await?;
//!
//! // Create multipart upload
//! let upload_id = store.create_multipart_upload("s3://my-bucket/output").await?;
//!
//! // Generate presigned URLs for FE to upload parts
//! let url_part1 = store.presign_upload_part_uri(
//!     "s3://my-bucket/output",
//!     1, // part number
//!     &upload_id,
//!     Duration::from_secs(3600), // expires in 1 hour
//! ).await?;
//!
//! // FE uploads directly using presigned URL...
//! // Then complete with ETags
//! store.complete_multipart_upload(
//!     "s3://my-bucket/output",
//!     &upload_id,
//!     vec!["etag1".to_string(), "etag2".to_string()],
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Executor: Range queries for parallel downloads
//!
//! ```rust,no_run
//! use blob_store::{BlobStore, BlobStoreDispatcher, BlobStorageConfig};
//!
//! # async fn example() -> Result<(), blob_store::BlobError> {
//! let config = BlobStorageConfig::default();
//! let store = BlobStoreDispatcher::new(config).await?;
//!
//! // Download specific byte range (e.g., for chunked processing)
//! let chunk = store.get_range("s3://bucket/data", 1000..2000).await?;
//! # Ok(())
//! # }
//! ```

mod backends;
mod config;
mod dispatcher;
mod error;
mod metadata;
mod metrics;
mod presign;
mod storage;
mod traits;

#[cfg(feature = "azure")]
pub use backends::azure::AzureBlobStore;
#[cfg(feature = "gcp")]
pub use backends::gcs::GcsBlobStore;
// Re-export backend types for direct usage
pub use backends::local::LocalBlobStore;
#[cfg(feature = "aws")]
pub use backends::s3::S3BlobStore;
pub use config::{BlobStorageConfig, default_blob_store_path};
pub use dispatcher::BlobStoreDispatcher;
pub use error::{BlobError, BlobResult};
pub use metadata::BlobMetadata;
pub use metrics::{BlobMetrics, Timer};
pub use presign::{
    HttpMethod,
    MAX_PRESIGN_EXPIRY,
    PresignedRequest,
    PresignedUrl,
    validate_expiry,
};
pub use storage::{BlobStorage, PutResult};
pub use traits::BlobStore;
