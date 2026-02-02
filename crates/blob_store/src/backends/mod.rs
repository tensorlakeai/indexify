//! Blob store backend implementations.

pub mod local;

#[cfg(feature = "aws")]
pub mod s3;

#[cfg(feature = "gcp")]
pub mod gcs;

#[cfg(feature = "azure")]
pub mod azure;
