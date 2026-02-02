//! Error types for blob store operations.

use std::fmt;

/// Result type for blob store operations.
pub type BlobResult<T> = Result<T, BlobError>;

/// Errors that can occur during blob store operations.
#[derive(Debug)]
pub enum BlobError {
    /// Blob not found at the specified location.
    NotFound { uri: String },

    /// Invalid URI format or scheme.
    InvalidUri { uri: String, reason: String },

    /// I/O error during blob operation.
    IoError { source: std::io::Error },

    /// Network error (S3/GCS/Azure).
    NetworkError { source: anyhow::Error },

    /// Multipart upload error.
    MultipartError { reason: String },

    /// Presigned URL generation error.
    PresignError { reason: String },

    /// Backend not supported.
    UnsupportedBackend { scheme: String },

    /// Generic error.
    Other { source: anyhow::Error },
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlobError::NotFound { uri } => write!(f, "Blob not found: {}", uri),
            BlobError::InvalidUri { uri, reason } => {
                write!(f, "Invalid URI '{}': {}", uri, reason)
            }
            BlobError::IoError { source } => write!(f, "I/O error: {}", source),
            BlobError::NetworkError { source } => write!(f, "Network error: {}", source),
            BlobError::MultipartError { reason } => write!(f, "Multipart upload error: {}", reason),
            BlobError::PresignError { reason } => {
                write!(f, "Presigned URL generation error: {}", reason)
            }
            BlobError::UnsupportedBackend { scheme } => {
                write!(f, "Unsupported backend: {}", scheme)
            }
            BlobError::Other { source } => write!(f, "Blob store error: {}", source),
        }
    }
}

impl std::error::Error for BlobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlobError::IoError { source } => Some(source),
            BlobError::NetworkError { source } => Some(source.as_ref()),
            BlobError::Other { source } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for BlobError {
    fn from(err: std::io::Error) -> Self {
        BlobError::IoError { source: err }
    }
}

impl From<anyhow::Error> for BlobError {
    fn from(err: anyhow::Error) -> Self {
        BlobError::Other { source: err }
    }
}

impl From<object_store::Error> for BlobError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { .. } => BlobError::NotFound {
                uri: err.to_string(),
            },
            _ => BlobError::NetworkError {
                source: anyhow::Error::from(err),
            },
        }
    }
}

impl From<url::ParseError> for BlobError {
    fn from(err: url::ParseError) -> Self {
        BlobError::InvalidUri {
            uri: String::new(),
            reason: err.to_string(),
        }
    }
}
