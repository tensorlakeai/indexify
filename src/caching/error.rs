use std::fmt::{self, Debug};

use flexbuffers::{DeserializationError, ReaderError};
use redis::RedisError;

#[derive(Debug)]
pub enum IndexifyCachingError {
    SerializationError(String),
    DeserializationError(String),

    IoError(std::io::Error),
    RedisCacheError(RedisError),
}

impl fmt::Display for IndexifyCachingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexifyCachingError::SerializationError(msg) => {
                write!(
                    f,
                    "Indexify Cache was unable to serialize an object: {}",
                    msg
                )
            }
            IndexifyCachingError::DeserializationError(msg) => {
                write!(
                    f,
                    "Indexify Cache was unable to deserialize an object: {}",
                    msg
                )
            }
            IndexifyCachingError::IoError(e) => {
                write!(f, "Indexify Cache encountered an IO error in communicating with the caching layer: {}", e.to_string())
            }
            IndexifyCachingError::RedisCacheError(e) => {
                write!(f, "Indexify Cache encountered an error in communicating with the Redis caching layer: {}", e.to_string())
            }
        }
    }
}

impl std::error::Error for IndexifyCachingError {}

impl From<redis::RedisError> for IndexifyCachingError {
    fn from(e: redis::RedisError) -> Self {
        match e.kind() {
            redis::ErrorKind::IoError => IndexifyCachingError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Redis IO Error: {}", e),
            )),
            _ => IndexifyCachingError::RedisCacheError(e),
        }
    }
}

impl From<serde::de::value::Error> for IndexifyCachingError {
    fn from(e: serde::de::value::Error) -> Self {
        IndexifyCachingError::DeserializationError(e.to_string())
    }
}

impl From<DeserializationError> for IndexifyCachingError {
    fn from(e: DeserializationError) -> Self {
        IndexifyCachingError::DeserializationError(e.to_string())
    }
}

impl From<ReaderError> for IndexifyCachingError {
    fn from(e: ReaderError) -> Self {
        IndexifyCachingError::DeserializationError(e.to_string())
    }
}

// Tests for error formatting
#[cfg(test)]
mod tests {
    use super::IndexifyCachingError;

    #[test]
    fn test_indexify_caching_error_display() {
        let err = IndexifyCachingError::SerializationError("test".to_string());
        assert_eq!(
            err.to_string(),
            "Indexify Cache was unable to serialize an object: test"
        );
        let err = IndexifyCachingError::DeserializationError("test".to_string());
        assert_eq!(
            err.to_string(),
            "Indexify Cache was unable to deserialize an object: test"
        );
        let err =
            IndexifyCachingError::IoError(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        assert_eq!(
            err.to_string(),
            "Indexify Cache encountered an IO error in communicating with the caching layer: test"
        );
        let redis_error =
            redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let err = IndexifyCachingError::from(redis_error);
        assert_eq!(err.to_string(), "Indexify Cache encountered an IO error in communicating with the caching layer: Redis IO Error: test");
    }
}
