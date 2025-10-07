use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use opentelemetry::KeyValue;
use slatedb::{Db, DbBuilder};

use crate::{
    blob_store::BlobStorage,
    metrics::{kv_storage::Metrics, Timer},
};

pub struct WriteContextData {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
    pub key: String,
    pub value: Vec<u8>,
}

pub struct ReadContextData {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
    pub key: String,
}

#[allow(clippy::upper_case_acronyms)]
pub struct KVS {
    kv_store: Arc<Db>,
    metrics: Metrics,
}

impl KVS {
    pub async fn new(blob_store: Arc<BlobStorage>, prefix: &str) -> Result<Self> {
        let kv_store = DbBuilder::new(
            blob_store.get_path().child(prefix),
            blob_store.get_object_store(),
        )
        .build()
        .await
        .context("error opening kv store")?;
        Ok(KVS {
            kv_store: Arc::new(kv_store),
            metrics: Metrics::new(),
        })
    }

    pub async fn close_db(&self) -> Result<()> {
        self.kv_store.flush().await?;
        self.kv_store.close().await?;
        Ok(())
    }

    pub async fn put_ctx_state(&self, req: WriteContextData) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "put_ctx_state")];
        let _timer = Timer::start_with_labels(&self.metrics.writes, timer_kvs);

        let key = format!(
            "{}|{}|{}|{}",
            req.namespace, req.application, req.request_id, req.key
        );
        self.kv_store.put(key.as_bytes(), &req.value).await?;
        Ok(())
    }

    pub async fn get_ctx_state_key(&self, req: ReadContextData) -> Result<Option<Bytes>> {
        let timer_kvs = &[KeyValue::new("op", "get_ctx_state_key")];
        let _timer = Timer::start_with_labels(&self.metrics.reads, timer_kvs);

        let key = format!(
            "{}|{}|{}|{}",
            req.namespace, req.application, req.request_id, req.key
        );
        let value = self.kv_store.get(key.as_bytes()).await?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob_store::BlobStorageConfig;

    #[tokio::test]
    async fn test_kvs() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let kv_store = KVS::new(
            Arc::new(BlobStorage::new(BlobStorageConfig {
                path: format!(
                    "file://{}",
                    temp_dir.path().join("blob_store").to_str().unwrap()
                ),
                region: None,
            })?),
            "test",
        )
        .await
        .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let key = "test_key";
        let value = b"test_value";
        kv_store
            .put_ctx_state(WriteContextData {
                namespace: "test_namespace".to_string(),
                application: "test_application".to_string(),
                request_id: "test_invocation_id".to_string(),
                key: key.to_string(),
                value: value.to_vec(),
            })
            .await
            .unwrap();

        let result = kv_store
            .get_ctx_state_key(ReadContextData {
                namespace: "test_namespace".to_string(),
                application: "test_application".to_string(),
                request_id: "test_invocation_id".to_string(),
                key: key.to_string(),
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result, Bytes::copy_from_slice(value));

        Ok(())
    }
}
