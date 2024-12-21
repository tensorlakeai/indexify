use std::sync::Arc;

use anyhow::{Context, Result};
use blob_store::BlobStorage;
use bytes::Bytes;
use metrics::{kv_storage::Metrics, Timer};
use opentelemetry::KeyValue;
use slatedb::{config::DbOptions, db::Db};

pub struct WriteContextData {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub key: String,
    pub value: Vec<u8>,
}

pub struct ReadContextData {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub key: String,
}

pub struct KVS {
    kv_store: Arc<Db>,
    metrics: Metrics,
}

impl KVS {
    pub async fn new(blob_store: Arc<BlobStorage>, prefix: &str) -> Result<Self> {
        let options = DbOptions::default();
        let kv_store = Db::open_with_opts(
            blob_store.get_path().child(prefix),
            options,
            Arc::new(blob_store.get_object_store()),
        )
        .await
        .context("error opening kv store")?;
        Ok(KVS {
            kv_store: Arc::new(kv_store),
            metrics: Metrics::new(),
        })
    }

    pub async fn put_ctx_state(&self, req: WriteContextData) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "put_ctx_state")];
        let _timer = Timer::start_with_labels(&self.metrics.writes, timer_kvs);

        let key = format!(
            "{}|{}|{}|{}",
            req.namespace, req.compute_graph, req.invocation_id, req.key
        );
        self.kv_store.put(key.as_bytes(), &req.value).await;
        Ok(())
    }

    pub async fn get_ctx_state_key(&self, req: ReadContextData) -> Result<Option<Bytes>> {
        let timer_kvs = &[KeyValue::new("op", "get_ctx_state_key")];
        let _timer = Timer::start_with_labels(&self.metrics.reads, timer_kvs);

        let key = format!(
            "{}|{}|{}|{}",
            req.namespace, req.compute_graph, req.invocation_id, req.key
        );
        let value = self.kv_store.get(key.as_bytes()).await?;
        Ok(value)
    }
}
