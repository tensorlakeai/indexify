use std::sync::Arc;

use anyhow::{Context, Result};
use blob_store::BlobStorage;
use bytes::Bytes;
use slatedb::{config::DbOptions, db::Db};

pub struct WriteContextData {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub key: String,
    pub value: Vec<u8>,
}
pub struct KVS {
    kv_store: Arc<Db>,
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
        })
    }

    pub async fn put_ctx_state(&self, req: WriteContextData) -> Result<()> {
        let key = format!(
            "{}|{}|{}|{}",
            req.namespace, req.compute_graph, req.invocation_id, req.key
        );
        self.kv_store.put(key.as_bytes(), &req.value).await;
        Ok(())
    }

    pub async fn get_ctx_state_key(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let key = format!("{}|{}|{}|{}", namespace, compute_graph, invocation_id, key);
        let value = self.kv_store.get(key.as_bytes()).await?;
        Ok(value)
    }
}
