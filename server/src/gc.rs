use std::sync::Arc;

use anyhow::Result;
use blob_store::BlobStorage;
use state_store::IndexifyState;

pub struct Gc {
    state: Arc<IndexifyState>,
    storage: Arc<BlobStorage>,
    rx: tokio::sync::watch::Receiver<()>,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
}

impl Gc {
    pub fn new(
        state: Arc<IndexifyState>,
        storage: Arc<BlobStorage>,
        shutdown_rx: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        let rx = state.get_gc_watcher();
        Self {
            state,
            storage,
            rx,
            shutdown_rx,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let state = self.state.clone();
        let storage = self.storage.clone();
        loop {
            if self.shutdown_rx.has_changed().unwrap_or(false) {
                println!("Shutdown signal received.");
                return Ok(());
            }

            let urls = state.reader().get_gc_urls(Some(10))?;
            if urls.is_empty() {
                tokio::select! {
                    _ = self.rx.changed() => { self.rx.borrow_and_update(); }
                    _ = self.shutdown_rx.changed() => {
                        println!("Shutdown signal received.");
                        return Ok(());
                    }
                }
            } else {
                for url in urls.iter() {
                    tracing::debug!("Deleting url {:?}", url);
                    if let Err(e) = storage.delete(url).await {
                        tracing::error!("Error deleting url {:?}: {:?}", url, e);
                    }
                }
                self.state
                    .write(state_store::requests::StateMachineUpdateRequest {
                        payload: state_store::requests::RequestPayload::RemoveGcUrls(urls),
                        state_changes_processed: vec![],
                    })
                    .await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use blob_store::BlobStorage;
    use bytes::Bytes;
    use data_model::{
        test_objects::tests::{mock_graph_a, TEST_NAMESPACE},
        NodeOutput,
    };
    use futures::stream;
    use state_store::{
        requests::{
            CreateComputeGraphRequest,
            DeleteComputeGraphRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        serializer::{JsonEncode, JsonEncoder},
        state_machine::IndexifyObjectsColumns,
        IndexifyState,
    };
    use tokio::sync::watch;
    use tracing::info;

    use super::*;

    #[tokio::test]
    async fn test_gc() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let state = IndexifyState::new(temp_dir.path().join("state"))
            .await
            .unwrap();
        let config =
            blob_store::BlobStorageConfig::new(temp_dir.path().join("blob").to_str().unwrap());
        let storage = Arc::new(BlobStorage::new(config)?);
        let (tx, rx) = watch::channel(());
        let mut gc = Gc::new(state.clone(), storage.clone(), rx);

        tokio::spawn(async move {
            info!("starting garbage collector");
            let _ = gc.start().await;
            info!("garbage collector shutdown");
        });
        // Create a compute graph
        let compute_graph = mock_graph_a(None);
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(CreateComputeGraphRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    compute_graph: compute_graph.clone(),
                }),
                state_changes_processed: vec![],
            })
            .await?;

        let data = "aaaa";
        let path = "qqqq";
        let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
        let res = storage.put(path, data_stream).await?;

        let output = NodeOutput {
            id: "id".to_string(),
            graph_version: Default::default(),
            namespace: TEST_NAMESPACE.to_string(),
            compute_fn_name: "fn_a".to_string(),
            compute_graph_name: "graph_A".to_string(),
            invocation_id: "invocation_id".to_string(),
            payload: data_model::OutputPayload::Fn(data_model::DataPayload {
                path: res.url.clone(),
                size: res.size_bytes,
                sha256_hash: res.sha256_hash,
            }),
            errors: None,
            reduced_state: false,
        };
        let key = output.key(&output.invocation_id);
        let serialized_output = JsonEncoder::encode(&output)?;
        state.db.put_cf(
            &IndexifyObjectsColumns::FnOutputs.cf_db(&state.db),
            key,
            &serialized_output,
        )?;

        storage.read_bytes(&res.url).await?;

        let request = RequestPayload::DeleteComputeGraph(DeleteComputeGraphRequest {
            namespace: TEST_NAMESPACE.to_string(),
            name: compute_graph.name.clone(),
        });
        state
            .write(StateMachineUpdateRequest {
                payload: request,
                state_changes_processed: vec![],
            })
            .await?;

        let time = std::time::Instant::now();
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            if time.elapsed().as_secs() > 10 {
                panic!("Timeout waiting for GC to finish");
            }
            let urls = state.reader().get_gc_urls(None)?;
            if urls.is_empty() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        assert!(storage.read_bytes(&res.url).await.is_err());

        tx.send(()).unwrap();

        Ok(())
    }
}
