use std::{collections::HashMap, sync::{Arc, RwLock}};

use anyhow::{Context, Result};
use blob_store::BlobStorage;
use indexify_utils::get_epoch_time_in_ms;
use metrics::kv_storage::Metrics;
use slatedb::{config::DbOptions, db::Db};
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::invocation_events::InvocationStateChangeEvent;

pub struct InvocationDiagnostics {
    kv_store: Arc<Db>,
    metrics: Metrics,
    channels: Arc<RwLock<HashMap<String, mpsc::Sender<InvocationStateChangeEvent>>>>,
}

impl InvocationDiagnostics {
    pub async fn new(blob_store: Arc<BlobStorage>, prefix: &str) -> Result<Self> {
        let options = DbOptions::default();
        let kv_store = Db::open_with_opts(
            blob_store.get_path().child(prefix),
            options,
            Arc::new(blob_store.get_object_store()),
        )
        .await
        .context("error opening kv store")?;
        Ok(InvocationDiagnostics {
            kv_store: Arc::new(kv_store),
            metrics: Metrics::new(),
            channels: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn start(&self, mut shutdown_ch: tokio::sync::watch::Receiver<()>, mut event_rx: tokio::sync::mpsc::Receiver<InvocationStateChangeEvent>) {
        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    if let Some(event) = event {
                        let key = format!("invocation_diagnostics|{}|{}", event.invocation_id(), get_epoch_time_in_ms());
                        let value = serde_json::to_vec(&event);
                        if let Err(err) = &value {
                            tracing::error!("error serializing event: {:?}", err);
                            continue;
                        }
                        let _ = self.kv_store.put(key.as_bytes(), &value.unwrap()).await;
                        if let Some(tx) = self.channels.read().unwrap().get(&event.invocation_id()) {
                            if let Err(TrySendError::Closed(_)) = tx.try_send(event.clone()) {
                                self.channels.write().unwrap().remove(&event.invocation_id());
                            }
                        }
                    }
                },
                _ = shutdown_ch.changed() => {
                    break;
                },
            }
        }
    }

    pub async fn subscribe(&self, invocation_id: &str) -> Result<mpsc::Receiver<InvocationStateChangeEvent>> {
        let (tx, rx) = mpsc::channel(100);
        self.channels.write().unwrap().insert(invocation_id.to_string(), tx);
        Ok(rx)
    }
}