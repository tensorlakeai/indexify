use std::{sync::Arc, time::Duration};

use anyhow::Result;
use blob_store::BlobStorage;
use state_store::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    IndexifyState,
};
use tokio::time::{self};
use tracing::{debug, error, info};

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

    pub async fn start(&self) {
        info!("starting garbage collector");

        let mut rx = self.rx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            rx.borrow_and_update();
            match self.run().await {
                Ok(has_more) => {
                    if has_more {
                        rx.mark_changed();
                        // throttling to avoid tight loop
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
                Err(err) => {
                    error!("error processing gc work: {:?}", err);
                    // prevent spurious errors from causing a tight loop
                    time::sleep(Duration::from_secs(30)).await;
                }
            }
            tokio::select! {
                _ = rx.changed() => {},
                _ = shutdown_rx.changed() => {
                    info!("gc executor shutting down");
                    break;
                }
            }
        }
    }

    pub async fn run(&self) -> Result<bool> {
        let state = self.state.clone();
        let storage = self.storage.clone();

        let mut deleted_urls = Vec::with_capacity(10);
        let urls = state.reader().get_gc_urls(Some(10))?;
        let urls_len = urls.len();
        for url in urls.iter() {
            debug!("Deleting url {:?}", url);
            if let Err(e) = storage.delete(url).await {
                error!("Error deleting url {:?}: {:?}", url, e);
            } else {
                deleted_urls.push(url.clone());
            }
        }
        if !deleted_urls.is_empty() {
            self.state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::RemoveGcUrls(deleted_urls),
                    processed_state_changes: vec![],
                })
                .await?;
        }

        // has more
        Ok(urls_len == 10)
    }
}
