use std::sync::Arc;

use anyhow::Result;

use crate::state_store::IndexifyState;

pub struct UsageProcessor {
    indexify_state: Arc<IndexifyState>,
}

impl UsageProcessor {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Result<Self> {
        Ok(Self { indexify_state })
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut usage_events_rx = self.indexify_state.usage_events_rx.clone();

        loop {
            tokio::select! {
                _ = usage_events_rx.changed() => {
                    usage_events_rx.borrow_and_update();

                    // TODO: submit the usage events to an external queue for processing
                    // Here is where I would load allocation usage events and submit them
                    // to an external system, like SQS, Kafka, etc.
                },
                _ = shutdown_rx.changed() => {
                    println!("UsageProcessor received shutdown signal.");
                    break;
                }
            }
        }
    }

    async fn process_allocation_usage_events(&self, cursor: &mut Option<Vec<u8>>) -> Result<()> {
        let (events, new_cursor) = self.indexify_state.reader().allocation_usage(cursor.clone().as_ref())?;
        for event in events {
            // Process each event
            println!("Processing allocation usage event: {:?}", event);
        }

        if let Some(c) = new_cursor {
            cursor.replace(c);
        };

        Ok(())
    }
}
