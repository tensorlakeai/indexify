use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info};

use crate::state_store::{
    IndexifyState,
    requests::{RequestPayload, StateMachineUpdateRequest},
};

pub struct UsageProcessor {
    indexify_state: Arc<IndexifyState>,
}

impl UsageProcessor {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Result<Self> {
        Ok(Self { indexify_state })
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut usage_events_rx = self.indexify_state.usage_events_rx.clone();
        let mut cursor: Option<Vec<u8>> = None;

        loop {
            tokio::select! {
                _ = usage_events_rx.changed() => {
                    usage_events_rx.borrow_and_update();

                    if let Err(error) = self.process_allocation_usage_events(&mut cursor).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }

                },
                _ = shutdown_rx.changed() => {
                    println!("UsageProcessor received shutdown signal.");
                    break;
                }
            }
        }
    }

    async fn process_allocation_usage_events(&self, cursor: &mut Option<Vec<u8>>) -> Result<()> {
        let (events, new_cursor) = self
            .indexify_state
            .reader()
            .allocation_usage(cursor.clone().as_ref())?;

        let mut processed_events = Vec::new();

        for event in events {
            info!(
                allocation_id = %event.allocation_id,
                application = %event.application,
                request_id = %event.request_id,
                "processing allocation usage event"
            );

            // TODO: submit the usage to an external system, like SQS, Kafka, etc.
            // At the moment we just delete it to avoid filling up the state store.
            processed_events.push(event);
        }

        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::ProcessAllocationUsageEvents(processed_events),
            })
            .await?;

        if let Some(c) = new_cursor {
            cursor.replace(c);
        };

        Ok(())
    }
}
