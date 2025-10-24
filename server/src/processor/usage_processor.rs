use std::{sync::Arc, time::Duration};

use anyhow::Result;
use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::sync::Notify;
use tracing::{error, info, instrument};

use crate::{
    data_model::AllocationUsage,
    metrics::{Timer, low_latency_boundaries},
    state_store::{IndexifyState, driver::Writer, state_machine},
};

pub struct UsageProcessor {
    indexify_state: Arc<IndexifyState>,

    processing_latency: Histogram<f64>,

    max_attempts: u8,
}

impl UsageProcessor {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Result<Self> {
        let meter = opentelemetry::global::meter("usage_processor_metrics");

        let processing_latency = meter
            .f64_histogram("indexify.usage.processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("usage processor event processing latency in seconds")
            .build();

        Ok(Self {
            indexify_state,
            processing_latency,
            max_attempts: 10,
        })
    }

    #[instrument(skip_all)]
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut usage_events_rx = self.indexify_state.usage_events_rx.clone();
        let mut cursor: Option<Vec<u8>> = None;

        let notify = Arc::new(Notify::new());
        loop {
            tokio::select! {
                _ = usage_events_rx.changed() => {
                    usage_events_rx.borrow_and_update();

                    if let Err(error) = self.process_allocation_usage_events(&mut cursor, &notify).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }

                },
                _ = notify.notified() => {
                    if let Err(error) = self.process_allocation_usage_events(&mut cursor, &notify).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("usage processor shutting down");
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn process_allocation_usage_events(
        &self,
        cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "process_allocation_usage_events")];
        Timer::start_with_labels(&self.processing_latency, timer_kvs);

        let (events, new_cursor) = self
            .indexify_state
            .reader()
            .allocation_usage(cursor.clone().as_ref())?;

        if events.is_empty() {
            return Ok(());
        }

        if let Some(c) = new_cursor {
            cursor.replace(c);
        };

        let mut processed_events = Vec::new();
        for event in events {
            if let Err(error) = self.send_to_queue(event.clone()).await {
                error!(
                    %error,
                    namespace = %event.namespace,
                    allocation_id = %event.allocation_id,
                    application = %event.application,
                    request_id = %event.request_id,
                    "error processing allocation usage event"
                );
            } else {
                processed_events.push(event);
            }
        }

        info!(
            processed_count = processed_events.len(),
            "processed allocation usage events"
        );

        if !processed_events.is_empty() {
            self.remove_and_commit_with_backoff(processed_events)
                .await?;
        }

        notify.notify_one();

        Ok(())
    }

    async fn remove_and_commit_with_backoff(
        &self,
        processed_events: Vec<AllocationUsage>,
    ) -> Result<()> {
        for attempt in 1..=10 {
            let txn = self.indexify_state.db.transaction();

            if let Err(error) =
                state_machine::remove_allocation_usage_events(&txn, processed_events.as_slice())
            {
                error!(
                    %error,
                    attempt,
                    "error removing processed allocation usage events, retrying..."
                );

                if attempt == self.max_attempts {
                    return Err(error);
                }

                let delay = Duration::from_secs(attempt as u64);
                tokio::time::sleep(delay).await;
            }

            match txn.commit() {
                Ok(_) => return Ok(()),
                Err(commit_error) => {
                    error!(
                        %commit_error,
                        attempt,
                        "error committing transaction to remove processed allocation usage events, retrying..."
                    );

                    if attempt == self.max_attempts {
                        return Err(anyhow::Error::new(commit_error));
                    }

                    let delay = Duration::from_secs(attempt as u64);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(anyhow::anyhow!("Failed to remove and commit events"))
    }

    async fn send_to_queue(&self, _event: AllocationUsage) -> Result<()> {
        // TODO: Implement the logic to send the usage event to the appropriate queue or
        // processing system. This is a placeholder implementation.
        Ok(())
    }
}
