use std::sync::Arc;

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
        })
    }

    #[instrument(skip_all)]
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut usage_events_rx = self.indexify_state.usage_events_rx.clone();
        let mut cached_events: Vec<_> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;

        let notify = Arc::new(Notify::new());
        loop {
            tokio::select! {
                _ = usage_events_rx.changed() => {
                    usage_events_rx.borrow_and_update();

                    if let Err(error) = self.process_allocation_usage_events(&mut cached_events, &mut cursor, &notify).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }

                },
                _ = notify.notified() => {
                    if let Err(error) = self.process_allocation_usage_events(&mut cached_events, &mut cursor, &notify).await {
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
        cached_events: &mut Vec<AllocationUsage>,
        cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "process_allocation_usage_events")];
        Timer::start_with_labels(&self.processing_latency, timer_kvs);

        if cached_events.is_empty() {
            let (events, new_cursor) = self
                .indexify_state
                .reader()
                .allocation_usage(cursor.clone().as_ref())?;

            if let Some(c) = new_cursor {
                cursor.replace(c);
            };

            cached_events.extend(events);
        }

        if cached_events.is_empty() {
            // No events to process
            return Ok(());
        }

        if !cached_events.is_empty() {
            notify.notify_one();
        }

        let usage_event = cached_events.pop().unwrap();

        let txn = self.indexify_state.db.transaction();
        state_machine::remove_allocation_usage_events(&txn, &[usage_event])?;
        txn.commit()?;

        Ok(())
    }
}
