use std::{sync::Arc, time::Duration};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Gauge, Histogram},
};
use otlp_logs_exporter::OtlpLogsExporter;
use tokio::sync::Notify;
use tracing::{error, info, instrument};

use crate::{
    cloud_events::create_batch_export_request,
    metrics::{Timer, low_latency_boundaries},
    state_store::{
        IndexifyState,
        driver::Writer,
        request_events::PersistedRequestStateChangeEvent,
        state_machine,
    },
};

pub struct RequestStateChangeProcessor {
    indexify_state: Arc<IndexifyState>,
    processing_latency: Histogram<f64>,
    events_counter: Gauge<u64>,
    max_attempts: u8,
}

impl RequestStateChangeProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let meter = opentelemetry::global::meter("request_state_change_processor_metrics");

        let processing_latency = meter
            .f64_histogram("indexify.request_state_change.processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("request state change processor event processing latency in seconds")
            .build();

        let events_counter = meter
            .u64_gauge("indexify.request_state_change.events_processed_total")
            .with_description("total number of processed request state change events")
            .build();

        Self {
            indexify_state,
            processing_latency,
            events_counter,
            max_attempts: 10,
        }
    }

    #[instrument(skip_all)]
    pub async fn start(
        &self,
        mut cloud_events_exporter: Option<OtlpLogsExporter>,
        mut shutdown_rx: tokio::sync::watch::Receiver<()>,
    ) {
        let mut request_events_rx = self.indexify_state.request_events_rx.clone();
        let mut cursor: Option<Vec<u8>> = None;

        let notify = Arc::new(Notify::new());
        loop {
            tokio::select! {
                _ = request_events_rx.changed() => {
                    request_events_rx.borrow_and_update();

                    if let Err(error) = self.process_request_state_change_events(&mut cursor, &notify, &mut cloud_events_exporter).await {
                        error!(
                            %error,
                            "error processing request state change events"
                        );
                    }
                },
                _ = notify.notified() => {
                    if let Err(error) = self.process_request_state_change_events(&mut cursor, &notify, &mut cloud_events_exporter).await {
                        error!(
                            %error,
                            "error processing request state change events"
                        );
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("request state change processor shutting down");
                    break;
                }
            }
        }
    }

    #[instrument(skip(self, notify), fields(calling_cursor = ?cursor))]
    async fn process_request_state_change_events(
        &self,
        cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
        cloud_events_exporter: &mut Option<OtlpLogsExporter>,
    ) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "process_request_state_change_events")];
        let _timer = Timer::start_with_labels(&self.processing_latency, timer_kvs);

        let (events, new_cursor) = self
            .indexify_state
            .reader()
            .request_state_change_events(cursor.as_ref())
            .await?;

        if events.is_empty() {
            return Ok(());
        }

        self.events_counter.record(events.len() as u64, &[]);

        // Send batch of events to OTLP exporter if configured
        if let Some(exporter) = cloud_events_exporter {
            if let Err(error) = self
                .send_batched_events_to_exporter(exporter, &events)
                .await
            {
                error!(
                    %error,
                    event_count = events.len(),
                    "error sending batched events to OTLP exporter"
                );
                return Err(error);
            }
        }

        // Push all events through the state
        for event in &events {
            if let Err(error) = self
                .indexify_state
                .push_request_event(event.event.clone())
                .await
            {
                error!(
                    %error,
                    event_id = %event.id,
                    namespace = %event.event.namespace(),
                    application = %event.event.application_name(),
                    request_id = %event.event.request_id(),
                    "error pushing request event to state"
                );
                return Err(error);
            }
        }

        if let Some(c) = new_cursor {
            cursor.replace(c);
        };

        info!(
            processed_events_len = events.len(),
            "removing processed events"
        );
        self.remove_and_commit_with_backoff(events).await?;

        notify.notify_one();

        Ok(())
    }

    async fn remove_and_commit_with_backoff(
        &self,
        processed_events: Vec<PersistedRequestStateChangeEvent>,
    ) -> Result<()> {
        for attempt in 1..=self.max_attempts {
            let txn = self.indexify_state.db.transaction();

            if let Err(error) =
                state_machine::remove_request_state_change_events(&txn, processed_events.as_slice())
                    .await
            {
                error!(
                    %error,
                    attempt,
                    "error removing processed request state change events, retrying..."
                );

                if attempt == self.max_attempts {
                    return Err(error);
                }

                let delay = Duration::from_secs(attempt as u64);
                tokio::time::sleep(delay).await;
            }

            match txn.commit().await {
                Ok(_) => return Ok(()),
                Err(commit_error) => {
                    error!(
                        %commit_error,
                        attempt,
                        "error committing transaction to remove processed request state change events, retrying..."
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

    async fn send_batched_events_to_exporter(
        &self,
        exporter: &mut OtlpLogsExporter,
        events: &[PersistedRequestStateChangeEvent],
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        info!(
            event_count = events.len(),
            "sending batched events to OTLP exporter"
        );

        let updates: Vec<_> = events.iter().map(|e| &e.event).collect();
        let requests = create_batch_export_request(&updates)?;
        let request_count = requests.len();

        for request in requests {
            exporter.send_request(request).await?;
        }

        info!(
            event_count = events.len(),
            request_count, "successfully sent batched events to OTLP exporter"
        );

        Ok(())
    }

    /// Process and remove all pending request state change events.
    /// This is useful for tests to drain accumulated events.
    #[allow(dead_code)]
    pub async fn drain_all_events(&self) -> Result<()> {
        let mut cursor: Option<Vec<u8>> = None;

        loop {
            let (events, new_cursor) = self
                .indexify_state
                .reader()
                .request_state_change_events(cursor.as_ref())
                .await?;

            if events.is_empty() {
                break;
            }

            if let Some(c) = new_cursor {
                cursor.replace(c);
            }

            // Process and delete all events
            let mut processed_events = Vec::new();
            for event in events {
                // Best effort push - always delete regardless of result
                let _ = self
                    .indexify_state
                    .push_request_event(event.event.clone())
                    .await;
                processed_events.push(event);
            }

            if !processed_events.is_empty() {
                self.remove_and_commit_with_backoff(processed_events)
                    .await?;
            }
        }

        Ok(())
    }
}
