use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_broadcast::Receiver;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use otlp_logs_exporter::OtlpLogsExporter;
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, instrument, warn};

use crate::{
    cloud_events::create_batched_export_request,
    metrics::{Timer, low_latency_boundaries},
    state_store::{
        IndexifyState,
        driver::Writer,
        request_events::{PersistedRequestStateChangeEvent, RequestStateChangeEvent},
        state_machine,
    },
};

// Constants
const HTTP_BATCH_SIZE: usize = 100;
const HTTP_BATCH_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_DELETE_ATTEMPTS: u8 = 10;
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RequestStateChangeProcessor {
    indexify_state: Arc<IndexifyState>,
    sse_processing_latency: Histogram<f64>,
    http_processing_latency: Histogram<f64>,
    sse_events_counter: Counter<u64>,
    http_events_counter: Counter<u64>,
    // Keep gauge alive so the callback continues to fire
    _channel_buffer_size: ObservableGauge<u64>,
}

impl RequestStateChangeProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let meter = opentelemetry::global::meter("request_state_change_processor_metrics");

        let sse_processing_latency = meter
            .f64_histogram("indexify.request_state_change.sse_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("SSE event delivery latency in seconds")
            .build();

        let http_processing_latency = meter
            .f64_histogram("indexify.request_state_change.http_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("HTTP export batch processing latency in seconds")
            .build();

        let sse_events_counter = meter
            .u64_counter("indexify.request_state_change.sse_events_total")
            .with_description("Total number of events delivered via SSE")
            .build();

        let http_events_counter = meter
            .u64_counter("indexify.request_state_change.http_events_total")
            .with_description("Total number of events exported via HTTP")
            .build();

        let state_for_gauge = indexify_state.clone();
        let _channel_buffer_size = meter
            .u64_observable_gauge("indexify.request_state_change.channel_buffer_size")
            .with_description("Number of events currently buffered in the broadcast channel")
            .with_callback(move |observer| {
                observer.observe(state_for_gauge.request_events_tx.len() as u64, &[]);
            })
            .build();

        Self {
            indexify_state,
            sse_processing_latency,
            http_processing_latency,
            sse_events_counter,
            http_events_counter,
            _channel_buffer_size,
        }
    }

    #[instrument(skip_all)]
    pub async fn start(
        &self,
        cloud_events_exporter: Option<OtlpLogsExporter>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        let cancel_token = CancellationToken::new();
        let tracker = TaskTracker::new();

        // Subscribe to request state change events from the broadcast channel
        let sse_rx = self.indexify_state.subscribe_request_state_changes();

        // Spawn SSE delivery worker
        tracker.spawn({
            let state = self.indexify_state.clone();
            let latency = self.sse_processing_latency.clone();
            let counter = self.sse_events_counter.clone();
            let token = cancel_token.child_token();
            async move {
                sse_delivery_worker(sse_rx, state, latency, counter, token).await;
            }
        });

        // Spawn HTTP export worker (if exporter is configured)
        if let Some(exporter) = cloud_events_exporter {
            let http_rx = self.indexify_state.subscribe_request_state_changes();
            let state = self.indexify_state.clone();
            let latency = self.http_processing_latency.clone();
            let counter = self.http_events_counter.clone();
            let token = cancel_token.child_token();
            tracker.spawn(async move {
                http_export_worker(http_rx, exporter, state, latency, counter, token).await;
            });
        } else {
            info!("HTTP exporter disabled - no exporter configured");
        }

        // Wait for shutdown signal
        let _ = shutdown_rx.changed().await;
        info!("Request state change processor shutting down");

        // Signal workers to stop and close the tracker
        cancel_token.cancel();
        tracker.close();

        // Wait for workers to finish with timeout
        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, tracker.wait())
            .await
            .is_err()
        {
            warn!(
                timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT.as_secs(),
                "Graceful shutdown timeout, some events may not have been delivered"
            );
        }

        info!("Request state change processor stopped");
    }
}

/// SSE delivery worker - receives events via broadcast channel and delivers
/// immediately. This is the fast path for connected SSE clients.
async fn sse_delivery_worker(
    mut rx: Receiver<RequestStateChangeEvent>,
    state: Arc<IndexifyState>,
    latency: Histogram<f64>,
    counter: Counter<u64>,
    cancel_token: CancellationToken,
) {
    info!("SSE delivery worker started");

    loop {
        // Wait for next event from broadcast channel
        let event = tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => event,
                    Err(async_broadcast::RecvError::Closed) => {
                        info!("SSE delivery worker: channel closed, shutting down");
                        return;
                    }
                    Err(async_broadcast::RecvError::Overflowed(n)) => {
                        warn!("SSE delivery worker: channel overflowed, lost {} events", n);
                        continue;
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("SSE delivery worker shutting down");
                return;
            }
        };

        let _timer = Timer::start_with_labels(&latency, &[]);

        // Push to SSE subscribers immediately (fire and forget)
        state.push_request_event(event).await;
        counter.add(1, &[]);
    }
}

/// HTTP export worker - persists events to DB, batches them, and exports via
/// HTTP. Provides stronger delivery guarantees than SSE.
async fn http_export_worker(
    mut rx: Receiver<RequestStateChangeEvent>,
    mut exporter: OtlpLogsExporter,
    state: Arc<IndexifyState>,
    latency: Histogram<f64>,
    counter: Counter<u64>,
    cancel_token: CancellationToken,
) {
    info!("HTTP export worker started");

    // Batch accumulator
    let mut batch: Vec<PersistedRequestStateChangeEvent> = Vec::with_capacity(HTTP_BATCH_SIZE);
    let mut batch_deadline = tokio::time::Instant::now() + HTTP_BATCH_TIMEOUT;

    loop {
        // Collect events until batch is full or timeout
        let should_flush = tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        // Persist event to DB immediately for durability
                        match persist_event(&state, event).await {
                            Ok(persisted) => {
                                batch.push(persisted);
                                batch.len() >= HTTP_BATCH_SIZE
                            }
                            Err(e) => {
                                error!(%e, "Failed to persist event to DB, event lost");
                                false
                            }
                        }
                    }
                    Err(async_broadcast::RecvError::Closed) => {
                        info!("HTTP export worker: channel closed, flushing remaining batch");
                        true
                    }
                    Err(async_broadcast::RecvError::Overflowed(n)) => {
                        warn!("HTTP export worker: channel overflowed, lost {} events", n);
                        false
                    }
                }
            }
            _ = tokio::time::sleep_until(batch_deadline) => {
                // Timeout reached, flush if we have events
                !batch.is_empty()
            }
            _ = cancel_token.cancelled() => {
                info!("HTTP export worker: shutdown requested, flushing remaining batch");
                true
            }
        };

        if should_flush && !batch.is_empty() {
            let _timer = Timer::start_with_labels(&latency, &[]);

            // Export the batch
            let batch_size = batch.len();
            match export_batch(&mut exporter, &batch).await {
                Ok(()) => {
                    // Successfully exported - delete from DB
                    if let Err(e) = remove_events_with_backoff(&state, &batch).await {
                        error!(%e, "Failed to delete exported events from DB");
                        // Events are exported but not deleted - they may be
                        // re-exported on restart
                        // This is acceptable since HTTP export should be
                        // idempotent
                    }
                    counter.add(batch_size as u64, &[]);
                    info!(count = batch_size, "HTTP batch exported successfully");
                }
                Err(e) => {
                    error!(%e, count = batch_size, "HTTP batch export failed after retries");
                    // Events remain in DB for retry on next startup
                    // Don't delete them
                }
            }

            batch.clear();
            batch_deadline = tokio::time::Instant::now() + HTTP_BATCH_TIMEOUT;
        }

        // Check if we should exit
        if cancel_token.is_cancelled() ||
            matches!(rx.try_recv(), Err(async_broadcast::TryRecvError::Closed))
        {
            // Final flush already done above
            if batch.is_empty() {
                info!("HTTP export worker shutting down");
                return;
            }
        }
    }
}

/// Persist a single event to the database.
async fn persist_event(
    state: &Arc<IndexifyState>,
    event: RequestStateChangeEvent,
) -> Result<PersistedRequestStateChangeEvent> {
    let txn = state.db.transaction();

    // Create persisted event with unique ID
    let event_id = state
        .request_event_id_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let persisted = PersistedRequestStateChangeEvent::new(event_id.into(), event);

    // Write to DB
    state_machine::persist_single_request_state_change_event(&txn, &persisted).await?;

    txn.commit().await?;
    Ok(persisted)
}

/// Export a batch of events via HTTP.
async fn export_batch(
    exporter: &mut OtlpLogsExporter,
    events: &[PersistedRequestStateChangeEvent],
) -> Result<()> {
    let raw_events: Vec<_> = events.iter().map(|e| e.event.clone()).collect();
    let request = create_batched_export_request(&raw_events)?;
    exporter.send_request(request).await?;
    Ok(())
}

/// Remove events from database with exponential backoff on failure.
async fn remove_events_with_backoff(
    state: &Arc<IndexifyState>,
    events: &[PersistedRequestStateChangeEvent],
) -> Result<()> {
    for attempt in 1..=MAX_DELETE_ATTEMPTS {
        let txn = state.db.transaction();

        if let Err(e) = state_machine::remove_request_state_change_events(&txn, events).await {
            error!(
                %e,
                attempt,
                max_attempts = MAX_DELETE_ATTEMPTS,
                "Error removing events, retrying..."
            );

            if attempt == MAX_DELETE_ATTEMPTS {
                return Err(e);
            }

            tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            continue;
        }

        match txn.commit().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                error!(
                    %e,
                    attempt,
                    max_attempts = MAX_DELETE_ATTEMPTS,
                    "Error committing transaction, retrying..."
                );

                if attempt == MAX_DELETE_ATTEMPTS {
                    return Err(e.into());
                }

                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to remove events after {} attempts",
        MAX_DELETE_ATTEMPTS
    ))
}
