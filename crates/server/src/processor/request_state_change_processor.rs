use std::{sync::Arc, time::Duration};

use async_broadcast::Receiver;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, instrument, warn};

use crate::{
    cloud_events,
    metrics::{Timer, low_latency_boundaries},
    queue::Queue,
    state_store::{IndexifyState, request_events::RequestStateChangeEvent},
};

// Constants
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RequestStateChangeProcessor {
    indexify_state: Arc<IndexifyState>,
    sse_processing_latency: Histogram<f64>,
    queue_processing_latency: Histogram<f64>,
    sse_events_counter: Counter<u64>,
    queue_events_counter: Counter<u64>,
    queue_send_errors: Counter<u64>,
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

        let queue_processing_latency = meter
            .f64_histogram("indexify.request_state_change.queue_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency to send a single event to the external queue")
            .build();

        let sse_events_counter = meter
            .u64_counter("indexify.request_state_change.sse_events_total")
            .with_description("Total number of events delivered via SSE")
            .build();

        let queue_events_counter = meter
            .u64_counter("indexify.request_state_change.queue_events_total")
            .with_description("Total number of events sent to the external queue")
            .build();

        let queue_send_errors = meter
            .u64_counter("indexify.request_state_change.queue_send_errors_total")
            .with_description("Total number of errors sending events to the external queue")
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
            queue_processing_latency,
            sse_events_counter,
            queue_events_counter,
            queue_send_errors,
            _channel_buffer_size,
        }
    }

    #[instrument(skip_all)]
    pub async fn start(&self, queue: Option<Arc<Queue>>, mut shutdown_rx: watch::Receiver<()>) {
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

        // Spawn queue export worker (if queue is configured)
        if let Some(queue) = queue {
            let queue_rx = self.indexify_state.subscribe_request_state_changes();
            let latency = self.queue_processing_latency.clone();
            let counter = self.queue_events_counter.clone();
            let errors = self.queue_send_errors.clone();
            let token = cancel_token.child_token();
            tracker.spawn(async move {
                queue_export_worker(queue_rx, queue, latency, counter, errors, token).await;
            });
        } else {
            info!("Queue export worker disabled - no queue configured");
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

/// Queue export worker - sends events individually to an external message
/// queue. No local persistence or batching; events are sent as they arrive.
async fn queue_export_worker(
    mut rx: Receiver<RequestStateChangeEvent>,
    queue: Arc<Queue>,
    latency: Histogram<f64>,
    counter: Counter<u64>,
    errors: Counter<u64>,
    cancel_token: CancellationToken,
) {
    info!("Queue export worker started");

    loop {
        let event = tokio::select! {
            result = rx.recv() => match result {
                Ok(event) => event,
                Err(async_broadcast::RecvError::Closed) => {
                    info!("Queue export worker: channel closed, shutting down");
                    return;
                }
                Err(async_broadcast::RecvError::Overflowed(n)) => {
                    warn!("Queue export worker: channel overflowed, lost {} events", n);
                    continue;
                }
            },
            _ = cancel_token.cancelled() => {
                info!("Queue export worker shutting down");
                return;
            }
        };

        let request = match cloud_events::create_batched_export_request(std::slice::from_ref(
            &event,
        )) {
            Ok(req) => req,
            Err(e) => {
                errors.add(1, &[]);
                error!(%e, event_type = event.message(), "Failed to build export request for queue");
                continue;
            }
        };

        let _timer = Timer::start_with_labels(&latency, &[]);
        match queue.send_json(&request).await {
            Ok(_) => counter.add(1, &[]),
            Err(e) => {
                errors.add(1, &[]);
                error!(%e, event_type = event.message(), "Failed to send event to queue");
            }
        }
    }
}
