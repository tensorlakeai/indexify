use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use otlp_logs_exporter::OtlpLogsExporter;
use tokio::sync::{RwLock, broadcast, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, instrument, warn};

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

// Constants
const MAX_DELETE_ATTEMPTS: u8 = 10;
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const CHECK_INTERVAL: Duration = Duration::from_millis(500);
const BROADCAST_CHANNEL_CAPACITY: usize = 1000;
const HTTP_BATCH_SIZE: usize = 100;

/// Tracks processing progress per worker
#[derive(Clone, Debug)]
struct WorkerCursor {
    processed_keys: Vec<Vec<u8>>,
}

/// Sends batched events to OTLP exporter
async fn send_batched_events_to_exporter(
    exporter: &mut OtlpLogsExporter,
    events: &[PersistedRequestStateChangeEvent],
) -> Result<Vec<Vec<u8>>> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    info!(
        event_count = events.len(),
        "sending batched events to OTLP exporter"
    );

    let requests = create_batch_export_request(events)?;
    let request_count = requests.len();

    let mut successful_requests = Vec::new();
    for request in requests {
        match exporter.send_request(request.export_request).await {
            Ok(_) => {
                successful_requests.extend(request.event_keys);
            }
            Err(err) => {
                error!(%err, "failed to send request to OTLP exporter");
                return Err(err.into());
            }
        }
    }

    info!(
        event_count = events.len(),
        request_count, "successfully sent batched events to OTLP exporter"
    );

    Ok(successful_requests)
}

pub struct RequestStateChangeProcessor {
    indexify_state: Arc<IndexifyState>,
    http_processing_latency: Histogram<f64>,
    broadcast_processing_latency: Histogram<f64>,
    events_counter: Counter<u64>,
    http_exporter_events: Counter<u64>,
    broadcast_events: Counter<u64>,
    deleted_events: Counter<u64>,
}

impl RequestStateChangeProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let meter = opentelemetry::global::meter("request_state_change_processor_metrics");

        let http_processing_latency = meter
            .f64_histogram("indexify.request_state_change.http_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("HTTP exporter worker event processing latency in seconds")
            .build();

        let broadcast_processing_latency = meter
            .f64_histogram("indexify.request_state_change.broadcast_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("broadcast worker event processing latency in seconds")
            .build();

        let events_counter = meter
            .u64_counter("indexify.request_state_change.events_processed_total")
            .with_description("total number of processed request state change events")
            .build();

        let http_exporter_events = meter
            .u64_counter("indexify.request_state_change.http_exporter_events_total")
            .with_description("total number of events processed by HTTP exporter worker")
            .build();

        let broadcast_events = meter
            .u64_counter("indexify.request_state_change.broadcast_events_total")
            .with_description("total number of events processed by broadcast worker")
            .build();

        let deleted_events = meter
            .u64_counter("indexify.request_state_change.deleted_events_total")
            .with_description("total number of events deleted from database")
            .build();

        Self {
            indexify_state,
            http_processing_latency,
            broadcast_processing_latency,
            events_counter,
            http_exporter_events,
            broadcast_events,
            deleted_events,
        }
    }

    #[instrument(skip_all)]
    pub async fn start(
        &self,
        cloud_events_exporter: Option<OtlpLogsExporter>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        let mut request_events_rx = self.indexify_state.request_events_rx.clone();
        let mut read_cursor: Option<Vec<u8>> = None;

        // Create broadcast channel
        let (tx, _rx) =
            broadcast::channel::<PersistedRequestStateChangeEvent>(BROADCAST_CHANNEL_CAPACITY);

        // Worker cursors tracking progress
        let http_cursor = Arc::new(RwLock::new(WorkerCursor {
            processed_keys: Vec::new(),
        }));
        let broadcast_cursor = Arc::new(RwLock::new(WorkerCursor {
            processed_keys: Vec::new(),
        }));

        // Create cancellation token for graceful shutdown of workers
        let cancellation_token = CancellationToken::new();

        // Task tracker to monitor worker task completion
        let task_tracker = TaskTracker::new();

        // Spawn HTTP exporter worker only if exporter is configured
        if let Some(exporter) = cloud_events_exporter {
            let http_cursor_clone = http_cursor.clone();
            let http_processing_latency = self.http_processing_latency.clone();
            let events_counter = self.events_counter.clone();
            let http_exporter_events = self.http_exporter_events.clone();
            let http_rx = tx.subscribe();
            let http_cancel_token = cancellation_token.child_token();
            task_tracker.spawn(async move {
                Self::http_exporter_worker(
                    http_rx,
                    http_cursor_clone,
                    exporter,
                    http_processing_latency,
                    events_counter,
                    http_exporter_events,
                    http_cancel_token,
                )
                .await
            });
        } else {
            info!("HTTP exporter worker disabled - no exporter configured");
        }

        // Spawn broadcast worker
        let broadcast_cursor_clone = broadcast_cursor.clone();
        let indexify_state_clone = self.indexify_state.clone();
        let broadcast_processing_latency = self.broadcast_processing_latency.clone();
        let broadcast_events = self.broadcast_events.clone();
        let broadcast_rx = tx.subscribe();
        let bc_cancel_token = cancellation_token.child_token();
        task_tracker.spawn(async move {
            Self::broadcast_worker(
                broadcast_rx,
                broadcast_cursor_clone,
                indexify_state_clone,
                broadcast_processing_latency,
                broadcast_events,
                bc_cancel_token,
            )
            .await
        });

        // Spawn deletion coordinator
        let http_cursor_clone = http_cursor.clone();
        let broadcast_cursor_clone = broadcast_cursor.clone();
        let indexify_state_clone = self.indexify_state.clone();
        let deleted_events = self.deleted_events.clone();
        let coord_cancel_token = cancellation_token.child_token();
        task_tracker.spawn(async move {
            Self::deletion_coordinator(
                http_cursor_clone,
                broadcast_cursor_clone,
                indexify_state_clone,
                deleted_events,
                coord_cancel_token,
            )
            .await
        });

        // Main event reading loop
        loop {
            tokio::select! {
                _ = request_events_rx.changed() => {
                    request_events_rx.borrow_and_update();

                    if let Err(error) = self.read_and_queue_events(&mut read_cursor, &tx).await {
                        error!(
                            %error,
                            "error reading request state change events"
                        );
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("request state change processor shutting down, draining in-flight events");
                    break;
                }
            }
        }

        // Graceful shutdown phase
        drop(tx);
        info!(
            "Request state change reader stopped, waiting for workers to process remaining queued events"
        );

        cancellation_token.cancel();
        task_tracker.close();

        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, task_tracker.wait())
            .await
            .is_err()
        {
            warn!(
                timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT.as_secs(),
                "graceful shutdown timeout, some events may not have been deleted from database"
            );
        }

        // All workers have stopped, do final cleanup of any remaining processed keys
        info!("performing final cleanup of remaining processed events");
        Self::final_cleanup(
            &http_cursor,
            &broadcast_cursor,
            &self.indexify_state,
            &self.deleted_events,
        )
        .await;
    }

    /// Reads events from database and queues them for workers
    #[instrument(skip_all, fields(cursor = ?cursor))]
    async fn read_and_queue_events(
        &self,
        cursor: &mut Option<Vec<u8>>,
        tx: &broadcast::Sender<PersistedRequestStateChangeEvent>,
    ) -> Result<()> {
        let (events, new_cursor) = self
            .indexify_state
            .reader()
            .request_state_change_events(cursor.as_ref())
            .await?;

        if events.is_empty() {
            return Ok(());
        }

        info!(events_count = events.len(), "queuing events for workers");

        for event in events {
            if let Err(error) = tx.send(event) {
                error!(%error, "failed to queue event, receiver dropped");
                return Err(anyhow::anyhow!("event queue receiver closed"));
            }
        }

        if let Some(c) = new_cursor {
            cursor.replace(c);
        }

        Ok(())
    }

    /// HTTP exporter worker processes events and sends them to OTLP
    #[instrument(skip_all)]
    async fn http_exporter_worker(
        mut rx: broadcast::Receiver<PersistedRequestStateChangeEvent>,
        cursor: Arc<RwLock<WorkerCursor>>,
        mut exporter: OtlpLogsExporter,
        http_processing_latency: Histogram<f64>,
        events_counter: Counter<u64>,
        http_exporter_events: Counter<u64>,
        cancel_token: CancellationToken,
    ) {
        let mut batch = Vec::new();

        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Ok(queued_event) => {
                            batch.push(queued_event);
                            if batch.len() >= HTTP_BATCH_SIZE {
                                let timer_kvs = &[KeyValue::new("op", "http_exporter_worker")];
                                let _timer = Timer::start_with_labels(&http_processing_latency, timer_kvs);
                                let event_count = batch.len();
                                events_counter.add(event_count as u64, &[]);

                                match send_batched_events_to_exporter(&mut exporter, &batch).await {
                                    Ok(successful_keys) => {
                                        let successful_count = successful_keys.len() as u64;
                                        let mut c = cursor.write().await;
                                        c.processed_keys.extend(successful_keys);
                                        http_exporter_events.add(successful_count, &[]);
                                        info!(processed_count = event_count, "HTTP exporter batch processed");
                                    }
                                    Err(e) => {
                                        error!(%e, event_count, "HTTP exporter batch failed");
                                    }
                                }
                                batch.clear();
                            }
                        }
                        Err(_) => {
                            info!("HTTP exporter worker: channel closed");
                            Self::send_http_final_batch(&mut batch, &mut exporter, &cursor, &http_processing_latency, &events_counter, &http_exporter_events).await;
                            return;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("HTTP exporter worker received cancellation signal");
                    Self::send_http_final_batch(&mut batch, &mut exporter, &cursor, &http_processing_latency, &events_counter, &http_exporter_events).await;
                    return;
                }
            }
        }
    }

    async fn send_http_final_batch(
        batch: &mut Vec<PersistedRequestStateChangeEvent>,
        exporter: &mut OtlpLogsExporter,
        cursor: &Arc<RwLock<WorkerCursor>>,
        http_processing_latency: &Histogram<f64>,
        events_counter: &Counter<u64>,
        http_exporter_events: &Counter<u64>,
    ) {
        if batch.is_empty() {
            return;
        }

        let timer_kvs = &[KeyValue::new("op", "http_exporter_worker_final")];
        let _timer = Timer::start_with_labels(http_processing_latency, timer_kvs);
        let event_count = batch.len();
        events_counter.add(event_count as u64, &[]);

        match send_batched_events_to_exporter(exporter, batch).await {
            Ok(successful_keys) => {
                let successful_count = successful_keys.len() as u64;
                let mut c = cursor.write().await;
                c.processed_keys.extend(successful_keys);
                http_exporter_events.add(successful_count, &[]);
                info!(
                    processed_count = event_count,
                    "HTTP exporter final batch processed"
                );
            }
            Err(e) => {
                error!(%e, event_count, "HTTP exporter final batch failed");
            }
        }
        batch.clear();
    }

    /// Broadcast worker processes events and sends them to SSE immediately
    #[instrument(skip_all)]
    async fn broadcast_worker(
        mut rx: broadcast::Receiver<PersistedRequestStateChangeEvent>,
        cursor: Arc<RwLock<WorkerCursor>>,
        indexify_state: Arc<IndexifyState>,
        broadcast_processing_latency: Histogram<f64>,
        broadcast_events: Counter<u64>,
        cancel_token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Ok(queued_event) => {
                            let _timer = Timer::start_with_labels(&broadcast_processing_latency, &[]);

                            if let Err(error) = indexify_state
                                .push_request_event(queued_event.event.clone())
                                .await
                            {
                                warn!(
                                    %error,
                                    event_id = %queued_event.id,
                                    "error pushing request event to SSE stream"
                                );
                            } else {
                                let mut c = cursor.write().await;
                                c.processed_keys.push(queued_event.key());
                                broadcast_events.add(1, &[]);
                            }
                        }
                        Err(_) => {
                            return;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    return;
                }
            }
        }
    }

    /// Coordination task that tracks which events both workers have processed
    #[instrument(skip_all)]
    async fn deletion_coordinator(
        http_cursor: Arc<RwLock<WorkerCursor>>,
        broadcast_cursor: Arc<RwLock<WorkerCursor>>,
        indexify_state: Arc<IndexifyState>,
        deleted_events: Counter<u64>,
        cancel_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::check_and_delete_common_keys(&http_cursor, &broadcast_cursor, &indexify_state, &deleted_events).await;
                }
                _ = cancel_token.cancelled() => {
                    info!("Deletion coordinator received cancellation signal");
                    return;
                }
            }
        }
    }

    async fn final_cleanup(
        http_cursor: &Arc<RwLock<WorkerCursor>>,
        broadcast_cursor: &Arc<RwLock<WorkerCursor>>,
        indexify_state: &Arc<IndexifyState>,
        deleted_events: &Counter<u64>,
    ) {
        let http = http_cursor.read().await;
        let broadcast = broadcast_cursor.read().await;

        // Collect all keys that have been processed by both workers
        let http_set: HashSet<_> = http.processed_keys.iter().cloned().collect();
        let mut all_keys_to_delete = Vec::new();

        for key in broadcast.processed_keys.iter() {
            if http_set.contains(key) {
                all_keys_to_delete.push(key.clone());
            }
        }

        if all_keys_to_delete.is_empty() {
            return;
        }

        info!(
            count = all_keys_to_delete.len(),
            "final cleanup: deleting all remaining processed events"
        );

        if let Err(error) =
            remove_and_commit_with_backoff(indexify_state, &all_keys_to_delete).await
        {
            error!(%error, "failed to delete remaining processed events during final cleanup");
        } else {
            deleted_events.add(all_keys_to_delete.len() as u64, &[]);
            info!("final cleanup completed successfully");
        }
    }

    async fn check_and_delete_common_keys(
        http_cursor: &Arc<RwLock<WorkerCursor>>,
        broadcast_cursor: &Arc<RwLock<WorkerCursor>>,
        indexify_state: &Arc<IndexifyState>,
        deleted_events: &Counter<u64>,
    ) {
        let http = http_cursor.read().await;
        let broadcast = broadcast_cursor.read().await;

        // Find common processed keys between both workers
        let http_set: std::collections::HashSet<_> = http.processed_keys.iter().collect();
        let mut common_keys = Vec::new();

        for key in &broadcast.processed_keys {
            if http_set.contains(key) {
                common_keys.push(key.clone());
            }
        }

        drop(http);
        drop(broadcast);

        if common_keys.is_empty() {
            return;
        }

        info!(
            count = common_keys.len(),
            "deleting processed events from database"
        );

        if let Err(error) = remove_and_commit_with_backoff(indexify_state, &common_keys).await {
            error!(%error, "failed to delete processed events");
        } else {
            // Remove deleted keys from both cursors
            let deleted_count = common_keys.len() as u64;
            deleted_events.add(deleted_count, &[]);

            let mut http = http_cursor.write().await;
            let mut broadcast = broadcast_cursor.write().await;

            let common_set: std::collections::HashSet<_> = common_keys.iter().collect();
            http.processed_keys.retain(|k| !common_set.contains(k));
            broadcast.processed_keys.retain(|k| !common_set.contains(k));

            info!("deleted events removed from worker cursors");
        }
    }

    /// Process and remove all pending request state change events.
    /// This is useful for tests to drain accumulated events.
    #[cfg(test)]
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

            // Process all events through SSE stream
            let mut processed_events = Vec::new();
            for event in events {
                // Best effort push - always delete regardless of result
                let _ = self
                    .indexify_state
                    .push_request_event(event.event.clone())
                    .await;
                processed_events.push(event.key());
            }

            if !processed_events.is_empty() {
                remove_and_commit_with_backoff(&self.indexify_state, &processed_events).await?;
            }
        }

        Ok(())
    }
}

/// Removes events from database with exponential backoff and jitter
async fn remove_and_commit_with_backoff(
    indexify_state: &Arc<IndexifyState>,
    processed_keys: &[Vec<u8>],
) -> Result<()> {
    for attempt in 1..=MAX_DELETE_ATTEMPTS {
        let txn = indexify_state.db.transaction();

        if let Err(error) =
            state_machine::remove_request_state_change_events(&txn, processed_keys).await
        {
            error!(
                %error,
                attempt,
                max_attempts = MAX_DELETE_ATTEMPTS,
                "error removing processed request state change events, retrying..."
            );

            if attempt == MAX_DELETE_ATTEMPTS {
                return Err(error);
            }

            let delay = Duration::from_secs(attempt as u64);
            tokio::time::sleep(delay).await;
            continue;
        }

        match txn.commit().await {
            Ok(_) => return Ok(()),
            Err(commit_error) => {
                error!(
                    %commit_error,
                    attempt,
                    max_attempts = MAX_DELETE_ATTEMPTS,
                    "error committing transaction to remove processed request state change events, retrying..."
                );

                if attempt == MAX_DELETE_ATTEMPTS {
                    return Err(commit_error.into());
                }

                let delay = Duration::from_secs(attempt as u64);
                tokio::time::sleep(delay).await;
                continue;
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to remove and commit events after {} attempts",
        MAX_DELETE_ATTEMPTS
    ))
}
