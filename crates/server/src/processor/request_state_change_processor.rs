use std::{slice, sync::Arc, time::Duration};

use anyhow::Result;
use opentelemetry::metrics::{Counter, Histogram};
use otlp_logs_exporter::{OtlpLogsExporter, runtime::Tokio};
use tokio::sync::{mpsc, watch};
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
const MAX_REMOVE_ATTEMPTS: u8 = 10;
const EXPORT_FAILURE_BACKOFF: Duration = Duration::from_secs(5);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RequestStateChangeProcessor {
    indexify_state: Arc<IndexifyState>,
    http_processing_latency: Histogram<f64>,
    http_events_counter: Counter<u64>,
}

impl RequestStateChangeProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let meter = opentelemetry::global::meter("request_state_change_processor_metrics");

        let http_processing_latency = meter
            .f64_histogram("indexify.request_state_change.http_processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("HTTP export batch processing latency in seconds")
            .build();

        let http_events_counter = meter
            .u64_counter("indexify.request_state_change.http_events_total")
            .with_description("Total number of events exported via HTTP")
            .build();

        Self {
            indexify_state,
            http_processing_latency,
            http_events_counter,
        }
    }

    #[instrument(skip_all)]
    pub async fn start(
        &self,
        cloud_events_exporter: Option<OtlpLogsExporter<Tokio>>,
        event_dump_path: Option<String>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        let cancel_token = CancellationToken::new();
        let tracker = TaskTracker::new();

        // Spawn file dump worker if a dump path is configured.
        if let Some(path) = event_dump_path {
            let (tx, rx) = mpsc::unbounded_channel();
            self.indexify_state.connect_file_dump(tx);
            let token = cancel_token.child_token();
            tracker.spawn(async move {
                file_dump_worker(rx, path, token).await;
            });
        }

        // Spawn HTTP drain worker (if exporter is configured).
        //
        // Events are written directly to RocksDB by the state machine as part
        // of the same transaction that produces the state change.  A
        // watch::channel notifies this worker when new events are available.
        // On each wakeup it drains the entire outbox (full pages of 100 events
        // per HTTP request) before returning to sleep.
        if let Some(exporter) = cloud_events_exporter {
            tracker.spawn({
                let notify_rx = self.indexify_state.request_events_db_rx.clone();
                let state = self.indexify_state.clone();
                let latency = self.http_processing_latency.clone();
                let counter = self.http_events_counter.clone();
                let token = cancel_token.child_token();
                async move {
                    http_drain_worker(exporter, state, notify_rx, latency, counter, token).await;
                }
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

/// Drain worker — wakes on the watch channel when the state machine writes new
/// events to the RocksDB outbox, then calls `drain_all` to export every
/// pending page before going back to sleep. An unconditional drain on startup
/// recovers events that were persisted in a previous run.
async fn http_drain_worker(
    mut exporter: OtlpLogsExporter<Tokio>,
    state: Arc<IndexifyState>,
    mut notify_rx: watch::Receiver<()>,
    latency: Histogram<f64>,
    counter: Counter<u64>,
    cancel_token: CancellationToken,
) {
    info!("HTTP drain worker started");

    let mut cursor: Option<Vec<u8>> = None;

    // Initial drain for crash recovery (events written in a previous run).
    drain_all(&mut exporter, &state, &mut cursor, &latency, &counter).await;

    loop {
        tokio::select! {
            result = notify_rx.changed() => {
                if result.is_err() {
                    info!("HTTP drain worker: watch channel closed, shutting down");
                    return;
                }
                drain_all(&mut exporter, &state, &mut cursor, &latency, &counter).await;
            }
            _ = cancel_token.cancelled() => {
                info!("HTTP drain worker shutting down");
                return;
            }
        }
    }
}

/// Export every page of pending events from RocksDB, looping until the outbox
/// is empty. On export failure the same page is retried after a backoff; on a
/// DB read error the loop pauses before retrying.
async fn drain_all(
    exporter: &mut OtlpLogsExporter<Tokio>,
    state: &Arc<IndexifyState>,
    cursor: &mut Option<Vec<u8>>,
    latency: &Histogram<f64>,
    counter: &Counter<u64>,
) {
    loop {
        match process_batch(exporter, state, cursor, latency, counter).await {
            Ok(true) => {} // more pages remain
            Ok(false) => break,
            Err(e) => {
                error!(error = ?e, "Error processing request state change batch");
                tokio::time::sleep(EXPORT_FAILURE_BACKOFF).await;
                // cursor is unchanged; the same page will be retried
            }
        }
    }
}

/// File dump worker - receives events via a dedicated mpsc channel and appends
/// each one as a JSON line to the configured file path.
async fn file_dump_worker(
    mut rx: mpsc::UnboundedReceiver<RequestStateChangeEvent>,
    path: String,
    cancel_token: CancellationToken,
) {
    use tokio::io::AsyncWriteExt as _;

    let file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!(error = ?e, path, "File dump worker: failed to open dump file, worker will not start");
            return;
        }
    };

    let mut writer = tokio::io::BufWriter::new(file);

    info!(path, "File dump worker started");

    loop {
        let event = tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(event) => event,
                    None => {
                        info!("File dump worker: channel closed, shutting down");
                        return;
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("File dump worker shutting down");
                return;
            }
        };

        let json = match serde_json::to_string(&event) {
            Ok(s) => s,
            Err(e) => {
                error!(error = ?e, "File dump worker: failed to serialize event to JSON");
                continue;
            }
        };

        let write_result = async {
            writer.write_all(json.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
            Ok::<_, std::io::Error>(())
        }
        .await;

        if let Err(e) = write_result {
            error!(error = ?e, path, "File dump worker: failed to write event to dump file");
        }
    }
}

/// Read one page of events from RocksDB, export via HTTP, and delete the
/// successfully exported ones. Returns `Ok(true)` when more pages remain or a
/// retry is needed, `Ok(false)` when the outbox is empty.
///
/// Fixes:
/// - Bug 1 (no DB drain): reads from DB on every wakeup, not from memory.
/// - Bug 2 (stale cursor): advances cursor on success so subsequent pages are
///   read in order; resets to `None` after the last page.
/// - Bug 4 (build error blocks queue): skips unserializable events so they
///   cannot permanently prevent later events from being exported.
async fn process_batch(
    exporter: &mut OtlpLogsExporter<Tokio>,
    state: &Arc<IndexifyState>,
    cursor: &mut Option<Vec<u8>>,
    latency: &Histogram<f64>,
    counter: &Counter<u64>,
) -> Result<bool> {
    let (events, new_cursor) = state
        .reader()
        .pending_request_state_change_events(cursor.as_ref())
        .await?;

    if events.is_empty() {
        return Ok(false);
    }

    let _timer = Timer::start_with_labels(latency, &[]);
    let has_more_pages = new_cursor.is_some();

    // Bug 4 fix: validate each event individually. Events that cannot be
    // serialised are skipped (treated as processed) so they do not permanently
    // block export of subsequent events.
    let mut valid_events: Vec<PersistedRequestStateChangeEvent> = Vec::new();
    let mut skipped_events: Vec<PersistedRequestStateChangeEvent> = Vec::new();
    for event in events {
        if let Err(e) = create_batched_export_request(slice::from_ref(&event.event)) {
            error!(
                error = ?e,
                event_id = %event.id,
                "Failed to build export request for event, skipping"
            );
            skipped_events.push(event);
        } else {
            valid_events.push(event);
        }
    }

    // Remove permanently-skipped events so they don't clog the outbox.
    if !skipped_events.is_empty() &&
        let Err(e) = remove_events_with_backoff(state, &skipped_events).await
    {
        error!(error = ?e, "Failed to remove skipped events from DB");
    }

    let had_failure = if !valid_events.is_empty() {
        let raw_events: Vec<RequestStateChangeEvent> =
            valid_events.iter().map(|e| e.event.clone()).collect();
        let request = create_batched_export_request(&raw_events)?;
        match exporter.send_request(request).await {
            Ok(()) => {
                counter.add(valid_events.len() as u64, &[]);
                info!(
                    count = valid_events.len(),
                    "HTTP batch exported successfully"
                );
                if let Err(e) = remove_events_with_backoff(state, &valid_events).await {
                    // Events were exported but not deleted - they will be
                    // re-exported on next startup. HTTP export must be idempotent.
                    error!(error = ?e, "Failed to remove exported events from DB");
                }
                false
            }
            Err(e) => {
                error!(error = ?e, count = valid_events.len(), "HTTP batch export failed");
                true
            }
        }
    } else {
        // All events on this page were skipped; no transport failure.
        false
    };

    if had_failure {
        // Keep cursor at current page start so the same events are retried.
        tokio::time::sleep(EXPORT_FAILURE_BACKOFF).await;
    } else {
        // Advance cursor on success. Because exported events are deleted a
        // scan from the next key will land on the first unprocessed event.
        if let Some(c) = new_cursor {
            cursor.replace(c);
        } else {
            *cursor = None;
        }
    }

    Ok(has_more_pages || had_failure)
}

/// Remove events from database with exponential backoff on failure.
async fn remove_events_with_backoff(
    state: &Arc<IndexifyState>,
    events: &[PersistedRequestStateChangeEvent],
) -> Result<()> {
    for attempt in 1..=MAX_REMOVE_ATTEMPTS {
        let txn = state.db.transaction();

        if let Err(e) = state_machine::remove_request_state_change_events(&txn, events).await {
            error!(
                error = ?e,
                attempt,
                max_attempts = MAX_REMOVE_ATTEMPTS,
                "Error removing events, retrying..."
            );

            if attempt == MAX_REMOVE_ATTEMPTS {
                return Err(e);
            }

            tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            continue;
        }

        match txn.commit().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                error!(
                    error = ?e,
                    attempt,
                    max_attempts = MAX_REMOVE_ATTEMPTS,
                    "Error committing transaction, retrying..."
                );

                if attempt == MAX_REMOVE_ATTEMPTS {
                    return Err(e.into());
                }

                tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "Failed to remove events after {} attempts",
        MAX_REMOVE_ATTEMPTS
    ))
}
