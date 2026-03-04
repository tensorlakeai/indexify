use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use opentelemetry::{
    InstrumentationScope,
    global,
    metrics::{Counter, Histogram},
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use crate::{log_batch::LogBatch, log_exporter::LogExporter, log_record::LogRecord};

// ---------------------------------------------------------------------------
// Control messages (sent over an unbounded channel so they are never dropped)
// ---------------------------------------------------------------------------

enum ControlMessage {
    ForceFlush(oneshot::Sender<OTelSdkResult>),
    Shutdown(oneshot::Sender<OTelSdkResult>),
}

// ---------------------------------------------------------------------------
// BatchConfig
// ---------------------------------------------------------------------------

/// Configuration for `AsyncBatchLogProcessor`.
pub struct BatchConfig {
    /// Maximum number of log records that can be queued before backpressure
    /// kicks in.
    pub max_queue_size: usize,
    /// Maximum number of records sent in a single export call.
    pub max_batch_size: usize,
    /// How often to flush buffered records even when the batch is not full.
    pub flush_interval: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 2048,
            max_batch_size: 512,
            flush_interval: Duration::from_secs(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ProcessorMetrics {
    records_emitted: Counter<u64>,
    records_exported: Counter<u64>,
    export_errors: Counter<u64>,
    batch_size: Histogram<u64>,
    export_duration_ms: Histogram<f64>,
}

impl ProcessorMetrics {
    fn new() -> Self {
        let meter = global::meter("otel_logs_sdk");
        Self {
            records_emitted: meter
                .u64_counter("otel_logs.processor.records_emitted_total")
                .with_description("Total log records submitted to the processor via emit()")
                .build(),
            records_exported: meter
                .u64_counter("otel_logs.processor.records_exported_total")
                .with_description("Total log records successfully exported")
                .build(),
            export_errors: meter
                .u64_counter("otel_logs.processor.export_errors_total")
                .with_description("Total number of failed batch export attempts")
                .build(),
            batch_size: meter
                .u64_histogram("otel_logs.processor.batch_size")
                .with_description("Number of records in each exported batch")
                .build(),
            export_duration_ms: meter
                .f64_histogram("otel_logs.processor.export_duration_ms")
                .with_description("Duration of each batch export call in milliseconds")
                .with_unit("ms")
                .build(),
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncBatchLogProcessor
// ---------------------------------------------------------------------------

/// A fully-async log processor that does not depend on `opentelemetry_sdk`.
///
/// Unlike the SDK processor this type:
/// * Uses bounded back-pressure (`send().await`) so no records are silently
///   dropped when the queue is full.
/// * Separates log records and control messages into two channels so that a
///   `Shutdown` command is always accepted even when the record channel is
///   full.
/// * Exposes `async fn` methods — no `block_on` and therefore no risk of
///   deadlocking a single-threaded runtime.
/// * Propagates exporter shutdown errors back to the caller.
#[derive(Debug, Clone)]
pub struct AsyncBatchLogProcessor {
    log_sender: mpsc::Sender<(LogRecord, InstrumentationScope)>,
    control_sender: mpsc::UnboundedSender<ControlMessage>,
    metrics: Arc<ProcessorMetrics>,
}

impl AsyncBatchLogProcessor {
    /// Creates a new processor and spawns its background worker.
    ///
    /// `exporter.set_resource(resource)` is called immediately before the
    /// worker is spawned.
    pub fn new<E: LogExporter + 'static>(
        mut exporter: E,
        resource: &Resource,
        config: BatchConfig,
    ) -> Self {
        exporter.set_resource(resource);

        let (log_sender, log_rx) = mpsc::channel(config.max_queue_size);
        let (control_sender, control_rx) = mpsc::unbounded_channel();
        let metrics = Arc::new(ProcessorMetrics::new());

        debug!("Spawning AsyncBatchLogProcessor worker");
        tokio::spawn(worker_loop(
            exporter,
            log_rx,
            control_rx,
            config,
            Arc::clone(&metrics),
        ));

        Self {
            log_sender,
            control_sender,
            metrics,
        }
    }

    /// Enqueues a log record.
    ///
    /// Awaits if the channel is full — providing natural back-pressure instead
    /// of silent drops.
    pub async fn emit(&self, record: &mut LogRecord, scope: &InstrumentationScope) {
        trace!(scope = scope.name(), "Emitting log record to processor");
        self.metrics.records_emitted.add(1, &[]);
        let _ = self.log_sender.send((record.clone(), scope.clone())).await;
    }

    /// Flushes all buffered records and waits until the export is confirmed.
    pub async fn force_flush(&self) -> OTelSdkResult {
        debug!("Force flush requested on AsyncBatchLogProcessor");
        let (tx, rx) = oneshot::channel();
        let _ = self.control_sender.send(ControlMessage::ForceFlush(tx));
        rx.await
            .unwrap_or_else(|_| Err(OTelSdkError::InternalFailure("worker gone".into())))
    }

    /// Flushes all buffered records, shuts down the exporter, and stops the
    /// worker. Propagates any export or exporter-shutdown errors to the caller.
    pub async fn shutdown(&self) -> OTelSdkResult {
        info!("Shutting down AsyncBatchLogProcessor");
        let (tx, rx) = oneshot::channel();
        let _ = self.control_sender.send(ControlMessage::Shutdown(tx));
        rx.await
            .unwrap_or_else(|_| Err(OTelSdkError::InternalFailure("worker gone".into())))
    }
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

async fn export_batch<E: LogExporter>(
    exporter: &E,
    buffer: &mut Vec<(LogRecord, InstrumentationScope)>,
    metrics: &ProcessorMetrics,
) -> OTelSdkResult {
    if buffer.is_empty() {
        return Ok(());
    }

    let batch_len = buffer.len();
    debug!(batch_size = batch_len, "Exporting batch");
    metrics.batch_size.record(batch_len as u64, &[]);

    let refs: Vec<(&LogRecord, &InstrumentationScope)> =
        buffer.iter().map(|(r, s)| (r, s)).collect();
    let batch = LogBatch::new(&refs);

    let start = Instant::now();
    let result = exporter.export(batch).await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    metrics.export_duration_ms.record(elapsed_ms, &[]);

    buffer.clear();

    match &result {
        Ok(_) => {
            debug!(
                batch_size = batch_len,
                elapsed_ms, "Batch exported successfully"
            );
            metrics.records_exported.add(batch_len as u64, &[]);
        }
        Err(err) => {
            warn!(?err, batch_size = batch_len, "Batch export failed");
            metrics.export_errors.add(1, &[]);
        }
    }

    result
}

async fn worker_loop<E: LogExporter>(
    exporter: E,
    mut log_rx: mpsc::Receiver<(LogRecord, InstrumentationScope)>,
    mut control_rx: mpsc::UnboundedReceiver<ControlMessage>,
    config: BatchConfig,
    metrics: Arc<ProcessorMetrics>,
) {
    debug!("Batch log processor worker started");
    let mut buffer: Vec<(LogRecord, InstrumentationScope)> =
        Vec::with_capacity(config.max_batch_size);
    let mut flush_timer = tokio::time::interval(config.flush_interval);
    // Skip the first immediate tick so we don't flush an empty buffer on startup.
    flush_timer.tick().await;

    loop {
        tokio::select! {
            // Incoming log record
            record = log_rx.recv() => {
                match record {
                    Some(entry) => {
                        buffer.push(entry);
                        if buffer.len() >= config.max_batch_size {
                            let _ = export_batch(&exporter, &mut buffer, &metrics).await;
                        }
                    }
                    None => {
                        // Sender dropped — flush remaining and exit.
                        let _ = export_batch(&exporter, &mut buffer, &metrics).await;
                        break;
                    }
                }
            }

            // Periodic flush
            _ = flush_timer.tick() => {
                if !buffer.is_empty() {
                    let _ = export_batch(&exporter, &mut buffer, &metrics).await;
                }
            }

            // Control message (always accepted because of unbounded channel)
            msg = control_rx.recv() => {
                match msg {
                    Some(ControlMessage::ForceFlush(reply)) => {
                        // Drain any remaining records from the log channel first.
                        while let Ok(entry) = log_rx.try_recv() {
                            buffer.push(entry);
                        }
                        let result = export_batch(&exporter, &mut buffer, &metrics).await;
                        let _ = reply.send(result);
                    }
                    Some(ControlMessage::Shutdown(reply)) => {
                        // Drain, export, shutdown exporter, then exit.
                        while let Ok(entry) = log_rx.try_recv() {
                            buffer.push(entry);
                        }
                        let export_result = export_batch(&exporter, &mut buffer, &metrics).await;
                        let shutdown_result = exporter.shutdown();
                        let combined = export_result.and(shutdown_result);
                        let _ = reply.send(combined);
                        debug!("Batch log processor worker stopped");
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::logs::{AnyValue, Severity};

    use super::*;
    use crate::in_memory_exporter::InMemoryLogExporter;

    #[tokio::test]
    async fn test_emit_and_force_flush() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig::default(),
        );

        let scope = InstrumentationScope::builder("test-scope").build();
        let mut record = LogRecord::default();
        processor.emit(&mut record, &scope).await;

        processor.force_flush().await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(1, logs.len());
        assert_eq!("test-scope", logs[0].instrumentation.name());
    }

    #[tokio::test]
    async fn test_shutdown_flushes_remaining() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig {
                // Long interval so the periodic flush won't fire during the test.
                flush_interval: Duration::from_secs(60),
                ..BatchConfig::default()
            },
        );

        let scope = InstrumentationScope::builder("scope").build();
        for _ in 0..3 {
            let mut record = LogRecord::default();
            processor.emit(&mut record, &scope).await;
        }
        processor.shutdown().await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(3, logs.len());
    }

    #[tokio::test]
    async fn test_batch_size_triggers_export() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig {
                max_batch_size: 2,
                max_queue_size: 10,
                // Long interval so the periodic flush won't fire during the test.
                flush_interval: Duration::from_secs(60),
            },
        );

        let scope = InstrumentationScope::builder("scope").build();
        let mut record = LogRecord::default();
        processor.emit(&mut record, &scope).await;
        processor.emit(&mut record, &scope).await;

        // Yield to let the background worker process the full batch.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(2, logs.len());
    }

    #[tokio::test]
    async fn test_multiple_scopes_preserve_order() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig::default(),
        );

        let scope_a = InstrumentationScope::builder("scope-a").build();
        let scope_b = InstrumentationScope::builder("scope-b").build();
        let mut record = LogRecord::default();
        processor.emit(&mut record, &scope_a).await;
        processor.emit(&mut record, &scope_b).await;

        processor.force_flush().await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(2, logs.len());
        assert_eq!("scope-a", logs[0].instrumentation.name());
        assert_eq!("scope-b", logs[1].instrumentation.name());
    }

    #[tokio::test]
    async fn test_record_fields_preserved() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig::default(),
        );

        let scope = InstrumentationScope::builder("scope").build();
        let mut record = LogRecord::default();
        record.set_body(AnyValue::String("hello world".into()));
        record.set_severity_text("INFO");
        record.set_severity_number(Severity::Info);
        processor.emit(&mut record, &scope).await;

        processor.force_flush().await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(1, logs.len());
        let emitted = &logs[0].record;
        assert_eq!(emitted.body, Some(AnyValue::String("hello world".into())));
        assert_eq!(emitted.severity_text, Some("INFO".to_string()));
        assert_eq!(emitted.severity_number, Some(Severity::Info));
    }

    #[tokio::test]
    async fn test_force_flush_empty_buffer_is_ok() {
        let exporter = InMemoryLogExporter::default();
        let processor = AsyncBatchLogProcessor::new(
            exporter.clone(),
            &Resource::builder_empty().build(),
            BatchConfig::default(),
        );

        // Flush with nothing queued should succeed.
        processor.force_flush().await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert!(logs.is_empty());
    }
}
