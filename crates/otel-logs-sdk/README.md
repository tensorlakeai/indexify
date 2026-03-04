# otel-logs-sdk

An async OpenTelemetry logs SDK for Rust that provides a batch log processor and an OTLP/gRPC log
exporter, independent of the `opentelemetry_sdk` log pipeline.

## Why this crate?

`opentelemetry_sdk::logs::BatchLogProcessor` relies on `SdkLogRecord::new()`, which is
`pub(crate)` and therefore cannot be constructed outside the SDK. This crate provides its own
`LogRecord` type and a fully async batch processor so that any service can build a log pipeline
without being coupled to the SDK's internal machinery.

Key properties:
- **No SDK dependency for the processor** — the processor works with any `LogExporter` impl.
- **Back-pressure by design** — `emit()` awaits when the queue is full; records are never silently
  dropped.
- **Async throughout** — no `block_on`, no risk of deadlocking a single-threaded Tokio runtime.
- **Retry and exponential backoff** — the OTLP exporter classifies gRPC errors per the OTLP spec
  and retries with configurable backoff and jitter.
- **Built-in observability** — emits OpenTelemetry metrics and `tracing` logs from both the
  processor and the OTLP exporter.

---

## Modules

| Module | Contents |
|---|---|
| `processor` | `AsyncBatchLogProcessor`, `BatchConfig` |
| `exporter` | `OtlpLogsExporter` — sends batches to a collector over gRPC/OTLP |
| `log_record` | `LogRecord` — SDK-independent record type with public constructors |
| `log_batch` | `LogBatch<'a>` — zero-copy batch passed to exporters |
| `log_exporter` | `LogExporter` trait — implement this to plug in a custom backend |
| `in_memory_exporter` | `InMemoryLogExporter` — captures records in memory; intended for tests |
| `retry` | `RetryPolicy`, gRPC error classification, `export_with_retry` |
| `error` | `Error` enum covering transport and URI errors |

Top-level re-exports: `AsyncBatchLogProcessor`, `BatchConfig`, `OtlpLogsExporter`,
`InMemoryLogExporter`, `OwnedLogData`, `LogBatch`, `LogExporter`, `LogRecord`.

---

## Adding as a dependency

This crate is not published to crates.io. Add it as a local path dependency in your workspace:

```toml
# workspace Cargo.toml
[workspace.dependencies]
otel-logs-sdk = { path = "./otel-logs-sdk" }

# your crate's Cargo.toml
[dependencies]
otel-logs-sdk = { workspace = true }
```

---

## Usage

### 1. Set up the OTLP exporter and processor

```rust
use otel_logs_sdk::{
    AsyncBatchLogProcessor, BatchConfig, OtlpLogsExporter,
    retry::RetryPolicy,
};
use opentelemetry_sdk::Resource;
use tonic::transport::Channel;

// Build a gRPC channel (you control keep-alive, TLS, etc.).
let channel = Channel::from_static("http://localhost:4317")
    .connect()
    .await?;

let retry_policy = RetryPolicy {
    max_retries: 5,
    initial_delay_ms: 100,
    max_delay_ms: 1600,
    jitter_ms: 100,
};

let exporter = OtlpLogsExporter::with_channel(channel, retry_policy);

let resource = Resource::builder()
    .with_service_name("my-service")
    .build();

let processor = AsyncBatchLogProcessor::new(
    exporter,
    &resource,
    BatchConfig::default(),
);
```

### 2. Emit log records

```rust
use otel_logs_sdk::LogRecord;
use opentelemetry::{InstrumentationScope, logs::{AnyValue, Severity}};

let scope = InstrumentationScope::builder("my-component").build();

let mut record = LogRecord::default();
record.set_body(AnyValue::String("something happened".into()));
record.set_severity_text("INFO");
record.set_severity_number(Severity::Info);
record.add_attribute("request_id", AnyValue::String("abc-123".into()));

processor.emit(&mut record, &scope).await;
```

### 3. Flush and shut down

```rust
// Wait for all queued records to be exported.
processor.force_flush().await?;

// Flush, shut down the exporter, and stop the background worker.
processor.shutdown().await?;
```

### 4. Implement a custom exporter

Implement the `LogExporter` trait to send records to any backend:

```rust
use otel_logs_sdk::{LogExporter, LogBatch};
use opentelemetry_sdk::{Resource, error::OTelSdkResult};

#[derive(Debug)]
struct StdoutExporter;

impl LogExporter for StdoutExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        for (record, scope) in batch.0 {
            println!("[{}] {:?}", scope.name(), record.body);
        }
        Ok(())
    }

    fn set_resource(&mut self, _resource: &Resource) {}
}

let processor = AsyncBatchLogProcessor::new(
    StdoutExporter,
    &resource,
    BatchConfig::default(),
);
```

### 5. In-memory exporter for tests

```rust
use otel_logs_sdk::{AsyncBatchLogProcessor, BatchConfig, InMemoryLogExporter};
use opentelemetry_sdk::Resource;

let exporter = InMemoryLogExporter::default();
let processor = AsyncBatchLogProcessor::new(
    exporter.clone(),
    &Resource::builder_empty().build(),
    BatchConfig::default(),
);

// ... emit records ...

processor.force_flush().await.unwrap();

let logs = exporter.get_emitted_logs().unwrap();
assert_eq!(logs[0].instrumentation.name(), "my-component");
assert_eq!(logs[0].record.severity_text, Some("INFO".to_string()));
```

---

## BatchConfig defaults

| Field | Default | Description |
|---|---|---|
| `max_queue_size` | `2048` | Channel capacity; `emit()` awaits when full (back-pressure) |
| `max_batch_size` | `512` | Records per export call; export is triggered immediately when reached |
| `flush_interval` | `1s` | How often the worker exports even when the batch is not full |

---

## Retry policy

`RetryPolicy` controls how the OTLP exporter handles transient failures. Error classification
follows the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/):

- **Retryable with backoff**: `CANCELLED`, `DEADLINE_EXCEEDED`, `ABORTED`, `OUT_OF_RANGE`,
  `UNAVAILABLE`, `DATA_LOSS`
- **Throttled** (uses server-specified delay from `RetryInfo`): `RESOURCE_EXHAUSTED` with
  `RetryInfo` metadata
- **Non-retryable**: all other gRPC status codes

```rust
use otel_logs_sdk::retry::RetryPolicy;

let policy = RetryPolicy {
    max_retries: 5,        // attempts after the first failure
    initial_delay_ms: 100, // first backoff delay
    max_delay_ms: 1600,    // cap on exponential backoff
    jitter_ms: 100,        // random jitter added to each delay
};
```

---

## Observability

The crate assumes that an OpenTelemetry metrics SDK has been registered globally before the first
processor or exporter is created. All instruments are obtained via `opentelemetry::global::meter`
with scope `"otel_logs_sdk"`.

### Metrics

**Processor** (`processor` module):

| Metric | Type | Description |
|---|---|---|
| `otel_logs.processor.records_emitted_total` | Counter | Records submitted via `emit()` |
| `otel_logs.processor.records_exported_total` | Counter | Records successfully exported |
| `otel_logs.processor.export_errors_total` | Counter | Failed batch export attempts |
| `otel_logs.processor.batch_size` | Histogram | Number of records per exported batch |
| `otel_logs.processor.export_duration_ms` | Histogram (unit: `ms`) | Duration of each batch export call |

**OTLP exporter** (`exporter` and `retry` modules):

| Metric | Type | Description |
|---|---|---|
| `otel_logs.exporter.export_requests_total` | Counter | OTLP export calls initiated |
| `otel_logs.exporter.export_errors_total` | Counter | Failed OTLP export calls |
| `otel_logs.exporter.export_duration_ms` | Histogram (unit: `ms`) | Duration of each OTLP call (including retries) |
| `otel_logs.exporter.retry_attempts_total` | Counter | Retry attempts made during OTLP export |

### Tracing logs

The crate uses the [`tracing`](https://docs.rs/tracing) crate. Attach a `tracing` subscriber in
your application to capture these events.

| Level | Source | Event |
|---|---|---|
| `trace` | `processor::emit` | Each record emitted (includes scope name) |
| `debug` | `processor` | Worker started/stopped, batch export start/success (size, elapsed ms), force flush |
| `info` | `processor::shutdown` | Processor shutdown initiated |
| `warn` | `processor` | Batch export failure |
| `debug` | `exporter::export` | Export start (record count), export success (record count, elapsed ms) |
| `warn` | `exporter::export` | Export failure |
| `debug` | `retry` | Successful request (attempt number) |
| `warn` | `retry` | Each retry attempt with delay |
| `error` | `retry` | Non-retryable error or max retries exhausted |
