use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::Result;
use pin_project::pin_project;
pub fn low_latency_boundaries() -> Vec<f64> {
    vec![
        0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0,
        500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0,
    ]
}

pub trait TimedFn: FnOnce(Duration) {}

#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub struct TimedFuture<F, C>
where
    F: Future,
    C: TimedFn,
{
    #[pin]
    inner: F,
    start: Instant,
    callback: Option<C>, /* This is an Option because the future might be polled even after
                          * completion */
}

impl<F, C> Future for TimedFuture<F, C>
where
    F: Future,
    C: TimedFn,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let poll_result = this.inner.poll(cx);

        if poll_result.is_ready() {
            let elapsed = this.start.elapsed();
            if let Some(callback) = this.callback.take() {
                callback(elapsed);
            }
        }

        poll_result
    }
}

#[allow(dead_code)]
pub struct CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    node_addr: &'a str,
    func: F,
}

impl<'a, F> Drop for CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    fn drop(&mut self) {
        (self.func)(self.node_addr, -1);
    }
}

use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};

pub fn init_provider(
    enable_metrics: bool,
    endpoint: Option<&String>,
    interval: Duration,
    instance_id: &str,
    service_version: &str,
) -> Result<()> {
    // Early exit if metrics are disabled
    if !enable_metrics {
        return Ok(());
    }

    let resource_builder = Resource::builder()
        .with_attribute(KeyValue::new("service.namespace", "indexify"))
        .with_attribute(KeyValue::new("service.name", "indexify-server"))
        .with_attribute(KeyValue::new(
            "indexify.instance.id",
            instance_id.to_string(),
        ))
        .with_attribute(KeyValue::new("indexify-instance", instance_id.to_string()))
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
        ));

    let resource = resource_builder.build();

    let mut exporter = MetricExporter::builder().with_tonic();
    if let Some(endpoint) = endpoint {
        exporter = exporter.with_endpoint(endpoint.to_owned());
    }
    let exporter = exporter.build()?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(interval)
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(provider);
    Ok(())
}

pub mod api_io_stats {
    use opentelemetry::metrics::Counter;

    #[derive(Debug)]
    pub struct Metrics {
        pub requests: Counter<u64>,
        pub request_input_bytes: Counter<u64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("service-api");
            let requests = meter
                .u64_counter("indexify.requests")
                .with_description("number of created requests")
                .build();
            let request_input_bytes = meter
                .u64_counter("indexify.request_input_bytes")
                .with_description("request input bytes ingested during request creations")
                .build();
            Metrics {
                requests,
                request_input_bytes,
            }
        }
    }
}

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};

pub trait TimerUpdate {
    fn add(&self, duration: Duration, labels: &[KeyValue]);
}

impl TimerUpdate for Histogram<f64> {
    fn add(&self, duration: Duration, labels: &[KeyValue]) {
        self.record(duration.as_secs_f64(), labels);
    }
}

pub struct Timer<'a, T: TimerUpdate + Sync> {
    start: Instant,
    metric: &'a T,
    labels: &'a [KeyValue],
}

impl<'a, T: TimerUpdate + Sync> Timer<'a, T> {
    #[must_use]
    pub fn start_with_labels(metric: &'a T, labels: &'a [KeyValue]) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels,
        }
    }
}

impl<'a, T: TimerUpdate + Sync> Drop for Timer<'a, T> {
    fn drop(&mut self) {
        self.metric.add(self.start.elapsed(), self.labels);
    }
}

pub trait AutoIncrement {
    fn increment(&self, labels: &[KeyValue]);
}

impl AutoIncrement for Counter<u64> {
    fn increment(&self, labels: &[KeyValue]) {
        self.add(1, labels);
    }
}

pub struct Increment<'a, T: AutoIncrement + Sync> {
    metric: &'a T,
    labels: &'a [KeyValue],
}

impl<'a, T: AutoIncrement + Sync> Increment<'a, T> {
    pub fn inc(metric: &'a T, labels: &'a [KeyValue]) -> Self {
        Self { metric, labels }
    }
}

impl<'a, T: AutoIncrement + Sync> Drop for Increment<'a, T> {
    fn drop(&mut self) {
        self.metric.increment(self.labels);
    }
}

pub mod blob_storage {
    use opentelemetry::metrics::Histogram;

    use crate::metrics::low_latency_boundaries;

    #[derive(Debug)]
    pub struct Metrics {
        pub operations: Histogram<f64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("blob-storage");

            let operations = meter
                .f64_histogram("indexify.blob_operations_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("blob store latencies in seconds")
                .build();

            Metrics { operations }
        }
    }
}

pub mod queue {
    use opentelemetry::metrics::Counter;

    pub struct Metrics {
        pub messages_sent: Counter<u64>,
        pub send_errors: Counter<u64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Self {
            let meter = opentelemetry::global::meter("queue");

            let messages_sent = meter
                .u64_counter("indexify.queue_messages_sent")
                .with_description("Number of messages successfully sent to the queue")
                .build();

            let send_errors = meter
                .u64_counter("indexify.queue_send_errors")
                .with_description("Number of errors encountered when sending messages to the queue")
                .build();

            Self {
                messages_sent,
                send_errors,
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct StateStoreMetrics {
    pub state_write: Histogram<f64>,
    pub state_read: Histogram<f64>,
    pub state_metrics_write: Histogram<f64>,
    pub driver_writes: Counter<u64>,
    pub driver_reads: Counter<u64>,
    pub driver_scans: Counter<u64>,
    pub driver_deletes: Counter<u64>,
    pub driver_commits: Counter<u64>,
    pub driver_commits_errors: Counter<u64>,
    pub state_write_persistent_storage: Histogram<f64>,
    pub state_write_in_memory: Histogram<f64>,
    pub state_write_container_scheduler: Histogram<f64>,
    pub state_write_executor_notify: Histogram<f64>,
    pub state_write_request_state_change: Histogram<f64>,
    pub state_change_notify: Histogram<f64>,
}

impl StateStoreMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("state_store");

        let state_write = meter
            .f64_histogram("indexify.state_machine_write_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State machine writing latency in seconds")
            .build();

        let state_read = meter
            .f64_histogram("indexify.state_machine_read_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State machine reading latency in seconds")
            .build();

        let state_metrics_write = meter
            .f64_histogram("indexify.state_metrics_write_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State metrics writing latency in seconds")
            .build();

        let driver_writes = meter
            .u64_counter("indexify.state_driver_writes")
            .with_description("Number of state driver writes")
            .build();

        let driver_reads = meter
            .u64_counter("indexify.state_driver_reads")
            .with_description("Number of state driver reads")
            .build();

        let driver_scans = meter
            .u64_counter("indexify.state_driver_scans")
            .with_description("Number of state driver scans")
            .build();

        let driver_deletes = meter
            .u64_counter("indexify.state_driver_deletes")
            .with_description("Number of state driver deletes")
            .build();

        let driver_commits = meter
            .u64_counter("indexify.state_driver_commits")
            .with_description("Number of state driver commits")
            .build();

        let driver_commits_errors = meter
            .u64_counter("indexify.state_driver_commits_errors")
            .with_description("Number of state driver commit errors")
            .build();

        let state_write_persistent_storage = meter
            .f64_histogram("indexify.state_machine_write_persistent_storage_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("RocksDB transaction commit latency in seconds")
            .build();

        let state_write_in_memory = meter
            .f64_histogram("indexify.state_machine_write_in_memory_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("In-memory state update latency in seconds")
            .build();

        let state_write_container_scheduler = meter
            .f64_histogram("indexify.state_machine_write_container_scheduler_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Container scheduler update latency in seconds")
            .build();

        let state_write_executor_notify = meter
            .f64_histogram("indexify.state_machine_write_executor_notify_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Executor state change notification latency in seconds")
            .build();

        let state_write_request_state_change = meter
            .f64_histogram("indexify.state_machine_write_request_state_change_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Request state change update latency in seconds")
            .build();

        let state_change_notify = meter
            .f64_histogram("indexify.state_change_notify_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State change notification latency in seconds")
            .build();

        Self {
            state_write,
            state_read,
            state_metrics_write,
            driver_writes,
            driver_reads,
            driver_scans,
            driver_deletes,
            driver_commits,
            driver_commits_errors,
            state_write_persistent_storage,
            state_write_in_memory,
            state_write_container_scheduler,
            state_write_executor_notify,
            state_write_request_state_change,
            state_change_notify,
        }
    }
}
