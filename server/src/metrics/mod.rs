use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::Result;
use pin_project_lite::pin_project;
pub fn low_latency_boundaries() -> Vec<f64> {
    vec![
        0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0,
        500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0,
    ]
}

trait TimedFn: FnOnce(Duration) {}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimedFuture<F, C>
    where
        F: Future,
        C: TimedFn,
    {
        #[pin]
        inner: F,
        start: Instant,
        callback: Option<C>, // This is an Option because the future might be polled even after completion
    }
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
    metrics::{PeriodicReader, SdkMeterProvider},
    Resource,
};

pub fn init_provider(
    enable_metrics: bool,
    endpoint: Option<&String>,
    interval: Duration,
    instance_id: Option<&String>,
    service_version: &str,
) -> Result<()> {
    // Early exit if metrics are disabled
    if !enable_metrics {
        return Ok(());
    }

    let mut resource_builder = Resource::builder()
        .with_attribute(KeyValue::new("service.namespace", "indexify"))
        .with_attribute(KeyValue::new("service.name", "indexify-server"))
        .with_attribute(KeyValue::new(
            "service.version",
            service_version.to_string(),
        ));

    if let Some(instance_id) = instance_id {
        resource_builder = resource_builder.with_attribute(KeyValue::new(
            "indexify.instance.id",
            instance_id.to_owned(),
        ));

        // Temporary non-compliant instance-id attribute to avoid observability gap
        // while we migrate rest of stack.
        resource_builder = resource_builder
            .with_attribute(KeyValue::new("indexify-instance", instance_id.to_owned()));
    }

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
        pub invocations: Counter<u64>,
        pub invocation_bytes: Counter<u64>,
        pub fn_outputs: Counter<u64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("service-api");
            let invocations = meter
                .u64_counter("indexify.invocations")
                .with_description("number of invocations")
                .build();
            let invocation_bytes = meter
                .u64_counter("indexify.invocation_bytes")
                .with_description("number of bytes ingested during invocations")
                .build();
            let fn_outputs = meter
                .u64_counter("indexify.fn_outputs")
                .with_description("number of fn outputs")
                .build();
            Metrics {
                invocations,
                invocation_bytes,
                fn_outputs,
            }
        }
    }
}

use opentelemetry::{
    metrics::{Counter, Histogram},
    KeyValue,
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

pub mod kv_storage {
    use opentelemetry::metrics::Histogram;

    use crate::metrics::low_latency_boundaries;

    #[derive(Debug)]
    pub struct Metrics {
        pub reads: Histogram<f64>,
        pub writes: Histogram<f64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("kv-storage");

            let reads = meter
                .f64_histogram("indexify.kv_storage_read_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("K/V store read latencies in seconds")
                .build();

            let writes = meter
                .f64_histogram("indexify.kv_storage_write_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("k/v store write latencies in seconds")
                .build();

            Metrics { reads, writes }
        }
    }
}

use std::fmt::Display;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct FnMetricsId {
    pub namespace: String,
    pub application: String,
    pub function: String,
}

impl Display for FnMetricsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.namespace, self.application, self.function
        )
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
}

impl Default for StateStoreMetrics {
    fn default() -> Self {
        Self::new()
    }
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

        Self {
            state_write,
            state_read,
            state_metrics_write,
            driver_writes,
            driver_reads,
            driver_scans,
            driver_deletes,
        }
    }
}
