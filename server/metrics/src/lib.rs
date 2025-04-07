use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::Result;
use pin_project_lite::pin_project;
use prometheus::Registry;

pub fn low_latency_boundaries() -> Vec<f64> {
    vec![
        0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0,
        500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0,
    ]
}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimedFuture<F, C>
    where
        F: Future,
        C: FnOnce(Duration),
    {
        #[pin]
        inner: F,
        start: Instant,
        callback: Option<C>, // This is an Option because the future might be polled even after completion
    }
}

impl<F, C> TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
{
    pub fn new(inner: F, callback: C) -> Self {
        Self {
            inner,
            callback: Some(callback),
            start: Instant::now(),
        }
    }
}

impl<F, C> Future for TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
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

pub fn create_timed_future<F, C>(future: F, callback: C) -> TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
{
    TimedFuture::new(future, callback)
}

pub struct CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    node_addr: &'a str,
    func: F,
}

impl<'a, F> CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    pub fn new(node_addr: &'a str, func: F) -> Self {
        func(node_addr, 1);
        Self { node_addr, func }
    }
}

impl<'a, F> Drop for CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    fn drop(&mut self) {
        (self.func)(self.node_addr, -1);
    }
}

use opentelemetry_sdk::{metrics::SdkMeterProvider, Resource};

pub fn init_provider() -> Result<Registry> {
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;
    let provider = SdkMeterProvider::builder()
        .with_resource(
            Resource::builder()
                .with_attribute(KeyValue::new("service.name", "indexify-server"))
                .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
                .build(),
        )
        .with_reader(exporter)
        .build();

    opentelemetry::global::set_meter_provider(provider);
    Ok(registry)
}

pub mod api_io_stats {
    use opentelemetry::metrics::Counter;

    #[derive(Debug)]
    pub struct Metrics {
        pub invocations: Counter<u64>,
        pub invocation_bytes: Counter<u64>,
        pub fn_outputs: Counter<u64>,
        pub fn_output_bytes: Counter<u64>,
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
                .u64_counter("invocations")
                .with_description("number of invocations")
                .build();
            let invocation_bytes = meter
                .u64_counter("invocation_bytes")
                .with_description("number of bytes ingested during invocations")
                .build();
            let fn_outputs = meter
                .u64_counter("fn_outputs")
                .with_description("number of fn outputs")
                .build();
            let fn_output_bytes = meter
                .u64_counter("fn_output_bytes")
                .with_description("number of bytes ingested for fn outputs")
                .build();
            Metrics {
                invocations,
                invocation_bytes,
                fn_outputs,
                fn_output_bytes,
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

impl TimerUpdate for Counter<f64> {
    fn add(&self, duration: Duration, labels: &[KeyValue]) {
        self.add(duration.as_secs_f64(), labels);
    }
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
    pub fn start(metric: &'a T) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels: &[],
        }
    }

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
        self.metric.add(self.start.elapsed(), &self.labels);
    }
}

pub mod blob_storage {
    use opentelemetry::metrics::Histogram;

    use crate::low_latency_boundaries;

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
                .f64_histogram("blob_operations_duration")
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

    use crate::low_latency_boundaries;

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
                .f64_histogram("kv_storage_read_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("K/V store read latencies in seconds")
                .build();

            let writes = meter
                .f64_histogram("kv_storage_write_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("k/v store write latencies in seconds")
                .build();

            Metrics { reads, writes }
        }
    }
}

use std::fmt::Display;

use data_model::Task;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct FnMetricsId {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
}

impl FnMetricsId {
    pub fn from_task(task: &Task) -> Self {
        Self {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            compute_fn: task.compute_fn_name.clone(),
        }
    }
}

impl Display for FnMetricsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.namespace, self.compute_graph, self.compute_fn
        )
    }
}

#[derive(Clone, Debug)]
pub struct StateStoreMetrics {
    pub state_write: Histogram<f64>,
    pub state_read: Histogram<f64>,
    pub state_metrics_write: Histogram<f64>,
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
            .f64_histogram("state_machine_write_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State machine writing latency in seconds")
            .build();

        let state_read = meter
            .f64_histogram("state_machine_read_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State machine reading latency in seconds")
            .build();

        let state_metrics_write = meter
            .f64_histogram("state_metrics_write_duration")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("State metrics writing latency in seconds")
            .build();

        Self {
            state_write,
            state_read,
            state_metrics_write,
        }
    }
}
