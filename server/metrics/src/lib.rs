use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use pin_project_lite::pin_project;

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

use opentelemetry_sdk::metrics::SdkMeterProvider;

pub fn init_provider() -> prometheus::Registry {
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build();
    let mut provider = SdkMeterProvider::builder();
    if let Ok(exporter) = exporter {
        provider = provider.with_reader(exporter);
    };
    opentelemetry::global::set_meter_provider(provider.build());
    registry
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
            let meter = opentelemetry::global::meter("indexify-server");
            let invocations = meter
                .u64_counter("indexify.server.invocations")
                .with_description("number of invocations")
                .init();
            let invocation_bytes = meter
                .u64_counter("indexify.server.invocation_bytes")
                .with_description("number of bytes ingested during invocations")
                .init();
            let fn_outputs = meter
                .u64_counter("indexify.server.fn_outputs")
                .with_description("number of fn outputs")
                .init();
            let fn_output_bytes = meter
                .u64_counter("indexify.server.fn_output_bytes")
                .with_description("number of bytes ingested for fn outputs")
                .init();
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
}

impl<'a, T: TimerUpdate + Sync> Timer<'a, T> {
    pub fn start(metric: &'a T) -> Self {
        Self {
            start: Instant::now(),
            metric,
        }
    }
}

impl<'a, T: TimerUpdate + Sync> Drop for Timer<'a, T> {
    fn drop(&mut self) {
        self.metric.add(self.start.elapsed(), &[]);
    }
}

pub mod scheduler_stats {
    use std::sync::{Arc, Mutex};

    use opentelemetry::{
        metrics::{Histogram, ObservableCounter, ObservableGauge},
        KeyValue,
    };
    use state_store::IndexifyState;

    #[derive(Debug)]
    pub struct Metrics {
        pub tasks_completed: ObservableCounter<u64>,
        pub tasks_errored: ObservableCounter<u64>,
        pub tasks_in_progress: ObservableGauge<u64>,
        pub executors_online: ObservableGauge<u64>,
        pub scheduler_invocations: Histogram<f64>,
        pub tasks_per_executor: ObservableGauge<u64>,
        pub state_machine_apply: Histogram<f64>,
    }

    impl Metrics {
        pub fn new(indexify_state: Arc<IndexifyState>) -> Metrics {
            let meter = opentelemetry::global::meter("state_machine_stats");

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_completed = meter
                .u64_observable_counter("tasks_completed")
                .with_callback({
                    let app = indexify_state.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app.metrics.tasks_completed.read().unwrap();
                        observer.observe(*value - *prev_value, &[]);
                        *prev_value = *value;
                    }
                })
                .with_description("Number of tasks completed")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_errored = meter
                .u64_observable_counter("tasks_errored")
                .with_callback({
                    let app = indexify_state.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = *app.metrics.tasks_completed_with_errors.read().unwrap();
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of tasks completed with error")
                .init();

            let tasks_in_progress = meter
                .u64_observable_gauge("tasks_in_progress")
                .with_callback({
                    let app = indexify_state.clone();
                    move |observer| {
                        app.metrics
                            .assigned_tasks
                            .read()
                            .unwrap()
                            .iter()
                            .for_each(|(k, v)| {
                                let k = k.to_string();
                                observer.observe(*v as u64, &[KeyValue::new("task_type", k)]);
                            });
                    }
                })
                .with_description("Number of tasks in progress")
                .init();
            let executors_online = meter
                .u64_observable_gauge("executors_online")
                .with_callback({
                    let app = indexify_state.clone();
                    move |observer| {
                        let value = app.metrics.executors_online.read().unwrap();
                        observer.observe(*value as u64, &[]);
                    }
                })
                .with_description("Number of executors online")
                .init();

            let scheduler_invocations = meter
                .f64_histogram("scheduler_invocations")
                .with_description("Scheduler invocation latencies in seconds")
                .init();

            let tasks_per_executor = meter
                .u64_observable_gauge("tasks_per_executor")
                .with_callback({
                    let app = indexify_state.clone();
                    move |observer| {
                        let counts = app.metrics.tasks_by_executor.read().unwrap();
                        for (executor_id, tasks) in counts.iter() {
                            observer.observe(
                                tasks.clone(),
                                &[KeyValue::new("executor_id", executor_id.to_string())],
                            );
                        }
                    }
                })
                .with_description("Number of tasks per executor")
                .init();

            let state_machine_apply = meter
                .f64_histogram("state_machine_apply")
                .with_description("State machine apply changes latencies in seconds")
                .init();
            Metrics {
                tasks_completed,
                tasks_errored,
                tasks_in_progress,
                executors_online,
                scheduler_invocations,
                tasks_per_executor,
                state_machine_apply,
            }
        }
    }
}

pub mod kv_storage {
    use opentelemetry::metrics::Histogram;

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
                .f64_histogram("indexify.kv_storage.reads")
                .with_description("K/V store read latencies in seconds")
                .init();

            let writes = meter
                .f64_histogram("indexify.metadata_added")
                .with_description("k/v store add latencies in seconds")
                .init();

            Metrics { reads, writes }
        }
    }
}
