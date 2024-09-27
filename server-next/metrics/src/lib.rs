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

pub mod server {
    use opentelemetry::metrics::Counter;

    #[derive(Debug)]
    pub struct Metrics {
        pub node_content_uploads: Counter<u64>,
        pub node_content_bytes_uploaded: Counter<u64>,
        pub node_content_extracted: Counter<u64>,
        pub node_content_bytes_extracted: Counter<u64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("indexify-server");
            let node_content_uploads = meter
                .u64_counter("indexify.server.node_content_uploads")
                .with_description("Number of contents uploaded on this node")
                .init();
            let node_content_bytes_uploaded = meter
                .u64_counter("indexify.server.node_content_bytes_uploaded")
                .with_description("Number of bytes uploaded on this node")
                .init();
            let node_content_extracted = meter
                .u64_counter("indexify.server.node_content_extracted")
                .with_description("Number of contetnts extracted on this node")
                .init();
            let node_content_bytes_extracted = meter
                .u64_counter("indexify.server.node_content_bytes_extracted")
                .with_description("Number of bytes extracted on this node")
                .init();
            Metrics {
                node_content_uploads,
                node_content_bytes_uploaded,
                node_content_extracted,
                node_content_bytes_extracted,
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

pub mod coordinator {
    use std::sync::{Arc, Mutex};

    use opentelemetry::{
        metrics::{Histogram, ObservableCounter, ObservableGauge},
        KeyValue,
    };

    use crate::state::store::StateMachineStore;

    #[derive(Debug)]
    pub struct Metrics {
        pub tasks_completed: ObservableCounter<u64>,
        pub tasks_errored: ObservableCounter<u64>,
        pub tasks_in_progress: ObservableGauge<u64>,
        pub executors_online: ObservableGauge<u64>,
        pub content_uploads: ObservableCounter<u64>,
        pub content_bytes_uploaded: ObservableCounter<u64>,
        pub content_extracted: ObservableCounter<u64>,
        pub content_extracted_bytes: ObservableCounter<u64>,
        pub scheduler_invocations: Histogram<f64>,
        pub tasks_per_executor: ObservableGauge<u64>,
    }

    impl Metrics {
        pub fn new(app: Arc<StateMachineStore>) -> Metrics {
            let meter = opentelemetry::global::meter("indexify-coordinator");

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_completed = meter
                .u64_observable_counter("indexify.coordinator.tasks_completed")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .tasks_completed;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of tasks completed")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_errored = meter
                .u64_observable_counter("indexify.coordinator.tasks_errored")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .tasks_completed_with_errors;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of tasks completed with error")
                .init();

            let tasks_in_progress = meter
                .u64_observable_gauge("indexify.coordinator.tasks_in_progress")
                .with_callback({
                    let app = app.clone();
                    move |observer| {
                        app.data
                            .indexify_state
                            .unfinished_tasks_by_extractor
                            .observe_task_counts(observer);
                    }
                })
                .with_description("Number of tasks in progress")
                .init();
            let executors_online = meter
                .u64_observable_gauge("indexify.coordinator.executors_online")
                .with_callback({
                    let app = app.clone();
                    move |observer| {
                        let value = app.data.indexify_state.executor_count();
                        observer.observe(value as u64, &[]);
                    }
                })
                .with_description("Number of executors online")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let content_uploads = meter
                .u64_observable_counter("indexify.coordinator.content_uploads")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .content_uploads;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of contents uploaded")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let content_bytes_uploaded = meter
                .u64_observable_counter("indexify.coordinator.content_bytes_uploaded")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .content_bytes;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of bytes uploaded")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let content_extracted = meter
                .u64_observable_counter("indexify.coordinator.content_extracted")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .content_extracted;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of contents extracted")
                .init();

            let prev_value = Arc::new(Mutex::new(0));
            let content_extracted_bytes = meter
                .u64_observable_counter("indexify.coordinator.content_bytes_extracted")
                .with_callback({
                    let app = app.clone();
                    let prev_value = prev_value.clone();
                    move |observer| {
                        let mut prev_value = prev_value.lock().unwrap();
                        let value = app
                            .data
                            .indexify_state
                            .metrics
                            .lock()
                            .unwrap()
                            .content_extracted_bytes;
                        observer.observe(value - *prev_value, &[]);
                        *prev_value = value;
                    }
                })
                .with_description("Number of bytes extracted")
                .init();

            let scheduler_invocations = meter
                .f64_histogram("indexify.coordinator.scheduler_invocations")
                .with_description("Scheduler invocation latencies in seconds")
                .init();

            let tasks_per_executor = meter
                .u64_observable_gauge("indexify.coordinator.tasks_per_executor")
                .with_callback({
                    let app = app.clone();
                    move |observer| {
                        let counts = app
                            .data
                            .indexify_state
                            .unfinished_tasks_by_executor
                            .read()
                            .unwrap();
                        for (executor_id, tasks) in counts.iter() {
                            observer.observe(
                                tasks.tasks.len() as u64,
                                &[KeyValue::new("executor_id", executor_id.to_string())],
                            );
                        }
                    }
                })
                .with_description("Number of tasks per executor")
                .init();

            Metrics {
                tasks_completed,
                tasks_errored,
                tasks_in_progress,
                executors_online,
                content_uploads,
                content_bytes_uploaded,
                content_extracted,
                content_extracted_bytes,
                scheduler_invocations,
                tasks_per_executor,
            }
        }
    }
}

pub mod state_machine {
    use opentelemetry::metrics::Histogram;

    #[derive(Debug)]
    pub struct Metrics {
        pub state_machine_apply: Histogram<f64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("indexify-state-machine");

            let state_machine_apply = meter
                .f64_histogram("indexify.state_machine_apply")
                .with_description("State machine apply changes latencies in seconds")
                .init();

            Metrics {
                state_machine_apply,
            }
        }
    }
}
