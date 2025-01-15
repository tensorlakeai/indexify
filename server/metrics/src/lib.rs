use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::Result;
use pin_project_lite::pin_project;
use prometheus::Registry;

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

pub fn init_provider() -> Result<Registry> {
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()?;
    let provider = SdkMeterProvider::builder()
        .with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", "indexify-server"),
            opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ]))
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

pub mod processors_metrics {
    use opentelemetry::metrics::{Histogram, UpDownCounter};

    #[derive(Debug)]
    pub struct Metrics {
        pub requests_queue_duration: Histogram<f64>,
        pub requests_inflight: UpDownCounter<i64>,
        pub processors_process_duration: Histogram<f64>,
    }

    impl Default for Metrics {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Metrics {
        pub fn new() -> Metrics {
            let meter = opentelemetry::global::meter("dispatcher_metrics");
            let low_latency_boundaries = vec![
                0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 75.0,
                100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0,
            ];

            let requests_queue_duration = meter
                .f64_histogram("requests_queue_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries.clone())
                .with_description("time spent waiting for a processor in seconds")
                .build();

            let requests_inflight = meter
                .i64_up_down_counter("requests_inflight")
                .with_description("number of requests in flight")
                .build();

            let processors_process_duration = meter
                .f64_histogram("processors_process_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries)
                .with_description("Processors processing latencies in seconds")
                .build();

            Metrics {
                requests_queue_duration,
                requests_inflight,
                processors_process_duration,
            }
        }
    }
}

pub mod state_metrics {
    use std::sync::{Arc, Mutex};

    use opentelemetry::{
        metrics::{ObservableCounter, ObservableGauge},
        KeyValue,
    };
    use tracing::error;

    use crate::StateStoreMetrics;

    #[derive(Debug)]
    pub struct Metrics {
        pub tasks_completed: ObservableCounter<u64>,
        pub tasks_errored: ObservableCounter<u64>,
        pub tasks_in_progress: ObservableGauge<u64>,
        pub executors_online: ObservableGauge<u64>,
        pub tasks_per_executor: ObservableGauge<u64>,
    }

    impl Metrics {
        pub fn new(state_store_metrics: Arc<StateStoreMetrics>) -> Metrics {
            let meter = opentelemetry::global::meter("scheduler_stats");

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_completed = meter
                .u64_observable_counter("tasks_completed")
                .with_callback({
                    let prev_value = prev_value.clone();
                    let store_metrics = state_store_metrics.clone();
                    move |observer| match prev_value.lock() {
                        Ok(mut prev_value) => match store_metrics.tasks_completed.read() {
                            Ok(value) => {
                                observer.observe(*value - *prev_value, &[]);
                                *prev_value = *value;
                            }
                            Err(e) => error!("Failed to read tasks_completed: {:?}", e),
                        },
                        Err(e) => error!("Failed to lock prev_value: {:?}", e),
                    }
                })
                .with_description("Number of tasks completed")
                .build();

            let prev_value = Arc::new(Mutex::new(0));
            let tasks_errored = meter
                .u64_observable_counter("tasks_errored")
                .with_callback({
                    let prev_value = prev_value.clone();
                    let store_metrics = state_store_metrics.clone();
                    move |observer| match prev_value.lock() {
                        Ok(mut prev_value) => {
                            match store_metrics.tasks_completed_with_errors.read() {
                                Ok(value) => {
                                    observer.observe(*value - *prev_value, &[]);
                                    *prev_value = *value;
                                }
                                Err(e) => {
                                    error!("Failed to read tasks_completed_with_errors: {:?}", e)
                                }
                            }
                        }
                        Err(e) => error!("Failed to lock prev_value: {:?}", e),
                    }
                })
                .with_description("Number of tasks completed with error")
                .build();

            let tasks_in_progress = meter
                .u64_observable_gauge("tasks_in_progress")
                .with_callback({
                    let store_metrics = state_store_metrics.clone();
                    move |observer| {
                        store_metrics
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
                .build();
            let executors_online = meter
                .u64_observable_gauge("executors_online")
                .with_callback({
                    let store_metrics = state_store_metrics.clone();
                    move |observer| match store_metrics.executors_online.read() {
                        Ok(value) => observer.observe(*value as u64, &[]),
                        Err(e) => error!("Failed to read executors_online: {:?}", e),
                    }
                })
                .with_description("Number of executors online")
                .build();

            let tasks_per_executor = meter
                .u64_observable_gauge("tasks_per_executor")
                .with_callback({
                    let store_metrics = state_store_metrics.clone();
                    move |observer| match store_metrics.tasks_by_executor.read() {
                        Ok(counts) => {
                            for (executor_id, tasks) in counts.iter() {
                                observer.observe(
                                    *tasks,
                                    &[KeyValue::new("executor_id", executor_id.to_string())],
                                );
                            }
                        }
                        Err(e) => error!("Failed to read tasks_by_executor: {:?}", e),
                    }
                })
                .with_description("Number of tasks per executor")
                .build();

            Metrics {
                tasks_completed,
                tasks_errored,
                tasks_in_progress,
                executors_online,
                tasks_per_executor,
            }
        }
    }
}

pub mod blob_storage {
    use opentelemetry::metrics::Histogram;

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
                .with_description("blob store latencies in seconds")
                .build();

            Metrics { operations }
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
                .f64_histogram("kv_storage_read_duration")
                .with_unit("s")
                .with_description("K/V store read latencies in seconds")
                .build();

            let writes = meter
                .f64_histogram("kv_storage_write_duration")
                .with_unit("s")
                .with_description("k/v store write latencies in seconds")
                .build();

            Metrics { reads, writes }
        }
    }
}

use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, RwLock},
};

use data_model::{Task, TaskOutcome};

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

/*
 * StateStoreMetrics is a struct that holds metrics for the state store.
 * It keeps track of the number of tasks completed, the number of tasks
 * completed with errors, the number of assigned tasks, and the number of
 * unassigned tasks. Currently metrics are not persisted across restarts.
 *
 * TODO: When the server starts up, we should scan the database for assigned
 * and unassigned tasks. But for now, it's fine to just emit metrics which
 * reflect the current state of the system since starting the server.
 */
#[derive(Clone, Debug)]
pub struct StateStoreMetrics {
    pub tasks_completed: Arc<RwLock<u64>>,
    pub tasks_completed_with_errors: Arc<RwLock<u64>>,
    pub assigned_tasks: Arc<RwLock<HashMap<FnMetricsId, u64>>>,
    pub unassigned_tasks: Arc<RwLock<HashMap<FnMetricsId, u64>>>,
    pub executors_online: Arc<RwLock<u64>>,
    pub tasks_by_executor: Arc<RwLock<HashMap<String, u64>>>,
    pub state_write: Histogram<f64>,
    pub state_read: Histogram<f64>,
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
            .with_description("State machine writing latency in seconds")
            .build();

        let state_read = meter
            .f64_histogram("state_machine_read_duration")
            .with_unit("s")
            .with_description("State machine reading latency in seconds")
            .build();

        Self {
            tasks_completed: Arc::new(RwLock::new(0)),
            tasks_completed_with_errors: Arc::new(RwLock::new(0)),
            assigned_tasks: Arc::new(RwLock::new(HashMap::new())),
            unassigned_tasks: Arc::new(RwLock::new(HashMap::new())),
            executors_online: Arc::new(RwLock::new(0)),
            tasks_by_executor: Arc::new(RwLock::new(HashMap::new())),
            state_write,
            state_read,
        }
    }

    pub fn update_task_completion(&self, outcome: TaskOutcome, task: Task, executor_id: &str) {
        match self.tasks_by_executor.write() {
            Ok(mut tasks_by_executor) => {
                tasks_by_executor
                    .entry(executor_id.to_string())
                    .and_modify(|e| {
                        if *e > 0 {
                            *e -= 1
                        }
                    })
                    .or_insert(0);
            }
            Err(e) => tracing::error!("Failed to lock tasks_by_executor: {:?}", e),
        }

        match outcome {
            TaskOutcome::Success => match self.tasks_completed.write() {
                Ok(mut tasks_completed) => *tasks_completed += 1,
                Err(e) => tracing::error!("Failed to lock tasks_completed: {:?}", e),
            },
            TaskOutcome::Failure => match self.tasks_completed_with_errors.write() {
                Ok(mut tasks_completed_with_errors) => *tasks_completed_with_errors += 1,
                Err(e) => tracing::error!("Failed to lock tasks_completed_with_errors: {:?}", e),
            },
            _ => (),
        }

        let id = FnMetricsId::from_task(&task);
        match self.assigned_tasks.write() {
            Ok(mut count) => {
                if *count.entry(id.clone()).or_insert(0) > 0 {
                    *count.entry(id).or_insert(0) -= 1;
                }
            }
            Err(e) => tracing::error!("Failed to lock assigned_tasks: {:?}", e),
        }
    }

    pub fn task_unassigned(&self, tasks: &[Task]) {
        for task in tasks {
            let id = FnMetricsId::from_task(task);
            match self.unassigned_tasks.write() {
                Ok(mut count) => *count.entry(id).or_insert(0) += 1,
                Err(e) => tracing::error!("Failed to lock unassigned_tasks: {:?}", e),
            }
        }
    }

    pub fn task_assigned(&self, tasks: &[Task], executor_id: &str) {
        match self.tasks_by_executor.write() {
            Ok(mut tasks_by_executor) => {
                tasks_by_executor
                    .entry(executor_id.to_string())
                    .and_modify(|e| *e += tasks.len() as u64)
                    .or_insert(tasks.len() as u64);
            }
            Err(e) => tracing::error!("Failed to lock tasks_by_executor: {:?}", e),
        }

        for task in tasks {
            let id = FnMetricsId::from_task(task);
            match self.assigned_tasks.write() {
                Ok(mut count_assigned) => *count_assigned.entry(id.clone()).or_insert(0) += 1,
                Err(e) => tracing::error!("Failed to lock assigned_tasks: {:?}", e),
            }
            match self.unassigned_tasks.write() {
                Ok(mut count_unassigned) => {
                    if *count_unassigned.entry(id.clone()).or_insert(0) > 0 {
                        *count_unassigned.entry(id).or_insert(0) -= 1;
                    }
                }
                Err(e) => tracing::error!("Failed to lock unassigned_tasks: {:?}", e),
            }
        }
    }

    pub fn add_executor(&self) {
        match self.executors_online.write() {
            Ok(mut executors_online) => *executors_online += 1,
            Err(e) => tracing::error!("Failed to lock executors_online: {:?}", e),
        }
    }

    pub fn remove_executor(&self, executor_id: &str) {
        match self.executors_online.write() {
            Ok(mut executors_online) => {
                self.tasks_by_executor.write().unwrap().remove(executor_id);
                if *executors_online > 0 {
                    *executors_online -= 1;
                }
            }
            Err(e) => tracing::error!("Failed to lock executors_online: {:?}", e),
        }
    }
}
