//! Metrics module for the dataplane service.
//!
//! Provides OTLP metrics export with:
//! - Gauges for current state (running containers, free resources)
//! - Counters for events (containers started, desired state updates)

use std::sync::Arc;

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, ObservableGauge},
};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use tokio::sync::Mutex;

use crate::config::TelemetryConfig;

/// Initialize the OpenTelemetry metrics provider.
///
/// This sets up the global meter provider with OTLP export if metrics are
/// enabled. Must be called early in the application lifecycle before any
/// metrics are recorded.
pub fn init_provider(config: &TelemetryConfig, instance_id: &str, executor_id: &str) -> Result<()> {
    if !config.enable_metrics {
        return Ok(());
    }

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.namespace", "indexify"))
        .with_attribute(KeyValue::new("service.name", "indexify-dataplane"))
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new(
            "indexify.instance.id",
            instance_id.to_string(),
        ))
        .with_attribute(KeyValue::new(
            "indexify.executor.id",
            executor_id.to_string(),
        ))
        .build();

    let mut exporter_builder = MetricExporter::builder().with_tonic();
    if let Some(endpoint) = &config.endpoint {
        exporter_builder = exporter_builder.with_endpoint(endpoint.clone());
    }
    let exporter = exporter_builder.build()?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(config.metrics_interval)
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(provider);

    tracing::info!(
        executor_id = executor_id,
        endpoint = ?config.endpoint,
        interval_secs = config.metrics_interval.as_secs(),
        "Metrics provider initialized"
    );

    Ok(())
}

/// Container state for metrics reporting.
#[derive(Debug, Clone, Default)]
pub struct ContainerCounts {
    pub running_functions: u64,
    pub running_sandboxes: u64,
    pub pending_functions: u64,
    pub pending_sandboxes: u64,
}

/// Resource availability for metrics reporting.
#[derive(Debug, Clone, Default)]
pub struct ResourceAvailability {
    pub free_cpu_percent: f64,
    pub free_memory_bytes: u64,
    pub free_disk_bytes: u64,
}

/// Shared state for observable gauges to read from.
#[derive(Default)]
pub struct MetricsState {
    pub container_counts: ContainerCounts,
    pub resources: ResourceAvailability,
}

/// Counters for tracking events.
#[derive(Clone)]
pub struct DataplaneCounters {
    /// Counter for containers started, labeled by container_type
    /// (function/sandbox)
    pub containers_started: Counter<u64>,
    /// Counter for containers terminated
    pub containers_terminated: Counter<u64>,
    /// Counter for desired state messages received from server
    pub desired_state_received: Counter<u64>,
    /// Counter for function executors in desired state
    pub desired_function_executors: Counter<u64>,
    /// Counter for allocations in desired state
    pub desired_allocations: Counter<u64>,
    /// Counter for heartbeat successes
    pub heartbeat_success: Counter<u64>,
    /// Counter for heartbeat failures
    pub heartbeat_failures: Counter<u64>,
    /// Counter for stream disconnections
    pub stream_disconnections: Counter<u64>,
    /// Counter for completed allocations (labeled by outcome)
    pub allocations_completed: Counter<u64>,
    /// Histogram for allocation execution duration in milliseconds
    pub allocation_duration_ms: Histogram<u64>,
}

impl DataplaneCounters {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        let containers_started = meter
            .u64_counter("indexify.dataplane.containers.started")
            .with_description("Number of containers started")
            .build();

        let containers_terminated = meter
            .u64_counter("indexify.dataplane.containers.terminated")
            .with_description("Number of containers terminated")
            .build();

        let desired_state_received = meter
            .u64_counter("indexify.dataplane.desired_state.received")
            .with_description("Number of desired state messages received from server")
            .build();

        let desired_function_executors = meter
            .u64_counter("indexify.dataplane.desired_state.function_executors")
            .with_description("Total function executors received in desired state messages")
            .build();

        let desired_allocations = meter
            .u64_counter("indexify.dataplane.desired_state.allocations")
            .with_description("Total allocations received in desired state messages")
            .build();

        let heartbeat_success = meter
            .u64_counter("indexify.dataplane.heartbeat.success")
            .with_description("Number of successful heartbeats")
            .build();

        let heartbeat_failures = meter
            .u64_counter("indexify.dataplane.heartbeat.failures")
            .with_description("Number of failed heartbeats")
            .build();

        let stream_disconnections = meter
            .u64_counter("indexify.dataplane.stream.disconnections")
            .with_description("Number of stream disconnections")
            .build();

        let allocations_completed = meter
            .u64_counter("indexify.dataplane.allocations.completed")
            .with_description("Number of completed allocations")
            .build();

        let allocation_duration_ms = meter
            .u64_histogram("indexify.dataplane.allocations.duration_ms")
            .with_description("Allocation execution duration in milliseconds")
            .with_unit("ms")
            .build();

        Self {
            containers_started,
            containers_terminated,
            desired_state_received,
            desired_function_executors,
            desired_allocations,
            heartbeat_success,
            heartbeat_failures,
            stream_disconnections,
            allocations_completed,
            allocation_duration_ms,
        }
    }

    /// Record a container started event.
    pub fn record_container_started(&self, container_type: &str) {
        self.containers_started.add(
            1,
            &[KeyValue::new("container_type", container_type.to_string())],
        );
    }

    /// Record a container terminated event.
    pub fn record_container_terminated(&self, container_type: &str, reason: &str) {
        self.containers_terminated.add(
            1,
            &[
                KeyValue::new("container_type", container_type.to_string()),
                KeyValue::new("reason", reason.to_string()),
            ],
        );
    }

    /// Record desired state received from server.
    pub fn record_desired_state(&self, num_function_executors: u64, num_allocations: u64) {
        self.desired_state_received.add(1, &[]);
        self.desired_function_executors
            .add(num_function_executors, &[]);
        self.desired_allocations.add(num_allocations, &[]);
    }

    /// Record heartbeat result.
    pub fn record_heartbeat(&self, success: bool) {
        if success {
            self.heartbeat_success.add(1, &[]);
        } else {
            self.heartbeat_failures.add(1, &[]);
        }
    }

    /// Record stream disconnection.
    pub fn record_stream_disconnection(&self, reason: &str) {
        self.stream_disconnections
            .add(1, &[KeyValue::new("reason", reason.to_string())]);
    }

    /// Record a completed allocation with its outcome and optional duration.
    pub fn record_allocation_completed(
        &self,
        outcome: &str,
        failure_reason: Option<&str>,
        duration_ms: Option<u64>,
    ) {
        let mut attrs = vec![KeyValue::new("outcome", outcome.to_string())];
        if let Some(reason) = failure_reason {
            attrs.push(KeyValue::new("failure_reason", reason.to_string()));
        }
        self.allocations_completed.add(1, &attrs);
        if let Some(ms) = duration_ms {
            self.allocation_duration_ms.record(ms, &attrs);
        }
    }
}

impl Default for DataplaneCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Observable gauges for current state metrics.
/// These are kept alive to maintain the gauge registrations.
#[allow(dead_code)]
pub struct DataplaneGauges {
    running_functions: ObservableGauge<u64>,
    running_sandboxes: ObservableGauge<u64>,
    pending_functions: ObservableGauge<u64>,
    pending_sandboxes: ObservableGauge<u64>,
    free_cpu_percent: ObservableGauge<f64>,
    free_memory_bytes: ObservableGauge<u64>,
    free_disk_bytes: ObservableGauge<u64>,
}

impl DataplaneGauges {
    /// Create observable gauges that read from the shared metrics state.
    pub fn new(state: Arc<Mutex<MetricsState>>) -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        let state_clone = state.clone();
        let running_functions = meter
            .u64_observable_gauge("indexify.dataplane.containers.running.functions")
            .with_description("Number of running function containers")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.container_counts.running_functions, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let running_sandboxes = meter
            .u64_observable_gauge("indexify.dataplane.containers.running.sandboxes")
            .with_description("Number of running sandbox containers")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.container_counts.running_sandboxes, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let pending_functions = meter
            .u64_observable_gauge("indexify.dataplane.containers.pending.functions")
            .with_description("Number of pending function containers")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.container_counts.pending_functions, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let pending_sandboxes = meter
            .u64_observable_gauge("indexify.dataplane.containers.pending.sandboxes")
            .with_description("Number of pending sandbox containers")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.container_counts.pending_sandboxes, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let free_cpu_percent = meter
            .f64_observable_gauge("indexify.dataplane.resources.free_cpu_percent")
            .with_description("Percentage of free CPU")
            .with_unit("%")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.resources.free_cpu_percent, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let free_memory_bytes = meter
            .u64_observable_gauge("indexify.dataplane.resources.free_memory_bytes")
            .with_description("Free memory in bytes")
            .with_unit("By")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.resources.free_memory_bytes, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let free_disk_bytes = meter
            .u64_observable_gauge("indexify.dataplane.resources.free_disk_bytes")
            .with_description("Free disk space in bytes")
            .with_unit("By")
            .with_callback(move |observer| {
                if let Ok(state) = state_clone.try_lock() {
                    observer.observe(state.resources.free_disk_bytes, &[]);
                }
            })
            .build();

        Self {
            running_functions,
            running_sandboxes,
            pending_functions,
            pending_sandboxes,
            free_cpu_percent,
            free_memory_bytes,
            free_disk_bytes,
        }
    }
}

/// Combined metrics handle for the dataplane.
pub struct DataplaneMetrics {
    pub counters: DataplaneCounters,
    pub state: Arc<Mutex<MetricsState>>,
    // Keep gauges alive - they register callbacks on construction
    #[allow(dead_code)]
    gauges: DataplaneGauges,
}

impl DataplaneMetrics {
    /// Create new dataplane metrics.
    /// The gauges will read from the shared state via callbacks.
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(MetricsState::default()));
        let counters = DataplaneCounters::new();
        let gauges = DataplaneGauges::new(state.clone());

        Self {
            counters,
            state,
            gauges,
        }
    }

    /// Update container counts in the shared state.
    pub async fn update_container_counts(&self, counts: ContainerCounts) {
        let mut state = self.state.lock().await;
        state.container_counts = counts;
    }

    /// Update resource availability in the shared state.
    pub async fn update_resources(&self, resources: ResourceAvailability) {
        let mut state = self.state.lock().await;
        state.resources = resources;
    }
}

impl Default for DataplaneMetrics {
    fn default() -> Self {
        Self::new()
    }
}
