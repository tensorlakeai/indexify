//! Metrics module for the dataplane service.
//!
//! Provides OTLP metrics export with:
//! - Gauges for current state (running containers, free resources)
//! - Counters for events (containers started, desired state updates)
//! - Histograms for latency measurements

use std::sync::Arc;

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, ObservableGauge, UpDownCounter},
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
    pub last_desired_state_allocations: u64,
    pub last_desired_state_containers: u64,
    pub last_desired_state_function_executors: u64,
    pub fe_counts_starting: u64,
    pub fe_counts_running: u64,
    pub fe_counts_terminated: u64,
    pub sandbox_counts_pending: u64,
    pub sandbox_counts_running: u64,
    pub sandbox_counts_stopping: u64,
}

/// Generate a `DataplaneCounters` struct with `new()` and `Default`.
///
/// Each entry is: `field_name: "metric.name", "Description";`
/// All fields are `Counter<u64>`.
///
/// The `allocation_duration_ms` histogram is hardcoded here because it lives
/// in the counters struct for historical reasons.
macro_rules! define_counters {
    ( $( $field:ident : $name:literal, $desc:literal; )* ) => {
        #[derive(Clone)]
        #[allow(dead_code)]
        pub struct DataplaneCounters {
            $( pub $field: Counter<u64>, )*
            // Histogram that lives in counters for historical reasons
            pub allocation_duration_ms: Histogram<u64>,
        }

        impl DataplaneCounters {
            pub fn new() -> Self {
                let meter = opentelemetry::global::meter("indexify-dataplane");
                Self {
                    $( $field: meter.u64_counter($name).with_description($desc).build(), )*
                    allocation_duration_ms: meter
                        .u64_histogram("indexify.dataplane.allocations.duration_ms")
                        .with_description("Allocation execution duration in milliseconds")
                        .with_unit("ms")
                        .build(),
                }
            }
        }

        impl Default for DataplaneCounters {
            fn default() -> Self { Self::new() }
        }
    };
}

/// Generate a `DataplaneHistograms` struct with `new()` and `Default`.
///
/// Each entry is: `field_name: "metric.name", "Description", "unit";`
/// All fields are `Histogram<f64>`.
macro_rules! define_histograms {
    ( $( $field:ident : $name:literal, $desc:literal, $unit:literal; )* ) => {
        #[derive(Clone)]
        #[allow(dead_code)]
        pub struct DataplaneHistograms {
            $( pub $field: Histogram<f64>, )*
        }

        impl DataplaneHistograms {
            pub fn new() -> Self {
                let meter = opentelemetry::global::meter("indexify-dataplane");
                Self {
                    $( $field: meter.f64_histogram($name).with_description($desc).with_unit($unit).build(), )*
                }
            }
        }

        impl Default for DataplaneHistograms {
            fn default() -> Self { Self::new() }
        }
    };
}

/// Generate a `DataplaneUpDownCounters` struct with `new()` and `Default`.
///
/// Each entry is: `field_name: "metric.name", "Description";`
/// All fields are `UpDownCounter<i64>`.
macro_rules! define_up_down_counters {
    ( $( $field:ident : $name:literal, $desc:literal; )* ) => {
        #[derive(Clone)]
        pub struct DataplaneUpDownCounters {
            $( pub $field: UpDownCounter<i64>, )*
        }

        impl DataplaneUpDownCounters {
            pub fn new() -> Self {
                let meter = opentelemetry::global::meter("indexify-dataplane");
                Self {
                    $( $field: meter.i64_up_down_counter($name).with_description($desc).build(), )*
                }
            }
        }

        impl Default for DataplaneUpDownCounters {
            fn default() -> Self { Self::new() }
        }
    };
}

// --- Counter definitions ---

define_counters! {
    containers_started: "indexify.dataplane.containers.started", "Number of containers started";
    containers_terminated: "indexify.dataplane.containers.terminated", "Number of containers terminated";
    desired_state_received: "indexify.dataplane.desired_state.received", "Number of desired state messages received from server";
    desired_containers: "indexify.dataplane.desired_state.containers", "Total function executors received in desired state messages";
    desired_allocations: "indexify.dataplane.desired_state.allocations", "Total allocations received in desired state messages";
    heartbeat_success: "indexify.dataplane.heartbeat.success", "Number of successful heartbeats";
    heartbeat_failures: "indexify.dataplane.heartbeat.failures", "Number of failed heartbeats";
    stream_disconnections: "indexify.dataplane.stream.disconnections", "Number of stream disconnections";
    allocations_completed: "indexify.dataplane.allocations.completed", "Number of completed allocations";

    // Application downloads
    application_downloads: "indexify.dataplane.application_downloads", "Number of application code downloads";
    application_download_errors: "indexify.dataplane.application_download_errors", "Number of application code download errors";
    application_downloads_from_cache: "indexify.dataplane.application_downloads_from_cache", "Number of application code cache hits";

    // Blob store operations
    blob_store_get_metadata_requests: "indexify.dataplane.blob_store.get_metadata.requests", "Number of blob store get_metadata requests";
    blob_store_get_metadata_errors: "indexify.dataplane.blob_store.get_metadata.errors", "Number of blob store get_metadata errors";
    blob_store_presign_uri_requests: "indexify.dataplane.blob_store.presign_uri.requests", "Number of blob store presign URI requests";
    blob_store_presign_uri_errors: "indexify.dataplane.blob_store.presign_uri.errors", "Number of blob store presign URI errors";
    blob_store_create_multipart_upload_requests: "indexify.dataplane.blob_store.create_multipart_upload.requests", "Number of blob store create_multipart_upload requests";
    blob_store_create_multipart_upload_errors: "indexify.dataplane.blob_store.create_multipart_upload.errors", "Number of blob store create_multipart_upload errors";
    blob_store_complete_multipart_upload_requests: "indexify.dataplane.blob_store.complete_multipart_upload.requests", "Number of blob store complete_multipart_upload requests";
    blob_store_complete_multipart_upload_errors: "indexify.dataplane.blob_store.complete_multipart_upload.errors", "Number of blob store complete_multipart_upload errors";
    blob_store_abort_multipart_upload_requests: "indexify.dataplane.blob_store.abort_multipart_upload.requests", "Number of blob store abort_multipart_upload requests";
    blob_store_abort_multipart_upload_errors: "indexify.dataplane.blob_store.abort_multipart_upload.errors", "Number of blob store abort_multipart_upload errors";

    // Allocation preparation
    allocation_preparations: "indexify.dataplane.allocation_preparations", "Number of allocation preparations started";
    allocation_preparation_errors: "indexify.dataplane.allocation_preparation_errors", "Number of allocation preparation errors";

    // Function executor lifecycle
    function_executor_creates: "indexify.dataplane.function_executor.creates", "Number of function executor create attempts";
    function_executor_create_errors: "indexify.dataplane.function_executor.create_errors", "Number of function executor create failures";

    // Allocation lifecycle
    allocations_fetched: "indexify.dataplane.allocations_fetched", "Number of allocations received from server";
    allocation_runs: "indexify.dataplane.allocation_runs", "Number of allocation runs started";
    call_function_rpcs: "indexify.dataplane.call_function_rpcs", "Number of call_function RPCs to server";
    call_function_rpc_errors: "indexify.dataplane.call_function_rpc_errors", "Number of call_function RPC errors";
    allocation_finalizations: "indexify.dataplane.allocation_finalizations", "Number of allocation finalizations started";
    allocation_finalization_errors: "indexify.dataplane.allocation_finalization_errors", "Number of allocation finalization errors";

    // State reporting
    state_report_rpcs: "indexify.dataplane.state_report_rpcs", "Number of state report RPCs";
    state_report_rpc_errors: "indexify.dataplane.state_report_rpc_errors", "Number of state report RPC errors";
    state_report_message_fragmentations: "indexify.dataplane.state_report_message_fragmentations", "Number of state report message fragmentations";

    // State reconciliation
    state_reconciliations: "indexify.dataplane.state_reconciliations", "Number of state reconciliations";
    state_reconciliation_errors: "indexify.dataplane.state_reconciliation_errors", "Number of state reconciliation errors";

    // Image metadata
    image_metadata_requests: "indexify.dataplane.image_metadata.requests", "Number of image metadata requests";
    image_metadata_request_errors: "indexify.dataplane.image_metadata.errors", "Number of image metadata request errors";

    // Secrets
    secret_fetches: "indexify.dataplane.secret_fetches", "Number of secret fetch requests";
    secret_fetch_errors: "indexify.dataplane.secret_fetch_errors", "Number of secret fetch errors";

    // Function executor lifecycle (detailed)
    function_executor_create_server_errors: "indexify.dataplane.function_executor.create_server_errors", "Number of FE process start failures";
    function_executor_establish_channel_errors: "indexify.dataplane.function_executor.establish_channel_errors", "Number of FE channel establishment failures";
    function_executor_get_info_rpc_errors: "indexify.dataplane.function_executor.get_info_rpc_errors", "Number of FE get_info RPC failures";
    function_executor_infos: "indexify.dataplane.function_executor.infos", "Count of function executor info events (labelled by version/SDK)";
    function_executor_initialize_rpc_errors: "indexify.dataplane.function_executor.initialize_rpc_errors", "Number of FE initialize RPC failures";
    function_executor_destroys: "indexify.dataplane.function_executor.destroys", "Number of function executors destroyed";
    function_executor_failed_health_checks: "indexify.dataplane.function_executor.failed_health_checks", "Number of failed FE health checks";

    // Allocation lifecycle (detailed)
    allocation_run_errors: "indexify.dataplane.allocation_run_errors", "Number of failed allocation runs";

    // Blob store get/put
    blob_store_get_requests: "indexify.dataplane.blob_store.get.requests", "Number of blob store get requests";
    blob_store_get_errors: "indexify.dataplane.blob_store.get.errors", "Number of blob store get errors";
    blob_store_put_requests: "indexify.dataplane.blob_store.put.requests", "Number of blob store put requests";
    blob_store_put_errors: "indexify.dataplane.blob_store.put.errors", "Number of blob store put errors";

    // Stream
    stream_creations: "indexify.dataplane.stream.creations", "Number of stream creations";

    // Sandbox lifecycle
    sandbox_warm_pool_claims: "indexify.dataplane.sandbox.warm_pool_claims", "Number of warm pool containers claimed by a sandbox";
    sandbox_failed_health_checks: "indexify.dataplane.sandbox.failed_health_checks", "Number of failed sandbox daemon health checks";
    sandbox_network_rules_errors: "indexify.dataplane.sandbox.network_rules_errors", "Number of sandbox network rule application failures";
    sandbox_daemon_connect_errors: "indexify.dataplane.sandbox.daemon_connect_errors", "Number of sandbox daemon connection failures";
}

// Helper methods on DataplaneCounters (not generated by macro)
impl DataplaneCounters {
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
    #[allow(dead_code)]
    pub fn record_desired_state(&self, num_containers: u64, num_allocations: u64) {
        self.desired_state_received.add(1, &[]);
        self.desired_containers.add(num_containers, &[]);
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

    /// Record a function executor info event with SDK labels.
    pub fn record_function_executor_info(
        &self,
        version: &str,
        sdk_version: &str,
        sdk_language: &str,
        sdk_language_version: &str,
    ) {
        self.function_executor_infos.add(
            1,
            &[
                KeyValue::new("version", version.to_string()),
                KeyValue::new("sdk_version", sdk_version.to_string()),
                KeyValue::new("sdk_language", sdk_language.to_string()),
                KeyValue::new("sdk_language_version", sdk_language_version.to_string()),
            ],
        );
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

// --- Histogram definitions ---

define_histograms! {
    // Application downloads
    application_download_latency_seconds: "indexify.dataplane.application_download_latency_seconds", "Application download latency", "s";

    // Blob store operations
    blob_store_get_metadata_latency_seconds: "indexify.dataplane.blob_store.get_metadata.latency_seconds", "Blob store get_metadata latency", "s";
    blob_store_presign_uri_latency_seconds: "indexify.dataplane.blob_store.presign_uri.latency_seconds", "Blob store presign URI latency", "s";
    blob_store_create_multipart_upload_latency_seconds: "indexify.dataplane.blob_store.create_multipart_upload.latency_seconds", "Blob store create_multipart_upload latency", "s";
    blob_store_complete_multipart_upload_latency_seconds: "indexify.dataplane.blob_store.complete_multipart_upload.latency_seconds", "Blob store complete_multipart_upload latency", "s";
    blob_store_abort_multipart_upload_latency_seconds: "indexify.dataplane.blob_store.abort_multipart_upload.latency_seconds", "Blob store abort_multipart_upload latency", "s";

    // Allocation preparation
    allocation_preparation_latency_seconds: "indexify.dataplane.allocation_preparation_latency_seconds", "Allocation preparation latency", "s";

    // Function executor lifecycle
    function_executor_create_latency_seconds: "indexify.dataplane.function_executor.create_latency_seconds", "Function executor overall create latency", "s";
    function_executor_create_server_latency_seconds: "indexify.dataplane.function_executor.create_server_latency_seconds", "FE process start latency", "s";

    // Allocation lifecycle
    allocation_run_latency_seconds: "indexify.dataplane.allocation_run_latency_seconds", "Allocation run latency", "s";
    call_function_rpc_latency_seconds: "indexify.dataplane.call_function_rpc_latency_seconds", "call_function RPC latency", "s";
    function_call_message_size_mb: "indexify.dataplane.function_call_message_size_mb", "Function call message size in MB", "MB";
    allocation_finalization_latency_seconds: "indexify.dataplane.allocation_finalization_latency_seconds", "Allocation finalization latency", "s";

    // State reporting
    state_report_rpc_latency_seconds: "indexify.dataplane.state_report_rpc_latency_seconds", "State report RPC latency", "s";
    state_report_message_size_mb: "indexify.dataplane.state_report_message_size_mb", "State report message size in MB", "MB";

    // State reconciliation
    state_reconciliation_latency_seconds: "indexify.dataplane.state_reconciliation_latency_seconds", "State reconciliation latency", "s";

    // Image metadata
    image_metadata_request_latency_seconds: "indexify.dataplane.image_metadata.latency_seconds", "Image metadata request latency", "s";

    // Secrets
    secret_fetch_latency_seconds: "indexify.dataplane.secret_fetch_latency_seconds", "Secret fetch latency", "s";

    // Function executor lifecycle (detailed)
    function_executor_establish_channel_latency_seconds: "indexify.dataplane.function_executor.establish_channel_latency_seconds", "FE channel establishment latency", "s";
    function_executor_get_info_rpc_latency_seconds: "indexify.dataplane.function_executor.get_info_rpc_latency_seconds", "FE get_info RPC latency", "s";
    function_executor_initialize_rpc_latency_seconds: "indexify.dataplane.function_executor.initialize_rpc_latency_seconds", "FE initialize RPC latency", "s";
    function_executor_health_check_latency_seconds: "indexify.dataplane.function_executor.health_check_latency_seconds", "FE health check latency", "s";

    // Blob store get/put
    blob_store_get_latency_seconds: "indexify.dataplane.blob_store.get.latency_seconds", "Blob store get latency", "s";
    blob_store_put_latency_seconds: "indexify.dataplane.blob_store.put.latency_seconds", "Blob store put latency", "s";

    // Sandbox lifecycle
    sandbox_startup_latency_seconds: "indexify.dataplane.sandbox.startup_latency_seconds", "Sandbox total startup latency (Pending to Running)", "s";
    sandbox_daemon_connect_latency_seconds: "indexify.dataplane.sandbox.daemon_connect_latency_seconds", "Sandbox daemon connection latency", "s";
    sandbox_health_check_latency_seconds: "indexify.dataplane.sandbox.health_check_latency_seconds", "Sandbox daemon health check latency", "s";
}

// --- UpDownCounter definitions ---

define_up_down_counters! {
    containers_count: "indexify.dataplane.containers_count", "Current number of containers";
    allocations_getting_prepared: "indexify.dataplane.allocations_getting_prepared", "Current number of allocations being prepared";
    allocation_runs_in_progress: "indexify.dataplane.allocation_runs_in_progress", "Current number of allocation runs in progress";
    allocations_finalizing: "indexify.dataplane.allocations_finalizing", "Current number of allocations being finalized";
    runnable_allocations: "indexify.dataplane.runnable_allocations", "Current number of allocations waiting for a free FE slot";
}

/// Observable gauges for current state metrics.
/// These are kept alive to maintain the gauge registrations.
#[allow(dead_code)] // Fields must stay alive to keep gauge callbacks registered
pub struct DataplaneGauges {
    running_functions: ObservableGauge<u64>,
    running_sandboxes: ObservableGauge<u64>,
    pending_functions: ObservableGauge<u64>,
    pending_sandboxes: ObservableGauge<u64>,
    free_cpu_percent: ObservableGauge<f64>,
    free_memory_bytes: ObservableGauge<u64>,
    free_disk_bytes: ObservableGauge<u64>,
    last_desired_state_allocations: ObservableGauge<u64>,
    last_desired_state_containers: ObservableGauge<u64>,
    function_executors_with_state: ObservableGauge<u64>,
    sandboxes_with_state: ObservableGauge<u64>,
}

impl DataplaneGauges {
    /// Create observable gauges that read from the shared metrics state.
    pub fn new(state: Arc<Mutex<MetricsState>>) -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        // Local macros to eliminate per-gauge boilerplate. Each gauge clones
        // the shared state Arc, locks it in the callback, and reads one field.
        macro_rules! u64_gauge {
            ($name:literal, $desc:literal, $($path:tt)+) => {{
                let s = state.clone();
                meter.u64_observable_gauge($name)
                    .with_description($desc)
                    .with_callback(move |observer| {
                        if let Ok(st) = s.try_lock() {
                            observer.observe(st.$($path)+, &[]);
                        }
                    })
                    .build()
            }};
        }

        macro_rules! u64_gauge_unit {
            ($name:literal, $desc:literal, $unit:literal, $($path:tt)+) => {{
                let s = state.clone();
                meter.u64_observable_gauge($name)
                    .with_description($desc)
                    .with_unit($unit)
                    .with_callback(move |observer| {
                        if let Ok(st) = s.try_lock() {
                            observer.observe(st.$($path)+, &[]);
                        }
                    })
                    .build()
            }};
        }

        Self {
            running_functions: u64_gauge!(
                "indexify.dataplane.containers.running.functions",
                "Number of running function containers",
                container_counts.running_functions
            ),
            running_sandboxes: u64_gauge!(
                "indexify.dataplane.containers.running.sandboxes",
                "Number of running sandbox containers",
                container_counts.running_sandboxes
            ),
            pending_functions: u64_gauge!(
                "indexify.dataplane.containers.pending.functions",
                "Number of pending function containers",
                container_counts.pending_functions
            ),
            pending_sandboxes: u64_gauge!(
                "indexify.dataplane.containers.pending.sandboxes",
                "Number of pending sandbox containers",
                container_counts.pending_sandboxes
            ),
            free_cpu_percent: {
                let s = state.clone();
                meter
                    .f64_observable_gauge("indexify.dataplane.resources.free_cpu_percent")
                    .with_description("Percentage of free CPU")
                    .with_unit("%")
                    .with_callback(move |observer| {
                        if let Ok(st) = s.try_lock() {
                            observer.observe(st.resources.free_cpu_percent, &[]);
                        }
                    })
                    .build()
            },
            free_memory_bytes: u64_gauge_unit!(
                "indexify.dataplane.resources.free_memory_bytes",
                "Free memory in bytes",
                "By",
                resources.free_memory_bytes
            ),
            free_disk_bytes: u64_gauge_unit!(
                "indexify.dataplane.resources.free_disk_bytes",
                "Free disk space in bytes",
                "By",
                resources.free_disk_bytes
            ),
            last_desired_state_allocations: u64_gauge!(
                "indexify.dataplane.last_desired_state_allocations",
                "Number of allocations in last desired state",
                last_desired_state_allocations
            ),
            last_desired_state_containers: u64_gauge!(
                "indexify.dataplane.last_desired_state_containers",
                "Number of containers in last desired state",
                last_desired_state_containers
            ),
            function_executors_with_state: {
                let s = state.clone();
                meter
                    .u64_observable_gauge("indexify.dataplane.function_executors_with_state")
                    .with_description("Number of function executors by state")
                    .with_callback(move |observer| {
                        if let Ok(st) = s.try_lock() {
                            observer.observe(
                                st.fe_counts_starting,
                                &[KeyValue::new("state", "starting")],
                            );
                            observer.observe(
                                st.fe_counts_running,
                                &[KeyValue::new("state", "running")],
                            );
                            observer.observe(
                                st.fe_counts_terminated,
                                &[KeyValue::new("state", "terminated")],
                            );
                        }
                    })
                    .build()
            },
            sandboxes_with_state: {
                let s = state.clone();
                meter
                    .u64_observable_gauge("indexify.dataplane.sandboxes_with_state")
                    .with_description("Number of sandbox containers by state")
                    .with_callback(move |observer| {
                        if let Ok(st) = s.try_lock() {
                            observer.observe(
                                st.sandbox_counts_pending,
                                &[KeyValue::new("state", "pending")],
                            );
                            observer.observe(
                                st.sandbox_counts_running,
                                &[KeyValue::new("state", "running")],
                            );
                            observer.observe(
                                st.sandbox_counts_stopping,
                                &[KeyValue::new("state", "stopping")],
                            );
                        }
                    })
                    .build()
            },
        }
    }
}

/// Combined metrics handle for the dataplane.
pub struct DataplaneMetrics {
    pub counters: DataplaneCounters,
    pub histograms: DataplaneHistograms,
    pub up_down_counters: DataplaneUpDownCounters,
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
        let histograms = DataplaneHistograms::new();
        let up_down_counters = DataplaneUpDownCounters::new();
        let gauges = DataplaneGauges::new(state.clone());

        Self {
            counters,
            histograms,
            up_down_counters,
            state,
            gauges,
        }
    }

    /// Update container counts in the shared state.
    /// Used by FunctionContainerManager for sandbox containers.
    pub async fn update_container_counts(&self, counts: ContainerCounts) {
        let mut state = self.state.lock().await;
        state.container_counts = counts;
    }

    /// Update only the function container counts (pending + running).
    /// Used by AllocationController, which owns function containers.
    /// Leaves sandbox counts (managed by FunctionContainerManager) untouched.
    pub async fn update_function_container_counts(&self, pending: u64, running: u64) {
        let mut state = self.state.lock().await;
        state.container_counts.pending_functions = pending;
        state.container_counts.running_functions = running;
    }

    /// Update resource availability in the shared state.
    pub async fn update_resources(&self, resources: ResourceAvailability) {
        let mut state = self.state.lock().await;
        state.resources = resources;
    }

    /// Update function executor state counts for the observable gauge.
    pub async fn update_fe_state_counts(&self, starting: u64, running: u64, terminated: u64) {
        let mut state = self.state.lock().await;
        state.fe_counts_starting = starting;
        state.fe_counts_running = running;
        state.fe_counts_terminated = terminated;
    }

    /// Update sandbox container state counts for the observable gauge.
    pub async fn update_sandbox_state_counts(&self, pending: u64, running: u64, stopping: u64) {
        let mut state = self.state.lock().await;
        state.sandbox_counts_pending = pending;
        state.sandbox_counts_running = running;
        state.sandbox_counts_stopping = stopping;
    }
}

impl Default for DataplaneMetrics {
    fn default() -> Self {
        Self::new()
    }
}
