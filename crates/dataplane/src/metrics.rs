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
}

/// Counters for tracking events.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DataplaneCounters {
    // --- Existing counters ---
    pub containers_started: Counter<u64>,
    pub containers_terminated: Counter<u64>,
    pub desired_state_received: Counter<u64>,
    pub desired_function_executors: Counter<u64>,
    pub desired_allocations: Counter<u64>,
    pub heartbeat_success: Counter<u64>,
    pub heartbeat_failures: Counter<u64>,
    pub stream_disconnections: Counter<u64>,
    pub allocations_completed: Counter<u64>,
    pub allocation_duration_ms: Histogram<u64>,

    // --- Application downloads ---
    pub application_downloads: Counter<u64>,
    pub application_download_errors: Counter<u64>,
    pub application_downloads_from_cache: Counter<u64>,

    // --- Blob store operations ---
    pub blob_store_get_metadata_requests: Counter<u64>,
    pub blob_store_get_metadata_errors: Counter<u64>,
    pub blob_store_presign_uri_requests: Counter<u64>,
    pub blob_store_presign_uri_errors: Counter<u64>,
    pub blob_store_upload_requests: Counter<u64>,
    pub blob_store_upload_errors: Counter<u64>,
    pub blob_store_create_multipart_upload_requests: Counter<u64>,
    pub blob_store_create_multipart_upload_errors: Counter<u64>,
    pub blob_store_complete_multipart_upload_requests: Counter<u64>,
    pub blob_store_complete_multipart_upload_errors: Counter<u64>,
    pub blob_store_abort_multipart_upload_requests: Counter<u64>,
    pub blob_store_abort_multipart_upload_errors: Counter<u64>,

    // --- Allocation preparation ---
    pub allocation_preparations: Counter<u64>,
    pub allocation_preparation_errors: Counter<u64>,

    // --- Function executor lifecycle ---
    pub function_executor_creates: Counter<u64>,
    pub function_executor_create_errors: Counter<u64>,
    pub function_executor_destroys: Counter<u64>,
    pub function_executor_destroy_errors: Counter<u64>,
    pub function_executor_create_server_errors: Counter<u64>,
    pub function_executor_destroy_server_errors: Counter<u64>,
    pub function_executor_establish_channel_errors: Counter<u64>,
    pub function_executor_destroy_channel_errors: Counter<u64>,
    pub function_executor_get_info_rpc_errors: Counter<u64>,
    pub function_executor_initialize_rpc_errors: Counter<u64>,
    pub function_executor_create_health_checker_errors: Counter<u64>,
    pub function_executor_destroy_health_checker_errors: Counter<u64>,
    pub function_executor_failed_health_checks: Counter<u64>,

    // --- Allocation lifecycle ---
    pub allocations_fetched: Counter<u64>,
    pub allocation_runs: Counter<u64>,
    pub allocation_run_errors: Counter<u64>,
    pub call_function_rpcs: Counter<u64>,
    pub call_function_rpc_errors: Counter<u64>,
    pub allocation_finalizations: Counter<u64>,
    pub allocation_finalization_errors: Counter<u64>,

    // --- State reporting ---
    pub state_report_rpcs: Counter<u64>,
    pub state_report_rpc_errors: Counter<u64>,
    pub state_report_messages_over_size_limit: Counter<u64>,
    pub state_report_message_fragmentations: Counter<u64>,

    // --- State reconciliation ---
    pub state_reconciliations: Counter<u64>,
    pub state_reconciliation_errors: Counter<u64>,

    // --- gRPC channel ---
    pub grpc_server_channel_creations: Counter<u64>,
    pub grpc_server_channel_creation_retries: Counter<u64>,
}

/// Histograms for latency and size measurements.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DataplaneHistograms {
    // --- Application downloads ---
    pub application_download_latency_seconds: Histogram<f64>,

    // --- Blob store operations ---
    pub blob_store_get_metadata_latency_seconds: Histogram<f64>,
    pub blob_store_presign_uri_latency_seconds: Histogram<f64>,
    pub blob_store_upload_latency_seconds: Histogram<f64>,
    pub blob_store_create_multipart_upload_latency_seconds: Histogram<f64>,
    pub blob_store_complete_multipart_upload_latency_seconds: Histogram<f64>,
    pub blob_store_abort_multipart_upload_latency_seconds: Histogram<f64>,

    // --- Allocation preparation ---
    pub allocation_preparation_latency_seconds: Histogram<f64>,

    // --- Function executor lifecycle ---
    pub function_executor_create_latency_seconds: Histogram<f64>,
    pub function_executor_destroy_latency_seconds: Histogram<f64>,
    pub function_executor_create_server_latency_seconds: Histogram<f64>,
    pub function_executor_destroy_server_latency_seconds: Histogram<f64>,
    pub function_executor_establish_channel_latency_seconds: Histogram<f64>,
    pub function_executor_destroy_channel_latency_seconds: Histogram<f64>,
    pub function_executor_get_info_rpc_latency_seconds: Histogram<f64>,
    pub function_executor_initialize_rpc_latency_seconds: Histogram<f64>,
    pub function_executor_create_health_checker_latency_seconds: Histogram<f64>,
    pub function_executor_destroy_health_checker_latency_seconds: Histogram<f64>,
    pub function_executor_health_check_latency_seconds: Histogram<f64>,

    // --- Allocation lifecycle ---
    pub allocation_completion_latency_seconds: Histogram<f64>,
    pub allocation_run_latency_seconds: Histogram<f64>,
    pub call_function_rpc_latency_seconds: Histogram<f64>,
    pub function_call_message_size_mb: Histogram<f64>,
    pub schedule_allocation_latency_seconds: Histogram<f64>,
    pub allocation_finalization_latency_seconds: Histogram<f64>,

    // --- State reporting ---
    pub state_report_rpc_latency_seconds: Histogram<f64>,
    pub state_report_message_size_mb: Histogram<f64>,

    // --- State reconciliation ---
    pub state_reconciliation_latency_seconds: Histogram<f64>,

    // --- gRPC channel ---
    pub grpc_server_channel_creation_latency_seconds: Histogram<f64>,
}

/// UpDownCounters for gauges that need inc/dec.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DataplaneUpDownCounters {
    pub function_executors_count: UpDownCounter<i64>,
    pub allocations_getting_prepared: UpDownCounter<i64>,
    pub allocation_runs_in_progress: UpDownCounter<i64>,
    pub allocations_finalizing: UpDownCounter<i64>,
    pub runnable_allocations: UpDownCounter<i64>,
}

impl DataplaneCounters {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        Self {
            containers_started: meter
                .u64_counter("indexify.dataplane.containers.started")
                .with_description("Number of containers started")
                .build(),
            containers_terminated: meter
                .u64_counter("indexify.dataplane.containers.terminated")
                .with_description("Number of containers terminated")
                .build(),
            desired_state_received: meter
                .u64_counter("indexify.dataplane.desired_state.received")
                .with_description("Number of desired state messages received from server")
                .build(),
            desired_function_executors: meter
                .u64_counter("indexify.dataplane.desired_state.function_executors")
                .with_description("Total function executors received in desired state messages")
                .build(),
            desired_allocations: meter
                .u64_counter("indexify.dataplane.desired_state.allocations")
                .with_description("Total allocations received in desired state messages")
                .build(),
            heartbeat_success: meter
                .u64_counter("indexify.dataplane.heartbeat.success")
                .with_description("Number of successful heartbeats")
                .build(),
            heartbeat_failures: meter
                .u64_counter("indexify.dataplane.heartbeat.failures")
                .with_description("Number of failed heartbeats")
                .build(),
            stream_disconnections: meter
                .u64_counter("indexify.dataplane.stream.disconnections")
                .with_description("Number of stream disconnections")
                .build(),
            allocations_completed: meter
                .u64_counter("indexify.dataplane.allocations.completed")
                .with_description("Number of completed allocations")
                .build(),
            allocation_duration_ms: meter
                .u64_histogram("indexify.dataplane.allocations.duration_ms")
                .with_description("Allocation execution duration in milliseconds")
                .with_unit("ms")
                .build(),

            // Application downloads
            application_downloads: meter
                .u64_counter("indexify.dataplane.application_downloads")
                .with_description("Number of application code downloads")
                .build(),
            application_download_errors: meter
                .u64_counter("indexify.dataplane.application_download_errors")
                .with_description("Number of application code download errors")
                .build(),
            application_downloads_from_cache: meter
                .u64_counter("indexify.dataplane.application_downloads_from_cache")
                .with_description("Number of application code cache hits")
                .build(),

            // Blob store operations
            blob_store_get_metadata_requests: meter
                .u64_counter("indexify.dataplane.blob_store.get_metadata.requests")
                .with_description("Number of blob store get_metadata requests")
                .build(),
            blob_store_get_metadata_errors: meter
                .u64_counter("indexify.dataplane.blob_store.get_metadata.errors")
                .with_description("Number of blob store get_metadata errors")
                .build(),
            blob_store_presign_uri_requests: meter
                .u64_counter("indexify.dataplane.blob_store.presign_uri.requests")
                .with_description("Number of blob store presign URI requests")
                .build(),
            blob_store_presign_uri_errors: meter
                .u64_counter("indexify.dataplane.blob_store.presign_uri.errors")
                .with_description("Number of blob store presign URI errors")
                .build(),
            blob_store_upload_requests: meter
                .u64_counter("indexify.dataplane.blob_store.upload.requests")
                .with_description("Number of blob store upload requests")
                .build(),
            blob_store_upload_errors: meter
                .u64_counter("indexify.dataplane.blob_store.upload.errors")
                .with_description("Number of blob store upload errors")
                .build(),
            blob_store_create_multipart_upload_requests: meter
                .u64_counter("indexify.dataplane.blob_store.create_multipart_upload.requests")
                .with_description("Number of blob store create_multipart_upload requests")
                .build(),
            blob_store_create_multipart_upload_errors: meter
                .u64_counter("indexify.dataplane.blob_store.create_multipart_upload.errors")
                .with_description("Number of blob store create_multipart_upload errors")
                .build(),
            blob_store_complete_multipart_upload_requests: meter
                .u64_counter("indexify.dataplane.blob_store.complete_multipart_upload.requests")
                .with_description("Number of blob store complete_multipart_upload requests")
                .build(),
            blob_store_complete_multipart_upload_errors: meter
                .u64_counter("indexify.dataplane.blob_store.complete_multipart_upload.errors")
                .with_description("Number of blob store complete_multipart_upload errors")
                .build(),
            blob_store_abort_multipart_upload_requests: meter
                .u64_counter("indexify.dataplane.blob_store.abort_multipart_upload.requests")
                .with_description("Number of blob store abort_multipart_upload requests")
                .build(),
            blob_store_abort_multipart_upload_errors: meter
                .u64_counter("indexify.dataplane.blob_store.abort_multipart_upload.errors")
                .with_description("Number of blob store abort_multipart_upload errors")
                .build(),

            // Allocation preparation
            allocation_preparations: meter
                .u64_counter("indexify.dataplane.allocation_preparations")
                .with_description("Number of allocation preparations started")
                .build(),
            allocation_preparation_errors: meter
                .u64_counter("indexify.dataplane.allocation_preparation_errors")
                .with_description("Number of allocation preparation errors")
                .build(),

            // Function executor lifecycle
            function_executor_creates: meter
                .u64_counter("indexify.dataplane.function_executor.creates")
                .with_description("Number of function executor create attempts")
                .build(),
            function_executor_create_errors: meter
                .u64_counter("indexify.dataplane.function_executor.create_errors")
                .with_description("Number of function executor create failures")
                .build(),
            function_executor_destroys: meter
                .u64_counter("indexify.dataplane.function_executor.destroys")
                .with_description("Number of function executor destroy attempts")
                .build(),
            function_executor_destroy_errors: meter
                .u64_counter("indexify.dataplane.function_executor.destroy_errors")
                .with_description("Number of function executor destroy failures")
                .build(),
            function_executor_create_server_errors: meter
                .u64_counter("indexify.dataplane.function_executor.create_server_errors")
                .with_description("Number of FE process start failures")
                .build(),
            function_executor_destroy_server_errors: meter
                .u64_counter("indexify.dataplane.function_executor.destroy_server_errors")
                .with_description("Number of FE process kill failures")
                .build(),
            function_executor_establish_channel_errors: meter
                .u64_counter("indexify.dataplane.function_executor.establish_channel_errors")
                .with_description("Number of FE gRPC channel establishment failures")
                .build(),
            function_executor_destroy_channel_errors: meter
                .u64_counter("indexify.dataplane.function_executor.destroy_channel_errors")
                .with_description("Number of FE gRPC channel destroy failures")
                .build(),
            function_executor_get_info_rpc_errors: meter
                .u64_counter("indexify.dataplane.function_executor.get_info_rpc_errors")
                .with_description("Number of FE get_info RPC failures")
                .build(),
            function_executor_initialize_rpc_errors: meter
                .u64_counter("indexify.dataplane.function_executor.initialize_rpc_errors")
                .with_description("Number of FE initialize RPC failures")
                .build(),
            function_executor_create_health_checker_errors: meter
                .u64_counter("indexify.dataplane.function_executor.create_health_checker_errors")
                .with_description("Number of FE health checker creation failures")
                .build(),
            function_executor_destroy_health_checker_errors: meter
                .u64_counter("indexify.dataplane.function_executor.destroy_health_checker_errors")
                .with_description("Number of FE health checker destroy failures")
                .build(),
            function_executor_failed_health_checks: meter
                .u64_counter("indexify.dataplane.function_executor.failed_health_checks")
                .with_description("Number of failed FE health checks")
                .build(),

            // Allocation lifecycle
            allocations_fetched: meter
                .u64_counter("indexify.dataplane.allocations_fetched")
                .with_description("Number of allocations received from server")
                .build(),
            allocation_runs: meter
                .u64_counter("indexify.dataplane.allocation_runs")
                .with_description("Number of allocation runs started")
                .build(),
            allocation_run_errors: meter
                .u64_counter("indexify.dataplane.allocation_run_errors")
                .with_description("Number of allocation run errors")
                .build(),
            call_function_rpcs: meter
                .u64_counter("indexify.dataplane.call_function_rpcs")
                .with_description("Number of call_function RPCs to server")
                .build(),
            call_function_rpc_errors: meter
                .u64_counter("indexify.dataplane.call_function_rpc_errors")
                .with_description("Number of call_function RPC errors")
                .build(),
            allocation_finalizations: meter
                .u64_counter("indexify.dataplane.allocation_finalizations")
                .with_description("Number of allocation finalizations started")
                .build(),
            allocation_finalization_errors: meter
                .u64_counter("indexify.dataplane.allocation_finalization_errors")
                .with_description("Number of allocation finalization errors")
                .build(),

            // State reporting
            state_report_rpcs: meter
                .u64_counter("indexify.dataplane.state_report_rpcs")
                .with_description("Number of state report RPCs")
                .build(),
            state_report_rpc_errors: meter
                .u64_counter("indexify.dataplane.state_report_rpc_errors")
                .with_description("Number of state report RPC errors")
                .build(),
            state_report_messages_over_size_limit: meter
                .u64_counter("indexify.dataplane.state_report_messages_over_size_limit")
                .with_description("Number of state report messages over size limit")
                .build(),
            state_report_message_fragmentations: meter
                .u64_counter("indexify.dataplane.state_report_message_fragmentations")
                .with_description("Number of state report message fragmentations")
                .build(),

            // State reconciliation
            state_reconciliations: meter
                .u64_counter("indexify.dataplane.state_reconciliations")
                .with_description("Number of state reconciliations")
                .build(),
            state_reconciliation_errors: meter
                .u64_counter("indexify.dataplane.state_reconciliation_errors")
                .with_description("Number of state reconciliation errors")
                .build(),

            // gRPC channel
            grpc_server_channel_creations: meter
                .u64_counter("indexify.dataplane.grpc_server_channel_creations")
                .with_description("Number of gRPC server channel creations")
                .build(),
            grpc_server_channel_creation_retries: meter
                .u64_counter("indexify.dataplane.grpc_server_channel_creation_retries")
                .with_description("Number of gRPC server channel creation retries")
                .build(),
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

impl DataplaneHistograms {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        Self {
            application_download_latency_seconds: meter
                .f64_histogram("indexify.dataplane.application_download_latency_seconds")
                .with_description("Application download latency")
                .with_unit("s")
                .build(),
            blob_store_get_metadata_latency_seconds: meter
                .f64_histogram("indexify.dataplane.blob_store.get_metadata.latency_seconds")
                .with_description("Blob store get_metadata latency")
                .with_unit("s")
                .build(),
            blob_store_presign_uri_latency_seconds: meter
                .f64_histogram("indexify.dataplane.blob_store.presign_uri.latency_seconds")
                .with_description("Blob store presign URI latency")
                .with_unit("s")
                .build(),
            blob_store_upload_latency_seconds: meter
                .f64_histogram("indexify.dataplane.blob_store.upload.latency_seconds")
                .with_description("Blob store upload latency")
                .with_unit("s")
                .build(),
            blob_store_create_multipart_upload_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.blob_store.create_multipart_upload.latency_seconds",
                )
                .with_description("Blob store create_multipart_upload latency")
                .with_unit("s")
                .build(),
            blob_store_complete_multipart_upload_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.blob_store.complete_multipart_upload.latency_seconds",
                )
                .with_description("Blob store complete_multipart_upload latency")
                .with_unit("s")
                .build(),
            blob_store_abort_multipart_upload_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.blob_store.abort_multipart_upload.latency_seconds",
                )
                .with_description("Blob store abort_multipart_upload latency")
                .with_unit("s")
                .build(),
            allocation_preparation_latency_seconds: meter
                .f64_histogram("indexify.dataplane.allocation_preparation_latency_seconds")
                .with_description("Allocation preparation latency")
                .with_unit("s")
                .build(),
            function_executor_create_latency_seconds: meter
                .f64_histogram("indexify.dataplane.function_executor.create_latency_seconds")
                .with_description("Function executor overall create latency")
                .with_unit("s")
                .build(),
            function_executor_destroy_latency_seconds: meter
                .f64_histogram("indexify.dataplane.function_executor.destroy_latency_seconds")
                .with_description("Function executor destroy latency")
                .with_unit("s")
                .build(),
            function_executor_create_server_latency_seconds: meter
                .f64_histogram("indexify.dataplane.function_executor.create_server_latency_seconds")
                .with_description("FE process start latency")
                .with_unit("s")
                .build(),
            function_executor_destroy_server_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.destroy_server_latency_seconds",
                )
                .with_description("FE process kill latency")
                .with_unit("s")
                .build(),
            function_executor_establish_channel_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.establish_channel_latency_seconds",
                )
                .with_description("FE gRPC channel establishment latency")
                .with_unit("s")
                .build(),
            function_executor_destroy_channel_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.destroy_channel_latency_seconds",
                )
                .with_description("FE gRPC channel destroy latency")
                .with_unit("s")
                .build(),
            function_executor_get_info_rpc_latency_seconds: meter
                .f64_histogram("indexify.dataplane.function_executor.get_info_rpc_latency_seconds")
                .with_description("FE get_info RPC latency")
                .with_unit("s")
                .build(),
            function_executor_initialize_rpc_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.initialize_rpc_latency_seconds",
                )
                .with_description("FE initialize RPC latency")
                .with_unit("s")
                .build(),
            function_executor_create_health_checker_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.create_health_checker_latency_seconds",
                )
                .with_description("FE health checker creation latency")
                .with_unit("s")
                .build(),
            function_executor_destroy_health_checker_latency_seconds: meter
                .f64_histogram(
                    "indexify.dataplane.function_executor.destroy_health_checker_latency_seconds",
                )
                .with_description("FE health checker destroy latency")
                .with_unit("s")
                .build(),
            function_executor_health_check_latency_seconds: meter
                .f64_histogram("indexify.dataplane.function_executor.health_check_latency_seconds")
                .with_description("FE health check latency")
                .with_unit("s")
                .build(),
            allocation_completion_latency_seconds: meter
                .f64_histogram("indexify.dataplane.allocation_completion_latency_seconds")
                .with_description("Allocation overall completion latency")
                .with_unit("s")
                .build(),
            allocation_run_latency_seconds: meter
                .f64_histogram("indexify.dataplane.allocation_run_latency_seconds")
                .with_description("Allocation run latency")
                .with_unit("s")
                .build(),
            call_function_rpc_latency_seconds: meter
                .f64_histogram("indexify.dataplane.call_function_rpc_latency_seconds")
                .with_description("call_function RPC latency")
                .with_unit("s")
                .build(),
            function_call_message_size_mb: meter
                .f64_histogram("indexify.dataplane.function_call_message_size_mb")
                .with_description("Function call message size in MB")
                .with_unit("MB")
                .build(),
            schedule_allocation_latency_seconds: meter
                .f64_histogram("indexify.dataplane.schedule_allocation_latency_seconds")
                .with_description("Schedule allocation latency")
                .with_unit("s")
                .build(),
            allocation_finalization_latency_seconds: meter
                .f64_histogram("indexify.dataplane.allocation_finalization_latency_seconds")
                .with_description("Allocation finalization latency")
                .with_unit("s")
                .build(),
            state_report_rpc_latency_seconds: meter
                .f64_histogram("indexify.dataplane.state_report_rpc_latency_seconds")
                .with_description("State report RPC latency")
                .with_unit("s")
                .build(),
            state_report_message_size_mb: meter
                .f64_histogram("indexify.dataplane.state_report_message_size_mb")
                .with_description("State report message size in MB")
                .with_unit("MB")
                .build(),
            state_reconciliation_latency_seconds: meter
                .f64_histogram("indexify.dataplane.state_reconciliation_latency_seconds")
                .with_description("State reconciliation latency")
                .with_unit("s")
                .build(),
            grpc_server_channel_creation_latency_seconds: meter
                .f64_histogram("indexify.dataplane.grpc_server_channel_creation_latency_seconds")
                .with_description("gRPC server channel creation latency")
                .with_unit("s")
                .build(),
        }
    }
}

impl Default for DataplaneHistograms {
    fn default() -> Self {
        Self::new()
    }
}

impl DataplaneUpDownCounters {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("indexify-dataplane");

        Self {
            function_executors_count: meter
                .i64_up_down_counter("indexify.dataplane.function_executors_count")
                .with_description("Current number of function executors")
                .build(),
            allocations_getting_prepared: meter
                .i64_up_down_counter("indexify.dataplane.allocations_getting_prepared")
                .with_description("Current number of allocations being prepared")
                .build(),
            allocation_runs_in_progress: meter
                .i64_up_down_counter("indexify.dataplane.allocation_runs_in_progress")
                .with_description("Current number of allocation runs in progress")
                .build(),
            allocations_finalizing: meter
                .i64_up_down_counter("indexify.dataplane.allocations_finalizing")
                .with_description("Current number of allocations being finalized")
                .build(),
            runnable_allocations: meter
                .i64_up_down_counter("indexify.dataplane.runnable_allocations")
                .with_description("Current number of runnable allocations queued")
                .build(),
        }
    }
}

impl Default for DataplaneUpDownCounters {
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
    last_desired_state_allocations: ObservableGauge<u64>,
    last_desired_state_function_executors: ObservableGauge<u64>,
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

        // These are placeholder gauges - values are set via the MetricsState
        // in handle_desired_state
        let state_clone = state.clone();
        let last_desired_state_allocations = meter
            .u64_observable_gauge("indexify.dataplane.last_desired_state_allocations")
            .with_description("Number of allocations in last desired state")
            .with_callback(move |observer| {
                if let Ok(_state) = state_clone.try_lock() {
                    // Value is set via MetricsState
                    observer.observe(0, &[]);
                }
            })
            .build();

        let state_clone = state.clone();
        let last_desired_state_function_executors = meter
            .u64_observable_gauge("indexify.dataplane.last_desired_state_function_executors")
            .with_description("Number of function executors in last desired state")
            .with_callback(move |observer| {
                if let Ok(_state) = state_clone.try_lock() {
                    observer.observe(0, &[]);
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
            last_desired_state_allocations,
            last_desired_state_function_executors,
        }
    }
}

/// Combined metrics handle for the dataplane.
#[allow(dead_code)]
pub struct DataplaneMetrics {
    pub counters: DataplaneCounters,
    pub histograms: DataplaneHistograms,
    pub up_down_counters: DataplaneUpDownCounters,
    pub state: Arc<Mutex<MetricsState>>,
    // Keep gauges alive - they register callbacks on construction
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
