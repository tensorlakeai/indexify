//! Shared configuration and utility functions for function executor management.

use std::{sync::Arc, time::Instant};

use proto_api::executor_api_pb::{AllocationFailureReason, CommandResponse};
use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::{
    blob_ops::BlobStore,
    code_cache::CodeCache,
    driver::ProcessDriver,
    function_container_manager::ImageResolver,
    secrets::SecretsProvider,
};

/// Shared configuration for spawning function executor controllers.
#[derive(Clone)]
pub struct FESpawnConfig {
    pub driver: Arc<dyn ProcessDriver>,
    pub image_resolver: Arc<dyn ImageResolver>,
    pub gpu_allocator: Arc<crate::gpu_allocator::GpuAllocator>,
    pub secrets_provider: Arc<dyn SecretsProvider>,
    /// Channel for allocation results (AllocationCompleted/AllocationFailed).
    pub result_tx: mpsc::UnboundedSender<CommandResponse>,
    /// Channel for container lifecycle events (ContainerTerminated/ContainerStarted).
    pub container_state_tx: mpsc::UnboundedSender<CommandResponse>,
    pub server_channel: Channel,
    pub blob_store: Arc<BlobStore>,
    pub code_cache: Arc<CodeCache>,
    pub executor_id: String,
    pub fe_binary_path: String,
    pub metrics: Arc<crate::metrics::DataplaneMetrics>,
}

/// Time an async phase, recording latency and adjusting in-progress/error
/// counters.
pub(crate) async fn timed_phase<T>(
    latency: &opentelemetry::metrics::Histogram<f64>,
    in_progress: &opentelemetry::metrics::UpDownCounter<i64>,
    errors: Option<&opentelemetry::metrics::Counter<u64>>,
    fut: impl std::future::Future<Output = T>,
    is_err: impl FnOnce(&T) -> bool,
) -> T {
    let start = Instant::now();
    let result = fut.await;
    latency.record(start.elapsed().as_secs_f64(), &[]);
    in_progress.add(-1, &[]);
    if let Some(counter) = errors &&
        is_err(&result)
    {
        counter.add(1, &[]);
    }
    result
}

/// Record allocation outcome metrics from a `CommandResponse`.
pub(crate) fn record_allocation_metrics(
    response: &CommandResponse,
    counters: &crate::metrics::DataplaneCounters,
) {
    use proto_api::executor_api_pb::command_response::Response;

    match &response.response {
        Some(Response::AllocationCompleted(c)) => {
            counters.record_allocation_completed("success", None, c.execution_duration_ms);
        }
        Some(Response::AllocationFailed(f)) => {
            let failure_reason = AllocationFailureReason::try_from(f.reason)
                .ok()
                .map(|reason| format!("{:?}", reason));
            counters.record_allocation_completed(
                "failure",
                failure_reason.as_deref(),
                f.execution_duration_ms,
            );
        }
        _ => {}
    }
}
