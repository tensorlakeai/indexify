use std::sync::Arc;

use arc_swap::ArcSwap;
use opentelemetry::metrics::{Histogram, ObservableGauge};

use crate::{metrics::low_latency_boundaries, state_store::AppState};

#[derive(Clone)]
pub struct InMemoryStoreMetrics {
    pub function_run_pending_latency: Histogram<f64>,
    pub allocation_running_latency: Histogram<f64>,
    pub allocation_completion_latency: Histogram<f64>,
    pub scheduler_update_delete_requests: Histogram<f64>,
    pub scheduler_update_insert_new_allocations: Histogram<f64>,
    pub scheduler_update_remove_executors: Histogram<f64>,
}

impl InMemoryStoreMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("state_store");
        // Create histogram metrics for task latency measurements
        let function_run_pending_latency = meter
            .f64_histogram("indexify.function_run_pending_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time function runs spend from creation to running")
            .build();

        let allocation_running_latency = meter
            .f64_histogram("indexify.allocation_running_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time function runs spend from running to completion")
            .build();

        let allocation_completion_latency = meter
            .f64_histogram("indexify.allocation_completion_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend from creation to completion")
            .build();

        let scheduler_update_delete_requests = meter
            .f64_histogram("indexify.scheduler_update.delete_requests")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend deleting requests")
            .build();

        let scheduler_update_insert_new_allocations = meter
            .f64_histogram("indexify.scheduler_update.insert_new_allocations")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend inserting new allocations")
            .build();

        let scheduler_update_remove_executors = meter
            .f64_histogram("indexify.scheduler_update.remove_executors")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend removing function executors")
            .build();

        Self {
            function_run_pending_latency,
            allocation_running_latency,
            allocation_completion_latency,
            scheduler_update_delete_requests,
            scheduler_update_insert_new_allocations,
            scheduler_update_remove_executors,
        }
    }
}

#[allow(dead_code)]
pub struct InMemoryStoreGauges {
    pub active_requests: ObservableGauge<u64>,
    pub active_allocations: ObservableGauge<u64>,
    pub active_function_runs: ObservableGauge<u64>,
    pub unallocated_function_runs: ObservableGauge<u64>,
}

impl InMemoryStoreGauges {
    pub fn new(app_state: Arc<ArcSwap<AppState>>) -> Self {
        let meter = opentelemetry::global::meter("state_store");
        let state_clone = app_state.clone();
        let active_requests = meter
            .u64_observable_gauge("indexify.active_requests")
            .with_description("Number of active requests")
            .with_callback(move |observer| {
                let state = state_clone.load();
                observer.observe(state.indexes.request_ctx.len() as u64, &[]);
            })
            .build();
        let state_clone = app_state.clone();
        let active_allocations = meter
            .u64_observable_gauge("indexify.active_allocations")
            .with_description("Number of active allocations")
            .with_callback(move |observer| {
                let state = state_clone.load();
                let total_allocations = state
                    .indexes
                    .allocations_by_executor
                    .iter()
                    .fold(0, |acc, (_, allocations)| acc + allocations.len());
                observer.observe(total_allocations as u64, &[]);
            })
            .build();
        let state_clone = app_state.clone();
        let unallocated_function_runs = meter
            .u64_observable_gauge("indexify.unallocated_function_runs")
            .with_description("Number of unallocated function runs")
            .with_callback(move |observer| {
                let state = state_clone.load();
                observer.observe(state.indexes.unallocated_function_runs.len() as u64, &[]);
            })
            .build();
        let state_clone = app_state.clone();
        let active_function_runs = meter
            .u64_observable_gauge("indexify.active_function_runs")
            .with_description("Number of active function runs")
            .with_callback(move |observer| {
                let state = state_clone.load();
                observer.observe(state.indexes.function_runs.len() as u64, &[]);
            })
            .build();
        Self {
            active_requests,
            active_allocations,
            active_function_runs,
            unallocated_function_runs,
        }
    }
}
