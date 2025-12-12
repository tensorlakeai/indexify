use opentelemetry::metrics::Histogram;

use crate::metrics::low_latency_boundaries;

#[derive(Clone)]
pub struct InMemoryStoreMetrics {
    pub function_run_pending_latency: Histogram<f64>,
    pub allocation_running_latency: Histogram<f64>,
    pub allocation_completion_latency: Histogram<f64>,
    pub scheduler_update_push_allocation_to_executors: Histogram<f64>,
    pub scheduler_update_index_function_run_by_catalog: Histogram<f64>,
    pub scheduler_update_delete_requests: Histogram<f64>,
    pub scheduler_update_insert_function_executors: Histogram<f64>,
    pub scheduler_update_insert_new_allocations: Histogram<f64>,
    pub scheduler_update_remove_function_executors: Histogram<f64>,
    pub scheduler_update_remove_executors: Histogram<f64>,
    pub scheduler_update_free_executor_resources: Histogram<f64>,
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

        let scheduler_update_push_allocation_to_executors = meter
            .f64_histogram("indexify.scheduler_update.push_allocation_to_executors")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend pushing an allocation to executors")
            .build();

        let scheduler_update_index_function_run_by_catalog = meter
            .f64_histogram("indexify.scheduler_update.index_function_run_by_catalog")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend indexing function runs by catalog")
            .build();

        let scheduler_update_delete_requests = meter
            .f64_histogram("indexify.scheduler_update.delete_requests")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend deleting requests")
            .build();

        let scheduler_update_insert_function_executors = meter
            .f64_histogram("indexify.scheduler_update.insert_function_executors")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend inserting function executors")
            .build();

        let scheduler_update_insert_new_allocations = meter
            .f64_histogram("indexify.scheduler_update.insert_new_allocations")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend inserting new allocations")
            .build();

        let scheduler_update_remove_function_executors = meter
            .f64_histogram("indexify.scheduler_update.remove_function_executors")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend updating function executors")
            .build();

        let scheduler_update_remove_executors = meter
            .f64_histogram("indexify.scheduler_update.remove_executors")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend removing function executors")
            .build();

        let scheduler_update_free_executor_resources = meter
            .f64_histogram("indexify.scheduler_update.free_executor_resources")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend freeing executor resources")
            .build();

        Self {
            function_run_pending_latency,
            allocation_running_latency,
            allocation_completion_latency,
            scheduler_update_push_allocation_to_executors,
            scheduler_update_index_function_run_by_catalog,
            scheduler_update_delete_requests,
            scheduler_update_insert_function_executors,
            scheduler_update_insert_new_allocations,
            scheduler_update_remove_function_executors,
            scheduler_update_remove_executors,
            scheduler_update_free_executor_resources,
        }
    }
}
