use anyhow::Result;
use opentelemetry::{KeyValue, metrics::Histogram};
use tracing::{debug, error, warn};

use crate::{
    data_model::{
        AllocationBuilder,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        RequestCtx,
        RequestCtxKey,
        RunningFunctionRunStatus,
    },
    metrics::low_latency_boundaries,
    processor::function_executor_manager::FunctionExecutorManager,
    state_store::{
        self,
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

/// Metrics for function run allocation
pub struct SchedulerMetrics {
    pub allocate_function_runs_duration: Histogram<f64>,
}

impl SchedulerMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("scheduler");
        Self {
            allocate_function_runs_duration: meter
                .f64_histogram("indexify.scheduler.allocate_function_runs_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("Time to allocate function runs in seconds")
                .build(),
        }
    }
}

impl Default for SchedulerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FunctionRunProcessor<'a> {
    clock: u64,
    fe_manager: &'a FunctionExecutorManager,
    metrics: SchedulerMetrics,
}

impl<'a> FunctionRunProcessor<'a> {
    pub fn new(clock: u64, fe_manager: &'a FunctionExecutorManager) -> Self {
        Self {
            clock,
            fe_manager,
            metrics: SchedulerMetrics::new(),
        }
    }

    pub fn allocate_request(
        &self,
        in_memory_state: &mut InMemoryState,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> Result<SchedulerUpdateRequest> {
        let request_key = RequestCtxKey::new(namespace, application, request_id);
        let Some(request_ctx) = in_memory_state.request_ctx.get(&request_key) else {
            error!("request context not found for request_id: {}", request_id);
            return Ok(SchedulerUpdateRequest::default());
        };
        let function_runs: Vec<FunctionRun> = request_ctx
            .function_runs
            .values()
            .filter(|fr| matches!(fr.status, FunctionRunStatus::Pending))
            .cloned()
            .collect();
        self.allocate_function_runs(in_memory_state, function_runs)
    }

    #[tracing::instrument(skip(self, in_memory_state, function_runs))]
    pub fn allocate_function_runs(
        &self,
        in_memory_state: &mut InMemoryState,
        function_runs: Vec<FunctionRun>,
    ) -> Result<SchedulerUpdateRequest> {
        let start = std::time::Instant::now();
        let num_function_runs = function_runs.len();
        let mut update = SchedulerUpdateRequest::default();
        let mut allocated_count = 0u32;
        let mut skipped_no_capacity = 0u32;
        // Track functions that have no FE capacity to avoid repeated checks
        let mut no_capacity_functions: std::collections::HashSet<(String, String, String, String)> =
            std::collections::HashSet::new();

        for function_run in function_runs {
            // Skip if we already know this function has no capacity
            let fn_key = (
                function_run.namespace.clone(),
                function_run.application.clone(),
                function_run.name.clone(),
                function_run.version.to_string(),
            );
            if no_capacity_functions.contains(&fn_key) {
                skipped_no_capacity += 1;
                continue;
            }

            let Some(mut ctx) = in_memory_state
                .request_ctx
                .get(&function_run.clone().into())
                .cloned()
            else {
                continue;
            };

            match self.create_allocation(in_memory_state, &function_run, &mut ctx) {
                Ok(allocation_update) => {
                    if allocation_update.new_allocations.is_empty() {
                        // No allocation was created - FE capacity exhausted for this function
                        no_capacity_functions.insert(fn_key);
                    } else {
                        allocated_count += 1;
                        // Capacity changed (new FE may have been created) - retry skipped functions
                        no_capacity_functions.clear();
                    }
                    update.extend(allocation_update);
                }
                Err(err) => {
                    // Check if this is a state store error we can handle gracefully
                    if let Some(state_store_error) =
                        err.downcast_ref::<state_store::in_memory_state::Error>()
                    {
                        warn!(
                            fn_call_id = function_run.id.to_string(),
                            namespace = function_run.namespace,
                            app = function_run.application,
                            app_version = state_store_error.version(),
                            "fn" = state_store_error.function_name(),
                            error = %state_store_error,
                            "Unable to allocate task"
                        );

                        // Check if this is a ConstraintUnsatisfiable error; if it is, we want to
                        // fail the function run and request.
                        //
                        // TODO: Turn this into a check at server startup.
                        if matches!(
                            state_store_error,
                            state_store::in_memory_state::Error::ConstraintUnsatisfiable { .. }
                        ) {
                            // Fail the task
                            let mut failed_function_run = function_run.clone();
                            failed_function_run.status = FunctionRunStatus::Completed;
                            failed_function_run.outcome = Some(FunctionRunOutcome::Failure(
                                FunctionRunFailureReason::ConstraintUnsatisfiable,
                            ));

                            // Add the failed function run to the update
                            ctx.outcome = Some(crate::data_model::RequestOutcome::Failure(
                                crate::data_model::RequestFailureReason::ConstraintUnsatisfiable,
                            ));
                            update.add_function_run(failed_function_run.clone(), &mut ctx);
                        }

                        continue;
                    }
                    // For any other error, return it
                    return Err(err);
                }
            }
        }

        if num_function_runs > 0 {
            self.metrics.allocate_function_runs_duration.record(
                start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("num_function_runs", num_function_runs as i64),
                    KeyValue::new("allocated_count", allocated_count as i64),
                    KeyValue::new("skipped_no_capacity", skipped_no_capacity as i64),
                ],
            );
        }

        Ok(update)
    }

    #[tracing::instrument(skip_all, fields(namespace = %function_run.namespace, fn_call_id = %function_run.id, request_id = %function_run.request_id, app = %function_run.application, fn_name = %function_run.name, app_version = %function_run.version))]
    fn create_allocation(
        &self,
        in_memory_state: &mut InMemoryState,
        function_run: &FunctionRun,
        ctx: &mut RequestCtx,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Use FunctionExecutorManager to handle function executor selection/creation
        let (selected_target, fe_update) = self
            .fe_manager
            .select_or_create_function_executor(in_memory_state, function_run)?;
        update.extend(fe_update);

        let Some(target) = selected_target else {
            return Ok(update);
        };
        let allocation = AllocationBuilder::default()
            .namespace(function_run.namespace.clone())
            .application(function_run.application.clone())
            .application_version(ctx.application_version.clone())
            .function(function_run.name.clone())
            .request_id(function_run.request_id.clone())
            .function_call_id(function_run.id.clone())
            .call_metadata(function_run.call_metadata.clone())
            .input_args(function_run.input_args.clone())
            .target(target)
            .attempt_number(function_run.attempt_number)
            .outcome(FunctionRunOutcome::Unknown)
            .build()?;

        debug!(allocation_id = %allocation.id, "created allocation");

        let mut updated_function_run = function_run.clone();
        updated_function_run.status = FunctionRunStatus::Running(RunningFunctionRunStatus {
            allocation_id: allocation.id.clone(),
        });

        update.add_function_run(updated_function_run.clone(), ctx);
        update
            .new_allocations
            .insert(allocation.id.clone(), allocation);
        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "task_allocator",
        )?;
        Ok(update)
    }
}
