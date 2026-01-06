use std::collections::HashSet;

use anyhow::Result;
use opentelemetry::metrics::Histogram;
use tracing::{error, info, warn};

use crate::{
    data_model::{
        AllocationBuilder,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        FunctionURI,
        RequestCtx,
        RequestCtxKey,
        RunningFunctionRunStatus,
    },
    metrics::Timer,
    processor::function_executor_manager::FunctionExecutorManager,
    state_store::{
        self,
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct FunctionRunProcessor<'a> {
    clock: u64,
    fe_manager: &'a FunctionExecutorManager,
}

impl<'a> FunctionRunProcessor<'a> {
    pub fn new(clock: u64, fe_manager: &'a FunctionExecutorManager) -> Self {
        Self { clock, fe_manager }
    }

    pub fn allocate_request(
        &self,
        in_memory_state: &mut InMemoryState,
        namespace: &str,
        application: &str,
        request_id: &str,
        allocate_function_runs_latency: &Histogram<f64>,
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
        self.allocate_function_runs(
            in_memory_state,
            function_runs,
            allocate_function_runs_latency,
        )
    }

    #[tracing::instrument(skip(self, in_memory_state, function_runs))]
    pub fn allocate_function_runs(
        &self,
        in_memory_state: &mut InMemoryState,
        function_runs: Vec<FunctionRun>,
        allocate_function_runs_latency: &Histogram<f64>,
    ) -> Result<SchedulerUpdateRequest> {
        let _ = Timer::start_with_labels(allocate_function_runs_latency, &[]);
        let mut update = SchedulerUpdateRequest::default();

        // Early exit if no executors are available - no point iterating function runs
        if in_memory_state.executors.is_empty() {
            return Ok(update);
        }

        // Track function URIs that have no resources available.
        // If allocation fails for one function run due to no resources,
        // all other runs of the same function will also fail - skip them.
        let mut no_resources_for_fn: HashSet<FunctionURI> = HashSet::new();

        for function_run in function_runs {
            // Skip if we already know there are no resources for this function
            let fn_uri = FunctionURI::from(&function_run);
            if no_resources_for_fn.contains(&fn_uri) {
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
                    // If no allocation was created, mark this function as having no resources
                    if allocation_update.new_allocations.is_empty() {
                        no_resources_for_fn.insert(fn_uri);
                    }
                    update.extend(allocation_update);
                }
                Err(err) => {
                    // Check if this is a state store error we can handle gracefully
                    if let Some(state_store_error) =
                        err.downcast_ref::<state_store::in_memory_state::Error>()
                    {
                        warn!(
                            fn_call_id = %function_run.id,
                            namespace = %function_run.namespace,
                            app = %function_run.application,
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
                            // Mark as unsatisfiable so we skip other runs of same function
                            no_resources_for_fn.insert(fn_uri);
                        }

                        continue;
                    }
                    // For any other error, return it
                    return Err(err);
                }
            }
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
            .call_metadata(function_run.compute_op.call_metadata().clone())
            .input_names(function_run.compute_op.input_names().cloned())
            .input_args(function_run.input_args.clone())
            .target(target)
            .attempt_number(function_run.attempt_number)
            .outcome(FunctionRunOutcome::Unknown)
            .build()?;

        info!(
            allocation_id = %allocation.id,
            fn_run_id = %function_run.id,
            request_id = %function_run.request_id,
            namespace = %function_run.namespace,
            app = %function_run.application,
            fn = %function_run.name,
            "created allocation",
        );

        let mut updated_function_run = function_run.clone();
        updated_function_run.status = FunctionRunStatus::Running(RunningFunctionRunStatus {
            allocation_id: allocation.id.clone(),
        });

        update.add_function_run(updated_function_run.clone(), ctx);
        update.new_allocations.push(allocation);
        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "task_allocator",
        )?;
        Ok(update)
    }
}
