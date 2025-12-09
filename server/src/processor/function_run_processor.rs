use anyhow::Result;
use opentelemetry::metrics::Histogram;
use tracing::{debug, error, warn};

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
        filter::Operator,
    },
    metrics::{Timer, low_latency_boundaries},
    processor::function_executor_manager::FunctionExecutorManager,
    state_store::{
        self,
        blocked_runs::FunctionRunRequirements,
        in_memory_state::{FunctionRunKey, InMemoryState},
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

    /// Allocate function runs. For new runs, checks the blocked index first to
    /// skip allocation attempts for functions that already have blocked
    /// runs.
    ///
    /// `is_retry`: Set to true when retrying runs from the blocked index. This
    /// skips the blocked index check since those runs ARE in the blocked
    /// index.
    pub fn allocate_function_runs(
        &self,
        in_memory_state: &mut InMemoryState,
        function_runs: Vec<FunctionRun>,
    ) -> Result<SchedulerUpdateRequest> {
        self.allocate_function_runs_inner(in_memory_state, function_runs, false)
    }

    /// Retry allocation for runs from the blocked index.
    /// Skips the blocked index check since these runs are already blocked.
    pub fn retry_blocked_runs(
        &self,
        in_memory_state: &mut InMemoryState,
        function_runs: Vec<FunctionRun>,
    ) -> Result<SchedulerUpdateRequest> {
        self.allocate_function_runs_inner(in_memory_state, function_runs, true)
    }

    fn allocate_function_runs_inner(
        &self,
        in_memory_state: &mut InMemoryState,
        function_runs: Vec<FunctionRun>,
        is_retry: bool,
    ) -> Result<SchedulerUpdateRequest> {
        let _timer = Timer::start_with_labels(&self.metrics.allocate_function_runs_duration, &[]);
        let mut update = SchedulerUpdateRequest::default();

        // Track functions that have no FE capacity to avoid repeated allocation
        // attempts within this batch. This is a local optimization - we use
        // FunctionURI as key.
        let mut no_capacity_functions: std::collections::HashSet<FunctionURI> =
            std::collections::HashSet::new();

        for function_run in function_runs {
            let fn_uri = FunctionURI::from(&function_run);

            // Skip if we already know this function has no capacity in this batch
            if no_capacity_functions.contains(&fn_uri) {
                let run_key = FunctionRunKey::from(&function_run);
                let requirements = self.get_run_requirements(in_memory_state, &function_run);
                update
                    .blocked_runs_to_add
                    .push((run_key, fn_uri, requirements));
                continue;
            }

            // For new runs (not retries), check if function already has blocked runs.
            // This avoids expensive allocation attempts when capacity is exhausted.
            if !is_retry &&
                in_memory_state
                    .blocked_runs_index
                    .has_blocked_for_function(&fn_uri)
            {
                no_capacity_functions.insert(fn_uri.clone());
                let run_key = FunctionRunKey::from(&function_run);
                let requirements = self.get_run_requirements(in_memory_state, &function_run);
                update
                    .blocked_runs_to_add
                    .push((run_key, fn_uri, requirements));
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
                        // No allocation was created - block and track locally
                        no_capacity_functions.insert(fn_uri.clone());
                        let run_key = FunctionRunKey::from(&function_run);
                        let requirements =
                            self.get_run_requirements(in_memory_state, &function_run);
                        update
                            .blocked_runs_to_add
                            .push((run_key, fn_uri, requirements));
                    } else {
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
                            "Unable to allocate task - adding to blocked"
                        );

                        // For ConstraintUnsatisfiable errors, we still fail the function run
                        if matches!(
                            state_store_error,
                            state_store::in_memory_state::Error::ConstraintUnsatisfiable { .. }
                        ) {
                            let mut failed_function_run = function_run.clone();
                            failed_function_run.status = FunctionRunStatus::Completed;
                            failed_function_run.outcome = Some(FunctionRunOutcome::Failure(
                                FunctionRunFailureReason::ConstraintUnsatisfiable,
                            ));
                            ctx.outcome = Some(crate::data_model::RequestOutcome::Failure(
                                crate::data_model::RequestFailureReason::ConstraintUnsatisfiable,
                            ));
                            update.add_function_run(failed_function_run.clone(), &mut ctx);
                        } else {
                            // Other errors - block and track locally
                            no_capacity_functions.insert(fn_uri.clone());
                            let run_key = FunctionRunKey::from(&function_run);
                            let requirements =
                                self.get_run_requirements(in_memory_state, &function_run);
                            update
                                .blocked_runs_to_add
                                .push((run_key, fn_uri, requirements));
                        }
                        continue;
                    }
                    // For any other error, return it
                    return Err(err);
                }
            }
        }

        // Update in-memory state with blocked runs so subsequent allocations see them
        if !update.blocked_runs_to_add.is_empty() {
            let blocked_runs_update = SchedulerUpdateRequest {
                blocked_runs_to_add: update.blocked_runs_to_add.clone(),
                ..Default::default()
            };
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(blocked_runs_update), vec![])),
                "task_allocator",
            )?;
        }

        // Mark allocated runs for removal from blocked index
        for allocation in update.new_allocations.values() {
            let run_key = FunctionRunKey::from(allocation);
            update.blocked_runs_to_remove.push(run_key);
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

        // Ensure a function executor exists (creates one if needed)
        let fe_update = self
            .fe_manager
            .ensure_function_executor(in_memory_state, function_run)?;
        update.extend(fe_update);

        // Select the best available function executor
        let Some(target) = self
            .fe_manager
            .select_function_executor(in_memory_state, function_run)?
        else {
            debug!(
                namespace = function_run.namespace,
                app = function_run.application,
                fn_name = function_run.name,
                request_id = function_run.request_id,
                fn_call_id = function_run.id.to_string(),
                "no FE selected for function run - will be blocked"
            );
            return Ok(update);
        };

        debug!(
            namespace = function_run.namespace,
            app = function_run.application,
            fn_name = function_run.name,
            executor_id = target.executor_id.get(),
            fe_id = target.function_executor_id.get(),
            "FE selected, creating allocation"
        );

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

    /// Extract requirements from a function run.
    /// Uses the function definition from the application version to get
    /// resource requirements.
    pub fn get_run_requirements(
        &self,
        in_memory_state: &InMemoryState,
        function_run: &FunctionRun,
    ) -> FunctionRunRequirements {
        let app_version = in_memory_state.get_existing_application_version(function_run);
        let function_def = app_version.and_then(|v| v.functions.get(&function_run.name));

        // Extract region from placement constraints (look for region==X)
        let region = function_def.and_then(|f| {
            f.placement_constraints.0.iter().find_map(|expr| {
                if expr.key == "region" && expr.operator == Operator::Eq {
                    Some(expr.value.clone())
                } else {
                    None
                }
            })
        });

        // Extract GPU info from function resources
        let (gpu_count, gpu_model) = function_def
            .and_then(|f| f.resources.gpu_configs.first())
            .map(|g| (g.count, Some(g.model.clone())))
            .unwrap_or((0, None));

        FunctionRunRequirements {
            cpu_ms: function_def
                .map(|f| f.resources.cpu_ms_per_sec)
                .unwrap_or(1000),
            memory_bytes: function_def
                .map(|f| f.resources.memory_mb * 1024 * 1024)
                .unwrap_or(256_000_000),
            disk_bytes: function_def
                .map(|f| f.resources.ephemeral_disk_mb * 1024 * 1024)
                .unwrap_or(0),
            gpu_count,
            gpu_model,
            region,
        }
    }
}
