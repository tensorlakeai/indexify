use std::collections::HashSet;

use anyhow::Result;
use opentelemetry::metrics::Histogram;
use tracing::{error, info, warn};

use crate::{
    data_model::{
        AllocationBuilder,
        AllocationTarget,
        ContainerPoolId,
        ContainerPoolKey,
        ContainerState,
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
    processor::container_scheduler::{self, ContainerScheduler},
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdatePayload, SchedulerUpdateRequest},
    },
};

pub struct FunctionRunProcessor {
    clock: u64,
    queue_size: u32,
}

impl FunctionRunProcessor {
    pub fn new(clock: u64, queue_size: u32) -> Self {
        Self { clock, queue_size }
    }

    pub fn allocate_request(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
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
            container_scheduler,
            function_runs,
            allocate_function_runs_latency,
        )
    }

    #[tracing::instrument(skip(self, in_memory_state, container_scheduler, function_runs))]
    pub fn allocate_function_runs(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        function_runs: Vec<FunctionRun>,
        allocate_function_runs_latency: &Histogram<f64>,
    ) -> Result<SchedulerUpdateRequest> {
        let _ = Timer::start_with_labels(allocate_function_runs_latency, &[]);
        let mut update = SchedulerUpdateRequest::default();

        // Early exit if no executors are available - no point iterating function runs
        if container_scheduler.executors.is_empty() {
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

            match self.create_allocation(
                in_memory_state,
                container_scheduler,
                &function_run,
                &mut ctx,
            ) {
                Ok(allocation_update) => {
                    // If no allocation was created, mark this function as having no resources
                    if allocation_update.new_allocations.is_empty() {
                        no_resources_for_fn.insert(fn_uri);
                    }
                    update.extend(allocation_update);
                }
                Err(err) => {
                    // Check if this is a container scheduler error we can handle gracefully
                    if let Some(scheduler_error) = err.downcast_ref::<container_scheduler::Error>()
                    {
                        warn!(
                            fn_call_id = %function_run.id,
                            namespace = %function_run.namespace,
                            app = %function_run.application,
                            app_version = scheduler_error.version(),
                            "fn" = scheduler_error.function_name(),
                            error = %scheduler_error,
                            "Unable to allocate task"
                        );

                        // Check if this is a ConstraintUnsatisfiable error; if it is, we want to
                        // fail the function run and request.
                        //
                        // TODO: Turn this into a check at server startup.
                        if matches!(
                            scheduler_error,
                            container_scheduler::Error::ConstraintUnsatisfiable { .. }
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
                            update.add_request_state(&ctx);
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
        container_scheduler: &mut ContainerScheduler,
        function_run: &FunctionRun,
        ctx: &mut RequestCtx,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // 1. Build FunctionURI from function_run
        let fn_uri = FunctionURI::from(function_run);

        // Helper: find best container below capacity
        let find_available_container = |container_scheduler: &ContainerScheduler,
                                        fn_uri: &FunctionURI,
                                        queue_size: u32|
         -> Option<AllocationTarget> {
            container_scheduler
                .containers_by_function_uri
                .get(fn_uri)
                .and_then(|ids| {
                    ids.iter()
                        .filter_map(|id| container_scheduler.function_containers.get(id))
                        .filter(|c| {
                            if let Some(executor) =
                                container_scheduler.executors.get(&c.executor_id)
                            {
                                if executor.tombstoned {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                            // Filter out terminated containers
                            if matches!(c.desired_state, ContainerState::Terminated { .. }) {
                                return false;
                            }
                            // Filter out containers at or above capacity
                            // Capacity = queue_size * max_concurrency
                            let capacity = queue_size * c.function_container.max_concurrency;
                            c.allocations.len() < capacity as usize
                        })
                        .min_by_key(|c| c.allocations.len())
                        .map(|c| {
                            AllocationTarget::new(
                                c.executor_id.clone(),
                                c.function_container.id.clone(),
                            )
                        })
                })
        };

        // Helper: try to create a new container (respects max_containers limit)
        let try_create_container = |in_memory_state: &InMemoryState,
                                    container_scheduler: &mut ContainerScheduler,
                                    function_run: &FunctionRun|
         -> Result<Option<SchedulerUpdateRequest>> {
            let Some(app_version) = in_memory_state.get_existing_application_version(function_run)
            else {
                return Ok(None);
            };
            let Some(function) = app_version.functions.get(&function_run.name) else {
                return Ok(None);
            };

            // Check max_containers limit from the pool
            let pool_id = ContainerPoolId::for_function(
                &function_run.namespace,
                &function_run.application,
                &function_run.name,
                &function_run.version,
            );
            let pool_key = ContainerPoolKey::new(&function_run.namespace, &pool_id);

            if let Some(pool) = container_scheduler.container_pools.get(&pool_key) &&
                let Some(max) = pool.max_containers
            {
                let fn_uri = FunctionURI {
                    namespace: function_run.namespace.clone(),
                    application: function_run.application.clone(),
                    function: function_run.name.clone(),
                    version: function_run.version.clone(),
                };
                let (active, idle) = container_scheduler.count_active_idle_containers(&fn_uri);
                let current = active + idle;

                if current >= max {
                    warn!(
                        namespace = %function_run.namespace,
                        application = %function_run.application,
                        function = %function_run.name,
                        current = current,
                        max = max,
                        "Function pool at capacity, cannot create container"
                    );
                    return Ok(None);
                }
            }

            container_scheduler.create_container_for_function(
                &function_run.namespace,
                &function_run.application,
                &function_run.version,
                function,
                &app_version.state,
                true, // Critical: Task allocation
            )
        };

        // 2. Try to find an available container
        let mut selected_target =
            find_available_container(container_scheduler, &fn_uri, self.queue_size);

        // 3. If no container available (none exist or all at capacity), try to create
        //    one
        if selected_target.is_none() &&
            let Some(create_update) =
                try_create_container(in_memory_state, container_scheduler, function_run)?
        {
            let payload =
                RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(create_update.clone()));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(self.clock, &payload, "function_run_processor")?;
            update.extend(create_update);

            // Try again to find an available container
            selected_target =
                find_available_container(container_scheduler, &fn_uri, self.queue_size);
        }

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
            .target(target.clone())
            .attempt_number(function_run.attempt_number)
            .outcome(FunctionRunOutcome::Unknown)
            .build()?;

        let mut updated_function_run = function_run.clone();
        updated_function_run.status = FunctionRunStatus::Running(RunningFunctionRunStatus {
            allocation_id: allocation.id.clone(),
        });

        update.add_function_run(updated_function_run.clone(), ctx);
        update.add_request_state(ctx);
        update.new_allocations.push(allocation.clone());

        // Increment num_allocations via updated_function_containers so
        // container_scheduler sees the updated count BEFORE any subsequent
        // vacuum or allocation logic runs.
        if let Some(fc) = container_scheduler
            .function_containers
            .get(&allocation.target.function_executor_id)
        {
            let mut updated_fc = *fc.clone();
            updated_fc.allocations.insert(allocation.id.clone());
            update.containers.insert(
                updated_fc.function_container.id.clone(),
                Box::new(updated_fc.clone()),
            );
        }

        info!(
            allocation_id = %allocation.id,
            fn_run_id = %function_run.id,
            request_id = %function_run.request_id,
            namespace = %function_run.namespace,
            app = %function_run.application,
            fn = %function_run.name,
            executor_id = %target.executor_id,
            container_id = %target.function_executor_id,
            "created allocation"
        );

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update.clone())),
            "task_allocator",
        )?;
        container_scheduler.update(&RequestPayload::SchedulerUpdate(
            SchedulerUpdatePayload::new(update.clone()),
        ))?;
        Ok(update)
    }
}
