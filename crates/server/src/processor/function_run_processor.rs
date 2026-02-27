use std::collections::{HashMap, HashSet};

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
    scheduler::{
        blocked::BlockingInfo,
        executor_class::ExecutorClass,
        placement::{self, FeasibilityCache, WorkloadKey},
    },
    state_store::{
        in_memory_state::{FunctionRunKey, InMemoryState, ResourceProfile},
        requests::SchedulerUpdateRequest,
    },
};

pub struct FunctionRunProcessor {
    queue_size: u32,
}

impl FunctionRunProcessor {
    pub fn new(queue_size: u32) -> Self {
        Self { queue_size }
    }

    pub fn allocate_request(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        namespace: &str,
        application: &str,
        request_id: &str,
        allocate_function_runs_latency: &Histogram<f64>,
        feas_cache: &mut FeasibilityCache,
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
            feas_cache,
        )
    }

    /// Allocate pending function runs using resource-grouped scheduling.
    ///
    /// Groups runs by `ResourceProfile`, processes groups smallest-first,
    /// and skips entire groups when memory feasibility fails. Within each
    /// group the existing `no_resources_for_fn` skip-set is preserved.
    #[tracing::instrument(skip(
        self,
        in_memory_state,
        container_scheduler,
        function_runs,
        allocate_function_runs_latency,
        feas_cache
    ))]
    pub fn allocate_function_runs(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        function_runs: Vec<FunctionRun>,
        allocate_function_runs_latency: &Histogram<f64>,
        feas_cache: &mut FeasibilityCache,
    ) -> Result<SchedulerUpdateRequest> {
        let _ = Timer::start_with_labels(allocate_function_runs_latency, &[]);
        let mut update = SchedulerUpdateRequest::default();

        // Early exit if no executors are available - no point iterating function runs.
        // Record all runs as escaped-blocked so they get retried when an
        // executor registers.
        if container_scheduler.executors.is_empty() {
            for run in &function_runs {
                let key = FunctionRunKey::from(run);
                let memory_mb = in_memory_state
                    .get_function(run)
                    .map(|f| f.resources.memory_mb)
                    .unwrap_or(0);
                container_scheduler.blocked_work.block_function_run(
                    key,
                    BlockingInfo {
                        eligible_classes: Default::default(),
                        escaped: true,
                        memory_mb,
                    },
                );
            }
            return Ok(update);
        }

        // Cap input at 10,000 function runs
        let capped_runs: Vec<FunctionRun> = function_runs.into_iter().take(10_000).collect();

        // Build FunctionURI → ResourceProfile map (one lookup per unique FunctionURI)
        let mut fn_uri_to_profile: HashMap<FunctionURI, Option<ResourceProfile>> = HashMap::new();
        for run in &capped_runs {
            let fn_uri = FunctionURI::from(run);
            fn_uri_to_profile.entry(fn_uri).or_insert_with_key(|_uri| {
                let app_version = in_memory_state.get_existing_application_version(run)?;
                let function = app_version.functions.get(&run.name)?;
                Some(ResourceProfile::from_function(function))
            });
        }

        // Group runs by ResourceProfile; runs with unknown profile go to fallback
        let mut groups: HashMap<ResourceProfile, Vec<FunctionRun>> = HashMap::new();
        let mut fallback_runs: Vec<FunctionRun> = Vec::new();
        for run in capped_runs {
            let fn_uri = FunctionURI::from(&run);
            match fn_uri_to_profile.get(&fn_uri) {
                Some(Some(profile)) => {
                    groups.entry(profile.clone()).or_default().push(run);
                }
                _ => {
                    fallback_runs.push(run);
                }
            }
        }

        // Sort groups smallest-first by (memory_mb, cpu_ms_per_sec, disk_mb, gpu_count)
        let mut sorted_groups: Vec<(ResourceProfile, Vec<FunctionRun>)> =
            groups.into_iter().collect();
        sorted_groups.sort_by_key(|(p, _)| (p.memory_mb, p.cpu_ms_per_sec, p.disk_mb, p.gpu_count));

        // Track function URIs that have no resources available.
        // If allocation fails for one function run due to no resources,
        // all other runs of the same function will also fail - skip them.
        let mut no_resources_for_fn: HashSet<FunctionURI> = HashSet::new();

        // Cache request contexts across allocations so multiple function runs
        // from the same request share accumulated state. Without this, fn_c's
        // ctx wouldn't include fn_b's Running status when both belong to the
        // same request, causing add_request_state to overwrite fn_b's snapshot.
        let mut request_ctx_cache: HashMap<String, RequestCtx> = HashMap::new();

        // Requests that have been marked terminal (e.g. ConstraintUnsatisfiable).
        // All remaining function runs from these requests are skipped to prevent
        // a successful allocation's add_request_state from overwriting the
        // failure snapshot in updated_request_states.
        let mut terminal_requests: HashSet<String> = HashSet::new();

        // Track the index where we stopped processing so we can record
        // remaining groups as blocked.
        let mut processed_up_to = sorted_groups.len();
        for (idx, (profile, runs)) in sorted_groups.iter().enumerate() {
            let failures_before = no_resources_for_fn.len();

            for function_run in runs {
                self.try_allocate_single_run(
                    in_memory_state,
                    container_scheduler,
                    function_run,
                    profile.memory_mb,
                    feas_cache,
                    &mut update,
                    &mut no_resources_for_fn,
                    &mut request_ctx_cache,
                    &mut terminal_requests,
                )?;
            }

            // If 3+ distinct functions failed within this group, break
            // and record remaining groups as blocked.
            let new_failures = no_resources_for_fn.len() - failures_before;
            if new_failures >= 3 {
                processed_up_to = idx + 1;
                break;
            }
        }

        // Record runs from skipped groups as blocked so they are retried
        // when capacity changes (new executor or freed resources).
        // Compute per-FunctionURI feasibility so only genuinely eligible
        // classes are recorded (avoids spurious retries on unrelated class
        // capacity changes). Cache per FunctionURI to avoid redundant checks.
        let mut fn_uri_eligible: HashMap<FunctionURI, HashSet<ExecutorClass>> = HashMap::new();
        for (profile, runs) in sorted_groups.into_iter().skip(processed_up_to) {
            for run in runs {
                let fn_uri = FunctionURI::from(&run);
                let eligible = fn_uri_eligible
                    .entry(fn_uri)
                    .or_insert_with(|| {
                        let workload_key = WorkloadKey::Function {
                            namespace: run.namespace.clone(),
                            application: run.application.clone(),
                            function_name: run.name.clone(),
                        };
                        let function = in_memory_state
                            .get_existing_application_version(&run)
                            .and_then(|app| app.functions.get(&run.name).cloned());
                        placement::compute_eligible_classes(
                            container_scheduler,
                            feas_cache,
                            &workload_key,
                            &run.namespace,
                            &run.application,
                            function.as_ref(),
                        )
                    })
                    .clone();
                let escaped = eligible.is_empty();
                container_scheduler.blocked_work.block_function_run(
                    FunctionRunKey::from(&run),
                    BlockingInfo {
                        eligible_classes: eligible,
                        escaped,
                        memory_mb: profile.memory_mb,
                    },
                );
            }
        }

        // Process fallback runs (unknown profile) last — preserves existing
        // error-handling behavior for runs whose app version is missing.
        for function_run in fallback_runs {
            self.try_allocate_single_run(
                in_memory_state,
                container_scheduler,
                &function_run,
                0, // fallback runs have unknown profile
                feas_cache,
                &mut update,
                &mut no_resources_for_fn,
                &mut request_ctx_cache,
                &mut terminal_requests,
            )?;
        }

        Ok(update)
    }

    /// Process a single function run: skip checks, allocation, error handling.
    ///
    /// Returns `Ok(())` on success or graceful skip, `Err` for fatal errors.
    /// Mutates `update`, `no_resources_for_fn`, `request_ctx_cache`, and
    /// `terminal_requests` as side effects.
    #[allow(clippy::too_many_arguments)]
    fn try_allocate_single_run(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        function_run: &FunctionRun,
        memory_mb: u64,
        feas_cache: &mut FeasibilityCache,
        update: &mut SchedulerUpdateRequest,
        no_resources_for_fn: &mut HashSet<FunctionURI>,
        request_ctx_cache: &mut HashMap<String, RequestCtx>,
        terminal_requests: &mut HashSet<String>,
    ) -> Result<()> {
        let fn_uri = FunctionURI::from(function_run);
        if no_resources_for_fn.contains(&fn_uri) {
            // Record as blocked so BlockedWorkTracker retries when capacity
            // frees up. Without this, runs silently skipped here are lost
            // forever — never allocated, never retried.
            let _ = container_scheduler.last_placement.take();
            Self::record_blocked_function_run(
                in_memory_state,
                container_scheduler,
                feas_cache,
                function_run,
                memory_mb,
            );
            return Ok(());
        }

        let request_key: RequestCtxKey = function_run.clone().into();
        let cache_key = request_key.0.clone();

        // Skip runs from requests already marked terminal (e.g.
        // ConstraintUnsatisfiable). Without this, a successful
        // allocation's add_request_state would overwrite the failure
        // snapshot in updated_request_states.
        if terminal_requests.contains(&cache_key) {
            return Ok(());
        }

        if !request_ctx_cache.contains_key(&cache_key) {
            let Some(ctx) = in_memory_state
                .request_ctx
                .get(&request_key)
                .map(|boxed| *boxed.clone())
            else {
                return Ok(());
            };
            request_ctx_cache.insert(cache_key.clone(), ctx);
        }
        let ctx = request_ctx_cache.get_mut(&cache_key).unwrap();

        // Clear stale placement so record_blocked_function_run
        // doesn't pick up a previous function run's result.
        let _ = container_scheduler.last_placement.take();

        match self.create_allocation(
            in_memory_state,
            container_scheduler,
            function_run,
            ctx,
            feas_cache,
        ) {
            Ok(allocation_update) => {
                if allocation_update.new_allocations.is_empty() {
                    no_resources_for_fn.insert(fn_uri);
                    Self::record_blocked_function_run(
                        in_memory_state,
                        container_scheduler,
                        feas_cache,
                        function_run,
                        memory_mb,
                    );
                }
                update.extend(allocation_update);
            }
            Err(err) => {
                if let Some(scheduler_error) = err.downcast_ref::<container_scheduler::Error>() {
                    warn!(
                        fn_call_id = %function_run.id,
                        namespace = %function_run.namespace,
                        app = %function_run.application,
                        app_version = scheduler_error.version(),
                        "fn" = scheduler_error.function_name(),
                        error = %scheduler_error,
                        "Unable to allocate task"
                    );

                    // ConstraintUnsatisfiable is a request-level terminal
                    // failure: set outcome on the shared ctx and mark the
                    // request terminal so no subsequent runs overwrite
                    // the failure snapshot.
                    if matches!(
                        scheduler_error,
                        container_scheduler::Error::ConstraintUnsatisfiable { .. }
                    ) {
                        let mut failed_function_run = function_run.clone();
                        failed_function_run.status = FunctionRunStatus::Completed;
                        failed_function_run.outcome = Some(FunctionRunOutcome::Failure(
                            FunctionRunFailureReason::ConstraintUnsatisfiable,
                        ));

                        ctx.outcome = Some(crate::data_model::RequestOutcome::Failure(
                            crate::data_model::RequestFailureReason::ConstraintUnsatisfiable,
                        ));
                        update.add_function_run(failed_function_run, ctx);
                        update.add_request_state(ctx);
                        no_resources_for_fn.insert(fn_uri);
                        terminal_requests.insert(cache_key);
                    }

                    return Ok(());
                }
                return Err(err);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(namespace = %function_run.namespace, fn_call_id = %function_run.id, request_id = %function_run.request_id, app = %function_run.application, fn_name = %function_run.name, app_version = %function_run.version))]
    fn create_allocation(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        function_run: &FunctionRun,
        ctx: &mut RequestCtx,
        feas_cache: &mut FeasibilityCache,
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
        let mut try_create_container = |in_memory_state: &InMemoryState,
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
                &function_run.application,
                &function_run.name,
                &function_run.version,
            );
            let pool_key = ContainerPoolKey::new(&function_run.namespace, &pool_id);

            if let Some(pool) = container_scheduler.get_pool(&pool_key) &&
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
                feas_cache,
            )
        };

        // 2. Try to find an available container
        let mut selected_target =
            find_available_container(container_scheduler, &fn_uri, self.queue_size);

        // 3. If no container available (none exist or all at capacity), try to create
        //    one
        if selected_target.is_none() {
            if let Some(create_update) =
                try_create_container(in_memory_state, container_scheduler, function_run)?
            {
                // Extract target directly from the newly created container — no re-query needed
                selected_target = create_update
                    .containers
                    .values()
                    .find(|c| !matches!(c.desired_state, ContainerState::Terminated { .. }))
                    .map(|c| {
                        AllocationTarget::new(
                            c.executor_id.clone(),
                            c.function_container.id.clone(),
                        )
                    });

                // Update scheduler indices so subsequent capacity checks work
                container_scheduler.apply_container_update(&create_update);
                update.extend(create_update);
            }
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

        // Increment num_allocations directly on container_scheduler so
        // find_available_container sees the updated count for subsequent
        // allocations to the same container. Also add to the update for
        // persistence.
        if let Some(fc) = container_scheduler
            .function_containers
            .get(&allocation.target.container_id)
        {
            let mut updated_fc = *fc.clone();
            updated_fc.allocations.insert(allocation.id.clone());
            updated_fc.idle_since = None; // now busy
            update.containers.insert(
                updated_fc.function_container.id.clone(),
                Box::new(updated_fc.clone()),
            );

            // Write directly to container_scheduler so subsequent
            // find_available_container calls see the allocation count.
            container_scheduler.function_containers.insert(
                updated_fc.function_container.id.clone(),
                Box::new(updated_fc),
            );
        }

        // Remove from blocked work tracker if it was previously blocked
        container_scheduler
            .blocked_work
            .remove_function_run(&FunctionRunKey::from(function_run));

        info!(
            allocation_id = %allocation.id,
            fn_run_id = %function_run.id,
            request_id = %function_run.request_id,
            namespace = %function_run.namespace,
            app = %function_run.application,
            fn = %function_run.name,
            executor_id = %target.executor_id,
            container_id = %target.container_id,
            "created allocation"
        );

        Ok(update)
    }

    /// Record a failed function run placement in the BlockedWorkTracker.
    ///
    /// Reads the PlacementResult from `container_scheduler.last_placement`
    /// (set by create_container) and records the eligible classes so the
    /// tracker can do targeted retry when capacity changes.
    fn record_blocked_function_run(
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        feas_cache: &mut FeasibilityCache,
        function_run: &FunctionRun,
        memory_mb: u64,
    ) {
        let key = FunctionRunKey::from(function_run);
        let (eligible, escaped) = match container_scheduler.last_placement.take() {
            Some(placement) => {
                let escaped = placement.eligible_classes.is_empty();
                (placement.eligible_classes, escaped)
            }
            None => {
                // No placement attempted (e.g., no container creation was
                // needed because all containers were at capacity, or app
                // version not found). Compute per-class feasibility so only
                // genuinely eligible classes are recorded. Fall back to
                // escaped when empty (no executors at all).
                let workload_key = WorkloadKey::Function {
                    namespace: function_run.namespace.clone(),
                    application: function_run.application.clone(),
                    function_name: function_run.name.clone(),
                };
                let function = in_memory_state
                    .get_existing_application_version(function_run)
                    .and_then(|app| app.functions.get(&function_run.name).cloned());
                let eligible = placement::compute_eligible_classes(
                    container_scheduler,
                    feas_cache,
                    &workload_key,
                    &function_run.namespace,
                    &function_run.application,
                    function.as_ref(),
                );
                let escaped = eligible.is_empty();
                (eligible, escaped)
            }
        };

        container_scheduler.blocked_work.block_function_run(
            key,
            BlockingInfo {
                eligible_classes: eligible,
                escaped,
                memory_mb,
            },
        );
    }
}
