use anyhow::{Result, anyhow};
use opentelemetry::{KeyValue, metrics::Histogram};
use rand::seq::IndexedRandom;
use tracing::{debug, error, info, warn};

use crate::{
    data_model::{
        AllocationTarget,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionExecutor,
        FunctionExecutorBuilder,
        FunctionExecutorServerMetadata,
        FunctionExecutorState,
        FunctionExecutorTerminationReason,
        FunctionResources,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        FunctionURI,
        RunningFunctionRunStatus,
    },
    metrics::{low_latency_boundaries, Timer},
    processor::retry_policy::FunctionRunRetryPolicy,
    state_store::{
        in_memory_state::{FunctionRunKey, InMemoryState},
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

/// Metrics for function executor management
pub struct FunctionExecutorMetrics {
    pub select_or_create_fe_duration: Histogram<f64>,
    pub create_fe_duration: Histogram<f64>,
}

impl FunctionExecutorMetrics {
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("scheduler");
        Self {
            select_or_create_fe_duration: meter
                .f64_histogram("indexify.scheduler.select_or_create_fe_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("Time to select or create a function executor in seconds")
                .build(),
            create_fe_duration: meter
                .f64_histogram("indexify.scheduler.create_fe_duration")
                .with_unit("s")
                .with_boundaries(low_latency_boundaries())
                .with_description("Time to create a function executor in seconds")
                .build(),
        }
    }
}

impl Default for FunctionExecutorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct FunctionExecutorManager {
    clock: u64,
    queue_size: u32,
    metrics: FunctionExecutorMetrics,
}

/// Implements the policy around function executors: when to create
/// them, when to terminate them, and the selection of function
/// executors appropriate to a given function run.
impl FunctionExecutorManager {
    pub fn new(clock: u64, queue_size: u32) -> Self {
        Self {
            clock,
            queue_size,
            metrics: FunctionExecutorMetrics::new(),
        }
    }

    /// Vacuum phase - identifies function executors from OTHER functions that should be terminated
    /// to free up resources. Only vacuums FEs from different functions than the requesting one.
    /// Returns scheduler update for cleanup actions.
    #[tracing::instrument(skip_all, target = "scheduler")]
    fn vacuum(
        &self,
        in_memory_state: &mut InMemoryState,
        fe_resource: &FunctionResources,
        requesting_fn_uri: &FunctionURI,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark =
            in_memory_state.vacuum_function_executors_candidates(fe_resource, requesting_fn_uri)?;

        debug!(
            "vacuum phase identified {} function executors from other functions to mark for termination",
            function_executors_to_mark.len(),
        );

        // Mark FEs for termination (change desired state to Terminated)
        // but don't actually remove them - reconciliation will handle that
        for fe in &function_executors_to_mark {
            let mut update_fe = fe.clone();
            update_fe.desired_state = FunctionExecutorState::Terminated {
                reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                failed_alloc_ids: Vec::new(),
            };
            update
                .new_function_executors
                .insert(update_fe.function_executor.id.clone(), update_fe);

            info!(
                app = fe.function_executor.application_name,
                fn = fe.function_executor.function_name,
                namespace = fe.function_executor.namespace,
                fn_executor_id = fe.function_executor.id.get(),
                executor_id = fe.executor_id.get(),
                "marked function executor for termination (vacuuming for different function)",
            );
        }
        Ok(update)
    }

    /// Creates a new function executor for the given function run
    #[tracing::instrument(skip_all, target = "scheduler", fields(namespace = %fn_run.namespace, request_id = %fn_run.request_id, fn_call_id = %fn_run.id, app = %fn_run.application, fn_name = %fn_run.name, app_version = %fn_run.version))]
    fn create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        fn_run: &FunctionRun,
    ) -> Result<SchedulerUpdateRequest> {
        let create_start = std::time::Instant::now();
        let mut update = SchedulerUpdateRequest::default();
        let mut candidates = in_memory_state.candidate_executors(fn_run)?;
        let mut vacuum_triggered = false;

        // If no executors have space, try to vacuum FEs from OTHER functions
        // (not the same function - those should be reused, not recreated)
        if candidates.is_empty() {
            debug!("no executors have capacity, trying to vacuum FEs from other functions");
            vacuum_triggered = true;
            let fe_resource = in_memory_state.fe_resource_for_function_run(fn_run)?;
            let requesting_fn_uri = FunctionURI::from(fn_run);
            let vacuum_update = self.vacuum(in_memory_state, &fe_resource, &requesting_fn_uri)?;
            update.extend(vacuum_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
                "function_executor_manager",
            )?;
            candidates = in_memory_state.candidate_executors(fn_run)?;
        }

        // If still no candidates after vacuum, return (function run will be blocked)
        if candidates.is_empty() {
            debug!(
                namespace = fn_run.namespace,
                app = fn_run.application,
                fn_name = fn_run.name,
                fn_call_id = fn_run.id.to_string(),
                "no executor capacity for new FE, function run will be blocked"
            );
            self.metrics.create_fe_duration.record(
                create_start.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("num_candidates", 0i64),
                    KeyValue::new("vacuum_triggered", if vacuum_triggered { 1i64 } else { 0i64 }),
                ],
            );
            return Ok(update);
        }

        debug!(
            "found {} candidates for creating function executor",
            candidates.len()
        );
        self.metrics.create_fe_duration.record(
            create_start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("num_candidates", candidates.len() as i64),
                KeyValue::new("vacuum_triggered", if vacuum_triggered { 1i64 } else { 0i64 }),
            ],
        );

        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            return Ok(update);
        };
        let executor_id = candidate.executor_id.clone();
        let node_resources = in_memory_state
            .get_fe_resources_by_uri(
                &fn_run.namespace,
                &fn_run.application,
                &fn_run.name,
                &fn_run.version,
            )
            .ok_or(anyhow!("failed to get function executor resources"))?;

        let fe_resources = candidate
            .free_resources
            .consume_function_resources(&node_resources)?;
        // Consume resources from executor
        update
            .updated_executor_resources
            .insert(executor_id.clone(), candidate.free_resources.clone());

        let node_max_concurrency = in_memory_state
            .get_fe_max_concurrency_by_uri(
                &fn_run.namespace,
                &fn_run.application,
                &fn_run.name,
                &fn_run.version,
            )
            .ok_or(anyhow!("failed to get function executor max concurrency"))?;

        // Create a new function executor
        let function_executor = FunctionExecutorBuilder::default()
            .namespace(fn_run.namespace.clone())
            .application_name(fn_run.application.clone())
            .function_name(fn_run.name.clone())
            .version(fn_run.version.clone())
            .state(FunctionExecutorState::Unknown)
            .resources(fe_resources.clone())
            .max_concurrency(node_max_concurrency)
            .build()?;

        info!(
            executor_id = executor_id.get(),
            fn_executor_id = function_executor.id.get(),
            "created function executor"
        );

        // Create with current timestamp for last_allocation_at
        let fe_server_metadata = FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor.clone(),
            FunctionExecutorState::Running, // Start with Running state
        );
        update
            .new_function_executors
            .insert(function_executor.id.clone(), fe_server_metadata);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "function_executor_manager",
        )?;
        Ok(update)
    }

    /// Reconciles function executor state between executor and server
    #[tracing::instrument(skip(self, in_memory_state, executor))]
    fn reconcile_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        // Get the function executors from the indexes
        let mut executor_server_metadata = in_memory_state
            .executor_states
            .get(&executor.id)
            .unwrap()
            .clone();
        let server_function_executors = executor_server_metadata.function_executors.clone();
        let mut function_executors_to_remove = Vec::new();
        let mut new_function_executors = std::collections::HashMap::new();

        let fes_exist_only_in_executor = executor
            .function_executors
            .iter()
            .filter(|(fe_id, _fe)| !server_function_executors.contains_key(fe_id))
            .map(|(_fe_id, fe)| fe.clone())
            .collect::<Vec<_>>();
        let fes_exist_only_in_server = server_function_executors
            .iter()
            .filter(|(fe_id, _fe)| !executor.function_executors.contains_key(fe_id))
            .map(|(_fe_id, fe)| fe.clone())
            .collect::<Vec<_>>();
        for fe in fes_exist_only_in_server {
            if matches!(fe.desired_state, FunctionExecutorState::Terminated { .. }) {
                function_executors_to_remove.push(fe.function_executor.clone());
            }
        }
        for fe in fes_exist_only_in_executor {
            if !matches!(fe.state, FunctionExecutorState::Terminated { .. }) &&
                executor_server_metadata
                    .free_resources
                    .can_handle_fe_resources(&fe.resources)
                    .is_ok()
            {
                let fe_meta =
                    FunctionExecutorServerMetadata::new(executor.id.clone(), fe.clone(), fe.state);
                new_function_executors.insert(fe.id.clone(), fe_meta);
                executor_server_metadata
                    .free_resources
                    .consume_fe_resources(&fe.resources)?;
                update.updated_executor_resources.insert(
                    executor.id.clone(),
                    executor_server_metadata.free_resources.clone(),
                );
            }
        }
        for (executor_fe_id, executor_fe) in &executor.function_executors {
            // If the Executor FE is also in the server's tracked FE lets sync them.
            if let Some(server_fe) = server_function_executors.get(executor_fe_id) {
                // If the executor's FE state is Terminated lets remove it from the server.
                if matches!(executor_fe.state, FunctionExecutorState::Terminated { .. }) {
                    function_executors_to_remove.push(executor_fe.clone());
                    continue;
                }

                // If the server's FE state is terminated we don't need to do anything heres
                if matches!(
                    server_fe.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }
                if executor_fe.state != server_fe.function_executor.state {
                    let mut server_fe_clone = server_fe.clone();
                    server_fe_clone.function_executor.update(executor_fe);
                    new_function_executors.insert(
                        server_fe_clone.function_executor.id.clone(),
                        *server_fe_clone,
                    );
                }
            }
        }
        update.new_function_executors.extend(new_function_executors);

        update.extend(self.remove_function_executors(
            in_memory_state,
            &mut executor_server_metadata,
            &function_executors_to_remove,
        )?);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "function_executor_manager",
        )?;

        Ok(update)
    }

    /// Removes function executors and handles associated function run cleanup
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_server_metadata.executor_id.get(), num_function_executors = function_executors_to_remove.len()))]
    fn remove_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_executors_to_remove: &[FunctionExecutor],
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if function_executors_to_remove.is_empty() {
            return Ok(update);
        }

        let mut failed_function_runs = 0;
        // Handle allocations for FEs to be removed and update function runs
        for fe in function_executors_to_remove {
            info!(
                namespace = fe.namespace,
                app = fe.application_name,
                fn = fe.function_name,
                fn_executor_id = fe.id.get(),
                fe_state = ?fe.state,
                "Removing function executor from executor",
            );

            let alloc_ids = in_memory_state
                .allocations_by_executor
                .get(&executor_server_metadata.executor_id)
                .and_then(|allocs_be_fe| allocs_be_fe.get(&fe.id))
                .cloned()
                .unwrap_or_default();
            for alloc_id in alloc_ids {
                let Some(alloc) = in_memory_state.get_allocation_by_id(&alloc_id) else {
                    continue;
                };
                let mut updated_alloc = alloc.clone();
                let Some(mut function_run) = in_memory_state
                    .function_runs
                    .get(&FunctionRunKey::from(&alloc))
                    .cloned()
                else {
                    update.cancel_allocation(&mut updated_alloc);
                    continue;
                };
                let Some(mut ctx) = in_memory_state
                    .request_ctx
                    .get(&function_run.clone().into())
                    .cloned()
                else {
                    update.cancel_allocation(&mut updated_alloc);
                    continue;
                };
                // Idempotency: we only act on this alloc's function run if the
                // function run is currently running this alloc. This is because we
                // handle allocation failures on FE termination and alloc output ingestion
                // paths.
                if function_run.status !=
                    FunctionRunStatus::Running(RunningFunctionRunStatus {
                        allocation_id: alloc.id.clone(),
                    })
                {
                    update.cancel_allocation(&mut updated_alloc);
                    continue;
                }
                let Some(application_version) = in_memory_state
                    .get_existing_application_version(&function_run)
                    .cloned()
                else {
                    update.cancel_allocation(&mut updated_alloc);
                    continue;
                };

                if let FunctionExecutorState::Terminated {
                    reason: termination_reason,
                    failed_alloc_ids: blame_alloc_ids,
                } = &fe.state
                {
                    // These are the cases where we know why an allocation failed.
                    // otherwise we assume the FE terminated so the allocations couldn't run.
                    // The assumption here is that bad user code or input can fail FE's but not the
                    // executor.
                    if blame_alloc_ids.contains(&alloc.id.to_string()) ||
                        termination_reason == &FunctionExecutorTerminationReason::ExecutorRemoved
                    {
                        updated_alloc.outcome =
                            FunctionRunOutcome::Failure((*termination_reason).into());
                    } else {
                        // This allocation wasn't blamed for the FE termination,
                        // retry without involving the function run retry policy but still fail the
                        // alloc.
                        updated_alloc.outcome = FunctionRunOutcome::Failure(
                            FunctionRunFailureReason::FunctionExecutorTerminated,
                        );
                    }
                    FunctionRunRetryPolicy::handle_allocation_outcome(
                        &mut function_run,
                        &updated_alloc,
                        &application_version,
                    );
                    update.add_updated_allocation(updated_alloc);
                    // Count failed function runs for logging
                    if function_run.status == FunctionRunStatus::Completed {
                        failed_function_runs += 1;
                    }
                } else {
                    // Function Executor is not terminated but getting removed. This means that
                    // we're cancelling all allocs on it. And retrying their
                    // function runs without increasing retries counters.
                    function_run.status = FunctionRunStatus::Pending;
                    update.add_updated_allocation(updated_alloc);
                }

                update.add_function_run(*function_run.clone(), &mut ctx);

                if function_run.status == FunctionRunStatus::Completed {
                    ctx.outcome = function_run.outcome.map(|o| o.into());
                    update
                        .updated_request_states
                        .insert(ctx.key(), *ctx.clone());
                }
            }
        }

        if failed_function_runs > 0 {
            info!(
                num_failed_allocations = failed_function_runs,
                "failed allocations on executor due to function executor terminations",
            );
        }

        // Add function executors to remove list
        for fe in function_executors_to_remove {
            update.add_removed_function_executor(
                &executor_server_metadata.executor_id,
                fe.id.clone(),
            );
        }

        for fe in function_executors_to_remove {
            if let Some(fe_resource_claim) = executor_server_metadata.resource_claims.get(&fe.id) {
                executor_server_metadata
                    .free_resources
                    .free(fe_resource_claim)?;
                update.updated_executor_resources.insert(
                    executor_server_metadata.executor_id.clone(),
                    executor_server_metadata.free_resources.clone(),
                );
            } else {
                error!(
                    fn_executor_id = fe.id.get(),
                    "resources not freed: function executor is not claiming resources on executor",
                );
            }
        }
        Ok(update)
    }

    /// Removes all function executors from an executor when it's being
    /// deregistered
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_id.get()))]
    fn remove_all_function_executors_for_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        let Some(mut executor_server_metadata) =
            in_memory_state.executor_states.get(executor_id).cloned()
        else {
            warn!("executor not found while removing function executors");
            let allocation_ids_by_fe = in_memory_state
                .allocations_by_executor
                .get(executor_id)
                .cloned()
                .unwrap_or_default();

            for alloc_ids in allocation_ids_by_fe.values() {
                for alloc_id in alloc_ids {
                    let Some(alloc) = in_memory_state.get_allocation_by_id(alloc_id) else {
                        continue;
                    };
                    info!(
                        alloc_id = alloc.id.to_string(),
                        request_id = alloc.request_id.clone(),
                        namespace = alloc.namespace.clone(),
                        app = alloc.application.clone(),
                        fn_call_id = alloc.function_call_id.to_string(),
                        "marking allocation as failed due to deregistered executor",
                    );
                    let mut updated_alloc = alloc.clone();
                    updated_alloc.outcome =
                        FunctionRunOutcome::Failure(FunctionRunFailureReason::ExecutorRemoved);
                    let Some(function_run) = in_memory_state
                        .function_runs
                        .get(&FunctionRunKey::from(&alloc))
                        .cloned()
                    else {
                        warn!(
                            fn_call_id = alloc.id.to_string(),
                            "function run not found while removing allocations for deregistered executor",
                        );
                        continue;
                    };

                    let Some(mut request_ctx) = in_memory_state
                        .request_ctx
                        .get(&function_run.clone().into())
                        .cloned()
                    else {
                        warn!(
                            fn_call_id = alloc.id.to_string(),
                            "request context not found while removing allocations for deregistered executor",
                        );
                        continue;
                    };
                    let Some(application_version) = in_memory_state
                        .get_existing_application_version(&function_run)
                        .cloned()
                    else {
                        warn!(
                            fn_call_id = alloc.id.to_string(),
                            "application version not found while removing allocations for deregistered executor",
                        );
                        continue;
                    };
                    FunctionRunRetryPolicy::handle_allocation_outcome(
                        &mut function_run.clone(),
                        &updated_alloc,
                        &application_version,
                    );
                    scheduler_update.add_updated_allocation(updated_alloc);
                    scheduler_update.add_function_run(*function_run.clone(), &mut request_ctx);
                }
            }
            return Ok(scheduler_update);
        };

        // Get all function executors to remove
        let function_executors_to_remove = &executor_server_metadata
            .function_executors
            .values()
            .map(|fe| {
                let mut fec = fe.function_executor.clone();
                if !matches!(fec.state, FunctionExecutorState::Terminated { .. }) {
                    fec.state = FunctionExecutorState::Terminated {
                        reason: FunctionExecutorTerminationReason::ExecutorRemoved,
                        failed_alloc_ids: Vec::new(),
                    };
                }
                fec
            })
            .collect::<Vec<_>>();

        self.remove_function_executors(
            in_memory_state,
            &mut executor_server_metadata,
            function_executors_to_remove,
        )
    }

    /// Ensures a function executor exists for the given function run.
    /// Creates a new FE if all existing FEs are at capacity.
    /// Returns any scheduler updates from FE creation.
    #[tracing::instrument(skip_all, target = "scheduler")]
    pub fn ensure_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        function_run: &FunctionRun,
    ) -> Result<SchedulerUpdateRequest> {
        let _timer = Timer::start_with_labels(&self.metrics.select_or_create_fe_duration, &[]);
        let mut update = SchedulerUpdateRequest::default();

        let function_executors =
            in_memory_state.candidate_function_executors(function_run, self.queue_size)?;

        // Create a new FE if all existing FEs are at capacity.
        // This allows scaling out when there's executor capacity.
        // Note: We intentionally don't check num_pending_function_executors here -
        // newly created FEs start in Unknown state (counted as "pending"), and waiting
        // for them to transition to Running would cause stalls and limit parallelism.
        // The create_function_executor call below will check if there's actual executor
        // capacity, so we won't over-create FEs beyond what the cluster can handle.
        if function_executors.function_executors.is_empty() {
            debug!(
                namespace = function_run.namespace,
                app = function_run.application,
                fn_name = function_run.name,
                fn_call_id = function_run.id.to_string(),
                num_pending = function_executors.num_pending_function_executors,
                num_total = function_executors.num_total_function_executors,
                "no FEs with capacity, attempting to create new FE"
            );
            let fe_update = self.create_function_executor(in_memory_state, function_run)?;
            update.extend(fe_update);
        }

        Ok(update)
    }

    /// Selects the best function executor for the given function run.
    /// Uses least-loaded policy (fewest allocations).
    /// Returns None if no FE has capacity.
    pub fn select_function_executor(
        &self,
        in_memory_state: &InMemoryState,
        function_run: &FunctionRun,
    ) -> Result<Option<AllocationTarget>> {
        let function_executors =
            in_memory_state.candidate_function_executors(function_run, self.queue_size)?;

        debug!(
            num_candidates = function_executors.function_executors.len(),
            num_pending = function_executors.num_pending_function_executors,
            num_total = function_executors.num_total_function_executors,
            "found function executors for function run",
        );

        // Select a function executor using least-loaded policy (fewest allocations)
        // BTreeSet is already ordered by allocation count, so just pick the first one
        let selected_fe = function_executors.function_executors.first().map(|fe| {
            debug!(
                executor_id = %fe.executor_id,
                function_executor_id = %fe.function_executor_id,
                allocation_count = fe.allocation_count,
                "selected function executor with least allocations",
            );
            AllocationTarget::new(fe.executor_id.clone(), fe.function_executor_id.clone())
        });

        Ok(selected_fe)
    }

    /// Completely deregisters an executor and handles all associated cleanup
    /// Returns scheduler update that includes executor removal and function
    /// executor cleanup
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub fn deregister_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };
        info!("de-registering executor");

        // Remove all function executors for this executor
        update
            .extend(self.remove_all_function_executors_for_executor(in_memory_state, executor_id)?);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "function_executor_manager",
        )?;

        Ok(update)
    }

    /// Reconciles executor state when an executor is upserted
    /// Returns scheduler update that includes function executor reconciliation
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub fn reconcile_executor_state(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let executor = in_memory_state
            .executors
            .get(executor_id)
            .ok_or(anyhow!("executor not found"))?
            .clone();

        tracing::debug!(?executor, "reconciling executor state for executor",);

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(in_memory_state, &executor)?);

        Ok(update)
    }
}
