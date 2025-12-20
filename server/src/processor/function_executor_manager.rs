use anyhow::{Result, anyhow};
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
    processor::retry_policy::FunctionRunRetryPolicy,
    state_store::{
        in_memory_state::{FunctionRunKey, InMemoryState},
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct FunctionExecutorManager {
    clock: u64,
    queue_size: u32,
}

/// Implements the policy around function executors: when to create
/// them, when to terminate them, and the selection of function
/// executors appropriate to a given function run.
impl FunctionExecutorManager {
    pub fn new(clock: u64, queue_size: u32) -> Self {
        Self { clock, queue_size }
    }

    /// Core FE building logic - shared by both reactive and proactive creation
    /// paths
    fn build_function_executor(
        in_memory_state: &InMemoryState,
        executor_id: &ExecutorId,
        fn_uri: &FunctionURI,
        free_resources: &mut crate::data_model::HostResources,
    ) -> Result<
        Option<(
            FunctionExecutorServerMetadata,
            crate::data_model::HostResources,
        )>,
    > {
        // Get function resources from application version
        let Some(node_resources) = in_memory_state.get_fe_resources_by_uri(
            &fn_uri.namespace,
            &fn_uri.application,
            &fn_uri.function,
            &fn_uri.version,
        ) else {
            debug!("function resources not found for FE creation");
            return Ok(None);
        };

        // Check if executor has enough resources
        let fe_resources = match free_resources.consume_function_resources(&node_resources) {
            Ok(resources) => resources,
            Err(e) => {
                debug!(error = %e, "executor doesn't have enough resources for FE creation");
                return Ok(None);
            }
        };

        // Get max concurrency
        let Some(max_concurrency) = in_memory_state.get_fe_max_concurrency_by_uri(
            &fn_uri.namespace,
            &fn_uri.application,
            &fn_uri.function,
            &fn_uri.version,
        ) else {
            debug!("max concurrency not found for FE creation");
            return Ok(None);
        };

        // Build the function executor
        let function_executor = FunctionExecutorBuilder::default()
            .namespace(fn_uri.namespace.clone())
            .application_name(fn_uri.application.clone())
            .function_name(fn_uri.function.clone())
            .version(fn_uri.version.clone())
            .state(FunctionExecutorState::Unknown)
            .resources(fe_resources)
            .max_concurrency(max_concurrency)
            .build()?;

        let fe_server_metadata = FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor,
            FunctionExecutorState::Running,
        );

        Ok(Some((fe_server_metadata, free_resources.clone())))
    }

    /// Vacuum phase - identifies function executors that should be terminated
    /// Returns scheduler update for cleanup actions
    #[tracing::instrument(skip_all, target = "scheduler")]
    fn vacuum(
        &self,
        in_memory_state: &mut InMemoryState,
        fe_resource: &FunctionResources,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark =
            in_memory_state.vacuum_function_executors_candidates(fe_resource)?;

        debug!(
            "vacuum phase identified {} function executors to mark for termination",
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
            update.new_function_executors.push(update_fe);

            info!(
                app = fe.function_executor.application_name,
                fn = fe.function_executor.function_name,
                namespace = fe.function_executor.namespace,
                fn_executor_id = fe.function_executor.id.get(),
                executor_id = fe.executor_id.get(),
                "marked function executor for termination",
            );
        }
        Ok(update)
    }

    /// Vacuum idle FEs on a specific executor to free resources for pending
    /// runs
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub fn vacuum_idle_fes(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let Some(executor_state) = in_memory_state.executor_states.get(executor_id) else {
            return Ok(update);
        };

        // Find idle FEs (no running allocations, no pending tasks)
        for (fe_id, fe_meta) in &executor_state.function_executors {
            // Skip if already marked for termination
            if matches!(
                fe_meta.desired_state,
                FunctionExecutorState::Terminated { .. }
            ) {
                continue;
            }

            // Check if FE has running allocations
            let has_running_allocations = in_memory_state
                .allocations_by_executor
                .get(executor_id)
                .and_then(|fe_allocs| fe_allocs.get(fe_id))
                .map(|allocs| !allocs.is_empty())
                .unwrap_or(false);

            if has_running_allocations {
                continue;
            }

            // Check if there are pending tasks for this FE's function
            let has_pending_tasks = in_memory_state.has_pending_tasks(fe_meta);
            if has_pending_tasks {
                continue;
            }

            // This FE is idle - mark for termination
            let mut update_fe = fe_meta.as_ref().clone();
            update_fe.desired_state = FunctionExecutorState::Terminated {
                reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                failed_alloc_ids: Vec::new(),
            };
            update.new_function_executors.push(update_fe);

            info!(
                app = fe_meta.function_executor.application_name,
                fn_name = fe_meta.function_executor.function_name,
                namespace = fe_meta.function_executor.namespace,
                fn_executor_id = fe_meta.function_executor.id.get(),
                executor_id = executor_id.get(),
                "vacuuming idle FE to free resources"
            );
        }

        // Apply the update to free resources
        if !update.new_function_executors.is_empty() {
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
                "vacuum_idle_fes",
            )?;
        }

        Ok(update)
    }

    /// Creates a new function executor for the given function run (reactive
    /// path)
    #[tracing::instrument(skip_all, target = "scheduler", fields(namespace = %fn_run.namespace, request_id = %fn_run.request_id, fn_call_id = %fn_run.id, app = %fn_run.application, fn = %fn_run.name, app_version = %fn_run.version))]
    fn create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        fn_run: &FunctionRun,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let mut candidates = in_memory_state.candidate_executors(fn_run)?;
        if candidates.is_empty() {
            info!(
                fn_name = fn_run.name,
                namespace = fn_run.namespace,
                app = fn_run.application,
                "no executors available, attempting vacuum to free resources"
            );
            let fe_resource = in_memory_state.fe_resource_for_function_run(fn_run)?;
            let vacuum_update = self.vacuum(in_memory_state, &fe_resource)?;
            info!(
                vacuumed_fes = vacuum_update.new_function_executors.len(),
                "vacuum completed"
            );
            update.extend(vacuum_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
                "function_executor_manager",
            )?;
            candidates = in_memory_state.candidate_executors(fn_run)?;
            info!(
                candidates_after_vacuum = candidates.len(),
                "candidates available after vacuum"
            );
        }

        // Filter candidates to respect max_fe_count per executor
        let fn_uri = FunctionURI {
            namespace: fn_run.namespace.clone(),
            application: fn_run.application.clone(),
            function: fn_run.name.clone(),
            version: fn_run.version.clone(),
        };
        if let Some(config) = in_memory_state.get_scaling_config(&fn_uri) {
            candidates.retain(|c| {
                let current_count = in_memory_state
                    .resource_placement_index
                    .fe_count_for_function(&c.executor_id, &fn_uri);
                current_count < config.max_fe_count as usize
            });
        }

        debug!(
            "found {} candidates for creating function executor (after max_fe_count filter)",
            candidates.len()
        );

        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            return Ok(update);
        };

        // Use shared builder
        let Some((fe_meta, updated_resources)) = Self::build_function_executor(
            in_memory_state,
            &candidate.executor_id,
            &fn_uri,
            &mut candidate.free_resources,
        )?
        else {
            return Ok(update);
        };

        info!(
            executor_id = candidate.executor_id.get(),
            fn_executor_id = fe_meta.function_executor.id.get(),
            "created function executor"
        );

        update
            .updated_executor_resources
            .insert(candidate.executor_id.clone(), updated_resources);
        update.new_function_executors.push(fe_meta);

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
        let mut new_function_executors = Vec::new();

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
                new_function_executors.push(FunctionExecutorServerMetadata::new(
                    executor.id.clone(),
                    fe.clone(),
                    fe.state,
                ));
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
                    new_function_executors.push(*server_fe_clone);
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

            let allocs: Vec<_> = in_memory_state
                .allocations_by_executor
                .get(&executor_server_metadata.executor_id)
                .and_then(|allocs_by_fe| allocs_by_fe.get(&fe.id))
                .map(|allocs| allocs.values().cloned().collect())
                .unwrap_or_default();
            for alloc in allocs {
                let mut updated_alloc = alloc.as_ref().clone();
                let Some(mut function_run) = in_memory_state
                    .function_runs
                    .get(&FunctionRunKey::from(alloc.as_ref()))
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
                        allocation_id: updated_alloc.id.clone(),
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
                    if blame_alloc_ids.contains(&updated_alloc.id.to_string()) ||
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
                    info!(
                        allocation_id = updated_alloc.id.to_string(),
                        request_id = updated_alloc.request_id.clone(),
                        namespace = updated_alloc.namespace.clone(),
                        app = updated_alloc.application.clone(),
                        fn = updated_alloc.function.clone(),
                        fn_executor_id = updated_alloc.target.function_executor_id.to_string(),
                        allocation_outcome = updated_alloc.outcome.to_string(),
                        fn_run_status = function_run.status.to_string(),
                        fn_run_outcome = function_run.outcome.map(|o| o.to_string()),
                        blame_allocation_id = blame_alloc_ids.contains(&updated_alloc.id.to_string()),
                        termination_reason = termination_reason.to_string(),
                        "function executor terminated, updating allocation outcome",
                    );
                    update.updated_allocations.push(updated_alloc);
                } else {
                    // Function Executor is not terminated but getting removed. This means that
                    // we're cancelling all allocs on it. And retrying their
                    // function runs without increasing retries counters.
                    function_run.status = FunctionRunStatus::Pending;
                    updated_alloc.outcome =
                        FunctionRunOutcome::Failure(FunctionRunFailureReason::FunctionRunCancelled);
                    info!(
                        allocation_id = updated_alloc.id.to_string(),
                        request_id = updated_alloc.request_id.clone(),
                        namespace = updated_alloc.namespace.clone(),
                        app = updated_alloc.application.clone(),
                        fn = updated_alloc.function.clone(),
                        fn_executor_id = updated_alloc.target.function_executor_id.to_string(),
                        allocation_outcome = updated_alloc.outcome.to_string(),
                        fn_run_status = function_run.status.to_string(),
                        fn_run_outcome = function_run.outcome.map(|o| o.to_string()),
                        "function executor is being removed, cancelling allocation",
                    );
                    update.updated_allocations.push(updated_alloc);
                }

                info!(
                    request_id = function_run.request_id.clone(),
                    namespace = function_run.namespace.clone(),
                    app = function_run.application.clone(),
                    fn = function_run.name.clone(),
                    fn_run_status = function_run.status.to_string(),
                    fn_run_outcome = function_run.outcome.map(|o| o.to_string()),
                    "updating function run to request context because function executor is being removed",
                );

                update.add_function_run(*function_run.clone(), &mut ctx);

                if function_run.status == FunctionRunStatus::Completed {
                    ctx.outcome = function_run.outcome.map(|o| o.into());
                    update
                        .updated_request_states
                        .insert(ctx.key(), *ctx.clone());
                }
            }
        }

        // Add function executors to remove list
        update
            .remove_function_executors
            .entry(executor_server_metadata.executor_id.clone())
            .or_default()
            .extend(function_executors_to_remove.iter().map(|fe| fe.id.clone()));

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
            let allocations = in_memory_state
                .allocations_by_executor
                .get(executor_id)
                .cloned()
                .unwrap_or_default();

            for allocs_by_fe in allocations.values() {
                for alloc in allocs_by_fe.values() {
                    info!(
                        allocation_id = alloc.id.to_string(),
                        request_id = alloc.request_id.clone(),
                        namespace = alloc.namespace.clone(),
                        app = alloc.application.clone(),
                        fn = alloc.function.clone(),
                        fn_call_id = alloc.function_call_id.to_string(),
                        "marking allocation as failed due to deregistered executor",
                    );
                    let mut updated_alloc = alloc.as_ref().clone();
                    updated_alloc.outcome =
                        FunctionRunOutcome::Failure(FunctionRunFailureReason::ExecutorRemoved);
                    let Some(function_run) = in_memory_state
                        .function_runs
                        .get(&FunctionRunKey::from(alloc.as_ref()))
                        .cloned()
                    else {
                        warn!(
                            fn_call_id = alloc.id.to_string(),
                            fn = alloc.function.clone(),
                            request_id = alloc.request_id.clone(),
                            namespace = alloc.namespace.clone(),
                            app = alloc.application.clone(),
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
                            fn = alloc.function.clone(),
                            request_id = alloc.request_id.clone(),
                            namespace = alloc.namespace.clone(),
                            app = alloc.application.clone(),
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
                            fn = alloc.function.clone(),
                            request_id = alloc.request_id.clone(),
                            namespace = alloc.namespace.clone(),
                            app = alloc.application.clone(),
                            "application version not found while removing allocations for deregistered executor",
                        );
                        continue;
                    };
                    let mut function_run = *function_run;
                    FunctionRunRetryPolicy::handle_allocation_outcome(
                        &mut function_run,
                        &updated_alloc,
                        &application_version,
                    );
                    scheduler_update.updated_allocations.push(updated_alloc);
                    info!(
                        allocation_id = alloc.id.to_string(),
                        request_id = function_run.request_id.clone(),
                        namespace = function_run.namespace.clone(),
                        app = function_run.application.clone(),
                        fn = function_run.name.clone(),
                        status = function_run.status.to_string(),
                        outcome = function_run.outcome.map(|o| o.to_string()),
                        "function run status after removing function executor because of FE termination",
                    );
                    scheduler_update.add_function_run(function_run, &mut request_ctx);
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

    /// Selects or creates a function executor for the given function run
    /// Returns the allocation target and any scheduler updates
    #[tracing::instrument(skip_all, target = "scheduler")]
    pub fn select_or_create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        function_run: &FunctionRun,
    ) -> Result<(Option<AllocationTarget>, SchedulerUpdateRequest)> {
        let mut update = SchedulerUpdateRequest::default();
        let mut function_executors =
            in_memory_state.candidate_function_executors(function_run, self.queue_size)?;

        // If no function executors are available, create one
        if function_executors.function_executors.is_empty() &&
            function_executors.num_pending_function_executors == 0
        {
            debug!(
                namespace = function_run.namespace,
                app = function_run.application,
                fn = function_run.name,
                request_id = function_run.request_id,
                "no function executors found"
            );
            let fe_update = self.create_function_executor(in_memory_state, function_run)?;
            update.extend(fe_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
                "function_executor_manager",
            )?;
            function_executors =
                in_memory_state.candidate_function_executors(function_run, self.queue_size)?;
        }

        debug!(
            num_function_executors = function_executors.function_executors.len(),
            num_pending_function_executors = function_executors.num_pending_function_executors,
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

        Ok((selected_fe, update))
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

    /// Ensure minimum FEs exist for functions this executor can run.
    /// Only checks functions in the executor's allowlist (if any).
    /// O(A) where A = allowlist size (typically small).
    pub fn ensure_min_fes(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let Some(executor) = in_memory_state.executors.get(executor_id).cloned() else {
            return Ok(update);
        };

        // Only process if executor has an allowlist - otherwise reactive path handles
        // it
        let Some(allowlist) = &executor.function_allowlist else {
            return Ok(update);
        };

        // For each allowlist entry, check if we need to create min FEs
        for entry in allowlist {
            // Build FunctionURI from allowlist entry (if fully specified)
            let (Some(ns), Some(app), Some(func), Some(ver)) = (
                entry.namespace.as_ref(),
                entry.application.as_ref(),
                entry.function.as_ref(),
                entry.version.as_ref(),
            ) else {
                // Skip wildcard entries - can't determine specific function
                continue;
            };

            let fn_uri = FunctionURI {
                namespace: ns.clone(),
                application: app.clone(),
                function: func.clone(),
                version: ver.clone(),
            };

            // Get scaling config for this function
            let Some(config) = in_memory_state.get_scaling_config(&fn_uri) else {
                continue;
            };

            // Skip if no minimum required
            if config.min_fe_count == 0 {
                continue;
            }

            // Check global FE count
            let global_count = in_memory_state
                .resource_placement_index
                .total_fe_count_for_function(&fn_uri) as u32;

            if global_count >= config.min_fe_count {
                continue;
            }

            // Need to create FEs - try to create on this executor
            let needed = config.min_fe_count - global_count;
            debug!(
                executor_id = executor_id.get(),
                fn_uri = %fn_uri,
                global_count,
                min_fe_count = config.min_fe_count,
                needed,
                "creating min FEs for function"
            );

            for _ in 0..needed {
                // Get executor state for resources
                let Some(executor_state) =
                    in_memory_state.executor_states.get(executor_id).cloned()
                else {
                    break;
                };

                let mut free_resources = executor_state.free_resources.clone();
                let Some((fe_meta, updated_resources)) = Self::build_function_executor(
                    in_memory_state,
                    executor_id,
                    &fn_uri,
                    &mut free_resources,
                )?
                else {
                    // No resources available
                    break;
                };

                info!(
                    executor_id = executor_id.get(),
                    fn_executor_id = fe_meta.function_executor.id.get(),
                    fn_uri = %fn_uri,
                    "created min FE for function"
                );

                // Apply immediately so resource tracking updates
                let mut fe_update = SchedulerUpdateRequest::default();
                fe_update
                    .updated_executor_resources
                    .insert(executor_id.clone(), updated_resources);
                fe_update.new_function_executors.push(fe_meta);

                in_memory_state.update_state(
                    self.clock,
                    &RequestPayload::SchedulerUpdate((Box::new(fe_update.clone()), vec![])),
                    "function_executor_manager_min_fe",
                )?;
                update.extend(fe_update);
            }
        }

        Ok(update)
    }
}
