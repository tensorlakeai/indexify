use std::collections::HashSet;

use anyhow::{Result, anyhow};
use rand::seq::IndexedRandom;
use tracing::{debug, error, info, info_span, warn};

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
        RunningFunctionRunStatus,
    },
    processor::{retry_policy::FunctionRunRetryPolicy, targets},
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

    /// Vacuum phase - identifies function executors that should be terminated
    /// Returns scheduler update for cleanup actions
    #[tracing::instrument(skip(self, in_memory_state))]
    fn vacuum(
        &self,
        in_memory_state: &mut InMemoryState,
        fe_resource: &FunctionResources,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark =
            in_memory_state.vacuum_function_executors_candidates(fe_resource)?;

        debug!(
            target: targets::SCHEDULER,
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
                target: targets::SCHEDULER,
                fn_executor_id = fe.function_executor.id.get(),
                executor_id = fe.executor_id.get(),
                "Marked function executor {} on executor {} for termination",
                fe.function_executor.id.get(),
                fe.executor_id.get()
            );
        }
        Ok(update)
    }

    /// Creates a new function executor for the given function run
    #[tracing::instrument(skip_all)]
    fn create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        fn_run: &FunctionRun,
    ) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            target: targets::SCHEDULER,
            "create_function_executor",
            namespace = fn_run.namespace,
            request_id = fn_run.request_id,
            fn_call_id = fn_run.id.to_string(),
            app = fn_run.application,
            "fn" = fn_run.name,
            app_version = fn_run.version.to_string(),
        );
        let _guard = span.enter();

        let mut update = SchedulerUpdateRequest::default();
        let mut candidates = in_memory_state.candidate_executors(fn_run)?;
        if candidates.is_empty() {
            info!(target: targets::SCHEDULER, "no executors are available to create function executor");
            let fe_resource = in_memory_state.fe_resource_for_function_run(fn_run)?;
            let vacuum_update = self.vacuum(in_memory_state, &fe_resource)?;
            update.extend(vacuum_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
                "function_executor_manager",
            )?;
            candidates = in_memory_state.candidate_executors(fn_run)?;
        }
        debug!(
            target: targets::SCHEDULER,
            "found {} candidates for creating function executor",
            candidates.len()
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
            target: targets::SCHEDULER,
            executor_id = executor_id.get(),
            fn_executor_id = function_executor.id.get(),
            "created function executor"
        );

        // Create with current timestamp for last_allocation_at
        let fe_server_metadata = FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor,
            FunctionExecutorState::Running, // Start with Running state
        );
        update.new_function_executors.push(fe_server_metadata);

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
    #[tracing::instrument(skip(
        self,
        in_memory_state,
        executor_server_metadata,
        function_executors_to_remove
    ))]
    fn remove_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_executors_to_remove: &[FunctionExecutor],
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let span = info_span!(
            target: targets::SCHEDULER,
            "remove_function_executors",
            executor_id = executor_server_metadata.executor_id.get(),
            num_function_executors = function_executors_to_remove.len(),
        );
        let _guard = span.enter();

        if function_executors_to_remove.is_empty() {
            return Ok(update);
        }

        let mut failed_function_runs = 0;
        // Handle allocations for FEs to be removed and update function runs
        for fe in function_executors_to_remove {
            info!(
                target: targets::SCHEDULER,
                namespace = fe.namespace,
                app = fe.application_name,
                fn = fe.function_name,
                executor_id = executor_server_metadata.executor_id.get(),
                fn_executor_id = fe.id.get(),
                fe_state = ?fe.state,
                "Removing function executor from executor",
            );

            let allocs = in_memory_state
                .allocations_by_executor
                .get(&executor_server_metadata.executor_id)
                .and_then(|allocs_be_fe| allocs_be_fe.get(&fe.id))
                .cloned()
                .unwrap_or_default();
            for alloc in allocs {
                let mut updated_alloc = *alloc.clone();
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
                    update.updated_allocations.push(updated_alloc);
                    // Count failed function runs for logging
                    if function_run.status == FunctionRunStatus::Completed {
                        failed_function_runs += 1;
                    }
                } else {
                    // Function Executor is not terminated but getting removed. This means that
                    // we're cancelling all allocs on it. And retrying their
                    // function runs without increasing retries counters.
                    function_run.status = FunctionRunStatus::Pending;
                    update.updated_allocations.push(updated_alloc);
                }

                update
                    .updated_function_runs
                    .entry(ctx.key())
                    .or_insert(HashSet::new())
                    .insert(function_run.id.clone());
                ctx.function_runs
                    .insert(function_run.id.clone(), *function_run.clone());
                if function_run.status == FunctionRunStatus::Completed {
                    ctx.outcome = function_run.outcome.map(|o| o.into());
                }
                update
                    .updated_request_states
                    .insert(ctx.key(), *ctx.clone());
            }
        }

        if failed_function_runs > 0 {
            info!(
                target: targets::SCHEDULER,
                num_failed_allocations = failed_function_runs,
                "failed allocations on executor due to function executor terminations",
            );
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
                    target: targets::SCHEDULER,
                    fn_executor_id = fe.id.get(),
                    "resources not freed: function executor is not claiming resources on executor",
                );
            }
        }
        Ok(update)
    }

    /// Removes all function executors from an executor when it's being
    /// deregistered
    fn remove_all_function_executors_for_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        let Some(mut executor_server_metadata) =
            in_memory_state.executor_states.get(executor_id).cloned()
        else {
            warn!(
                target: targets::SCHEDULER,
                executor_id = executor_id.get(),
                "executor {} not found while removing function executors",
                executor_id.get()
            );
            let allocations = in_memory_state
                .allocations_by_executor
                .get(executor_id)
                .cloned()
                .unwrap_or_default();

            for allocs_by_fe in allocations.values() {
                for alloc in allocs_by_fe {
                    info!(
                        target: targets::SCHEDULER,
                        alloc_id = alloc.id.to_string(),
                        request_id = alloc.request_id.clone(),
                        namespace = alloc.namespace.clone(),
                        app = alloc.application.clone(),
                        fn_call_id = alloc.function_call_id.to_string(),
                        "marking allocation as failed due to deregistered executor",
                    );
                    let mut updated_alloc = *alloc.clone();
                    updated_alloc.outcome =
                        FunctionRunOutcome::Failure(FunctionRunFailureReason::ExecutorRemoved);
                    let Some(function_run) = in_memory_state
                        .function_runs
                        .get(&FunctionRunKey::from(alloc))
                        .cloned()
                    else {
                        warn!(
                            target: targets::SCHEDULER,
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
                            target: targets::SCHEDULER,
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
                            target: targets::SCHEDULER,
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
                    scheduler_update.updated_allocations.push(updated_alloc);
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

    /// Selects or creates a function executor for the given function run
    /// Returns the allocation target and any scheduler updates
    #[tracing::instrument(skip(self, in_memory_state, function_run))]
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
            info!(target: targets::SCHEDULER, "no function executors found");
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
            target: targets::SCHEDULER,
            num_function_executors = function_executors.function_executors.len(),
            num_pending_function_executors = function_executors.num_pending_function_executors,
            "found function executors for function run",
        );

        // Select a function executor using least-loaded policy (fewest allocations)
        // BTreeSet is already ordered by allocation count, so just pick the first one
        let selected_fe = function_executors.function_executors.first().map(|fe| {
            debug!(
                target: targets::SCHEDULER,
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
    #[tracing::instrument(skip(self, in_memory_state, executor_id))]
    pub fn deregister_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

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
    #[tracing::instrument(skip(self, in_memory_state, executor_id))]
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

        tracing::debug!(
            target: targets::SCHEDULER,
            executor_id = executor_id.get(),
            ?executor,
            "reconciling executor state for executor",
        );

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(in_memory_state, &executor)?);

        Ok(update)
    }
}
