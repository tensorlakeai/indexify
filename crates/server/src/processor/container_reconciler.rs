use anyhow::{Result, anyhow};
use tracing::{info, warn};

use crate::{
    data_model::{
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionContainer,
        FunctionContainerServerMetadata,
        FunctionContainerState,
        FunctionExecutorTerminationReason,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        RunningFunctionRunStatus,
    },
    processor::{container_scheduler::ContainerScheduler, retry_policy::FunctionRunRetryPolicy},
    state_store::{
        in_memory_state::{FunctionRunKey, InMemoryState},
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct ContainerReconciler {
    clock: u64,
}

/// Reconciles container state between executors and the server.
/// Handles container cleanup when executors are removed or containers
/// terminate.
impl ContainerReconciler {
    pub fn new(clock: u64) -> Self {
        Self { clock }
    }

    /// Reconciles function executor state between executor and server
    #[tracing::instrument(skip(self, in_memory_state, container_scheduler, executor))]
    fn reconcile_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let Some(mut executor_server_metadata) = container_scheduler
            .executor_states
            .get(&executor.id)
            .cloned()
        else {
            return Ok(update);
        };
        let mut function_containers_to_remove = Vec::new();

        let containers_only_in_executor = executor
            .function_executors
            .iter()
            .filter(|(fe_id, _fe)| {
                !executor_server_metadata
                    .function_container_ids
                    .contains(fe_id)
            })
            .map(|(_fe_id, fe)| fe.clone())
            .collect::<Vec<_>>();
        let containers_only_in_server = executor_server_metadata
            .function_container_ids
            .iter()
            .filter(|fe_id| !executor.function_executors.contains_key(fe_id))
            .collect::<Vec<_>>();
        for container_id in containers_only_in_server {
            let Some(function_container) =
                container_scheduler.function_containers.get(container_id)
            else {
                continue;
            };
            if matches!(
                function_container.desired_state,
                FunctionContainerState::Terminated { .. }
            ) {
                function_containers_to_remove.push(function_container.function_container.clone());
            }
        }
        for fe in containers_only_in_executor {
            if !matches!(fe.state, FunctionContainerState::Terminated { .. }) &&
                executor_server_metadata
                    .free_resources
                    .can_handle_fe_resources(&fe.resources)
                    .is_ok()
            {
                let existing_fe = FunctionContainerServerMetadata::new(
                    executor.id.clone(),
                    fe.clone(),
                    fe.state.clone(),
                );
                executor_server_metadata.add_container(&fe)?;
                update.updated_executor_states.insert(
                    executor_server_metadata.executor_id.clone(),
                    executor_server_metadata.clone(),
                );
                update.function_containers.insert(
                    existing_fe.function_container.id.clone(),
                    Box::new(existing_fe.clone()),
                );
            }
        }
        container_scheduler.update(&RequestPayload::SchedulerUpdate((
            Box::new(update.clone()),
            vec![],
        )))?;
        for (executor_fe_id, executor_fe) in &executor.function_executors {
            // If the Executor FE is also in the server's tracked FE lets sync them.
            if let Some(server_fe) = container_scheduler.function_containers.get(executor_fe_id) {
                // If the executor's FE state is Terminated lets remove it from the server.
                if matches!(executor_fe.state, FunctionContainerState::Terminated { .. }) {
                    function_containers_to_remove.push(executor_fe.clone());
                    executor_server_metadata.remove_container(executor_fe)?;
                    continue;
                }

                // If the server's FE state is terminated we don't need to do anything heres
                if matches!(
                    server_fe.desired_state,
                    FunctionContainerState::Terminated { .. }
                ) {
                    continue;
                }
                if executor_fe.state != server_fe.function_container.state {
                    let mut server_fe_clone = server_fe.clone();
                    server_fe_clone.function_container.update(executor_fe);
                    update.function_containers.insert(
                        server_fe_clone.function_container.id.clone(),
                        server_fe_clone.clone(),
                    );
                }
            }
        }
        container_scheduler.update(&RequestPayload::SchedulerUpdate((
            Box::new(update.clone()),
            vec![],
        )))?;

        update.extend(self.remove_function_containers(
            in_memory_state,
            &mut executor_server_metadata,
            function_containers_to_remove,
        )?);

        // Apply update to container_scheduler so removed containers are no longer
        // visible for subsequent allocate_function_runs call
        let payload = RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![]));
        container_scheduler.update(&payload)?;

        in_memory_state.update_state(self.clock, &payload, "container_reconciler")?;

        Ok(update)
    }

    /// Removes function executors and handles associated function run cleanup
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_server_metadata.executor_id.get(), num_function_executors = function_containers.len()))]
    fn remove_function_containers(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_containers: Vec<FunctionContainer>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if function_containers.is_empty() {
            return Ok(update);
        }

        // Handle allocations for FEs to be removed and update function runs
        for fe in &function_containers {
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

                if let FunctionContainerState::Terminated {
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
                            FunctionRunOutcome::Failure(termination_reason.into());
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
                        allocation_id = %updated_alloc.id,
                        request_id = %updated_alloc.request_id,
                        namespace = %updated_alloc.namespace,
                        app = %updated_alloc.application,
                        fn = %updated_alloc.function,
                        fn_executor_id = %updated_alloc.target.function_executor_id,
                        allocation_outcome = %updated_alloc.outcome,
                        fn_run_status = %function_run.status,
                        fn_run_outcome = ?function_run.outcome.as_ref(),
                        fn_run_id = %function_run.id,
                        blame_allocation_id = blame_alloc_ids.contains(&updated_alloc.id.to_string()),
                        termination_reason = %termination_reason,
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
                        allocation_id = %updated_alloc.id,
                        request_id = %updated_alloc.request_id,
                        namespace = %updated_alloc.namespace,
                        app = %updated_alloc.application,
                        fn = %updated_alloc.function,
                        fn_executor_id = %updated_alloc.target.function_executor_id,
                        allocation_outcome = %updated_alloc.outcome,
                        fn_run_status = %function_run.status,
                        fn_run_outcome = ?function_run.outcome.as_ref(),
                        fn_run_id = %function_run.id,
                        "function executor is being removed, cancelling allocation",
                    );
                    update.updated_allocations.push(updated_alloc);
                }

                info!(
                    request_id = %function_run.request_id,
                    namespace = %function_run.namespace,
                    app = %function_run.application,
                    fn = %function_run.name,
                    fn_run_status = %function_run.status,
                    fn_run_outcome = ?function_run.outcome.as_ref(),
                    fn_run_id = %function_run.id,
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

        for fc in function_containers {
            executor_server_metadata.remove_container(&fc)?;
        }
        update.updated_executor_states.insert(
            executor_server_metadata.executor_id.clone(),
            Box::new(executor_server_metadata.clone()),
        );
        Ok(update)
    }

    /// Removes all function executors from an executor when it's being
    /// deregistered
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_id.get()))]
    fn remove_all_function_executors_for_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        let Some(mut executor_server_metadata) = container_scheduler
            .executor_states
            .get(executor_id)
            .cloned()
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
                        allocation_id = %alloc.id,
                        request_id = %alloc.request_id,
                        namespace = %alloc.namespace,
                        app = %alloc.application,
                        fn = %alloc.function,
                        fn_call_id = %alloc.function_call_id,
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
                            fn_call_id = %alloc.id,
                            fn = %alloc.function,
                            request_id = %alloc.request_id,
                            namespace = %alloc.namespace,
                            app = %alloc.application,
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
                            fn_call_id = %alloc.id,
                            fn = %alloc.function,
                            request_id = %alloc.request_id,
                            namespace = %alloc.namespace,
                            app = %alloc.application,
                            "request context not found while removing allocations for deregistered executor",
                        );
                        continue;
                    };
                    let Some(application_version) = in_memory_state
                        .get_existing_application_version(&function_run)
                        .cloned()
                    else {
                        warn!(
                            fn_call_id = %alloc.id,
                            fn = %alloc.function,
                            request_id = %alloc.request_id,
                            namespace = %alloc.namespace,
                            app = %alloc.application,
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
                        allocation_id = %alloc.id,
                        request_id = %function_run.request_id,
                        namespace = %function_run.namespace,
                        app = %function_run.application,
                        fn = %function_run.name,
                        status = %function_run.status,
                        outcome = ?function_run.outcome.as_ref(),
                        "function run status after removing function executor because of FE termination",
                    );
                    scheduler_update.add_function_run(function_run, &mut request_ctx);
                }
            }
            return Ok(scheduler_update);
        };

        // Get all function executors to remove
        let mut function_containers_to_remove = Vec::new();
        for container_id in &executor_server_metadata.function_container_ids {
            let Some(function_container) =
                container_scheduler.function_containers.get(container_id)
            else {
                continue;
            };
            let mut fec = function_container.function_container.clone();
            if !matches!(fec.state, FunctionContainerState::Terminated { .. }) {
                fec.state = FunctionContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::ExecutorRemoved,
                    failed_alloc_ids: Vec::new(),
                };
                function_containers_to_remove.push(fec);
            }
        }
        let scheduler_update = self.remove_function_containers(
            in_memory_state,
            &mut executor_server_metadata,
            function_containers_to_remove,
        )?;
        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(scheduler_update.clone()), vec![])),
            "container_reconciler",
        )?;
        container_scheduler.update(&RequestPayload::SchedulerUpdate((
            Box::new(scheduler_update.clone()),
            vec![],
        )))?;
        Ok(scheduler_update)
    }

    /// Completely deregisters an executor and handles all associated cleanup
    /// Returns scheduler update that includes executor removal and function
    /// executor cleanup
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub fn deregister_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };
        info!("de-registering executor");

        // Remove all function executors for this executor
        update.extend(self.remove_all_function_executors_for_executor(
            in_memory_state,
            container_scheduler,
            executor_id,
        )?);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "container_reconciler",
        )?;

        // Apply the update to container_scheduler immediately so the executor
        // is removed before subsequent allocate_function_runs calls
        container_scheduler.update(&RequestPayload::SchedulerUpdate((
            Box::new(update.clone()),
            vec![],
        )))?;

        Ok(update)
    }

    /// Reconciles executor state when an executor is upserted
    /// Returns scheduler update that includes function executor reconciliation
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub fn reconcile_executor_state(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let executor = container_scheduler
            .executors
            .get(executor_id)
            .ok_or(anyhow!("executor not found"))?
            .clone();

        tracing::debug!(?executor, "reconciling executor state for executor",);

        // Create ExecutorServerMetadata if it doesn't exist
        if !container_scheduler
            .executor_states
            .contains_key(executor_id)
        {
            let executor_server_metadata = ExecutorServerMetadata {
                executor_id: executor_id.clone(),
                function_container_ids: std::collections::HashSet::new(),
                free_resources: executor.host_resources.clone(),
                resource_claims: std::collections::HashMap::new(),
            };
            update
                .updated_executor_states
                .insert(executor_id.clone(), Box::new(executor_server_metadata));
            container_scheduler.update(
                &crate::state_store::requests::RequestPayload::SchedulerUpdate((
                    Box::new(update.clone()),
                    vec![],
                )),
            )?;
        }

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(
            in_memory_state,
            container_scheduler,
            &executor,
        )?);

        Ok(update)
    }
}
