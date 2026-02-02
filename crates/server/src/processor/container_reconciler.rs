use std::sync::Arc;

use anyhow::{Result, anyhow};
use tracing::{info, warn};

use crate::{
    data_model::{
        Container,
        ContainerId,
        ContainerServerMetadata,
        ContainerState,
        ContainerType,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionExecutorTerminationReason,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        RunningFunctionRunStatus,
        SandboxFailureReason,
        SandboxKey,
        SandboxOutcome,
        SandboxStatus,
    },
    processor::{container_scheduler::ContainerScheduler, retry_policy::FunctionRunRetryPolicy},
    state_store::{
        IndexifyState,
        in_memory_state::{FunctionRunKey, InMemoryState},
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct ContainerReconciler {
    clock: u64,
    indexify_state: Arc<IndexifyState>,
}

/// Reconciles container state between executors and the server.
/// Handles container cleanup when executors are removed or containers
/// terminate.
impl ContainerReconciler {
    pub fn new(clock: u64, indexify_state: Arc<IndexifyState>) -> Self {
        Self {
            clock,
            indexify_state,
        }
    }

    /// Reconciles function executor state between executor and server
    #[tracing::instrument(skip(self, in_memory_state, container_scheduler, executor))]
    async fn reconcile_function_executors(
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
            .containers
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
            .filter(|fe_id| !executor.containers.contains_key(fe_id))
            .collect::<Vec<_>>();
        for container_id in containers_only_in_server {
            let Some(function_container) =
                container_scheduler.function_containers.get(container_id)
            else {
                continue;
            };
            if matches!(
                function_container.desired_state,
                ContainerState::Terminated { .. }
            ) {
                function_containers_to_remove.push(function_container.function_container.clone());
            }
        }
        for fe in containers_only_in_executor {
            // Skip terminated containers
            if matches!(fe.state, ContainerState::Terminated { .. }) {
                continue;
            }

            if fe.container_type == ContainerType::Sandbox {
                // For sandbox containers, container ID == sandbox ID
                let reader = self.indexify_state.reader();
                if let Ok(Some(sandbox)) = reader.get_sandbox(&fe.namespace, fe.id.get()).await &&
                    sandbox.status == SandboxStatus::Terminated
                {
                    warn!(
                        container_id = %fe.id,
                        namespace = %fe.namespace,
                        "Ignoring container from executor - associated sandbox is terminated"
                    );
                    continue;
                }
            }

            // Always track containers that exist on the executor, regardless of whether
            // our resource accounting shows enough free resources. The container is already
            // running and consuming resources - our tracking should reflect reality.
            // This may result in free_resources going negative, indicating overcommit.
            info!(
                executor_id = %executor.id,
                container_id = %fe.id,
                container_cpu_ms = %fe.resources.cpu_ms_per_sec,
                container_memory_mb = %fe.resources.memory_mb,
                free_cpu_before = %executor_server_metadata.free_resources.cpu_ms_per_sec,
                free_mem_before = %executor_server_metadata.free_resources.memory_bytes,
                "force_add_container: adopting untracked container from executor"
            );

            let existing_fe =
                ContainerServerMetadata::new(executor.id.clone(), fe.clone(), fe.state.clone());
            executor_server_metadata.force_add_container(&fe);

            info!(
                executor_id = %executor.id,
                container_id = %fe.id,
                free_cpu_after = %executor_server_metadata.free_resources.cpu_ms_per_sec,
                free_mem_after = %executor_server_metadata.free_resources.memory_bytes,
                num_containers = %executor_server_metadata.function_container_ids.len(),
                "force_add_container: container adopted"
            );

            update.updated_executor_states.insert(
                executor_server_metadata.executor_id.clone(),
                executor_server_metadata.clone(),
            );
            update.containers.insert(
                existing_fe.function_container.id.clone(),
                Box::new(existing_fe.clone()),
            );
        }
        container_scheduler.update(&RequestPayload::SchedulerUpdate((
            Box::new(update.clone()),
            vec![],
        )))?;
        for (executor_c_id, executor_c) in &executor.containers {
            // If the Executor FE is also in the server's tracked FE lets sync them.
            if let Some(server_c) = container_scheduler.function_containers.get(executor_c_id) {
                // If the executor's container state is Terminated lets remove it from the
                // server.
                if matches!(executor_c.state, ContainerState::Terminated { .. }) {
                    function_containers_to_remove.push(executor_c.clone());
                    executor_server_metadata.remove_container(executor_c)?;
                    continue;
                }

                // If the server's FE state is terminated we don't need to do anything heres
                if matches!(server_c.desired_state, ContainerState::Terminated { .. }) {
                    continue;
                }
                // Check if state changed
                if executor_c.state != server_c.function_container.state {
                    let mut server_c_clone = server_c.clone();
                    server_c_clone.function_container.update(executor_c);
                    update.containers.insert(
                        server_c_clone.function_container.id.clone(),
                        server_c_clone.clone(),
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

    /// Handles allocations for a terminated container by marking them as failed
    /// and updating their associated function runs according to retry policy.
    #[tracing::instrument(skip_all, target = "scheduler", fields(
        executor_id = %executor_id.get(),
        container_id = %container_id.get(),
        reason = %termination_reason
    ))]
    fn handle_allocations_for_container_termination(
        &self,
        in_memory_state: &InMemoryState,
        executor_id: &ExecutorId,
        container_id: &ContainerId,
        termination_reason: FunctionExecutorTerminationReason,
        blamed_alloc_ids: &[String],
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let allocs: Vec<_> = in_memory_state
            .allocations_by_executor
            .get(executor_id)
            .and_then(|allocs_by_fe| allocs_by_fe.get(container_id))
            .map(|allocs| allocs.values().cloned().collect())
            .unwrap_or_default();

        for alloc in allocs {
            let mut updated_alloc = alloc.as_ref().clone();

            let Some(function_run) = in_memory_state
                .function_runs
                .get(&FunctionRunKey::from(alloc.as_ref()))
                .cloned()
            else {
                info!(
                    allocation_id = %alloc.id,
                    request_id = %alloc.request_id,
                    namespace = %alloc.namespace,
                    app = %alloc.application,
                    fn_name = %alloc.function,
                    "function run not found, cancelling allocation"
                );
                update.cancel_allocation(&mut updated_alloc);
                continue;
            };

            let Some(mut ctx) = in_memory_state
                .request_ctx
                .get(&function_run.clone().into())
                .cloned()
            else {
                info!(
                    allocation_id = %alloc.id,
                    request_id = %alloc.request_id,
                    namespace = %alloc.namespace,
                    app = %alloc.application,
                    fn_name = %alloc.function,
                    "request context not found, cancelling allocation"
                );
                update.cancel_allocation(&mut updated_alloc);
                continue;
            };

            // Idempotency: only act on this alloc's function run if the function run
            // is currently running this allocation
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
                info!(
                    allocation_id = %alloc.id,
                    request_id = %alloc.request_id,
                    namespace = %alloc.namespace,
                    app = %alloc.application,
                    fn_name = %alloc.function,
                    "application version not found, cancelling allocation"
                );
                update.cancel_allocation(&mut updated_alloc);
                continue;
            };

            let mut function_run = *function_run;

            // Determine allocation outcome based on whether this allocation was blamed
            // for the termination or if the executor was removed
            if blamed_alloc_ids.contains(&updated_alloc.id.to_string()) ||
                termination_reason == FunctionExecutorTerminationReason::ExecutorRemoved
            {
                updated_alloc.outcome = FunctionRunOutcome::Failure((&termination_reason).into());
            } else {
                // This allocation wasn't blamed for the FE termination,
                // retry without involving the function run retry policy but still fail the
                // alloc
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
                fn_name = %updated_alloc.function,
                allocation_outcome = %updated_alloc.outcome,
                fn_run_status = %function_run.status,
                fn_run_outcome = ?function_run.outcome.as_ref(),
                "handled allocation for container termination"
            );

            update.updated_allocations.push(updated_alloc);

            let is_completed = function_run.status == FunctionRunStatus::Completed;
            let fn_run_outcome = function_run.outcome.clone();
            update.add_function_run(function_run, &mut ctx);

            // FIXME - At the moment if any function is marked as completed after we have to
            // remove their allocation because a container crashed it means we are not
            // giving blocking functions a chance to handle failures. We need to
            // fix this.
            if is_completed {
                ctx.outcome = fn_run_outcome.map(|o| o.into());
                update
                    .updated_request_states
                    .insert(ctx.key(), *ctx.clone());
            }
        }

        Ok(update)
    }

    /// Removes function executors and handles associated function run cleanup
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_server_metadata.executor_id.get(), num_function_executors = function_containers.len()))]
    fn remove_function_containers(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_containers: Vec<Container>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if function_containers.is_empty() {
            return Ok(update);
        }

        // Handle allocations and sandboxes for FEs to be removed, updating
        // in_memory_state after each container to ensure subsequent containers
        // see the updated RequestCtx state.
        for fe in &function_containers {
            info!(
                namespace = fe.namespace,
                app = fe.application_name,
                fn_name = fe.function_name,
                fn_executor_id = fe.id.get(),
                fe_state = ?fe.state,
                "removing function container from executor",
            );

            if let ContainerState::Terminated {
                reason,
                failed_alloc_ids,
            } = &fe.state
            {
                // Container is terminated - use the allocation termination handler
                let container_update = self.handle_allocations_for_container_termination(
                    in_memory_state,
                    &executor_server_metadata.executor_id,
                    &fe.id,
                    *reason,
                    failed_alloc_ids,
                )?;

                in_memory_state.update_state(
                    self.clock,
                    &RequestPayload::SchedulerUpdate((Box::new(container_update.clone()), vec![])),
                    "container_reconciler_remove_fc",
                )?;

                update.extend(container_update);
            }

            // Terminate associated sandbox
            let sandbox_update = self.terminate_sandbox_for_container(in_memory_state, fe)?;

            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(sandbox_update.clone()), vec![])),
                "container_reconciler_remove_fc_sandbox",
            )?;

            update.extend(sandbox_update);
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

    /// Terminates sandbox associated with a container when the container
    /// terminates. Uses container.sandbox_id to find the associated sandbox.
    fn terminate_sandbox_for_container(
        &self,
        in_memory_state: &InMemoryState,
        container: &Container,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Use the container's sandbox_id to find the associated sandbox
        let Some(sandbox_id) = &container.sandbox_id else {
            return Ok(update); // Container not associated with a sandbox
        };

        let sandbox_key = SandboxKey::new(&container.namespace, sandbox_id.get());
        let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key) else {
            return Ok(update); // Sandbox not found (may have already been terminated)
        };

        if sandbox.status != SandboxStatus::Running {
            return Ok(update); // Already terminated
        }

        info!(
            sandbox_id = %sandbox.id,
            namespace = %sandbox.namespace,
            container_id = %container.id,
            "terminating sandbox due to container termination"
        );

        let mut terminated_sandbox = sandbox.as_ref().clone();
        terminated_sandbox.status = SandboxStatus::Terminated;

        // Determine outcome based on container termination reason
        terminated_sandbox.outcome = match &container.state {
            ContainerState::Terminated { reason, .. } => Some(SandboxOutcome::Failure(
                SandboxFailureReason::ContainerTerminated(*reason),
            )),
            _ => Some(SandboxOutcome::Failure(SandboxFailureReason::Unknown)),
        };

        update
            .updated_sandboxes
            .insert(sandbox_key, terminated_sandbox);

        Ok(update)
    }

    /// Removes all function executors from an executor when it's being
    /// deregistered. Handles allocations and sandboxes uniformly whether or not
    /// the executor exists in container_scheduler.
    /// Note: Does not update executor metadata since the executor is being
    /// removed.
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_id.get()))]
    fn remove_all_function_executors_for_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();

        // Get container IDs from executor state if available, otherwise from
        // allocations
        let container_ids: Vec<_> = if let Some(executor_meta) =
            container_scheduler.executor_states.get(executor_id)
        {
            executor_meta
                .function_container_ids
                .iter()
                .cloned()
                .collect()
        } else {
            warn!(
                "executor not found in container_scheduler, deriving containers from allocations"
            );
            in_memory_state
                .allocations_by_executor
                .get(executor_id)
                .map(|allocs| allocs.keys().cloned().collect())
                .unwrap_or_default()
        };

        // Handle allocations and sandboxes for each container
        for container_id in &container_ids {
            // Handle allocations for this container
            let container_update = self.handle_allocations_for_container_termination(
                in_memory_state,
                executor_id,
                container_id,
                FunctionExecutorTerminationReason::ExecutorRemoved,
                &[],
            )?;

            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(container_update.clone()), vec![])),
                "container_reconciler_per_container",
            )?;

            scheduler_update.extend(container_update);

            let terminated_state = ContainerState::Terminated {
                reason: FunctionExecutorTerminationReason::ExecutorRemoved,
                failed_alloc_ids: vec![],
            };
            if let Some(fc) = container_scheduler.function_containers.get(container_id) {
                let mut terminated_fc = *fc.clone();
                terminated_fc.desired_state = terminated_state.clone();
                terminated_fc.function_container.state = terminated_state.clone();

                let container_term_update = SchedulerUpdateRequest {
                    containers: [(container_id.clone(), Box::new(terminated_fc.clone()))].into(),
                    ..Default::default()
                };
                scheduler_update.extend(container_term_update.clone());
                container_scheduler.update(&RequestPayload::SchedulerUpdate((
                    Box::new(container_term_update),
                    vec![],
                )))?;

                // Terminate associated sandbox
                let sandbox_update = self.terminate_sandbox_for_container(
                    in_memory_state,
                    &terminated_fc.function_container,
                )?;

                in_memory_state.update_state(
                    self.clock,
                    &RequestPayload::SchedulerUpdate((Box::new(sandbox_update.clone()), vec![])),
                    "container_reconciler_per_container_sandbox",
                )?;

                scheduler_update.extend(sandbox_update);
            }
        }

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

        // Terminate all sandboxes running on this executor FIRST
        // This ensures sandboxes get the ExecutorRemoved reason, not
        // ContainerTerminated
        let sandbox_update = self.terminate_sandboxes_for_executor(in_memory_state, executor_id)?;
        update.extend(sandbox_update.clone());

        // Apply sandbox updates to in_memory_state so terminate_sandbox_for_container
        // sees the updated status and skips already-terminated sandboxes
        if !sandbox_update.updated_sandboxes.is_empty() {
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate((Box::new(sandbox_update), vec![])),
                "container_reconciler_sandbox_termination",
            )?;
        }

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

    /// Terminates all sandboxes running on an executor when it's deregistered
    fn terminate_sandboxes_for_executor(
        &self,
        in_memory_state: &InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Find all sandboxes running on this executor
        for (sandbox_key, sandbox) in in_memory_state.sandboxes.iter() {
            if let Some(ref sandbox_executor_id) = sandbox.executor_id &&
                sandbox_executor_id == executor_id &&
                sandbox.status == SandboxStatus::Running
            {
                info!(
                    sandbox_id = %sandbox.id,
                    namespace = %sandbox.namespace,
                    executor_id = %executor_id,
                    "terminating sandbox due to executor deregistration"
                );

                let mut terminated_sandbox = sandbox.as_ref().clone();
                terminated_sandbox.status = SandboxStatus::Terminated;
                terminated_sandbox.outcome = Some(SandboxOutcome::Failure(
                    SandboxFailureReason::ExecutorRemoved,
                ));

                update
                    .updated_sandboxes
                    .insert(sandbox_key.clone(), terminated_sandbox);
            }
        }

        Ok(update)
    }

    /// Reconciles executor state when an executor is upserted
    /// Returns scheduler update that includes function executor reconciliation
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = executor_id.get()))]
    pub async fn reconcile_executor_state(
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
            info!(
                executor_id = %executor_id,
                host_cpu_ms = %executor.host_resources.cpu_ms_per_sec,
                host_memory_bytes = %executor.host_resources.memory_bytes,
                "Creating new ExecutorServerMetadata with full host resources as free"
            );
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
        } else {
            let existing = container_scheduler
                .executor_states
                .get(executor_id)
                .unwrap();
            info!(
                executor_id = %executor_id,
                free_cpu_ms = %existing.free_resources.cpu_ms_per_sec,
                free_memory_bytes = %existing.free_resources.memory_bytes,
                num_containers = %existing.function_container_ids.len(),
                "Using existing ExecutorServerMetadata"
            );
        }

        // Reconcile function executors
        update.extend(
            self.reconcile_function_executors(in_memory_state, container_scheduler, &executor)
                .await?,
        );

        Ok(update)
    }
}
