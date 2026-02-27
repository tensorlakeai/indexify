use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info, warn};

use crate::{
    data_model::{
        Allocation,
        Container,
        ContainerId,
        ContainerServerMetadata,
        ContainerState,
        ContainerTerminationReason,
        ContainerType,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionRunOutcome,
        FunctionRunStatus,
        RunningFunctionRunStatus,
        Sandbox,
        SandboxFailureReason,
        SandboxKey,
        SandboxOutcome,
        SandboxStatus,
        SandboxSuccessReason,
    },
    processor::{container_scheduler::ContainerScheduler, retry_policy::FunctionRunRetryPolicy},
    state_store::{
        IndexifyState,
        in_memory_state::{FunctionRunKey, InMemoryState},
        requests::{RequestPayload, SchedulerUpdatePayload, SchedulerUpdateRequest},
    },
};

pub struct ContainerReconciler {
    clock: u64,
    _indexify_state: Arc<IndexifyState>,
}

/// Reconciles container state between executors and the server.
/// Handles container cleanup when executors are removed or containers
/// terminate.
impl ContainerReconciler {
    pub fn new(clock: u64, indexify_state: Arc<IndexifyState>) -> Self {
        Self {
            clock,
            _indexify_state: indexify_state,
        }
    }

    /// Reconciles function executor state between executor and server
    #[tracing::instrument(skip(self, in_memory_state, container_scheduler, executor))]
    async fn reconcile_function_containers(
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
            error!(
                "ExecutorServerMetadata not found for executor: {:?}, but should have been created by reconcile_executor_state",
                executor.id
            );
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
        let mut containers_adopted = false;
        for fe in containers_only_in_executor {
            // Skip terminated containers
            if matches!(fe.state, ContainerState::Terminated { .. }) {
                continue;
            }

            // Check 1: Pool must exist. Standalone sandbox containers have
            // pool_id=None and don't need a pool.
            let pool_exists = match fe.pool_key() {
                Some(key) => container_scheduler.get_pool(&key).is_some(),
                None => true,
            };

            // Check 2: If sandbox assigned, it must be Running or Pending
            let sandbox_valid = match &fe.sandbox_id {
                Some(sandbox_id) => {
                    let sandbox_key = SandboxKey::new(&fe.namespace, sandbox_id.get());
                    in_memory_state
                        .sandboxes
                        .get(&sandbox_key)
                        .is_some_and(|s| {
                            matches!(
                                s.status,
                                SandboxStatus::Running | SandboxStatus::Pending { .. }
                            )
                        })
                }
                None => true,
            };

            // Check 3: Application version and function must exist (only for function
            // containers)
            let app_fn_exists = fe.container_type == ContainerType::Sandbox ||
                in_memory_state
                    .application_version(&fe.namespace, &fe.application_name, &fe.version)
                    .is_some_and(|av| av.functions.contains_key(&fe.function_name));

            if !pool_exists || !sandbox_valid || !app_fn_exists {
                info!(
                    executor_id = %executor.id,
                    container_id = %fe.id,
                    sandbox_id = ?fe.sandbox_id,
                    pool_exists,
                    sandbox_valid,
                    app_fn_exists,
                    "rejecting untracked container from executor — purpose no longer exists"
                );
                let terminated = ContainerServerMetadata::new(
                    executor.id.clone(),
                    fe.clone(),
                    ContainerState::Terminated {
                        reason: ContainerTerminationReason::DesiredStateRemoved,
                    },
                );
                executor_server_metadata.force_add_container(&fe);
                update
                    .containers
                    .insert(fe.id.clone(), Box::new(terminated));
                containers_adopted = true;
                continue;
            }

            // Adopt the container — it passed all validation checks.
            // Track it regardless of whether our resource accounting shows enough
            // free resources. The container is already running and consuming
            // resources — our tracking should reflect reality.
            info!(
                executor_id = %executor.id,
                container_id = %fe.id,
                sandbox_id = ?fe.sandbox_id,
                "adopting untracked container from executor"
            );

            let existing_fe =
                ContainerServerMetadata::new(executor.id.clone(), fe.clone(), fe.state.clone());
            executor_server_metadata.force_add_container(&fe);
            update.containers.insert(
                existing_fe.function_container.id.clone(),
                Box::new(existing_fe.clone()),
            );
            containers_adopted = true;
        }

        // Update executor metadata and apply adoption data so the sync loop
        // below can find adopted containers in function_containers.
        if containers_adopted {
            update.updated_executor_states.insert(
                executor_server_metadata.executor_id.clone(),
                executor_server_metadata.clone(),
            );

            // Apply only the adoption subset (containers + executor states) so
            // the sync loop can look up adopted containers. The full update
            // (including sync/promote/remove data) is applied in the final write.
            let adoption_update = SchedulerUpdateRequest {
                containers: update.containers.clone(),
                updated_executor_states: update.updated_executor_states.clone(),
                ..Default::default()
            };
            container_scheduler.update(&RequestPayload::SchedulerUpdate(
                SchedulerUpdatePayload::new(adoption_update),
            ))?;
        }

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

                // If the server's FE state is terminated we don't need to do anything here
                if matches!(server_c.desired_state, ContainerState::Terminated { .. }) {
                    continue;
                }
                // Sync state if changed
                if executor_c.state != server_c.function_container.state {
                    let mut server_c_clone = server_c.clone();
                    server_c_clone.function_container.update(executor_c);
                    update.containers.insert(
                        server_c_clone.function_container.id.clone(),
                        server_c_clone.clone(),
                    );
                }

                // Promote sandbox from Pending → Running if container is now Running.
                // Uses the server-side container (which has sandbox_id) with the
                // executor-reported state (heartbeat doesn't carry sandbox_id).
                // This also handles warm pool containers that were already Running.
                let promote_update = self.promote_sandbox_if_container_running(
                    in_memory_state,
                    &server_c.function_container,
                    &executor_c.state,
                )?;
                update.extend(promote_update);
            }
        }

        // Add container removals to main update
        update.extend(self.remove_function_containers(
            in_memory_state,
            container_scheduler,
            &mut executor_server_metadata,
            function_containers_to_remove,
        )?);

        // Apply all updates atomically to both container_scheduler and in_memory_state
        let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update.clone()));
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
    pub fn handle_allocations_for_container_termination(
        &self,
        in_memory_state: &InMemoryState,
        executor_id: &ExecutorId,
        container_id: &ContainerId,
        termination_reason: ContainerTerminationReason,
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
                    "fn" = %alloc.function,
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
                    "fn" = %alloc.function,
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
                    "fn" = %alloc.function,
                    "application version not found, cancelling allocation"
                );
                update.cancel_allocation(&mut updated_alloc);
                continue;
            };

            let mut function_run = *function_run;

            updated_alloc.outcome = FunctionRunOutcome::Failure((&termination_reason).into());

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
                "fn" = %updated_alloc.function,
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
            }
            update.add_request_state(&ctx);
        }

        Ok(update)
    }

    /// Handles orphaned containers on first executor heartbeat after server
    /// restart. Detects allocations/sandboxes for containers that no longer
    /// exist on the executor.
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor.id.get()))]
    fn handle_orphaned_containers_on_first_heartbeat(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut total_update = SchedulerUpdateRequest::default();

        // Find allocations with missing containers
        let orphaned_allocs: Vec<Box<Allocation>> = in_memory_state
            .allocations_by_executor
            .get(&executor.id)
            .map(|allocations_by_container| {
                allocations_by_container
                    .iter()
                    .filter(|(container_id, _)| {
                        !executor.containers.contains_key(container_id) &&
                            !container_scheduler
                                .function_containers
                                .contains_key(container_id)
                    })
                    .flat_map(|(_, allocs)| allocs.values().cloned())
                    .collect()
            })
            .unwrap_or_default();

        // Handle orphaned allocations - group by container to avoid duplicates
        for alloc in orphaned_allocs {
            let container_update = self.handle_allocations_for_container_termination(
                in_memory_state,
                &alloc.target.executor_id,
                &alloc.target.container_id,
                ContainerTerminationReason::Unknown,
            )?;

            let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                container_update.clone(),
            ));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(
                self.clock,
                &payload,
                "container_reconciler_orphaned_alloc",
            )?;

            total_update.extend(container_update);
        }

        // Find and handle running/pending sandboxes with missing containers
        let orphaned_sandboxes: Vec<Box<Sandbox>> = in_memory_state
            .sandboxes_by_executor
            .get(&executor.id)
            .map(|sandbox_keys| {
                sandbox_keys
                    .iter()
                    .filter_map(|sandbox_key| in_memory_state.sandboxes.get(sandbox_key).cloned())
                    .filter(|sandbox| {
                        matches!(
                            sandbox.status,
                            SandboxStatus::Running | SandboxStatus::Pending { .. }
                        ) && sandbox.container_id.as_ref().is_some_and(|container_id| {
                            !executor.containers.contains_key(container_id) &&
                                !container_scheduler
                                    .function_containers
                                    .contains_key(container_id)
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        for sandbox in orphaned_sandboxes {
            let mut updated_sandbox = sandbox.as_ref().clone();
            updated_sandbox.status = SandboxStatus::Terminated;
            updated_sandbox.outcome = Some(SandboxOutcome::Failure(
                SandboxFailureReason::ContainerTerminated(ContainerTerminationReason::Unknown),
            ));

            let mut sandbox_update = SchedulerUpdateRequest::default();
            sandbox_update
                .updated_sandboxes
                .insert(SandboxKey::from_sandbox(&updated_sandbox), updated_sandbox);

            let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                sandbox_update.clone(),
            ));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(
                self.clock,
                &payload,
                "container_reconciler_orphaned_sandbox",
            )?;

            total_update.extend(sandbox_update);
        }

        Ok(total_update)
    }

    /// Removes function executors and handles associated function run cleanup
    /// Applies incremental updates to both container_scheduler and
    /// in_memory_state
    #[tracing::instrument(skip_all, target = "scheduler", fields(executor_id = %executor_server_metadata.executor_id.get(), num_function_executors = function_containers.len()))]
    fn remove_function_containers(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
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
                "fn" = fe.function_name,
                container_id = fe.id.get(),
                sandbox_id = ?fe.sandbox_id,
                container_state = ?fe.state,
                "removing function container from executor",
            );

            if let ContainerState::Terminated { reason } = &fe.state {
                // Container is terminated - use the allocation termination handler
                let container_update = self.handle_allocations_for_container_termination(
                    in_memory_state,
                    &executor_server_metadata.executor_id,
                    &fe.id,
                    *reason,
                )?;

                // Apply incremental updates to both stores
                let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                    container_update.clone(),
                ));
                container_scheduler.update(&payload)?;
                in_memory_state.update_state(
                    self.clock,
                    &payload,
                    "container_reconciler_remove_fc",
                )?;

                update.extend(container_update);
            }

            // Terminate associated sandbox
            let sandbox_update = self.terminate_sandbox_for_container(in_memory_state, fe)?;

            // Apply incremental updates to both stores
            let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                sandbox_update.clone(),
            ));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(
                self.clock,
                &payload,
                "container_reconciler_remove_fc_sandbox",
            )?;

            update.extend(sandbox_update);
        }

        for fc in function_containers {
            // Mark container as terminated in the update so the scheduler's
            // executor state merge logic knows to drop it rather than preserve
            // it from the old state. Use the container's actual termination
            // reason if available.
            let desired_state = match &fc.state {
                ContainerState::Terminated { .. } => fc.state.clone(),
                _ => ContainerState::Terminated {
                    reason: ContainerTerminationReason::DesiredStateRemoved,
                },
            };
            let terminated_meta = ContainerServerMetadata::new(
                executor_server_metadata.executor_id.clone(),
                fc.clone(),
                desired_state,
            );
            update
                .containers
                .insert(fc.id.clone(), Box::new(terminated_meta));
            executor_server_metadata.remove_container(&fc)?;
        }
        update.updated_executor_states.insert(
            executor_server_metadata.executor_id.clone(),
            Box::new(executor_server_metadata.clone()),
        );
        Ok(update)
    }

    /// Promotes a sandbox from Pending to Running when its container reports
    /// Running state via heartbeat.
    ///
    /// Takes the server-side container (which has sandbox_id) and the
    /// executor-reported state separately, because the heartbeat container
    /// doesn't carry sandbox_id.
    fn promote_sandbox_if_container_running(
        &self,
        in_memory_state: &InMemoryState,
        server_container: &Container,
        executor_state: &ContainerState,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let Some(sandbox_id) = &server_container.sandbox_id else {
            return Ok(update);
        };

        if !matches!(executor_state, ContainerState::Running) {
            return Ok(update);
        }

        let sandbox_key = SandboxKey::new(&server_container.namespace, sandbox_id.get());
        let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key) else {
            return Ok(update);
        };

        if !sandbox.status.is_pending() {
            return Ok(update);
        }

        info!(
            sandbox_id = %sandbox.id,
            namespace = %sandbox.namespace,
            container_id = %server_container.id,
            "promoting sandbox from Pending to Running — container is running"
        );

        let mut promoted_sandbox = sandbox.as_ref().clone();
        promoted_sandbox.status = SandboxStatus::Running;

        update
            .updated_sandboxes
            .insert(sandbox_key, promoted_sandbox);

        Ok(update)
    }

    /// Promotes a container from Pending to Running when the dataplane reports
    /// ContainerStarted, and promotes the associated sandbox (if any) as well.
    pub fn promote_sandbox_for_started_container(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &ContainerScheduler,
        container_id: &ContainerId,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(server_meta) = container_scheduler.function_containers.get(container_id) else {
            return Ok(SchedulerUpdateRequest::default());
        };
        let mut update = SchedulerUpdateRequest::default();

        // Update the container's own state to Running if it's still Pending.
        if matches!(
            server_meta.function_container.state,
            ContainerState::Pending
        ) {
            let mut updated_meta = server_meta.clone();
            updated_meta.function_container.state = ContainerState::Running;
            update.containers.insert(container_id.clone(), updated_meta);
        }

        // Promote the associated sandbox from Pending to Running (if any).
        let sandbox_update = self.promote_sandbox_if_container_running(
            in_memory_state,
            &server_meta.function_container,
            &ContainerState::Running,
        )?;
        update.extend(sandbox_update);

        Ok(update)
    }

    /// Terminates sandbox associated with a container when the container
    /// terminates. Uses container.sandbox_id to find the associated sandbox.
    pub fn terminate_sandbox_for_container(
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

        if !matches!(
            sandbox.status,
            SandboxStatus::Running | SandboxStatus::Pending { .. }
        ) {
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

        // Determine outcome based on container termination reason.
        // FunctionTimeout means the sandbox ran until its timeout expired —
        // this is normal/expected, not a failure.
        terminated_sandbox.outcome = match &container.state {
            ContainerState::Terminated { reason, .. }
                if *reason == ContainerTerminationReason::FunctionTimeout =>
            {
                Some(SandboxOutcome::Success(SandboxSuccessReason::Timeout))
            }
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
        let container_ids = if let Some(executor_meta) =
            container_scheduler.executor_states.get(executor_id)
        {
            executor_meta
                .function_container_ids
                .iter()
                .cloned()
                .collect::<Vec<ContainerId>>()
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
                ContainerTerminationReason::ExecutorRemoved,
            )?;

            let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                container_update.clone(),
            ));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(
                self.clock,
                &payload,
                "container_reconciler_per_container",
            )?;

            scheduler_update.extend(container_update);

            let terminated_state = ContainerState::Terminated {
                reason: ContainerTerminationReason::ExecutorRemoved,
            };
            if let Some(fc) = container_scheduler.function_containers.get(container_id) {
                let mut terminated_fc = *fc.clone();
                terminated_fc.desired_state = terminated_state.clone();
                terminated_fc.function_container.state = terminated_state.clone();

                let container_term_update = SchedulerUpdateRequest {
                    containers: std::iter::once((
                        container_id.clone(),
                        Box::new(terminated_fc.clone()),
                    ))
                    .collect(),
                    ..Default::default()
                };
                scheduler_update.extend(container_term_update.clone());
                container_scheduler.update(&RequestPayload::SchedulerUpdate(
                    SchedulerUpdatePayload::new(container_term_update),
                ))?;

                // Terminate associated sandbox
                let sandbox_update = self.terminate_sandbox_for_container(
                    in_memory_state,
                    &terminated_fc.function_container,
                )?;

                let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                    sandbox_update.clone(),
                ));
                container_scheduler.update(&payload)?;
                in_memory_state.update_state(
                    self.clock,
                    &payload,
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

        // Apply sandbox updates to both stores so terminate_sandbox_for_container
        // sees the updated status and skips already-terminated sandboxes
        if !sandbox_update.updated_sandboxes.is_empty() {
            let payload =
                RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(sandbox_update));
            container_scheduler.update(&payload)?;
            in_memory_state.update_state(
                self.clock,
                &payload,
                "container_reconciler_sandbox_termination",
            )?;
        }

        // Remove all function executors for this executor
        update.extend(self.remove_all_function_executors_for_executor(
            in_memory_state,
            container_scheduler,
            executor_id,
        )?);

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
                matches!(
                    sandbox.status,
                    SandboxStatus::Running | SandboxStatus::Pending { .. }
                )
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
        let Some(executor) = container_scheduler.executors.get(executor_id).cloned() else {
            error!(
                executor_id = %executor_id,
                "trying to reconcile executor state for a non-existent executor"
            );
            return Ok(update);
        };
        tracing::debug!(?executor, "reconciling executor state for executor",);

        // Create ExecutorServerMetadata if it doesn't exist
        let is_first_heartbeat = !container_scheduler
            .executor_states
            .contains_key(executor_id);

        if is_first_heartbeat {
            info!(
                executor_id = %executor_id,
                "creating executor server metadata"
            );
            let executor_server_metadata = ExecutorServerMetadata {
                executor_id: executor_id.clone(),
                function_container_ids: imbl::HashSet::new(),
                free_resources: executor.host_resources.clone(),
                resource_claims: imbl::HashMap::new(),
            };
            update
                .updated_executor_states
                .insert(executor_id.clone(), Box::new(executor_server_metadata));
            container_scheduler.update(
                &crate::state_store::requests::RequestPayload::SchedulerUpdate(
                    crate::state_store::requests::SchedulerUpdatePayload::new(update.clone()),
                ),
            )?;
        }

        // Reconcile function executors
        update.extend(
            self.reconcile_function_containers(in_memory_state, container_scheduler, &executor)
                .await?,
        );

        // On first heartbeat after server restart, check for orphaned containers
        if is_first_heartbeat {
            update.extend(self.handle_orphaned_containers_on_first_heartbeat(
                in_memory_state,
                container_scheduler,
                &executor,
            )?);
        }

        Ok(update)
    }
}
