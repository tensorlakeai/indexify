use anyhow::Result;
use tracing::{info, warn};

use crate::{
    data_model::{
        ContainerPoolKey,
        ContainerState,
        Sandbox,
        SandboxFailureReason,
        SandboxKey,
        SandboxOutcome,
        SandboxPendingReason,
        SandboxStatus,
        SandboxSuccessReason,
    },
    processor::container_scheduler::{self, ContainerScheduler},
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdatePayload, SchedulerUpdateRequest},
    },
};

pub struct SandboxProcessor {
    clock: u64,
}

impl SandboxProcessor {
    pub fn new(clock: u64) -> Self {
        Self { clock }
    }

    /// Allocate all pending sandboxes that can be scheduled
    pub fn allocate_sandboxes(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Early exit if no executors are available
        if container_scheduler.executors.is_empty() {
            return Ok(update);
        }

        // Collect pending sandbox keys to avoid borrowing issues
        let pending_keys: Vec<SandboxKey> =
            in_memory_state.pending_sandboxes.iter().cloned().collect();

        for sandbox_key in pending_keys {
            let sandbox = if let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key) {
                sandbox.as_ref().clone()
            } else {
                continue;
            };

            match self.allocate_sandbox(in_memory_state, container_scheduler, &sandbox) {
                Ok(sandbox_update) => {
                    update.extend(sandbox_update);
                }
                Err(err) => {
                    warn!(
                        sandbox_id = %sandbox.id,
                        namespace = %sandbox.namespace,
                        pool_id = sandbox.pool_id.as_ref().map(|id| id.get()).unwrap_or(""),
                        container_id = sandbox.container_id.as_ref().map(|id| id.get()).unwrap_or(""),
                        error = %err,
                        "Failed to allocate sandbox"
                    );
                }
            }
        }

        Ok(update)
    }

    /// Allocate a specific sandbox by key
    pub fn allocate_sandbox_by_key(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        namespace: &str,
        sandbox_id: &str,
    ) -> Result<SchedulerUpdateRequest> {
        let sandbox_key = SandboxKey::new(namespace, sandbox_id);
        let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key).cloned() else {
            return Ok(SchedulerUpdateRequest::default());
        };

        self.allocate_sandbox(in_memory_state, container_scheduler, &sandbox)
    }

    /// Allocate a single sandbox
    #[tracing::instrument(skip_all, fields(
        namespace = %sandbox.namespace,
        sandbox_id = %sandbox.id
    ))]
    fn allocate_sandbox(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        sandbox: &Sandbox,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Skip if sandbox is not pending
        if !sandbox.status.is_pending() {
            return Ok(update);
        }

        // Skip if a container has already been scheduled for this sandbox.
        // It is waiting for the container to report Running and should not
        // be re-allocated — doing so would claim a duplicate pool slot.
        if sandbox.has_scheduled() {
            return Ok(update);
        }

        // If sandbox has a pool_id, try to claim from pool first
        if let Some(pool_id) = &sandbox.pool_id {
            let container_pool_key = ContainerPoolKey::new(&sandbox.namespace, pool_id);

            // Try to claim a warm container from the pool
            if let Some(claim_update) = self.try_claim_from_pool(
                in_memory_state,
                container_scheduler,
                sandbox,
                &container_pool_key,
            )? {
                return Ok(claim_update);
            }

            // No warm container available - check if we can create on-demand
            if let Some(pool) = container_scheduler.get_pool(&container_pool_key) {
                let (claimed, warm) =
                    container_scheduler.count_pool_containers(&container_pool_key);
                let current = claimed + warm;

                if let Some(max) = pool.max_containers &&
                    current >= max
                {
                    warn!(
                        sandbox_id = %sandbox.id,
                        pool_id = %pool_id,
                        current = current,
                        max = max,
                        "Pool at capacity, cannot create sandbox"
                    );
                    // Pool at capacity - keep sandbox pending with reason
                    Self::set_pending_reason(
                        &mut update,
                        sandbox,
                        SandboxPendingReason::PoolAtCapacity,
                    );
                    return Ok(update);
                }
            }
            // Fall through to create on-demand
        }

        // Resolve snapshot URI if the sandbox was created from a snapshot
        let snapshot_uri = sandbox.snapshot_id.as_ref().and_then(|snap_id| {
            let key = crate::data_model::SnapshotKey::new(&sandbox.namespace, snap_id.get());
            in_memory_state
                .snapshots
                .get(&key)
                .and_then(|s| s.snapshot_uri.clone())
        });

        // Try to create a container for the sandbox using consolidated method
        match container_scheduler.create_container_for_sandbox(sandbox, snapshot_uri.as_deref()) {
            Ok(Some(container_update)) => {
                // Find the newly placed container, skipping any vacuum-marked
                // containers that have Terminated desired state.
                if let Some(fc_metadata) = container_update
                    .containers
                    .values()
                    .find(|c| !matches!(c.desired_state, ContainerState::Terminated { .. }))
                {
                    // Container created successfully, keep sandbox Pending until container
                    // reports Running via heartbeat
                    let mut updated_sandbox = sandbox.clone();
                    updated_sandbox.executor_id = Some(fc_metadata.executor_id.clone());
                    updated_sandbox.container_id = Some(fc_metadata.function_container.id.clone());
                    updated_sandbox.status = SandboxStatus::Pending {
                        reason: SandboxPendingReason::WaitingForContainer,
                    };

                    let sandbox_key = SandboxKey::from(&updated_sandbox);
                    update
                        .updated_sandboxes
                        .insert(sandbox_key, updated_sandbox);

                    info!(
                        sandbox_id = %sandbox.id,
                        namespace = %sandbox.namespace,
                        pool_id = sandbox.pool_id.as_ref().map(|id| id.get()).unwrap_or(""),
                        container_id = %fc_metadata.function_container.id,
                        "Sandbox allocated successfully"
                    );

                    update.extend(container_update);

                    // Apply update to local copies so subsequent iterations see the changes
                    let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                        update.clone(),
                    ));
                    container_scheduler.update(&payload)?;
                    in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;
                } else {
                    // No container created (vacuum may have run but no host available)
                    // Apply any vacuum updates
                    if !container_update.containers.is_empty() ||
                        !container_update.updated_executor_states.is_empty()
                    {
                        let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                            container_update.clone(),
                        ));
                        container_scheduler.update(&payload)?;
                        in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;
                        update.extend(container_update);
                    }

                    // Distinguish between "no executors exist" and "executors
                    // exist but lack capacity" by checking the executor list.
                    if container_scheduler.has_executors() {
                        info!(
                            sandbox_id = %sandbox.id,
                            namespace = %sandbox.namespace,
                            pool_id = sandbox.pool_id.as_ref().map(|id| id.get()).unwrap_or(""),
                            "No resources available for sandbox, keeping as pending"
                        );
                        Self::set_pending_reason(
                            &mut update,
                            sandbox,
                            SandboxPendingReason::NoResourcesAvailable,
                        );
                    } else {
                        info!(
                            sandbox_id = %sandbox.id,
                            namespace = %sandbox.namespace,
                            pool_id = sandbox.pool_id.as_ref().map(|id| id.get()).unwrap_or(""),
                            "No executors available for sandbox, keeping as pending"
                        );
                        Self::set_pending_reason(
                            &mut update,
                            sandbox,
                            SandboxPendingReason::NoExecutorsAvailable,
                        );
                    };
                }
            }
            Ok(None) => {
                // No executors available at all
                Self::set_pending_reason(
                    &mut update,
                    sandbox,
                    SandboxPendingReason::NoExecutorsAvailable,
                );

                info!(
                    sandbox_id = %sandbox.id,
                    namespace = %sandbox.namespace,
                    "No executors available for sandbox, keeping as pending"
                );
            }
            Err(err) => {
                // Check if this is a ConstraintUnsatisfiable error
                if let Some(scheduler_error) = err.downcast_ref::<container_scheduler::Error>() &&
                    matches!(
                        scheduler_error,
                        container_scheduler::Error::ConstraintUnsatisfiable { .. }
                    )
                {
                    // Mark sandbox as terminated with ConstraintUnsatisfiable reason
                    let mut failed_sandbox = sandbox.clone();
                    failed_sandbox.status = SandboxStatus::Terminated;
                    failed_sandbox.outcome = Some(SandboxOutcome::Failure(
                        SandboxFailureReason::ConstraintUnsatisfiable,
                    ));

                    let sandbox_key = SandboxKey::from(&failed_sandbox);
                    update.updated_sandboxes.insert(sandbox_key, failed_sandbox);

                    warn!(
                        sandbox_id = %sandbox.id,
                        namespace = %sandbox.namespace,
                        pool_id = sandbox.pool_id.as_ref().map(|id| id.get()).unwrap_or(""),
                        "Sandbox allocation failed: constraint unsatisfiable"
                    );

                    // Apply update to local copies
                    let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                        update.clone(),
                    ));
                    container_scheduler.update(&payload)?;
                    in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;
                }
                return Err(err);
            }
        }

        Ok(update)
    }

    pub fn terminate_sandbox(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        namespace: &str,
        sandbox_id: &str,
    ) -> Result<SchedulerUpdateRequest> {
        let sandbox_key = SandboxKey::new(namespace, sandbox_id);
        let mut update = SchedulerUpdateRequest::default();

        let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key) else {
            return Ok(update);
        };

        if matches!(
            sandbox.status,
            SandboxStatus::Running | SandboxStatus::Pending { .. }
        ) && let Some(container_id) = &sandbox.container_id &&
            let Some(container_update) = container_scheduler.terminate_container(container_id)?
        {
            update.extend(container_update);
        }

        let mut terminated_sandbox = sandbox.as_ref().clone();
        terminated_sandbox.status = SandboxStatus::Terminated;
        terminated_sandbox.outcome = Some(SandboxOutcome::Success(
            SandboxSuccessReason::UserTerminated,
        ));
        update
            .updated_sandboxes
            .insert(sandbox_key, terminated_sandbox);

        Ok(update)
    }

    /// Try to claim a warm container from a sandbox pool
    fn try_claim_from_pool(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        sandbox: &Sandbox,
        pool_key: &ContainerPoolKey,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Try to claim a warm container - returns (container_id, executor_id,
        // container_update)
        let Some((container_id, executor_id, container_update)) =
            container_scheduler.claim_pool_container(pool_key, &sandbox.id, sandbox.timeout_secs)
        else {
            return Ok(None); // No warm container available
        };

        let mut update = SchedulerUpdateRequest::default();

        // Include the container update (sets sandbox_id on the container)
        update.extend(container_update);

        // Check if the warm container is already Running — if so, promote
        // the sandbox directly to Running. Warm pool containers are always
        // Running so we skip the Pending → WaitingForContainer state that
        // would require a ContainerStarted event from the dataplane.
        let container_state = container_scheduler
            .function_containers
            .get(&container_id)
            .map(|meta| meta.function_container.state.clone())
            .unwrap_or_default();

        let mut updated_sandbox = sandbox.clone();
        if matches!(container_state, ContainerState::Running) {
            updated_sandbox.status = SandboxStatus::Running;
        } else {
            updated_sandbox.status = SandboxStatus::Pending {
                reason: SandboxPendingReason::WaitingForContainer,
            };
        }
        updated_sandbox.executor_id = Some(executor_id.clone());
        updated_sandbox.container_id = Some(container_id.clone());

        let sandbox_key = SandboxKey::from(&updated_sandbox);
        update
            .updated_sandboxes
            .insert(sandbox_key.clone(), updated_sandbox.clone());

        info!(
            sandbox_id = %sandbox.id,
            namespace = %sandbox.namespace,
            pool_id = %pool_key.pool_id,
            container_id = %container_id,
            executor_id = %executor_id,
            "Sandbox claimed warm container from pool"
        );

        // Apply update to local copies
        let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update.clone()));
        container_scheduler.update(&payload)?;
        in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;

        Ok(Some(update))
    }

    /// Sets the pending reason on a sandbox and adds it to the update.
    fn set_pending_reason(
        update: &mut SchedulerUpdateRequest,
        sandbox: &Sandbox,
        reason: SandboxPendingReason,
    ) {
        let mut pending_sandbox = sandbox.clone();
        pending_sandbox.status = SandboxStatus::Pending { reason };
        let sandbox_key = SandboxKey::from(&pending_sandbox);
        update
            .updated_sandboxes
            .insert(sandbox_key, pending_sandbox);
    }
}
