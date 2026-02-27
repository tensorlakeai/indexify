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
    scheduler::{
        blocked::BlockingInfo,
        placement::{self, FeasibilityCache, WorkloadKey},
    },
    state_store::{in_memory_state::InMemoryState, requests::SchedulerUpdateRequest},
};

pub struct SandboxProcessor;

impl SandboxProcessor {
    pub fn new() -> Self {
        Self
    }

    /// Allocate a specific sandbox by key
    pub fn allocate_sandbox_by_key(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        namespace: &str,
        sandbox_id: &str,
        cache: &mut FeasibilityCache,
    ) -> Result<SchedulerUpdateRequest> {
        let sandbox_key = SandboxKey::new(namespace, sandbox_id);
        let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key).cloned() else {
            return Ok(SchedulerUpdateRequest::default());
        };

        let (update, _) =
            self.allocate_sandbox(in_memory_state, container_scheduler, &sandbox, cache)?;
        Ok(update)
    }

    /// Allocate a single sandbox.
    ///
    /// Returns `(update, exhausted)` where `exhausted` is true when the failure
    /// indicates cluster-wide resource exhaustion (no resources / no executors
    /// / create_container returned None). Non-exhaustion cases: pool claim
    /// success, container placed, pool at capacity, already scheduled,
    /// ConstraintUnsatisfiable.
    #[tracing::instrument(skip_all, fields(
        namespace = %sandbox.namespace,
        sandbox_id = %sandbox.id
    ))]
    fn allocate_sandbox(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        sandbox: &Sandbox,
        cache: &mut FeasibilityCache,
    ) -> Result<(SchedulerUpdateRequest, bool)> {
        let mut update = SchedulerUpdateRequest::default();

        // Skip if sandbox is not pending
        if !sandbox.status.is_pending() {
            return Ok((update, false));
        }

        // Skip if a container has already been scheduled for this sandbox.
        // It is waiting for the container to report Running and should not
        // be re-allocated — doing so would claim a duplicate pool slot.
        if sandbox.has_scheduled() {
            return Ok((update, false));
        }

        // If sandbox has a pool_id, try to claim from pool first
        if let Some(pool_id) = &sandbox.pool_id {
            let container_pool_key = ContainerPoolKey::new(&sandbox.namespace, pool_id);

            // Try to claim a warm container from the pool
            if let Some(claim_update) =
                self.try_claim_from_pool(container_scheduler, sandbox, &container_pool_key)?
            {
                return Ok((claim_update, false));
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
                    return Ok((update, false));
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
        match container_scheduler.create_container_for_sandbox(
            sandbox,
            snapshot_uri.as_deref(),
            cache,
        ) {
            Ok(Some(container_update)) => {
                // Container placed — extract executor/container info.
                let (executor_id, container_id) = container_update
                    .containers
                    .values()
                    .find(|c| !matches!(c.desired_state, ContainerState::Terminated { .. }))
                    .map(|fc| (fc.executor_id.clone(), fc.function_container.id.clone()))
                    .expect("create_container returned Some but no placed container");

                // Container created successfully, keep sandbox Pending until container
                // reports Running via heartbeat
                let mut updated_sandbox = sandbox.clone();
                updated_sandbox.executor_id = Some(executor_id);
                updated_sandbox.container_id = Some(container_id.clone());
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
                    container_id = %container_id,
                    "Sandbox allocated successfully"
                );

                // Update scheduler indices so subsequent capacity checks work
                container_scheduler.apply_container_update(&container_update);
                update.extend(container_update);

                // Remove from blocked work tracker if it was previously blocked
                container_scheduler
                    .blocked_work
                    .remove_sandbox(&SandboxKey::from(sandbox));

                Ok((update, false))
            }
            Ok(None) => {
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
                        "No executors available for sandbox, keeping as pending"
                    );
                    Self::set_pending_reason(
                        &mut update,
                        sandbox,
                        SandboxPendingReason::NoExecutorsAvailable,
                    );
                }

                // Record failed placement in BlockedWorkTracker for targeted retry
                Self::record_blocked_sandbox(container_scheduler, cache, sandbox);

                // Exhausted: no host could fit this sandbox
                Ok((update, true))
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

                    // Return Ok so the update (with terminated sandbox) is merged
                    // into the batch and persisted. Returning Err would drop the
                    // update, leaving the sandbox stuck in Pending forever.
                    return Ok((update, false));
                }
                Err(err)
            }
        }
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
            .insert(sandbox_key.clone(), terminated_sandbox);

        // Clean up from blocked work tracker
        container_scheduler
            .blocked_work
            .remove_sandbox(&sandbox_key);

        Ok(update)
    }

    /// Try to claim a warm container from a sandbox pool
    fn try_claim_from_pool(
        &self,
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

        // Remove from blocked work tracker if it was previously blocked
        container_scheduler
            .blocked_work
            .remove_sandbox(&SandboxKey::from(sandbox));

        // Note: claim_pool_container already updated function_containers and
        // warm_containers_by_pool inline, so no apply_container_update needed.

        Ok(Some(update))
    }

    /// Record a failed sandbox placement in the BlockedWorkTracker.
    ///
    /// Reads the PlacementResult from `container_scheduler.last_placement`
    /// (set by create_container) and records the eligible/ineligible classes so
    /// the tracker can do targeted retry when capacity changes.
    fn record_blocked_sandbox(
        container_scheduler: &mut ContainerScheduler,
        cache: &mut FeasibilityCache,
        sandbox: &Sandbox,
    ) {
        let sandbox_key = SandboxKey::from(sandbox);
        let (eligible, escaped) = match container_scheduler.last_placement.take() {
            Some(placement) => {
                let escaped = placement.eligible_classes.is_empty();
                (placement.eligible_classes, escaped)
            }
            None => {
                // No placement attempted (e.g., create_container returned None
                // before calling select_executor). Compute per-class
                // feasibility so only genuinely eligible classes are recorded.
                // Fall back to escaped when empty (no executors).
                let workload_key = WorkloadKey::Sandbox {
                    namespace: sandbox.namespace.clone(),
                };
                let eligible = placement::compute_eligible_classes(
                    container_scheduler,
                    cache,
                    &workload_key,
                    &sandbox.namespace,
                    "",
                    None,
                );
                let escaped = eligible.is_empty();
                (eligible, escaped)
            }
        };
        container_scheduler.blocked_work.block_sandbox(
            sandbox_key,
            BlockingInfo {
                eligible_classes: eligible,
                escaped,
                memory_mb: sandbox.resources.memory_mb,
            },
        );
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
