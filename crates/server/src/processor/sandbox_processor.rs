use anyhow::Result;
use tracing::{info, warn};

use crate::{
    data_model::{
        ContainerId,
        Sandbox,
        SandboxFailureReason,
        SandboxKey,
        SandboxOutcome,
        SandboxStatus,
    },
    processor::container_scheduler::{self, ContainerScheduler},
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
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
        if sandbox.status != SandboxStatus::Pending {
            return Ok(update);
        }

        // Try to create a container for the sandbox using consolidated method
        match container_scheduler.create_container_for_sandbox(sandbox) {
            Ok(Some(container_update)) => {
                // Check if a container was actually created (has function_containers)
                if let Some(fc_metadata) = container_update.containers.values().next() {
                    // Container created successfully, update sandbox status to Running
                    let mut updated_sandbox = sandbox.clone();
                    updated_sandbox.executor_id = Some(fc_metadata.executor_id.clone());
                    updated_sandbox.status = SandboxStatus::Running;

                    let sandbox_key = SandboxKey::from(&updated_sandbox);
                    update
                        .updated_sandboxes
                        .insert(sandbox_key, updated_sandbox);
                    update.extend(container_update);

                    info!(
                        sandbox_id = %sandbox.id,
                        namespace = %sandbox.namespace,
                        "Sandbox allocated successfully"
                    );

                    // Apply update to local copies so subsequent iterations see the changes
                    let payload =
                        RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![]));
                    container_scheduler.update(&payload)?;
                    in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;
                } else {
                    // No container created (vacuum may have run but no host available)
                    // Apply any vacuum updates
                    if !container_update.containers.is_empty() ||
                        !container_update.updated_executor_states.is_empty()
                    {
                        let payload = RequestPayload::SchedulerUpdate((
                            Box::new(container_update.clone()),
                            vec![],
                        ));
                        container_scheduler.update(&payload)?;
                        in_memory_state.update_state(self.clock, &payload, "sandbox_processor")?;
                        update.extend(container_update);
                    }

                    info!(
                        sandbox_id = %sandbox.id,
                        namespace = %sandbox.namespace,
                        "No resources available for sandbox, keeping as pending"
                    );
                }
            }
            Ok(None) => {
                // No executors available at all
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
                        "Sandbox allocation failed: constraint unsatisfiable"
                    );

                    // Apply update to local copies
                    let payload =
                        RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![]));
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

        if sandbox.status == SandboxStatus::Running {
            let container_id = ContainerId::from(&sandbox.id);
            if let Some(container_update) =
                container_scheduler.terminate_container(&container_id)?
            {
                update.extend(container_update);
            }
        }

        let mut terminated_sandbox = sandbox.as_ref().clone();
        terminated_sandbox.status = SandboxStatus::Terminated;
        update
            .updated_sandboxes
            .insert(sandbox_key, terminated_sandbox);

        Ok(update)
    }
}
