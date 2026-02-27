use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use arc_swap::ArcSwap;
use opentelemetry::metrics::ObservableGauge;
use tracing::info;

use crate::{
    data_model::{
        ApplicationState,
        Container,
        ContainerBuilder,
        ContainerId,
        ContainerPool,
        ContainerPoolId,
        ContainerPoolKey,
        ContainerPoolType,
        ContainerResources,
        ContainerServerMetadata,
        ContainerState,
        ContainerTerminationReason,
        ContainerType,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        Function,
        FunctionResources,
        FunctionURI,
        HostResources,
        Sandbox,
        SandboxId,
    },
    scheduler::{
        blocked::BlockedWorkTracker,
        executor_class::ExecutorClass,
        placement::{self, FeasibilityCache, PlacementResult, WorkloadKey},
    },
    state_store::{
        requests::{RequestPayload, SchedulerUpdateRequest},
        scanner::StateReader,
        state_machine::IndexifyObjectsColumns,
    },
};

/// Gauges for monitoring the container scheduler state.
/// Must be kept alive for callbacks to fire.
#[allow(dead_code)]
pub struct ContainerSchedulerGauges {
    pub total_executors: ObservableGauge<u64>,
}

impl ContainerSchedulerGauges {
    pub fn new(container_scheduler: Arc<ArcSwap<ContainerScheduler>>) -> Self {
        let meter = opentelemetry::global::meter("container_scheduler");
        let scheduler_clone = container_scheduler.clone();
        let total_executors = meter
            .u64_observable_gauge("indexify.total_executors")
            .with_description("Total number of executors")
            .with_callback(move |observer| {
                let scheduler = scheduler_clone.load();
                observer.observe(scheduler.executor_states.len() as u64, &[]);
            })
            .build();
        Self { total_executors }
    }
}

#[derive(Debug, Clone)]
pub enum Error {
    ConstraintUnsatisfiable {
        reason: String,
        version: String,
        function_name: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ConstraintUnsatisfiable { reason, .. } => reason.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn version(&self) -> &str {
        match self {
            Error::ConstraintUnsatisfiable { version, .. } => version,
        }
    }

    pub fn function_name(&self) -> &str {
        match self {
            Error::ConstraintUnsatisfiable { function_name, .. } => function_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContainerScheduler {
    // ExecutorId -> ExecutorMetadata
    // This is the metadata that executor is sending us, not the **Desired** state
    // from the perspective of the state store.
    pub executors: imbl::HashMap<ExecutorId, Box<ExecutorMetadata>>,
    pub containers_by_function_uri: imbl::HashMap<FunctionURI, imbl::HashSet<ContainerId>>,
    pub function_containers: imbl::HashMap<ContainerId, Box<ContainerServerMetadata>>,
    // ExecutorId -> (FE ID -> List of Function Executors)
    pub executor_states: imbl::HashMap<ExecutorId, Box<ExecutorServerMetadata>>,
    // Pool key -> Container IDs (excludes terminated; for O(1) pool container counts)
    pub containers_by_pool: imbl::HashMap<ContainerPoolKey, imbl::HashSet<ContainerId>>,
    // Pool key -> Warm container IDs (sandbox_id is None, for O(1) warm container lookups)
    pub warm_containers_by_pool: imbl::HashMap<ContainerPoolKey, imbl::HashSet<ContainerId>>,
    // Function pools configuration (auto-created for functions)
    pub function_pools: imbl::HashMap<ContainerPoolKey, Box<ContainerPool>>,
    // Sandbox pools configuration (user-created and per-sandbox)
    pub sandbox_pools: imbl::HashMap<ContainerPoolKey, Box<ContainerPool>>,
    // Pools whose container count or config changed since last reconciliation.
    // BufferReconciler only processes dirty pools instead of scanning all pools.
    pub dirty_pools: imbl::HashSet<ContainerPoolKey>,
    // Pools that need containers but couldn't get resources. Skipped by
    // BufferReconciler until resources become available (new executor joins or
    // container terminates).
    pub blocked_pools: imbl::HashSet<ContainerPoolKey>,
    // Sorted-set index: executors ordered by free memory (bytes).
    // Tuple is (free_memory_bytes, executor_id) for O(log E) range queries.
    // Maintained by set_executor_state / remove_executor_state / register_container.
    pub(crate) executors_by_free_memory: imbl::OrdSet<(u64, ExecutorId)>,
    // Computed equivalence class per executor (Nomad's ComputedClass).
    pub executor_classes: imbl::HashMap<ExecutorId, ExecutorClass>,
    // Reverse index: executor class -> set of executor IDs with that class.
    pub executors_by_class: imbl::HashMap<ExecutorClass, imbl::HashSet<ExecutorId>>,
    // Tracks failed placements for targeted retry on capacity changes.
    pub blocked_work: BlockedWorkTracker,
    // Containers with no active allocations, ordered by when they became idle.
    // Maintained by update_container_indices and remove_container_from_indices.
    // Reaper drains from the front (oldest idle first).
    pub(crate) idle_containers: imbl::OrdSet<(tokio::time::Instant, ContainerId)>,
    // Containers reaped this batch, keyed by FunctionURI.
    // create_container_for_function checks this before creating new containers.
    // Ephemeral — populated by reaper, consumed by restore.
    pub(crate) reaped_containers: HashMap<FunctionURI, Vec<ContainerId>>,
    // Last placement result from select_executor(), available for callers
    // (sandbox_processor, function_run_processor) to feed into BlockedWorkTracker.
    pub(crate) last_placement: Option<PlacementResult>,
}

impl ContainerScheduler {
    pub async fn new(reader: &StateReader) -> Result<Self> {
        let mut function_pools = imbl::HashMap::new();
        let mut sandbox_pools = imbl::HashMap::new();

        // Load from new column families (post-migration)
        let fn_pools: Vec<(String, ContainerPool)> = reader
            .get_all_rows_from_cf::<ContainerPool>(IndexifyObjectsColumns::FunctionPools)
            .await?;
        for (_key, mut pool) in fn_pools {
            pool.pool_type = ContainerPoolType::Function;
            let pool_key = ContainerPoolKey::from(&pool);
            function_pools.insert(pool_key, Box::new(pool));
        }

        let sb_pools: Vec<(String, ContainerPool)> = reader
            .get_all_rows_from_cf::<ContainerPool>(IndexifyObjectsColumns::SandboxPools)
            .await?;
        for (_key, mut pool) in sb_pools {
            pool.pool_type = ContainerPoolType::Sandbox;
            let pool_key = ContainerPoolKey::from(&pool);
            sandbox_pools.insert(pool_key, Box::new(pool));
        }

        // Don't mark pools dirty at startup — no executors are connected
        // yet, so reconciliation would just block everything. Pools become
        // dirty naturally when state changes occur (app creates, executor
        // heartbeats, container events). Blocked pools are retried in Phase 4
        // when executors with capacity appear.

        Ok(Self {
            executors: imbl::HashMap::new(),
            containers_by_function_uri: imbl::HashMap::new(),
            function_containers: imbl::HashMap::new(),
            executor_states: imbl::HashMap::new(),
            containers_by_pool: imbl::HashMap::new(),
            warm_containers_by_pool: imbl::HashMap::new(),
            function_pools,
            sandbox_pools,
            dirty_pools: imbl::HashSet::new(),
            blocked_pools: imbl::HashSet::new(),
            executors_by_free_memory: imbl::OrdSet::new(),
            executor_classes: imbl::HashMap::new(),
            executors_by_class: imbl::HashMap::new(),
            blocked_work: BlockedWorkTracker::new(),
            idle_containers: imbl::OrdSet::new(),
            reaped_containers: HashMap::new(),
            last_placement: None,
        })
    }

    pub fn update(&mut self, state_machine_update_request: &RequestPayload) -> Result<()> {
        match state_machine_update_request {
            RequestPayload::UpsertExecutor(request) => {
                self.upsert_executor(&request.executor);
            }
            RequestPayload::DeleteApplicationRequest((request, _)) => {
                self.delete_application(&request.namespace, &request.name);
            }
            RequestPayload::SchedulerUpdate(payload) => {
                self.update_scheduler_update(&payload.update);
            }
            RequestPayload::DeregisterExecutor(request) => {
                self.deregister_executor(&request.executor_id);
            }
            RequestPayload::CreateContainerPool(request) => {
                let pool_key = ContainerPoolKey::from(&request.pool);
                self.pool_map_mut(&request.pool)
                    .insert(pool_key.clone(), Box::new(request.pool.clone()));
                self.mark_pool_dirty(pool_key);
            }
            RequestPayload::UpdateContainerPool(request) => {
                let pool_key = ContainerPoolKey::from(&request.pool);
                self.pool_map_mut(&request.pool)
                    .insert(pool_key.clone(), Box::new(request.pool.clone()));
                self.mark_pool_dirty(pool_key);
            }
            RequestPayload::TombstoneContainerPool(request) => {
                let pool_key = ContainerPoolKey::new(&request.namespace, &request.pool_id);
                // Try both maps — the pool only exists in one.
                self.function_pools.remove(&pool_key);
                self.sandbox_pools.remove(&pool_key);
                self.mark_pool_dirty(pool_key);
            }
            RequestPayload::DeleteContainerPool((request, _)) => {
                self.delete_container_pool(&request.namespace, &request.pool_id);
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                // Only update pools if container_pools is non-empty.
                // Empty container_pools means "no change to pools" (e.g., from
                // validate_and_upgrade_constraints), not "delete all pools".
                if !req.container_pools.is_empty() {
                    // Delete old function pools for this application before inserting new ones
                    let pool_prefix = format!("{}|", req.application.name);
                    let pools_to_remove: Vec<_> = self
                        .function_pools
                        .iter()
                        .filter(|(key, _)| {
                            key.namespace == req.namespace &&
                                key.pool_id.get().starts_with(&pool_prefix)
                        })
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in pools_to_remove {
                        self.function_pools.remove(&key);
                        self.mark_pool_dirty(key);
                    }

                    // Insert container pools from the request (always function pools)
                    for pool in &req.container_pools {
                        let pool_key = ContainerPoolKey::from(pool);
                        self.function_pools
                            .insert(pool_key.clone(), Box::new(pool.clone()));
                        self.mark_pool_dirty(pool_key);
                    }
                }
            }
            _ => return Ok(()),
        }
        Ok(())
    }

    fn upsert_executor(&mut self, executor_metadata: &ExecutorMetadata) {
        // Only update executor metadata here.
        // num_allocations updates are handled in update_scheduler_update
        // when processing updated_allocations from the processors.
        self.executors.insert(
            executor_metadata.id.clone(),
            executor_metadata.clone().into(),
        );

        // Update executor class indexes
        let new_class = ExecutorClass::from_executor(executor_metadata);
        let executor_id = &executor_metadata.id;

        // Remove from old class index if class changed
        if let Some(old_class) = self.executor_classes.get(executor_id) {
            if old_class != &new_class {
                if let Some(set) = self.executors_by_class.get_mut(old_class) {
                    set.remove(executor_id);
                    if set.is_empty() {
                        self.executors_by_class.remove(old_class);
                    }
                }
            }
        }

        // Insert into new class index
        self.executor_classes
            .insert(executor_id.clone(), new_class.clone());
        self.executors_by_class
            .entry(new_class)
            .or_default()
            .insert(executor_id.clone());

        // New executor capacity is handled by the buffer reconciler's
        // blocked pool phase — it checks for available capacity and tries
        // blocked pools until resources are consumed.
    }

    fn deregister_executor(&mut self, executor_id: &ExecutorId) {
        if let Some(executor) = self.executors.get_mut(executor_id) {
            executor.tombstoned = true;
        }
        // Remove from memory index immediately so has_available_capacity()
        // doesn't trigger wasteful Phase 4 probes for a dying executor.
        if let Some(state) = self.executor_states.get(executor_id) {
            let memory = state.free_resources.memory_bytes;
            self.executors_by_free_memory
                .remove(&(memory, executor_id.clone()));
        }

        // Remove from executor class indexes
        if let Some(old_class) = self.executor_classes.remove(executor_id) {
            if let Some(set) = self.executors_by_class.get_mut(&old_class) {
                set.remove(executor_id);
                if set.is_empty() {
                    self.executors_by_class.remove(&old_class);
                }
            }
        }
    }

    fn delete_application(&mut self, namespace: &str, name: &str) {
        // Use containers_by_function_uri index to find containers for this
        // application — O(F_app × C_f) instead of O(C_total).
        let matching_uris: Vec<FunctionURI> = self
            .containers_by_function_uri
            .keys()
            .filter(|uri| uri.namespace == namespace && uri.application == name)
            .cloned()
            .collect();

        let mut containers_to_remove: Vec<(
            Option<ContainerPoolKey>,
            FunctionURI,
            ContainerId,
            bool,
            Option<tokio::time::Instant>, // idle_since for idle_containers cleanup
        )> = Vec::new();

        for fn_uri in &matching_uris {
            if let Some(container_ids) = self.containers_by_function_uri.get(fn_uri) {
                for container_id in container_ids.iter() {
                    if let Some(fc) = self.function_containers.get_mut(container_id) {
                        if !matches!(fc.desired_state, ContainerState::Terminated { .. }) {
                            let was_warm = fc.function_container.sandbox_id.is_none();
                            containers_to_remove.push((
                                fc.function_container.pool_key(),
                                fn_uri.clone(),
                                container_id.clone(),
                                was_warm,
                                fc.idle_since,
                            ));
                        }

                        fc.desired_state = ContainerState::Terminated {
                            reason: ContainerTerminationReason::DesiredStateRemoved,
                        };
                    }
                }
            }
        }

        // Remove terminated containers from all indices and mark pools dirty
        for (pool_key, fn_uri, container_id, was_warm, idle_since) in containers_to_remove {
            // Remove from idle set
            if let Some(idle_since) = idle_since {
                self.idle_containers
                    .remove(&(idle_since, container_id.clone()));
            }

            if let Some(ref pool_key) = pool_key {
                if let Some(pool_ids) = self.containers_by_pool.get_mut(pool_key) {
                    pool_ids.retain(|id| id != &container_id);
                    if pool_ids.is_empty() {
                        self.containers_by_pool.remove(pool_key);
                    }
                }

                if was_warm && let Some(warm_ids) = self.warm_containers_by_pool.get_mut(pool_key) {
                    warm_ids.retain(|id| id != &container_id);
                    if warm_ids.is_empty() {
                        self.warm_containers_by_pool.remove(pool_key);
                    }
                }
            }

            if let Some(container_ids) = self.containers_by_function_uri.get_mut(&fn_uri) {
                container_ids.retain(|id| id != &container_id);
                if container_ids.is_empty() {
                    self.containers_by_function_uri.remove(&fn_uri);
                }
            }

            if let Some(pool_key) = pool_key {
                self.mark_pool_dirty(pool_key);
            }
        }

        // Remove function pools for this application
        let pool_prefix = format!("{}|", name);
        let pools_to_remove: Vec<ContainerPoolKey> = self
            .function_pools
            .keys()
            .filter(|key| key.namespace == namespace && key.pool_id.get().starts_with(&pool_prefix))
            .cloned()
            .collect();

        for pool_key in pools_to_remove {
            self.function_pools.remove(&pool_key);
            self.mark_pool_dirty(pool_key);
        }

        // Note: containers are only *marked* for termination here
        // (desired_state = Terminated). Resources are not freed until the
        // dataplane confirms termination, at which point
        // update_container_indices will unblock matching pools.
    }

    fn delete_container_pool(&mut self, namespace: &str, pool_id: &ContainerPoolId) {
        // Mark pool containers' desired_state as Terminated
        // Only marks warm (unclaimed) containers - claimed containers are terminated
        // when their associated sandbox terminates.
        let pool_key = ContainerPoolKey::new(namespace, pool_id);

        // Get warm container IDs from the index (O(1) lookup)
        let warm_ids: Vec<ContainerId> = self
            .warm_containers_by_pool
            .get(&pool_key)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default();

        // Collect fn_uris and idle_since before mutable borrow to mark terminated
        let fn_uris: Vec<(ContainerId, FunctionURI, Option<tokio::time::Instant>)> = warm_ids
            .iter()
            .filter_map(|container_id| {
                self.function_containers.get(container_id).and_then(|fc| {
                    if !matches!(fc.desired_state, ContainerState::Terminated { .. }) {
                        Some((
                            container_id.clone(),
                            FunctionURI::from(&fc.function_container),
                            fc.idle_since,
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Mark each warm container as terminated
        for (container_id, ..) in &fn_uris {
            if let Some(fc) = self.function_containers.get_mut(container_id) {
                fc.desired_state = ContainerState::Terminated {
                    reason: ContainerTerminationReason::DesiredStateRemoved,
                };
            }
        }

        // Remove from idle set
        for (container_id, _, idle_since) in &fn_uris {
            if let Some(idle_since) = idle_since {
                self.idle_containers
                    .remove(&(*idle_since, container_id.clone()));
            }
        }

        // Clear the warm index for this pool since all are now terminated
        self.warm_containers_by_pool.remove(&pool_key);

        // Remove terminated containers from containers_by_pool
        if let Some(pool_ids) = self.containers_by_pool.get_mut(&pool_key) {
            for container_id in &warm_ids {
                pool_ids.remove(container_id);
            }
            if pool_ids.is_empty() {
                self.containers_by_pool.remove(&pool_key);
            }
        }

        // Remove terminated containers from containers_by_function_uri
        for (container_id, fn_uri, _) in &fn_uris {
            if let Some(container_ids) = self.containers_by_function_uri.get_mut(fn_uri) {
                container_ids.remove(container_id);
                if container_ids.is_empty() {
                    self.containers_by_function_uri.remove(fn_uri);
                }
            }
        }

        self.mark_pool_dirty(pool_key);
    }

    /// Get a pool from either function_pools or sandbox_pools
    pub fn get_pool(&self, key: &ContainerPoolKey) -> Option<&ContainerPool> {
        self.function_pools
            .get(key)
            .or_else(|| self.sandbox_pools.get(key))
            .map(|b| &**b)
    }

    /// Get a mutable reference to the correct pool map based on pool type
    fn pool_map_mut(
        &mut self,
        pool: &ContainerPool,
    ) -> &mut imbl::HashMap<ContainerPoolKey, Box<ContainerPool>> {
        if pool.is_function_pool() {
            &mut self.function_pools
        } else {
            &mut self.sandbox_pools
        }
    }

    /// Returns true if any executor has free memory. O(1).
    pub fn has_available_capacity(&self) -> bool {
        !self.executors_by_free_memory.is_empty()
    }

    /// Insert or replace an executor state, keeping the memory index
    /// in sync. All mutations to `executor_states` must go through this
    /// method or `remove_executor_state`.
    pub(crate) fn set_executor_state(
        &mut self,
        id: ExecutorId,
        state: Box<ExecutorServerMetadata>,
    ) {
        // Remove old index entry (extract memory_bytes to avoid borrow conflict)
        if let Some(old) = self.executor_states.get(&id) {
            let old_memory = old.free_resources.memory_bytes;
            self.executors_by_free_memory
                .remove(&(old_memory, id.clone()));
        }
        // Add new index entry
        self.insert_resource_index(&id, &state.free_resources);
        self.executor_states.insert(id, state);
    }

    /// Remove an executor state, keeping the memory index in sync.
    fn remove_executor_state(&mut self, id: &ExecutorId) {
        if let Some(old) = self.executor_states.remove(id) {
            self.remove_resource_index(id, &old.free_resources);
        }
    }

    fn insert_resource_index(&mut self, id: &ExecutorId, resources: &HostResources) {
        if resources.memory_bytes > 0 && resources.cpu_ms_per_sec > 0 {
            self.executors_by_free_memory
                .insert((resources.memory_bytes, id.clone()));
        }
    }

    fn remove_resource_index(&mut self, id: &ExecutorId, resources: &HostResources) {
        self.executors_by_free_memory
            .remove(&(resources.memory_bytes, id.clone()));
    }

    /// Mark a pool as dirty (needs reconciliation). Also removes it from the
    /// blocked set since a state change may have made it satisfiable.
    fn mark_pool_dirty(&mut self, pool_key: ContainerPoolKey) {
        self.blocked_pools.remove(&pool_key);
        self.dirty_pools.insert(pool_key);
    }

    /// Take the dirty pool set, replacing it with an empty set.
    /// Called by BufferReconciler to consume the dirty set.
    pub fn take_dirty_pools(&mut self) -> imbl::HashSet<ContainerPoolKey> {
        std::mem::take(&mut self.dirty_pools)
    }

    fn update_scheduler_update(&mut self, scheduler_update: &SchedulerUpdateRequest) {
        // Containers confirmed terminated during the merge — must be removed
        // from all indices after processing rather than re-inserted by
        // update_container_indices.
        let mut dropped_containers: Vec<ContainerId> = Vec::new();

        for (executor_id, new_executor_server_metadata) in &scheduler_update.updated_executor_states
        {
            // Merge with existing state rather than replacing. The update was
            // built from a clone that may be stale — concurrent writes (executor
            // heartbeats, other state changes) could have added containers to
            // the real executor state between clone and write. A naive replace
            // would drop those containers.
            let mut merged = (**new_executor_server_metadata).clone();
            if let Some(old_state) = self.executor_states.get(executor_id) {
                for container_id in &old_state.function_container_ids {
                    if !merged.function_container_ids.contains(container_id) {
                        // Container is in old state but not in the update.
                        // Only drop it if explicitly terminated in this update.
                        let being_terminated = scheduler_update
                            .containers
                            .get(container_id)
                            .is_some_and(|c| {
                                matches!(c.desired_state, ContainerState::Terminated { .. })
                            });
                        if !being_terminated {
                            merged.function_container_ids.insert(container_id.clone());
                            // Preserve the resource claim so free_resources stays
                            // accurate. Without this, the scheduler would think
                            // the executor has more free resources than it does.
                            if let Some(claim) = old_state.resource_claims.get(container_id) &&
                                !merged.resource_claims.contains_key(container_id)
                            {
                                merged
                                    .resource_claims
                                    .insert(container_id.clone(), claim.clone());
                                merged.free_resources.force_consume_fe_resources(claim);
                            }
                        } else {
                            dropped_containers.push(container_id.clone());
                        }
                    }
                }
            }

            self.set_executor_state(executor_id.clone(), Box::new(merged));
        }

        for (container_id, new_function_container) in &scheduler_update.containers {
            // Skip containers that were dropped during the merge — they are
            // terminated signals from remove_function_containers. Calling
            // update_container_indices would re-insert them as orphaned
            // terminated entries that no executor owns and no cleanup path
            // would ever reach.
            if dropped_containers.contains(container_id) {
                continue;
            }
            self.update_container_indices(container_id, new_function_container);
        }

        // Clean up dropped containers from all indices
        for container_id in &dropped_containers {
            self.remove_container_from_indices(container_id);
        }

        for removed_executor_id in &scheduler_update.remove_executors {
            // Clean up containers for this executor before removing it
            if let Some(old_state) = self.executor_states.get(removed_executor_id) {
                for container_id in old_state.function_container_ids.clone() {
                    self.remove_container_from_indices(&container_id);
                }
            }
            // Remove executor server metadata (scheduler's view of the
            // executor's containers) but keep the executor in `self.executors`.
            // The executor metadata is already tombstoned (via deregister_executor).
            // If the executor reconnects, upsert_executor refreshes the entry
            // (clears tombstoned), and the reconciler adopts whatever containers
            // the executor reports — starting from an empty executor_states.
            self.remove_executor_state(removed_executor_id);
        }

        // Propagate blocked pools from the buffer reconciler (runs on the clone)
        // back to the real scheduler for cross-cycle persistence.
        self.blocked_pools
            .extend(scheduler_update.newly_blocked_pools.iter().cloned());
    }

    /// Remove a container from all indices: function_containers,
    /// containers_by_function_uri, containers_by_pool, and
    /// warm_containers_by_pool.
    fn remove_container_from_indices(&mut self, container_id: &ContainerId) {
        if let Some(fc) = self.function_containers.remove(container_id) {
            if let Some(idle_since) = fc.idle_since {
                self.idle_containers
                    .remove(&(idle_since, container_id.clone()));
            }
            let fn_uri = FunctionURI::from(&fc.function_container);
            if let Some(container_ids) = self.containers_by_function_uri.get_mut(&fn_uri) {
                container_ids.retain(|id| id != container_id);
                if container_ids.is_empty() {
                    self.containers_by_function_uri.remove(&fn_uri);
                }
            }
            if let Some(pool_key) = fc.function_container.pool_key() {
                if let Some(pool_ids) = self.containers_by_pool.get_mut(&pool_key) {
                    pool_ids.retain(|id| id != container_id);
                    if pool_ids.is_empty() {
                        self.containers_by_pool.remove(&pool_key);
                    }
                }
                if let Some(warm_ids) = self.warm_containers_by_pool.get_mut(&pool_key) {
                    warm_ids.retain(|id| id != container_id);
                    if warm_ids.is_empty() {
                        self.warm_containers_by_pool.remove(&pool_key);
                    }
                }
                self.mark_pool_dirty(pool_key);
            }
            // Note: this path is used for dropped containers (executor state
            // merge) and removed executors — in both cases the executor is
            // gone, so no resources are freed on any live executor.
            // Affected pools are already marked dirty above for
            // re-evaluation.
        }
    }

    pub fn create_container_for_function(
        &mut self,
        namespace: &str,
        application: &str,
        version: &str,
        function: &Function,
        application_state: &ApplicationState,
        cache: &mut FeasibilityCache,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Check if the application is disabled
        if let ApplicationState::Disabled { reason } = application_state {
            return Err(Error::ConstraintUnsatisfiable {
                version: version.to_string(),
                function_name: function.name.clone(),
                reason: reason.clone(),
            }
            .into());
        }

        // Anti-churn: restore a reaped container for this function instead of
        // creating a brand new one.
        let fn_uri = FunctionURI {
            namespace: namespace.to_string(),
            application: application.to_string(),
            function: function.name.clone(),
            version: version.to_string(),
        };
        if let Some(restore_update) = self.restore_reaped_container(&fn_uri) {
            return Ok(Some(restore_update));
        }

        let container_resources = ContainerResources {
            cpu_ms_per_sec: function.resources.cpu_ms_per_sec,
            memory_mb: function.resources.memory_mb,
            ephemeral_disk_mb: function.resources.ephemeral_disk_mb,
            gpu: function.resources.gpu_configs.first().cloned(),
        };

        // Build pool_id for this function container
        let pool_id = ContainerPoolId::for_function(application, &function.name, version);

        let function_container = ContainerBuilder::default()
            .namespace(namespace.to_string())
            .application_name(application.to_string())
            .function_name(function.name.clone())
            .version(version.to_string())
            .state(ContainerState::Pending)
            .resources(container_resources)
            .max_concurrency(function.max_concurrency)
            .container_type(ContainerType::Function)
            .secret_names(function.secret_names.clone().unwrap_or_default())
            .timeout_secs(function.timeout.0 as u64 / 1000) // Convert ms to secs
            .pool_id(Some(pool_id.clone()))
            .build()?;

        self.create_container(
            namespace,
            application,
            Some(function),
            &function.resources,
            function_container,
            ContainerType::Function,
            cache,
        )
    }

    pub fn create_container_for_sandbox(
        &mut self,
        sandbox: &Sandbox,
        snapshot_uri: Option<&str>,
        cache: &mut FeasibilityCache,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let resources = FunctionResources {
            cpu_ms_per_sec: sandbox.resources.cpu_ms_per_sec,
            memory_mb: sandbox.resources.memory_mb,
            ephemeral_disk_mb: sandbox.resources.ephemeral_disk_mb,
            gpu_configs: sandbox
                .resources
                .gpu
                .clone()
                .map(|g| vec![g])
                .unwrap_or_default(),
        };

        // Use sandbox ID as the container ID (1:1 relationship for direct sandboxes)
        let container_id = ContainerId::from(&sandbox.id);

        // Use sandbox's pool_id directly — standalone sandboxes have None.
        let pool_id = sandbox.pool_id.clone();

        info!(
            namespace = %sandbox.namespace,
            sandbox_id = %sandbox.id.get(),
            container_id = %container_id.get(),
            image = %sandbox.image,
            "Creating container for sandbox"
        );

        let function_container = ContainerBuilder::default()
            .id(container_id)
            .namespace(sandbox.namespace.clone())
            .application_name(String::new()) // Sandboxes are not associated with applications
            .function_name(sandbox.id.get().to_string())
            .version(String::new()) // Sandboxes don't have versions
            .state(ContainerState::Pending)
            .resources(sandbox.resources.clone())
            .max_concurrency(1u32)
            .container_type(ContainerType::Sandbox)
            .image(Some(sandbox.image.clone()))
            .secret_names(sandbox.secret_names.clone())
            .timeout_secs(sandbox.timeout_secs)
            .entrypoint(sandbox.entrypoint.clone().unwrap_or_default())
            .network_policy(sandbox.network_policy.clone())
            .sandbox_id(Some(sandbox.id.clone()))
            .pool_id(pool_id)
            .snapshot_uri(snapshot_uri.map(|s| s.to_string()))
            .build()?;

        self.create_container(
            &sandbox.namespace,
            "",   // Sandboxes are not associated with applications
            None, // No function for sandboxes
            &resources,
            function_container,
            ContainerType::Sandbox,
            cache,
        )
    }

    fn create_container(
        &mut self,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        resources: &FunctionResources,
        function_container: Container,
        container_type: ContainerType,
        cache: &mut FeasibilityCache,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Build workload key for constraint caching
        let workload_key = if let Some(f) = function {
            WorkloadKey::Function {
                namespace: namespace.to_string(),
                application: application.to_string(),
                function_name: f.name.clone(),
            }
        } else {
            WorkloadKey::Sandbox {
                namespace: namespace.to_string(),
            }
        };

        let result = placement::select_executor(
            self,
            cache,
            &workload_key,
            namespace,
            application,
            function,
            resources,
            2, // Power-of-two-choices: collect 2 random candidates, pick best
        );

        // Store placement result so callers (sandbox_processor,
        // function_run_processor) can read it for BlockedWorkTracker.
        self.last_placement = Some(result);

        let placement = self.last_placement.take().unwrap();
        let Some(executor_id) = placement.executor_id.clone() else {
            self.last_placement = Some(placement);
            return Ok(None);
        };

        if !self.executor_states.contains_key(&executor_id) {
            self.last_placement = Some(placement);
            return Ok(None);
        }

        // Store placement info back for the caller
        self.last_placement = Some(placement);

        // Register the container (updates executors_by_free_memory inline
        // so subsequent select_executor() calls see reduced capacity)
        let update =
            self.register_container(executor_id.clone(), function_container, container_type)?;

        Ok(Some(update))
    }

    /// Checks if an executor can run a container based on placement
    /// constraints.s
    ///
    /// For functions: checks tombstone, allowlist, placement constraints, and
    /// version. For sandboxes: checks tombstone, allowlist, and version.
    pub(crate) fn executor_matches_constraints(
        &self,
        executor: &ExecutorMetadata,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
    ) -> bool {
        if executor.tombstoned {
            return false;
        }

        if let Some(func) = function {
            // Function container constraints
            if !executor.is_function_allowed(namespace, application, func) {
                return false;
            }
            if !func.placement_constraints.matches(&executor.labels) {
                return false;
            }
        } else if !executor.is_namespace_allowed(namespace) {
            return false;
        }

        true
    }

    fn register_container(
        &mut self,
        executor_id: ExecutorId,
        function_container: Container,
        container_type: ContainerType,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(executor_server_metadata) = self.executor_states.get_mut(&executor_id) else {
            return Err(anyhow::anyhow!(
                "Executor state not found for {}",
                executor_id.get()
            ));
        };

        info!(
            executor_id = executor_id.get(),
            container_id = function_container.id.get(),
            sandbox_id = ?function_container.sandbox_id,
            container_type = ?container_type,
            "registering container"
        );

        // Maintain memory index: remove old entry before mutation, re-insert after.
        // We access executors_by_free_memory directly (not via helper methods) to avoid
        // conflicting mutable borrows — executor_server_metadata borrows
        // executor_states, but Rust can split borrows on separate struct
        // fields.
        let old_memory = executor_server_metadata.free_resources.memory_bytes;
        self.executors_by_free_memory
            .remove(&(old_memory, executor_id.clone()));
        executor_server_metadata.add_container(&function_container)?;
        let new_resources = &executor_server_metadata.free_resources;
        if new_resources.memory_bytes > 0 && new_resources.cpu_ms_per_sec > 0 {
            self.executors_by_free_memory
                .insert((new_resources.memory_bytes, executor_id.clone()));
        }

        let fe_server_metadata = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container,
            desired_state: ContainerState::Running,
            container_type,
            allocations: imbl::HashSet::new(),
            idle_since: Some(tokio::time::Instant::now()),
        };

        let mut update = SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id, executor_server_metadata.clone());
        update.containers.insert(
            fe_server_metadata.function_container.id.clone(),
            Box::new(fe_server_metadata),
        );

        Ok(update)
    }

    fn fe_can_be_removed(&self, fe_meta: &ContainerServerMetadata) -> bool {
        // Check if this container matches the executor's allowlist
        if let Some(executor) = self.executors.get(&fe_meta.executor_id) &&
            let Some(allowlist) = &executor.function_allowlist
        {
            for allowlist_entry in allowlist {
                if allowlist_entry.matches_function_executor(&fe_meta.function_container) {
                    return false;
                }
            }
        }

        // Check if container has active allocations or is a sandbox
        // Buffer reconciler handles maintaining min/buffer counts
        fe_meta.can_be_removed()
    }

    /// Terminate a container by its ID
    pub fn terminate_container(
        &mut self,
        container_id: &ContainerId,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Extract keys before mutable borrow
        let (pool_key, fn_uri) = {
            let Some(fc) = self.function_containers.get(container_id) else {
                return Ok(None); // Container not found, nothing to terminate
            };
            (
                fc.function_container.pool_key(),
                FunctionURI::from(&fc.function_container),
            )
        };

        // Remove from pool indices only if container belongs to a pool
        if let Some(ref pool_key) = pool_key {
            if let Some(warm_ids) = self.warm_containers_by_pool.get_mut(pool_key) {
                warm_ids.retain(|id| id != container_id);
                if warm_ids.is_empty() {
                    self.warm_containers_by_pool.remove(pool_key);
                }
            }

            if let Some(pool_ids) = self.containers_by_pool.get_mut(pool_key) {
                pool_ids.retain(|id| id != container_id);
                if pool_ids.is_empty() {
                    self.containers_by_pool.remove(pool_key);
                }
            }
        }

        // Remove from containers_by_function_uri
        if let Some(container_ids) = self.containers_by_function_uri.get_mut(&fn_uri) {
            container_ids.retain(|id| id != container_id);
            if container_ids.is_empty() {
                self.containers_by_function_uri.remove(&fn_uri);
            }
        }

        // Remove from idle set
        let fc = self
            .function_containers
            .get(container_id)
            .expect("container must exist — checked at function entry");
        if let Some(idle_since) = fc.idle_since {
            self.idle_containers
                .remove(&(idle_since, container_id.clone()));
        }

        // Mark for termination
        let fc = self
            .function_containers
            .get_mut(container_id)
            .expect("container must exist — checked at function entry");
        fc.desired_state = ContainerState::Terminated {
            reason: ContainerTerminationReason::DesiredStateRemoved,
        };

        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(container_id.clone(), fc.clone());

        Ok(Some(update))
    }

    /// Eagerly reap idle containers: terminate them, optimistically free
    /// their resources so subsequent allocations see available capacity, and
    /// track reaped containers for restoration (anti-churn).
    ///
    /// `min_idle_age` controls the minimum time a container must have been idle
    /// before it is eligible for reaping. Pass `Duration::ZERO` to reap all
    /// idle containers (used in the event-loop batch for immediate capacity
    /// recovery). Pass the configured vacuum threshold for background cleanup.
    pub fn reap_idle_containers(
        &mut self,
        min_idle_age: std::time::Duration,
    ) -> SchedulerUpdateRequest {
        self.reaped_containers.clear();
        let mut update = SchedulerUpdateRequest::default();
        let mut affected_executors: imbl::HashSet<ExecutorId> = imbl::HashSet::new();
        let now = tokio::time::Instant::now();

        // Drain from front of the ordered set (oldest idle first).
        // O(1) imbl clone for iteration while mutating via terminate_container.
        let snapshot = self.idle_containers.clone();
        for (idle_since, container_id) in &snapshot {
            // OrdSet is sorted oldest-first. Once a container is too young,
            // all subsequent ones are too — break instead of continue.
            if now.duration_since(*idle_since) < min_idle_age {
                break;
            }
            let Some(fc) = self.function_containers.get(container_id) else {
                continue;
            };

            // Already terminated (stale entry)
            if matches!(fc.desired_state, ContainerState::Terminated { .. }) {
                continue;
            }

            // Can't be removed (sandbox, has allocations, on allowlist)
            if !self.fe_can_be_removed(fc) {
                continue;
            }

            // Respect min_containers and buffer_containers per pool.
            // terminate_container already decrements pool_container_count, so
            // reading pool_container_count() here reflects prior reaps in this
            // loop — no separate eviction counter needed.
            if let Some(pool_key) = fc.function_container.pool_key() {
                let count = self.pool_container_count(&pool_key);
                let floor = self
                    .get_pool(&pool_key)
                    .map(|p| {
                        let min = p.min_containers.unwrap_or(0);
                        let buffer = p.buffer_containers.unwrap_or(0);
                        min.max(buffer)
                    })
                    .unwrap_or(0);
                if count <= floor {
                    continue;
                }
            }

            let executor_id = fc.executor_id.clone();
            let container = fc.function_container.clone();
            let fn_uri = FunctionURI::from(&fc.function_container);

            // Terminate: marks Terminated, cleans pool/fn_uri/idle indices
            if let Ok(Some(term_update)) = self.terminate_container(container_id) {
                update.extend(term_update);
            }

            // Optimistically free resources on executor.
            // Inline index manipulation to avoid conflicting borrows.
            if let Some(executor_state) = self.executor_states.get_mut(&executor_id) {
                let old_mem = executor_state.free_resources.memory_bytes;
                self.executors_by_free_memory
                    .remove(&(old_mem, executor_id.clone()));
                let _ = executor_state.remove_container(&container);
                let new_res = &executor_state.free_resources;
                if new_res.memory_bytes > 0 && new_res.cpu_ms_per_sec > 0 {
                    self.executors_by_free_memory
                        .insert((new_res.memory_bytes, executor_id.clone()));
                }
            }
            affected_executors.insert(executor_id);

            // Track for restoration by create_container_for_function
            self.reaped_containers
                .entry(fn_uri)
                .or_default()
                .push(container_id.clone());
        }

        // Include updated executor states for persistence
        for executor_id in &affected_executors {
            if let Some(state) = self.executor_states.get(executor_id) {
                update
                    .updated_executor_states
                    .insert(executor_id.clone(), state.clone());
            }
        }

        update
    }

    /// Restore a container that was reaped this batch for the given function.
    /// Un-terminates the container, re-consumes resources on its executor, and
    /// returns an update for persistence. Returns None if no reaped container
    /// exists for this function or if the executor is no longer available.
    fn restore_reaped_container(&mut self, fn_uri: &FunctionURI) -> Option<SchedulerUpdateRequest> {
        let reaped = self.reaped_containers.get_mut(fn_uri)?;
        // LIFO: pop from back so that the most-recently-reaped container (likely
        // still warm) is restored first.
        let container_id = reaped.pop()?;
        if reaped.is_empty() {
            self.reaped_containers.remove(fn_uri);
        }

        let fc = self.function_containers.get(&container_id)?;
        let executor_id = fc.executor_id.clone();
        let container = fc.function_container.clone();

        // Verify executor still exists before restoring
        if !self.executor_states.contains_key(&executor_id) {
            return None;
        }

        // Re-consume resources on executor FIRST (reaper freed them).
        // If this fails, bail out before touching indices — no corruption.
        if let Some(executor_state) = self.executor_states.get_mut(&executor_id) {
            let old_mem = executor_state.free_resources.memory_bytes;
            self.executors_by_free_memory
                .remove(&(old_mem, executor_id.clone()));
            if executor_state.add_container(&container).is_err() {
                // Re-insert the old memory index entry (resources unchanged)
                self.executors_by_free_memory
                    .insert((old_mem, executor_id.clone()));
                return None;
            }
            let new_res = &executor_state.free_resources;
            if new_res.memory_bytes > 0 && new_res.cpu_ms_per_sec > 0 {
                self.executors_by_free_memory
                    .insert((new_res.memory_bytes, executor_id.clone()));
            }
        }

        // Resources consumed successfully — now un-terminate and update indices.
        let fc = self.function_containers.get_mut(&container_id)?;
        fc.desired_state = ContainerState::Running;
        fc.idle_since = Some(tokio::time::Instant::now());

        let fc_snapshot = *fc.clone();
        self.update_container_indices(&container_id, &fc_snapshot);

        let mut update = SchedulerUpdateRequest::default();
        if let Some(fc) = self.function_containers.get(&container_id) {
            update.containers.insert(container_id.clone(), fc.clone());
        }
        if let Some(state) = self.executor_states.get(&executor_id) {
            update
                .updated_executor_states
                .insert(executor_id, state.clone());
        }
        Some(update)
    }

    // ====================================
    // Buffer/Pool Helper Methods
    // ====================================

    /// Get the count of non-terminated containers for a pool.
    /// Terminated containers are excluded from containers_by_pool at index
    /// update time, so this is O(1).
    pub fn pool_container_count(&self, pool_key: &ContainerPoolKey) -> u32 {
        self.containers_by_pool
            .get(pool_key)
            .map(|ids| ids.len() as u32)
            .unwrap_or(0)
    }

    /// Count active (with allocations) and idle (without) containers for a
    /// function. Terminated containers are excluded from
    /// containers_by_function_uri at index update time.
    pub fn count_active_idle_containers(&self, fn_uri: &FunctionURI) -> (u32, u32) {
        let mut active = 0;
        let mut idle = 0;

        if let Some(ids) = self.containers_by_function_uri.get(fn_uri) {
            for id in ids {
                if let Some(meta) = self.function_containers.get(id) {
                    if meta.allocations.is_empty() {
                        idle += 1;
                    } else {
                        active += 1;
                    }
                }
            }
        }

        (active, idle)
    }

    /// Count claimed and warm containers for a pool.
    /// Returns true if any executors are registered.
    pub fn has_executors(&self) -> bool {
        !self.executor_states.is_empty()
    }

    /// Pool containers have pool_id set. Claimed ones also have sandbox_id set.
    /// Uses the warm_containers_by_pool index for O(1) warm count.
    pub fn count_pool_containers(&self, pool_key: &ContainerPoolKey) -> (u32, u32) {
        // Warm count is O(1) from the warm index
        let warm = self
            .warm_containers_by_pool
            .get(pool_key)
            .map(|ids| ids.len() as u32)
            .unwrap_or(0);

        // For claimed, count total non-terminated and subtract warm
        let total_non_terminated = self.pool_container_count(pool_key);

        let claimed = total_non_terminated.saturating_sub(warm);

        (claimed, warm)
    }

    /// Select idle containers to terminate (for trimming excess).
    /// Terminated containers are excluded from containers_by_function_uri at
    /// index update time.
    pub fn select_idle_containers(&self, fn_uri: &FunctionURI, count: u32) -> Vec<ContainerId> {
        let mut result = Vec::new();

        if let Some(ids) = self.containers_by_function_uri.get(fn_uri) {
            for id in ids {
                if result.len() >= count as usize {
                    break;
                }
                if let Some(meta) = self.function_containers.get(id) &&
                    meta.allocations.is_empty()
                {
                    result.push(id.clone());
                }
            }
        }

        result
    }

    /// Select warm pool containers to terminate.
    /// Warm containers have pool_id set but sandbox_id is None.
    /// Uses the warm_containers_by_pool index for O(k) lookup where k is count.
    pub fn select_warm_pool_containers(
        &self,
        pool_key: &ContainerPoolKey,
        count: u32,
    ) -> Vec<ContainerId> {
        self.warm_containers_by_pool
            .get(pool_key)
            .map(|warm_ids| warm_ids.iter().take(count as usize).cloned().collect())
            .unwrap_or_default()
    }

    /// Create a warm container for a container pool
    pub fn create_container_for_pool(
        &mut self,
        pool: &ContainerPool,
        cache: &mut FeasibilityCache,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let resources = FunctionResources {
            cpu_ms_per_sec: pool.resources.cpu_ms_per_sec,
            memory_mb: pool.resources.memory_mb,
            ephemeral_disk_mb: pool.resources.ephemeral_disk_mb,
            gpu_configs: pool
                .resources
                .gpu
                .clone()
                .map(|g| vec![g])
                .unwrap_or_default(),
        };

        let container = ContainerBuilder::default()
            .namespace(pool.namespace.clone())
            .application_name(String::new())
            .function_name(format!("pool:{}", pool.id.get()))
            .version(String::new())
            .state(ContainerState::Pending)
            .resources(pool.resources.clone())
            .max_concurrency(1u32)
            .container_type(ContainerType::Sandbox)
            .image(Some(pool.image.clone()))
            .secret_names(pool.secret_names.clone())
            .timeout_secs(pool.timeout_secs)
            .entrypoint(pool.entrypoint.clone().unwrap_or_default())
            .network_policy(pool.network_policy.clone())
            .pool_id(Some(pool.id.clone()))
            .build()?;

        self.create_container(
            &pool.namespace,
            "",
            None,
            &resources,
            container,
            ContainerType::Sandbox,
            cache,
        )
    }

    /// Update container indices for a single container.
    /// Handles function_containers, containers_by_function_uri,
    /// containers_by_pool, and warm_containers_by_pool indices.
    ///
    /// Terminated containers are removed from containers_by_pool and
    /// containers_by_function_uri so that pool_container_count() is O(1).
    fn update_container_indices(
        &mut self,
        container_id: &ContainerId,
        meta: &ContainerServerMetadata,
    ) {
        let pool_key = meta.function_container.pool_key();
        let fn_uri = FunctionURI::from(&meta.function_container);
        let is_terminated = matches!(meta.desired_state, ContainerState::Terminated { .. });

        // Check if this container was previously warm (for transition tracking)
        // A container is warm only if sandbox_id is None AND it's not terminated
        let was_warm = self
            .function_containers
            .get(container_id)
            .is_some_and(|old| {
                old.function_container.sandbox_id.is_none() &&
                    !matches!(old.desired_state, ContainerState::Terminated { .. })
            });

        // Remove old idle entry if present
        if let Some(old_fc) = self.function_containers.get(container_id) {
            if let Some(old_idle) = old_fc.idle_since {
                self.idle_containers
                    .remove(&(old_idle, container_id.clone()));
            }
        }

        // Add new idle entry if container is idle, not terminated, and is a
        // Function container. Sandbox containers are never reaped so keeping
        // them out of the idle set avoids unnecessary iteration.
        if !is_terminated && meta.container_type == ContainerType::Function {
            if let Some(idle_since) = meta.idle_since {
                self.idle_containers
                    .insert((idle_since, container_id.clone()));
            }
        }

        // Always update function_containers (keeps metadata for terminated containers)
        self.function_containers
            .insert(container_id.clone(), Box::new(meta.clone()));

        if is_terminated {
            // Remove terminated containers from count/lookup indices
            if let Some(container_ids) = self.containers_by_function_uri.get_mut(&fn_uri) {
                container_ids.retain(|id| id != container_id);
                if container_ids.is_empty() {
                    self.containers_by_function_uri.remove(&fn_uri);
                }
            }

            // Only update pool indices if container belongs to a pool
            if let Some(ref pool_key) = pool_key {
                if let Some(pool_ids) = self.containers_by_pool.get_mut(pool_key) {
                    pool_ids.retain(|id| id != container_id);
                    if pool_ids.is_empty() {
                        self.containers_by_pool.remove(pool_key);
                    }
                }

                if was_warm && let Some(warm_ids) = self.warm_containers_by_pool.get_mut(pool_key) {
                    warm_ids.retain(|id| id != container_id);
                    if warm_ids.is_empty() {
                        self.warm_containers_by_pool.remove(pool_key);
                    }
                }
            }

            if let Some(pool_key) = pool_key {
                self.mark_pool_dirty(pool_key);
            }
            // No need to unblock other pools here. The terminated container's
            // own pool is marked dirty above. Other blocked pools will be
            // retried when new capacity appears (new executor joins).
        } else {
            // Non-terminated: add to count/lookup indices
            self.containers_by_function_uri
                .entry(fn_uri)
                .or_default()
                .insert(container_id.clone());

            // Only update pool indices if container belongs to a pool
            if let Some(pool_key) = pool_key {
                self.containers_by_pool
                    .entry(pool_key.clone())
                    .or_default()
                    .insert(container_id.clone());

                // Update warm_containers_by_pool index based on warm status
                let is_warm = meta.function_container.sandbox_id.is_none();
                if is_warm {
                    self.warm_containers_by_pool
                        .entry(pool_key.clone())
                        .or_default()
                        .insert(container_id.clone());
                } else if was_warm {
                    // Container was warm but is now claimed - remove from warm index
                    if let Some(warm_ids) = self.warm_containers_by_pool.get_mut(&pool_key) {
                        warm_ids.retain(|id| id != container_id);
                        if warm_ids.is_empty() {
                            self.warm_containers_by_pool.remove(&pool_key);
                        }
                    }
                }

                self.mark_pool_dirty(pool_key);
            }
        }
    }

    /// Apply a container update to internal state (for iterative creation)
    pub fn apply_container_update(&mut self, update: &SchedulerUpdateRequest) {
        // Update function containers index
        for (id, meta) in &update.containers {
            self.update_container_indices(id, meta);
        }

        // Update executor states
        for (id, state) in &update.updated_executor_states {
            self.set_executor_state(id.clone(), state.clone());
        }
    }

    /// Claim a warm pool container for a sandbox.
    /// Finds an unclaimed container (pool_id matches, sandbox_id is None) and
    /// claims it. Returns the container ID and executor ID if found, along
    /// with a scheduler update.
    /// Uses the warm_containers_by_pool index for O(1) lookup.
    pub fn claim_pool_container(
        &mut self,
        pool_key: &ContainerPoolKey,
        sandbox_id: &SandboxId,
        sandbox_timeout_secs: u64,
    ) -> Option<(ContainerId, ExecutorId, SchedulerUpdateRequest)> {
        let warm_container = self
            .warm_containers_by_pool
            .get(pool_key)
            .and_then(|warm_ids| {
                warm_ids.iter().find_map(|id| {
                    self.function_containers
                        .get(id)
                        .map(|meta| (id.clone(), meta.executor_id.clone()))
                })
            });

        let (container_id, executor_id) = warm_container?;

        // Claim it by setting sandbox_id and the sandbox's timeout
        // The warm index only contains containers with sandbox_id = None
        let meta = self.function_containers.get_mut(&container_id)?;
        meta.function_container.sandbox_id = Some(sandbox_id.clone());
        meta.function_container.timeout_secs = sandbox_timeout_secs;

        // Remove from warm index (container is no longer warm after claiming)
        if let Some(warm_ids) = self.warm_containers_by_pool.get_mut(pool_key) {
            warm_ids.retain(|id| id != &container_id);
            if warm_ids.is_empty() {
                self.warm_containers_by_pool.remove(pool_key);
            }
        }

        // Create update to persist the claim
        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(container_id.clone(), meta.clone());

        // Pool's warm count changed
        self.mark_pool_dirty(pool_key.clone());

        info!(
            namespace = %pool_key.namespace,
            pool_id = %pool_key.pool_id.get(),
            sandbox_id = %sandbox_id.get(),
            container_id = %container_id.get(),
            "Warm pool container claimed for sandbox"
        );

        Some((container_id, executor_id, update))
    }
}
