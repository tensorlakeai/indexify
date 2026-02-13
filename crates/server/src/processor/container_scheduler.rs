use std::sync::Arc;

use anyhow::Result;
use opentelemetry::metrics::ObservableGauge;
use rand::seq::IndexedRandom;
use tokio::sync::RwLock;
use tracing::{error, info};

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
        ContainerType,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        Function,
        FunctionExecutorTerminationReason,
        FunctionResources,
        FunctionURI,
        Sandbox,
        SandboxId,
    },
    state_store::{
        requests::{RequestPayload, SchedulerUpdatePayload, SchedulerUpdateRequest},
        scanner::StateReader,
        state_machine::IndexifyObjectsColumns,
    },
};

const SANDBOX_ENABLED_DATAPLANE_VERSION: &str = "0.2.0";

fn dataplane_functions_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("DATAPLANE_FUNCTIONS_ENABLED")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false)
    })
}

/// Gauges for monitoring the container scheduler state.
/// Must be kept alive for callbacks to fire.
#[allow(dead_code)]
pub struct ContainerSchedulerGauges {
    pub total_executors: ObservableGauge<u64>,
}

impl ContainerSchedulerGauges {
    pub fn new(container_scheduler: Arc<RwLock<ContainerScheduler>>) -> Self {
        let meter = opentelemetry::global::meter("container_scheduler");
        let scheduler_clone = container_scheduler.clone();
        let total_executors = meter
            .u64_observable_gauge("indexify.total_executors")
            .with_description("Total number of executors")
            .with_callback(move |observer| {
                if let Ok(scheduler) = scheduler_clone.try_read() {
                    observer.observe(scheduler.executor_states.len() as u64, &[]);
                }
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

/// Priority for container eviction during vacuum.
/// Lower values = higher priority to evict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EvictionPriority {
    /// Pool is above buffer level - prefer to evict
    AboveBuffer = 0,
    /// Pool is above min but at/below buffer - can evict if needed
    AboveMin = 1,
    /// Pool is at min - only evict if requestor is desperate (below min)
    AtMin = 2,
    /// Pool is below min - never evict
    BelowMin = 3,
}

#[derive(Debug)]
pub struct ContainerScheduler {
    pub clock: u64,
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
    pub dirty_pools: std::collections::HashSet<ContainerPoolKey>,
    // Pools that need containers but couldn't get resources. Skipped by
    // BufferReconciler until resources become available (new executor joins or
    // container terminates).
    pub blocked_pools: std::collections::HashSet<ContainerPoolKey>,
}

impl ContainerScheduler {
    pub async fn new(clock: u64, reader: &StateReader) -> Result<Self> {
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

        // Mark all loaded pools as dirty for initial reconciliation
        let mut dirty_pools = std::collections::HashSet::new();
        for key in function_pools.keys() {
            dirty_pools.insert(key.clone());
        }
        for key in sandbox_pools.keys() {
            dirty_pools.insert(key.clone());
        }

        Ok(Self {
            executors: imbl::HashMap::new(),
            containers_by_function_uri: imbl::HashMap::new(),
            function_containers: imbl::HashMap::new(),
            executor_states: imbl::HashMap::new(),
            containers_by_pool: imbl::HashMap::new(),
            warm_containers_by_pool: imbl::HashMap::new(),
            function_pools,
            sandbox_pools,
            clock,
            dirty_pools,
            blocked_pools: std::collections::HashSet::new(),
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

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        Arc::new(tokio::sync::RwLock::new(ContainerScheduler {
            clock: self.clock,
            executors: self.executors.clone(),
            containers_by_function_uri: self.containers_by_function_uri.clone(),
            function_containers: self.function_containers.clone(),
            executor_states: self.executor_states.clone(),
            containers_by_pool: self.containers_by_pool.clone(),
            warm_containers_by_pool: self.warm_containers_by_pool.clone(),
            function_pools: self.function_pools.clone(),
            sandbox_pools: self.sandbox_pools.clone(),
            dirty_pools: self.dirty_pools.clone(),
            blocked_pools: self.blocked_pools.clone(),
        }))
    }

    fn upsert_executor(&mut self, executor_metadata: &ExecutorMetadata) {
        // Only update executor metadata here.
        // num_allocations updates are handled in update_scheduler_update
        // when processing updated_allocations from the processors.
        let is_new = !self.executors.contains_key(&executor_metadata.id);
        self.executors.insert(
            executor_metadata.id.clone(),
            executor_metadata.clone().into(),
        );
        // New executor may have resources for previously blocked pools
        if is_new && !executor_metadata.tombstoned {
            self.unblock_all_pools();
        }
    }

    fn deregister_executor(&mut self, executor_id: &ExecutorId) {
        if let Some(executor) = self.executors.get_mut(executor_id) {
            executor.tombstoned = true;
        }
    }

    fn delete_application(&mut self, namespace: &str, name: &str) {
        // Mark existing containers' desired_state as Terminated and collect
        // info for index removal (terminated containers are excluded from
        // containers_by_pool and containers_by_function_uri).
        let mut containers_to_remove: Vec<(
            Option<ContainerPoolKey>,
            FunctionURI,
            ContainerId,
            bool,
        )> = Vec::new();

        for (container_id, fc) in self.function_containers.iter_mut() {
            if fc.function_container.namespace == namespace &&
                fc.function_container.application_name == name
            {
                // Track non-terminated containers that we're about to terminate
                if !matches!(fc.desired_state, ContainerState::Terminated { .. }) {
                    let was_warm = fc.function_container.sandbox_id.is_none();
                    containers_to_remove.push((
                        fc.function_container.pool_key(),
                        FunctionURI::from(&fc.function_container),
                        container_id.clone(),
                        was_warm,
                    ));
                }

                fc.desired_state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                    failed_alloc_ids: vec![],
                };
            }
        }

        // Remove terminated containers from all indices and mark pools dirty
        for (pool_key, fn_uri, container_id, was_warm) in containers_to_remove {
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

        // Collect fn_uris before mutable borrow to mark terminated
        let fn_uris: Vec<(ContainerId, FunctionURI)> = warm_ids
            .iter()
            .filter_map(|container_id| {
                self.function_containers.get(container_id).and_then(|fc| {
                    if !matches!(fc.desired_state, ContainerState::Terminated { .. }) {
                        Some((
                            container_id.clone(),
                            FunctionURI::from(&fc.function_container),
                        ))
                    } else {
                        None
                    }
                })
            })
            .collect();

        // Mark each warm container as terminated
        for (container_id, _) in &fn_uris {
            if let Some(fc) = self.function_containers.get_mut(container_id) {
                fc.desired_state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                    failed_alloc_ids: vec![],
                };
            }
        }

        // Clear the warm index for this pool since all are now terminated
        self.warm_containers_by_pool.remove(&pool_key);

        // Remove terminated containers from containers_by_pool
        if let Some(pool_ids) = self.containers_by_pool.get_mut(&pool_key) {
            for container_id in &warm_ids {
                pool_ids.retain(|id| id != container_id);
            }
            if pool_ids.is_empty() {
                self.containers_by_pool.remove(&pool_key);
            }
        }

        // Remove terminated containers from containers_by_function_uri
        for (container_id, fn_uri) in &fn_uris {
            if let Some(container_ids) = self.containers_by_function_uri.get_mut(fn_uri) {
                container_ids.retain(|id| id != container_id);
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

    /// Mark a pool as dirty (needs reconciliation). Also removes it from the
    /// blocked set since a state change may have made it satisfiable.
    fn mark_pool_dirty(&mut self, pool_key: ContainerPoolKey) {
        self.blocked_pools.remove(&pool_key);
        self.dirty_pools.insert(pool_key);
    }

    /// Take the dirty pool set, replacing it with an empty set.
    /// Called by BufferReconciler to consume the dirty set.
    pub fn take_dirty_pools(&mut self) -> std::collections::HashSet<ContainerPoolKey> {
        std::mem::take(&mut self.dirty_pools)
    }

    /// Move all blocked pools to the dirty set for re-evaluation.
    /// Called when new resources become available (e.g., new executor joins).
    pub fn unblock_all_pools(&mut self) {
        self.dirty_pools.extend(self.blocked_pools.drain());
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

            self.executor_states
                .insert(executor_id.clone(), Box::new(merged));
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
            let _ = self.executor_states.remove(removed_executor_id);
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
            // Removed container frees resources — unblock all pools
            self.unblock_all_pools();
        }
    }

    pub fn create_container_for_function(
        &mut self,
        namespace: &str,
        application: &str,
        version: &str,
        function: &Function,
        application_state: &ApplicationState,
        is_critical: bool,
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

        let pool_key = ContainerPoolKey::new(namespace, &pool_id);

        self.create_container(
            namespace,
            application,
            Some(function),
            &function.resources,
            function_container,
            ContainerType::Function,
            Some(&pool_key),
            is_critical,
        )
    }

    pub fn create_container_for_sandbox(
        &mut self,
        sandbox: &Sandbox,
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
            .build()?;

        self.create_container(
            &sandbox.namespace,
            "",   // Sandboxes are not associated with applications
            None, // No function for sandboxes
            &resources,
            function_container,
            ContainerType::Sandbox,
            None, // Sandboxes don't have a requesting pool for vacuum priority
            true, // Critical: Sandbox creation
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_container(
        &mut self,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        resources: &FunctionResources,
        function_container: Container,
        container_type: ContainerType,
        requesting_pool_key: Option<&ContainerPoolKey>,
        is_critical: bool,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let mut candidates = self.candidate_hosts(namespace, application, function, resources);
        let mut update = SchedulerUpdateRequest::default();

        // If no candidates, try vacuuming to free up resources
        // NOTE: We only mark containers for termination here, we do NOT free their
        // resources. Resources are only freed when the executor confirms the
        // container is actually terminated (via the reconciler). This prevents
        // overcommit where we schedule new containers before old ones have
        // actually stopped.
        if candidates.is_empty() {
            let function_executors_to_remove = self.vacuum_function_container_candidates(
                resources,
                requesting_pool_key,
                namespace,
                application,
                function,
                is_critical,
            );
            for fe in function_executors_to_remove {
                let mut update_fe = fe.clone();
                update_fe.desired_state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                    failed_alloc_ids: Vec::new(),
                };
                info!(
                    executor_id = %fe.executor_id,
                    container_id = %fe.function_container.id,
                    sandbox_id = ?fe.function_container.sandbox_id,
                    "vacuum: marking container for termination"
                );
                // Don't call remove_container here - that would free resources immediately.
                // Resources should only be freed when executor confirms termination.
                update.containers.insert(
                    update_fe.function_container.id.clone(),
                    Box::new(update_fe.clone()),
                );

                // Notify the executor so it receives the updated desired state
                // (with this container excluded) and terminates the container.
                // Without this, the executor never learns about the vacuum and
                // keeps running the container, permanently consuming resources.
                if let Some(executor_state) = self.executor_states.get(&fe.executor_id) {
                    update
                        .updated_executor_states
                        .insert(fe.executor_id.clone(), executor_state.clone());
                }
            }
        }

        // Apply vacuum updates (just marks containers for termination, doesn't free
        // resources)
        self.update(&RequestPayload::SchedulerUpdate(
            SchedulerUpdatePayload::new(update.clone()),
        ))?;

        // Try again after vacuum
        candidates = self.candidate_hosts(namespace, application, function, resources);
        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            // No host available, return vacuum update (container stays pending)
            return Ok(Some(update));
        };

        let executor_id = candidate.executor_id.clone();
        if !self.executor_states.contains_key(&executor_id) {
            return Ok(Some(update));
        }

        // Consume resources
        let _ = candidate
            .free_resources
            .consume_function_resources(resources)?;

        // Register the container
        let container_update =
            self.register_container(executor_id, function_container, container_type)?;
        update.extend(container_update);

        Ok(Some(update))
    }

    /// Checks if an executor can run a container based on placement
    /// constraints.s
    ///
    /// For functions: checks tombstone, allowlist, placement constraints, and
    /// version. For sandboxes: checks tombstone, allowlist, and version.
    fn executor_matches_constraints(
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
            // Functions can't run on sandbox-enabled dataplanes unless opted in.
            if !dataplane_functions_enabled() &&
                executor
                    .executor_version
                    .eq_ignore_ascii_case(SANDBOX_ENABLED_DATAPLANE_VERSION)
            {
                return false;
            }
        } else {
            // Sandbox container constraints
            if !executor
                .executor_version
                .eq_ignore_ascii_case(SANDBOX_ENABLED_DATAPLANE_VERSION)
            {
                return false;
            }
            if !executor.is_app_allowed(namespace, application) {
                return false;
            }
        }

        true
    }

    fn candidate_hosts(
        &self,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        resources: &FunctionResources,
    ) -> Vec<ExecutorServerMetadata> {
        let mut candidates = Vec::new();

        for (_, executor_state) in &self.executor_states {
            let Some(executor) = self.executors.get(&executor_state.executor_id) else {
                error!(
                    executor_id = executor_state.executor_id.get(),
                    "executor not found for candidate executors but was found in executor_states"
                );
                continue;
            };

            if !self.executor_matches_constraints(executor, namespace, application, function) {
                continue;
            }

            // Check resources
            let resource_check = executor_state
                .free_resources
                .can_handle_function_resources(resources);

            if resource_check.is_ok() {
                candidates.push(*executor_state.clone());
            }
        }

        candidates
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

        executor_server_metadata.add_container(&function_container)?;

        let fe_server_metadata = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container,
            desired_state: ContainerState::Running,
            container_type,
            allocations: std::collections::HashSet::new(),
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

    #[tracing::instrument(skip_all)]
    pub fn vacuum_function_container_candidates(
        &self,
        fe_resource: &FunctionResources,
        requesting_pool_key: Option<&ContainerPoolKey>,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        is_critical: bool,
    ) -> Vec<ContainerServerMetadata> {
        // Determine if the requesting pool is below its min (desperate mode)
        let requesting_pool_desperate = requesting_pool_key
            .and_then(|key| {
                self.get_pool(key).map(|pool| {
                    let min = pool.min_containers.unwrap_or(0);
                    let current = self.pool_container_count(key);
                    current < min
                })
            })
            .unwrap_or(false);

        // Helper to compute eviction priority for a container given current pool count
        let compute_priority =
            |pool_key: &ContainerPoolKey, current_count: u32| -> EvictionPriority {
                let (min, buffer) = self
                    .get_pool(pool_key)
                    .map(|p| {
                        (
                            p.min_containers.unwrap_or(0),
                            p.buffer_containers.unwrap_or(0),
                        )
                    })
                    .unwrap_or((0, 0));

                if current_count <= min {
                    if current_count < min {
                        EvictionPriority::BelowMin
                    } else {
                        EvictionPriority::AtMin
                    }
                } else if current_count > min + buffer {
                    EvictionPriority::AboveBuffer
                } else {
                    EvictionPriority::AboveMin
                }
            };

        // For each executor in the system that can run the target container
        for (executor_id, executor) in &self.executors {
            // Only vacuum from executors that could actually run the new container
            if !self.executor_matches_constraints(executor, namespace, application, function) {
                continue;
            }
            let Some(executor_state) = self.executor_states.get(executor_id) else {
                continue;
            };

            // Get function executors for this executor from our in-memory state
            let function_executors = executor_state.function_container_ids.clone();

            // Start with the current free resources on this executor
            let mut available_resources = executor_state.free_resources.clone();

            // Collect candidates with their initial eviction priority (based on real
            // counts)
            let mut candidates: Vec<(EvictionPriority, ContainerServerMetadata)> = Vec::new();

            for fe_metadata in function_executors.iter() {
                // Skip if the FE is already marked for termination
                let Some(fe_server_metadata) = self.function_containers.get(fe_metadata) else {
                    continue;
                };
                if matches!(
                    fe_server_metadata.desired_state,
                    ContainerState::Terminated { .. }
                ) {
                    continue;
                }

                if self.fe_can_be_removed(fe_server_metadata) {
                    // Get pool key for this container
                    let pool_key = fe_server_metadata.function_container.pool_key();

                    // Compute priority using REAL count (not simulated)
                    // Containers without a pool (standalone sandboxes) are freely evictable
                    let priority = match &pool_key {
                        Some(key) => {
                            let current = self.pool_container_count(key);
                            compute_priority(key, current)
                        }
                        None => EvictionPriority::AboveBuffer,
                    };

                    candidates.push((priority, *fe_server_metadata.clone()));
                }
            }

            // Sort candidates by priority (lowest priority value = highest priority to
            // evict)
            candidates.sort_by_key(|(priority, _)| *priority);

            // Track how many containers we've decided to evict from each pool
            let mut evicted_from_pool: imbl::HashMap<ContainerPoolKey, u32> = imbl::HashMap::new();

            let mut function_executors_to_remove = Vec::new();
            for (priority, fe_server_metadata) in candidates {
                let pool_key = fe_server_metadata.function_container.pool_key();

                // Re-compute priority based on what we've already decided to evict
                // Containers without a pool are always AboveBuffer (freely evictable)
                let (already_evicted, current_priority) = match &pool_key {
                    Some(key) => {
                        let already = evicted_from_pool.get(key).copied().unwrap_or(0);
                        let effective_count =
                            self.pool_container_count(key).saturating_sub(already);
                        (already, compute_priority(key, effective_count))
                    }
                    None => (0, EvictionPriority::AboveBuffer),
                };

                // Use the stricter of the initial vs current priority
                let effective_priority = std::cmp::max(priority, current_priority);

                // Skip BelowMin always
                if effective_priority == EvictionPriority::BelowMin {
                    continue;
                }

                // If this is not a critical request (e.g. just filling a buffer),
                // we can ONLY steal from AboveBuffer (true excess).
                // We cannot steal from AboveMin (which is the other pool's buffer).
                if !is_critical && effective_priority > EvictionPriority::AboveBuffer {
                    continue;
                }

                // Skip AtMin unless desperate
                if effective_priority == EvictionPriority::AtMin && !requesting_pool_desperate {
                    continue;
                }

                // In desperate mode, don't steal from the requesting pool itself
                if requesting_pool_desperate &&
                    let Some(req_key) = requesting_pool_key &&
                    pool_key.as_ref() == Some(req_key)
                {
                    continue;
                }

                let mut simulated_resources = available_resources.clone();
                if simulated_resources
                    .free(&fe_server_metadata.function_container.resources)
                    .is_err()
                {
                    continue;
                }

                // Record that we're evicting from this pool
                if let Some(key) = pool_key {
                    evicted_from_pool.insert(key, already_evicted + 1);
                }

                function_executors_to_remove.push(fe_server_metadata);
                available_resources = simulated_resources;

                if available_resources
                    .can_handle_function_resources(fe_resource)
                    .is_ok()
                {
                    return function_executors_to_remove;
                }
            }
        }

        Vec::new()
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

        // Mark for termination
        let fc = self.function_containers.get_mut(container_id).unwrap();
        fc.desired_state = ContainerState::Terminated {
            reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
            failed_alloc_ids: vec![],
        };

        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(container_id.clone(), fc.clone());

        Ok(Some(update))
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
        is_critical: bool,
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

        // Build requesting pool key for vacuum priority
        let pool_key = ContainerPoolKey::new(&pool.namespace, &pool.id);

        self.create_container(
            &pool.namespace,
            "",
            None,
            &resources,
            container,
            ContainerType::Sandbox,
            Some(&pool_key),
            is_critical,
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
            // Terminated container frees resources on its executor, which could
            // satisfy any blocked pool — not just the one this container belonged to.
            self.unblock_all_pools();
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
            self.executor_states.insert(id.clone(), state.clone());
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

        // Claim it by setting sandbox_id
        // The warm index only contains containers with sandbox_id = None
        let meta = self.function_containers.get_mut(&container_id)?;
        meta.function_container.sandbox_id = Some(sandbox_id.clone());

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

        Some((container_id, executor_id, update))
    }
}
