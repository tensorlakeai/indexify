use std::collections::HashSet;

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::{
    data_model::{ApplicationState, ContainerPool, ContainerPoolKey, ContainerState},
    processor::container_scheduler::ContainerScheduler,
    state_store::{
        in_memory_state::{InMemoryState, ResourceProfile, ResourceProfileHistogram},
        requests::SchedulerUpdateRequest,
    },
};

/// Reconciles container buffers for all container pools.
/// This processor runs after work allocation to ensure the cluster
/// has the correct number of warm containers.
#[derive(Default)]
pub struct BufferReconciler;

impl BufferReconciler {
    pub fn new() -> Self {
        Self
    }

    /// Main entry point: reconcile pool buffers for dirty pools only.
    /// Called at the end of each state change processing cycle.
    ///
    /// Only processes pools whose container count or config changed since the
    /// last reconciliation (dirty pools). Pools that previously failed to get
    /// resources (blocked pools) are skipped until resources become available.
    ///
    /// ## buffer_containers Semantics
    ///
    /// When `buffer_containers` is not configured (None):
    /// - **Phase 1**: Not affected (creates up to min_containers)
    /// - **Phase 2**: Treats as 0 (no additional buffer containers created)
    /// - **Phase 3**: Preserves existing containers (no trimming for warm
    ///   starts)
    /// - **Deficit**: Treats as 0 (only reports deficit for min, not buffer)
    ///
    /// When `buffer_containers` is explicitly set (including 0):
    /// - All phases use the configured value
    /// - Setting to 0 enables aggressive scale-to-zero behavior
    pub fn reconcile(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Only process pools that changed since last reconciliation
        let dirty_pools = container_scheduler.take_dirty_pools();

        // Fast path: nothing to do. Deficit values in the state store are
        // preserved because we return None for the deficit fields.
        if dirty_pools.is_empty() && container_scheduler.blocked_pools.is_empty() {
            return Ok(update);
        }

        // Look up each dirty pool, skipping blocked pools and tombstoned pools
        let pools: Vec<ContainerPool> = dirty_pools
            .iter()
            .filter(|key| !container_scheduler.blocked_pools.contains(key))
            .filter_map(|key| container_scheduler.get_pool(key).cloned())
            .collect();

        // Collect previously blocked pools for deficit-only computation in Phase 3.
        // These pools are excluded from container creation (Phases 1/2) but their
        // deficits still need to be reported so the API doesn't drop to zero.
        let blocked_pools: Vec<ContainerPool> = container_scheduler
            .blocked_pools
            .iter()
            .filter_map(|key| container_scheduler.get_pool(key).cloned())
            .collect();

        // Track pools that can't be satisfied within this reconciliation cycle
        let mut blocked: HashSet<ContainerPoolKey> = HashSet::new();

        // Phase 1: Ensure minimums are met (highest priority)
        for pool in &pools {
            let pool_key = ContainerPoolKey::from(pool);
            if blocked.contains(&pool_key) {
                continue;
            }

            let min = pool.min_containers.unwrap_or(0);
            let max = pool.max_containers.unwrap_or(u32::MAX);
            // Don't exceed max even when meeting min (handles invalid min > max config)
            let effective_min = min.min(max);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current = active + idle;

            if current < effective_min {
                let needed = effective_min - current;
                let (ns, identity) = self.pool_identity(pool);
                info!(
                    namespace = %ns,
                    pool_identity = %identity,
                    active,
                    idle,
                    min,
                    needed,
                    phase = "ensure_min",
                    "Creating containers to meet minimum"
                );
                for _ in 0..needed {
                    match self.create_container_for_pool(
                        pool,
                        in_memory_state,
                        container_scheduler,
                        true,
                    ) {
                        Ok(Some(u)) => {
                            let placed = u.containers.values().any(|c| {
                                !matches!(c.desired_state, ContainerState::Terminated { .. })
                            });
                            container_scheduler.apply_container_update(&u);
                            update.extend(u);
                            if !placed {
                                blocked.insert(pool_key.clone());
                                break;
                            }
                        }
                        Ok(None) => {
                            warn!(pool_id = %pool.id.get(), "Cannot meet min - no resources");
                            blocked.insert(pool_key.clone());
                            break;
                        }
                        Err(e) => {
                            warn!(pool_id = %pool.id.get(), error = %e, "Error creating pool container");
                            break;
                        }
                    }
                }
            }
        }

        // Phase 2: Fill buffers with remaining resources (round-robin for fairness)
        loop {
            let mut any_created = false;

            for pool in &pools {
                let pool_key = ContainerPoolKey::from(pool);
                if blocked.contains(&pool_key) {
                    continue;
                }

                let buffer = pool.buffer_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let (active, idle) = self.count_pool_containers(pool, container_scheduler);

                // Need more idle and not at max
                if idle < buffer && (active + idle) < max {
                    let (ns, identity) = self.pool_identity(pool);
                    debug!(
                        namespace = %ns,
                        pool_identity = %identity,
                        active,
                        idle,
                        buffer,
                        max,
                        phase = "fill_buffer",
                        "Filling buffer containers"
                    );
                    match self.create_container_for_pool(
                        pool,
                        in_memory_state,
                        container_scheduler,
                        false,
                    ) {
                        Ok(Some(u)) => {
                            // Only count as progress if a container was actually
                            // placed on an executor (not just a vacuum update).
                            // create_container returns Ok(Some(update)) even when
                            // no host has resources — treating that as progress
                            // causes an infinite loop.
                            let placed = u.containers.values().any(|c| {
                                !matches!(c.desired_state, ContainerState::Terminated { .. })
                            });
                            container_scheduler.apply_container_update(&u);
                            update.extend(u);
                            if placed {
                                any_created = true;
                            } else {
                                blocked.insert(pool_key);
                            }
                        }
                        Ok(None) => {
                            blocked.insert(pool_key);
                        }
                        Err(e) => {
                            warn!(pool_id = %pool.id.get(), error = %e, "Error filling buffer");
                        }
                    }
                }
            }

            if !any_created {
                break;
            }
        }

        // Phase 3: Trim excess idle containers AND compute deficits
        // (Combined to avoid duplicate container counting)
        let mut function_pool_deficits = ResourceProfileHistogram::default();
        let mut sandbox_pool_deficits = ResourceProfileHistogram::default();

        for pool in &pools {
            let min = pool.min_containers.unwrap_or(0);
            let max = pool.max_containers.unwrap_or(u32::MAX);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current_total = active + idle;

            let pool_key = ContainerPoolKey::from(pool);
            if !blocked.contains(&pool_key) {
                // If buffer is explicitly set, target = active + buffer (at least min)
                // If buffer is not set, preserve existing containers for warm starts
                let target_total = if let Some(buffer) = pool.buffer_containers {
                    (active + buffer).max(min)
                } else {
                    current_total.max(min)
                };
                let effective_limit = target_total.min(max);

                // Trim excess idle containers
                if current_total > effective_limit && idle > 0 {
                    let excess = (current_total - effective_limit).min(idle);
                    let (ns, identity) = self.pool_identity(pool);
                    info!(
                        namespace = %ns,
                        pool_identity = %identity,
                        active,
                        idle,
                        current_total,
                        target_total,
                        effective_limit,
                        excess,
                        phase = "trim_excess",
                        "Trimming excess idle containers"
                    );
                    let trim_update =
                        self.trim_idle_containers(pool, excess, container_scheduler)?;
                    update.extend(trim_update);
                }
            }

            // Compute deficit for this pool (uses buffer=0 when None for deficit reporting)
            let buffer = pool.buffer_containers.unwrap_or(0);
            let deficit_target = (active + buffer).max(min).min(max);
            if current_total < deficit_target {
                let deficit = deficit_target - current_total;
                let profile = ResourceProfile::from_container_resources(&pool.resources);
                if pool.is_function_pool() {
                    function_pool_deficits.increment_by(profile, deficit as u64);
                } else {
                    sandbox_pool_deficits.increment_by(profile, deficit as u64);
                }
            }
        }

        // Also compute deficits for previously blocked pools (excluded from `pools`
        // for container creation, but their resource gaps must still be reported).
        for pool in &blocked_pools {
            let min = pool.min_containers.unwrap_or(0);
            let max = pool.max_containers.unwrap_or(u32::MAX);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current_total = active + idle;

            let buffer = pool.buffer_containers.unwrap_or(0);
            let deficit_target = (active + buffer).max(min).min(max);
            if current_total < deficit_target {
                let deficit = deficit_target - current_total;
                let profile = ResourceProfile::from_container_resources(&pool.resources);
                if pool.is_function_pool() {
                    function_pool_deficits.increment_by(profile, deficit as u64);
                } else {
                    sandbox_pool_deficits.increment_by(profile, deficit as u64);
                }
            }
        }

        // Propagate newly blocked pools to the scheduler before Phase 4
        container_scheduler
            .blocked_pools
            .extend(blocked.iter().cloned());

        // Phase 4: Try to satisfy blocked pools with available capacity.
        // When a pool is unblocked (first container placed), run full
        // min+buffer reconciliation for it inline — don't defer to the next
        // cycle. Stops as soon as a full pass makes no progress (capacity
        // exhausted).
        if !container_scheduler.blocked_pools.is_empty()
            && container_scheduler.has_available_capacity()
        {
            let blocked_keys: Vec<ContainerPoolKey> =
                container_scheduler.blocked_pools.iter().cloned().collect();

            // Probe: try to place one container per blocked pool.
            // If successful, run full min+buffer for that pool.
            for pool_key in &blocked_keys {
                if !container_scheduler.blocked_pools.contains(pool_key) {
                    continue;
                }
                let Some(pool) = container_scheduler.get_pool(pool_key).cloned() else {
                    container_scheduler.blocked_pools.remove(pool_key);
                    continue;
                };

                // Probe: can we place at least one container?
                let probed = match self.create_container_for_pool(
                    &pool,
                    in_memory_state,
                    container_scheduler,
                    true,
                ) {
                    Ok(Some(u)) => {
                        let placed = u.containers.values().any(|c| {
                            !matches!(c.desired_state, ContainerState::Terminated { .. })
                        });
                        container_scheduler.apply_container_update(&u);
                        update.extend(u);
                        placed
                    }
                    _ => false,
                };

                if !probed {
                    continue;
                }

                // Pool is unblocked — run full min+buffer inline
                container_scheduler.blocked_pools.remove(pool_key);

                // Phase 4a: ensure min
                let min = pool.min_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let effective_min = min.min(max);

                loop {
                    let (active, idle) =
                        self.count_pool_containers(&pool, container_scheduler);
                    if active + idle >= effective_min {
                        break;
                    }
                    match self.create_container_for_pool(
                        &pool,
                        in_memory_state,
                        container_scheduler,
                        true,
                    ) {
                        Ok(Some(u)) => {
                            let placed = u.containers.values().any(|c| {
                                !matches!(c.desired_state, ContainerState::Terminated { .. })
                            });
                            container_scheduler.apply_container_update(&u);
                            update.extend(u);
                            if !placed {
                                break;
                            }
                        }
                        _ => break,
                    }
                }

                // Phase 4b: fill buffer
                let buffer = pool.buffer_containers.unwrap_or(0);
                loop {
                    let (active, idle) =
                        self.count_pool_containers(&pool, container_scheduler);
                    if idle >= buffer || (active + idle) >= max {
                        break;
                    }
                    match self.create_container_for_pool(
                        &pool,
                        in_memory_state,
                        container_scheduler,
                        false,
                    ) {
                        Ok(Some(u)) => {
                            let placed = u.containers.values().any(|c| {
                                !matches!(c.desired_state, ContainerState::Terminated { .. })
                            });
                            container_scheduler.apply_container_update(&u);
                            update.extend(u);
                            if !placed {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
            }

            // Recompute deficits for pools that are still blocked after Phase 4
            function_pool_deficits = ResourceProfileHistogram::default();
            sandbox_pool_deficits = ResourceProfileHistogram::default();

            // Re-include deficits from dirty pools (Phase 3 already computed these
            // but the histograms were consumed, so recompute for all non-satisfied pools)
            for pool in &pools {
                let min = pool.min_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let (active, idle) = self.count_pool_containers(pool, container_scheduler);
                let current_total = active + idle;
                let buffer = pool.buffer_containers.unwrap_or(0);
                let deficit_target = (active + buffer).max(min).min(max);
                if current_total < deficit_target {
                    let deficit = deficit_target - current_total;
                    let profile = ResourceProfile::from_container_resources(&pool.resources);
                    if pool.is_function_pool() {
                        function_pool_deficits.increment_by(profile, deficit as u64);
                    } else {
                        sandbox_pool_deficits.increment_by(profile, deficit as u64);
                    }
                }
            }

            for pool_key in &blocked_keys {
                if !container_scheduler.blocked_pools.contains(pool_key) {
                    continue;
                }
                let Some(pool) = container_scheduler.get_pool(pool_key) else {
                    continue;
                };
                let min = pool.min_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let (active, idle) = self.count_pool_containers(pool, container_scheduler);
                let current_total = active + idle;
                let buffer = pool.buffer_containers.unwrap_or(0);
                let deficit_target = (active + buffer).max(min).min(max);
                if current_total < deficit_target {
                    let deficit = deficit_target - current_total;
                    let profile = ResourceProfile::from_container_resources(&pool.resources);
                    if pool.is_function_pool() {
                        function_pool_deficits.increment_by(profile, deficit as u64);
                    } else {
                        sandbox_pool_deficits.increment_by(profile, deficit as u64);
                    }
                }
            }
        }

        update.function_pool_deficits = Some(function_pool_deficits);
        update.sandbox_pool_deficits = Some(sandbox_pool_deficits);
        update.newly_blocked_pools = blocked;

        Ok(update)
    }

    /// Count containers for a pool (active with work, idle without work)
    fn count_pool_containers(
        &self,
        pool: &ContainerPool,
        container_scheduler: &ContainerScheduler,
    ) -> (u32, u32) {
        if pool.is_function_pool() {
            // For function pools, parse the function URI and count function containers
            if let Some(fn_uri) = self.parse_function_pool_uri(pool) {
                container_scheduler.count_active_idle_containers(&fn_uri)
            } else {
                (0, 0)
            }
        } else {
            // For sandbox pools, count pool containers
            let pool_key = ContainerPoolKey::from(pool);
            container_scheduler.count_pool_containers(&pool_key)
        }
    }

    /// Create a container for a pool
    fn create_container_for_pool(
        &self,
        pool: &ContainerPool,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        is_critical: bool,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        if pool.is_function_pool() {
            // For function pools, look up the function and create a function container
            let Some(fn_uri) = self.parse_function_pool_uri(pool) else {
                return Ok(None);
            };

            let app_key = format!("{}|{}", fn_uri.namespace, fn_uri.application);
            let Some(app) = in_memory_state.applications.get(&app_key) else {
                return Ok(None);
            };

            // Skip if application is disabled
            if matches!(app.state, ApplicationState::Disabled { .. }) {
                return Ok(None);
            }

            let Some(function) = app.functions.get(&fn_uri.function) else {
                return Ok(None);
            };

            container_scheduler.create_container_for_function(
                &fn_uri.namespace,
                &fn_uri.application,
                &fn_uri.version,
                function,
                &app.state,
                is_critical,
            )
        } else {
            // For sandbox pools, create a pool container
            container_scheduler.create_container_for_pool(pool, is_critical)
        }
    }

    /// Trim idle containers from a pool
    fn trim_idle_containers(
        &self,
        pool: &ContainerPool,
        count: u32,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let containers = if pool.is_function_pool() {
            if let Some(fn_uri) = self.parse_function_pool_uri(pool) {
                container_scheduler.select_idle_containers(&fn_uri, count)
            } else {
                vec![]
            }
        } else {
            // Only construct pool_key for sandbox pools where it's needed
            let pool_key = ContainerPoolKey::from(pool);
            container_scheduler.select_warm_pool_containers(&pool_key, count)
        };

        for container_id in containers {
            if let Some(u) = container_scheduler.terminate_container(&container_id)? {
                update.extend(u);
            }
        }

        Ok(update)
    }

    /// Return (namespace, identity_string) for logging.
    /// Function pools: "app=X fn=Y ver=Z"
    /// Sandbox pools: "pool_id=X"
    fn pool_identity(&self, pool: &ContainerPool) -> (String, String) {
        let ns = pool.namespace.clone();
        if pool.is_function_pool() {
            if let Some(fn_uri) = self.parse_function_pool_uri(pool) {
                (
                    ns,
                    format!(
                        "app={} fn={} ver={}",
                        fn_uri.application, fn_uri.function, fn_uri.version
                    ),
                )
            } else {
                (ns, format!("pool_id={}", pool.id.get()))
            }
        } else {
            (ns, format!("pool_id={}", pool.id.get()))
        }
    }

    /// Parse function pool ID to extract FunctionURI
    /// Pool ID format: {app}|{function}|{version}
    /// Namespace comes from pool.namespace (not embedded in the ID).
    fn parse_function_pool_uri(
        &self,
        pool: &ContainerPool,
    ) -> Option<crate::data_model::FunctionURI> {
        let id = pool.id.get();
        let parts: Vec<&str> = id.splitn(3, '|').collect();
        if parts.len() != 3 {
            return None;
        }

        Some(crate::data_model::FunctionURI {
            namespace: pool.namespace.clone(),
            application: parts[0].to_string(),
            function: parts[1].to_string(),
            version: parts[2].to_string(),
        })
    }
}
