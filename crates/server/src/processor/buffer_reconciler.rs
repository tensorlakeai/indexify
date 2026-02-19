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

        // Phase 0: Drain idle/warm containers on cordoned executors.
        // These containers can't accept new work, so terminate them to free
        // resources and let subsequent phases create replacements on healthy
        // executors.
        let drain_update = self.drain_cordoned_containers(container_scheduler)?;
        update.extend(drain_update);

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

        // Track pools that can't be satisfied within this reconciliation cycle
        let mut newly_blocked: HashSet<ContainerPoolKey> = HashSet::new();

        // Phase 1: Ensure minimums are met (highest priority)
        for pool in &pools {
            if !self.ensure_pool_min(pool, in_memory_state, container_scheduler, &mut update)? {
                warn!(pool_id = %pool.id.get(), "Cannot meet min containers, marking pool as blocked");
                newly_blocked.insert(ContainerPoolKey::from(pool));
            }
        }

        // Phase 2: Fill buffers with remaining resources (round-robin for fairness)
        loop {
            let mut any_created = false;

            for pool in &pools {
                let pool_key = ContainerPoolKey::from(pool);
                if newly_blocked.contains(&pool_key) {
                    continue;
                }

                if !self.pool_needs_buffer(pool, container_scheduler) {
                    continue;
                }

                let (ns, identity) = self.pool_identity(pool);
                let buffer = pool.buffer_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let (active, idle) = self.count_pool_containers(pool, container_scheduler);
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

                match self.try_place_container(
                    pool,
                    in_memory_state,
                    container_scheduler,
                    false,
                    &mut update,
                ) {
                    Ok(true) => {
                        any_created = true;
                    }
                    Ok(false) => {
                        newly_blocked.insert(pool_key);
                    }
                    Err(e) => {
                        warn!(pool_id = %pool.id.get(), error = %e, "Error filling buffer");
                    }
                }
            }

            if !any_created {
                break;
            }
        }

        // Phase 3: Trim excess idle containers
        for pool in &pools {
            let pool_key = ContainerPoolKey::from(pool);
            if !newly_blocked.contains(&pool_key) {
                self.trim_pool_excess(pool, container_scheduler, &mut update)?;
            }
        }

        // Propagate newly blocked pools to the scheduler before Phase 4
        container_scheduler
            .blocked_pools
            .extend(newly_blocked.iter().cloned());

        // Phase 4: Try to satisfy blocked pools with available capacity.
        // When a pool is unblocked (first container placed), run full
        // min+buffer reconciliation for it inline — don't defer to the next
        // cycle. Stops as soon as a full pass makes no progress (capacity
        // exhausted).
        if !container_scheduler.blocked_pools.is_empty() &&
            container_scheduler.has_available_capacity()
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
                if !self
                    .try_place_container(
                        &pool,
                        in_memory_state,
                        container_scheduler,
                        true,
                        &mut update,
                    )
                    .unwrap_or(false)
                {
                    continue;
                }

                // Pool is unblocked — run full min+buffer inline
                container_scheduler.blocked_pools.remove(pool_key);

                // Reuse ensure_pool_min (probe already placed 1, so it creates fewer)
                self.ensure_pool_min(&pool, in_memory_state, container_scheduler, &mut update)?;

                // Fill buffer
                while self.pool_needs_buffer(&pool, container_scheduler) {
                    if !self
                        .try_place_container(
                            &pool,
                            in_memory_state,
                            container_scheduler,
                            false,
                            &mut update,
                        )
                        .unwrap_or(false)
                    {
                        break;
                    }
                }
            }
        }

        // Compute deficits ONCE at the end (after all phases)
        let mut fn_deficits = ResourceProfileHistogram::default();
        let mut sb_deficits = ResourceProfileHistogram::default();

        // Dirty pools (includes newly-blocked; satisfied pools contribute 0)
        let dirty_keys: HashSet<ContainerPoolKey> =
            pools.iter().map(ContainerPoolKey::from).collect();
        self.accumulate_pool_deficits(
            pools.iter(),
            container_scheduler,
            &mut fn_deficits,
            &mut sb_deficits,
        );

        // Still-blocked pools that weren't in the dirty set (avoids double-counting).
        // These are disjoint because dirty_pools filters out blocked_pools at the
        // start, and newly_blocked is a subset of dirty.
        self.accumulate_pool_deficits(
            container_scheduler
                .blocked_pools
                .iter()
                .filter(|k| !dirty_keys.contains(k))
                .filter_map(|k| container_scheduler.get_pool(k)),
            container_scheduler,
            &mut fn_deficits,
            &mut sb_deficits,
        );

        update.function_pool_deficits = Some(fn_deficits);
        update.sandbox_pool_deficits = Some(sb_deficits);
        update.newly_blocked_pools = newly_blocked;

        Ok(update)
    }

    /// Create containers until a pool meets its min_containers floor.
    /// Returns Ok(true) if min is satisfied, Ok(false) if blocked or on error.
    fn ensure_pool_min(
        &self,
        pool: &ContainerPool,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        update: &mut SchedulerUpdateRequest,
    ) -> Result<bool> {
        let min = pool.min_containers.unwrap_or(0);
        let max = pool.max_containers.unwrap_or(u32::MAX);
        // Don't exceed max even when meeting min (handles invalid min > max config)
        let effective_min = min.min(max);
        let (active, idle) = self.count_pool_containers(pool, container_scheduler);
        let current = active + idle;

        if current >= effective_min {
            return Ok(true);
        }

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
            match self.try_place_container(pool, in_memory_state, container_scheduler, true, update)
            {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(false);
                }
                Err(e) => {
                    warn!(pool_id = %pool.id.get(), error = %e, "Error creating pool container");
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Returns true if a pool needs more idle containers for its buffer.
    fn pool_needs_buffer(
        &self,
        pool: &ContainerPool,
        container_scheduler: &ContainerScheduler,
    ) -> bool {
        let buffer = pool.buffer_containers.unwrap_or(0);
        let max = pool.max_containers.unwrap_or(u32::MAX);
        let (active, idle) = self.count_pool_containers(pool, container_scheduler);
        idle < buffer && (active + idle) < max
    }

    /// Trim idle containers that exceed the pool's target.
    fn trim_pool_excess(
        &self,
        pool: &ContainerPool,
        container_scheduler: &mut ContainerScheduler,
        update: &mut SchedulerUpdateRequest,
    ) -> Result<()> {
        let min = pool.min_containers.unwrap_or(0);
        let max = pool.max_containers.unwrap_or(u32::MAX);
        let (active, idle) = self.count_pool_containers(pool, container_scheduler);
        let current_total = active + idle;
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
            let trim_update = self.trim_idle_containers(pool, excess, container_scheduler)?;
            update.extend(trim_update);
        }
        Ok(())
    }

    /// Try to place one container for a pool. Returns:
    /// - `Ok(true)`  — container placed on an executor
    /// - `Ok(false)` — no resources available or only a vacuum update
    /// - `Err(e)`    — unexpected error
    fn try_place_container(
        &self,
        pool: &ContainerPool,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        is_critical: bool,
        update: &mut SchedulerUpdateRequest,
    ) -> Result<bool> {
        match self.create_container_for_pool(
            pool,
            in_memory_state,
            container_scheduler,
            is_critical,
        ) {
            Ok(Some(u)) => {
                let placed = u
                    .containers
                    .values()
                    .any(|c| !matches!(c.desired_state, ContainerState::Terminated { .. }));
                container_scheduler.apply_container_update(&u);
                update.extend(u);
                Ok(placed)
            }
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Compute resource deficits for a set of pools and accumulate into the
    /// provided histograms.
    fn accumulate_pool_deficits<'a>(
        &self,
        pools: impl Iterator<Item = &'a ContainerPool>,
        container_scheduler: &ContainerScheduler,
        function_deficits: &mut ResourceProfileHistogram,
        sandbox_deficits: &mut ResourceProfileHistogram,
    ) {
        for pool in pools {
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
                    function_deficits.increment_by(profile, deficit as u64);
                } else {
                    sandbox_deficits.increment_by(profile, deficit as u64);
                }
            }
        }
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

    /// Terminate idle/warm containers stranded on cordoned executors.
    /// Active containers (with allocations or sandbox_id) are left to finish
    /// naturally — only idle (function pools) and warm (sandbox pools)
    /// containers are drained.
    fn drain_cordoned_containers(
        &self,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Collect cordoned executor IDs
        let cordoned_executor_ids: Vec<_> = container_scheduler
            .executors
            .iter()
            .filter(|(_, e)| e.state.is_scheduling_disabled())
            .map(|(id, _)| id.clone())
            .collect();

        // Collect container IDs to terminate: idle/warm containers on cordoned executors
        let mut to_terminate = Vec::new();
        for executor_id in &cordoned_executor_ids {
            if let Some(executor_state) = container_scheduler.executor_states.get(executor_id) {
                for container_id in &executor_state.function_container_ids {
                    if let Some(meta) = container_scheduler.function_containers.get(container_id) {
                        if matches!(meta.desired_state, ContainerState::Terminated { .. }) {
                            continue;
                        }
                        // Function containers: drain if no allocations (idle)
                        // Sandbox pool containers: drain if no sandbox_id (warm)
                        let is_idle = meta.allocations.is_empty()
                            && meta.function_container.sandbox_id.is_none();
                        if is_idle {
                            to_terminate.push(container_id.clone());
                        }
                    }
                }
            }
        }

        for container_id in to_terminate {
            info!(
                container_id = %container_id.get(),
                phase = "drain_cordoned",
                "Terminating idle container on cordoned executor"
            );
            if let Some(u) = container_scheduler.terminate_container(&container_id)? {
                update.extend(u);
            }
        }

        Ok(update)
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
