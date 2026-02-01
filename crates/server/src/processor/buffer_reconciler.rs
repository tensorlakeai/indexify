use anyhow::Result;
use tracing::warn;

use crate::{
    data_model::{ApplicationState, ContainerPool, ContainerPoolKey},
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

    /// Main entry point: reconcile all pool buffers
    /// Called at the end of each state change processing cycle
    pub fn reconcile(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Clone pools to avoid borrow conflicts when mutating container_scheduler
        let pools: Vec<Box<ContainerPool>> = container_scheduler
            .container_pools
            .values()
            .cloned()
            .collect();

        // Phase 1: Ensure minimums are met (highest priority)
        for pool in &pools {
            let min = pool.min_containers.unwrap_or(0);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current = active + idle;

            if current < min {
                let needed = min - current;
                for _ in 0..needed {
                    match self.create_container_for_pool(pool, in_memory_state, container_scheduler)
                    {
                        Ok(Some(u)) => {
                            self.apply_container_update(pool, &u, container_scheduler);
                            update.extend(u);
                        }
                        Ok(None) => {
                            warn!(pool_id = %pool.id.get(), "Cannot meet min - no resources");
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
                let buffer = pool.buffer_containers.unwrap_or(0);
                let max = pool.max_containers.unwrap_or(u32::MAX);
                let (active, idle) = self.count_pool_containers(pool, container_scheduler);

                // Need more idle and not at max
                if idle < buffer &&
                    (active + idle) < max &&
                    let Ok(Some(u)) = self.create_container_for_pool(
                        pool,
                        in_memory_state,
                        container_scheduler,
                    )
                {
                    self.apply_container_update(pool, &u, container_scheduler);
                    update.extend(u);
                    any_created = true;
                }
            }

            if !any_created {
                break;
            }
        }

        // Phase 3: Trim excess idle containers
        for pool in &pools {
            let pool_key = ContainerPoolKey::from(pool.as_ref());
            let buffer = pool.buffer_containers.unwrap_or(0);
            let min = pool.min_containers.unwrap_or(0);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);

            let target_idle = buffer;
            let target_total = (active + target_idle).max(min);
            let current_total = active + idle;

            if current_total > target_total && idle > 0 {
                let excess = (current_total - target_total).min(idle);
                let trim_update =
                    self.trim_idle_containers(&pool_key, pool, excess, container_scheduler)?;
                update.extend(trim_update);
            }
        }

        // Phase 4: Compute pool deficits for capacity reporting
        // This tells autoscalers how many containers are needed but couldn't be created
        let pool_deficits = self.compute_pool_deficits(&pools, container_scheduler);
        update.pool_deficits = Some(pool_deficits);

        Ok(update)
    }

    /// Compute the deficit for each pool: gap between target and current
    /// containers. Returns a histogram of resource profiles with counts
    /// representing unmet demand.
    ///
    /// Target calculation matches Phase 2/3 logic:
    /// - target = min(max_containers, max(min_containers, active +
    ///   buffer_containers))
    /// - This ensures we have at least `min` total AND at least `buffer` idle,
    ///   but never exceed `max`.
    fn compute_pool_deficits(
        &self,
        pools: &[Box<ContainerPool>],
        container_scheduler: &ContainerScheduler,
    ) -> ResourceProfileHistogram {
        let mut deficits = ResourceProfileHistogram::default();

        for pool in pools {
            let min = pool.min_containers.unwrap_or(0);
            let buffer = pool.buffer_containers.unwrap_or(0);
            let max = pool.max_containers.unwrap_or(u32::MAX);

            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current = active + idle;

            // Target: need at least `min` total, AND at least `buffer` idle,
            // but never exceed `max`
            let target = (active + buffer).max(min).min(max);

            if current < target {
                let deficit = target - current;
                let profile = ResourceProfile::from_container_resources(&pool.resources);
                deficits.increment_by(profile, deficit as u64);
            }
        }

        deficits
    }

    /// Count containers for a pool (active with work, idle without work)
    fn count_pool_containers(
        &self,
        pool: &ContainerPool,
        container_scheduler: &ContainerScheduler,
    ) -> (u32, u32) {
        if pool.id.is_function_pool() {
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
    ) -> Result<Option<SchedulerUpdateRequest>> {
        if pool.id.is_function_pool() {
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
            )
        } else {
            // For sandbox pools, create a pool container
            container_scheduler.create_container_for_pool(pool)
        }
    }

    /// Apply container update to scheduler
    fn apply_container_update(
        &self,
        pool: &ContainerPool,
        update: &SchedulerUpdateRequest,
        container_scheduler: &mut ContainerScheduler,
    ) {
        if pool.id.is_function_pool() {
            container_scheduler.apply_container_update(update);
        } else {
            container_scheduler.apply_pool_container_update(update);
        }
    }

    /// Trim idle containers from a pool
    fn trim_idle_containers(
        &self,
        pool_key: &ContainerPoolKey,
        pool: &ContainerPool,
        count: u32,
        container_scheduler: &mut ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let containers = if pool.id.is_function_pool() {
            if let Some(fn_uri) = self.parse_function_pool_uri(pool) {
                container_scheduler.select_idle_containers(&fn_uri, count)
            } else {
                vec![]
            }
        } else {
            container_scheduler.select_warm_pool_containers(pool_key, count)
        };

        for container_id in containers {
            if let Some(u) = container_scheduler.terminate_container(&container_id)? {
                update.extend(u);
            }
        }

        Ok(update)
    }

    /// Parse function pool ID to extract FunctionURI
    /// Pool ID format: fn:{namespace}:{app}:{function}:{version}
    fn parse_function_pool_uri(
        &self,
        pool: &ContainerPool,
    ) -> Option<crate::data_model::FunctionURI> {
        let id = pool.id.get();
        if !id.starts_with("fn:") {
            return None;
        }

        let parts: Vec<&str> = id[3..].splitn(4, ':').collect();
        if parts.len() != 4 {
            return None;
        }

        Some(crate::data_model::FunctionURI {
            namespace: parts[0].to_string(),
            application: parts[1].to_string(),
            function: parts[2].to_string(),
            version: parts[3].to_string(),
        })
    }
}
