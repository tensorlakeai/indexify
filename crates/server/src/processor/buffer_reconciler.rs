use anyhow::Result;
use tracing::warn;

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

    /// Main entry point: reconcile all pool buffers
    /// Called at the end of each state change processing cycle
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

        // Clone pools to avoid borrow conflicts when mutating container_scheduler
        let pools: Vec<Box<ContainerPool>> = container_scheduler
            .container_pools
            .values()
            .cloned()
            .collect();

        // Phase 1: Ensure minimums are met (highest priority)
        for pool in &pools {
            let min = pool.min_containers.unwrap_or(0);
            let max = pool.max_containers.unwrap_or(u32::MAX);
            // Don't exceed max even when meeting min (handles invalid min > max config)
            let effective_min = min.min(max);
            let (active, idle) = self.count_pool_containers(pool, container_scheduler);
            let current = active + idle;

            if current < effective_min {
                let needed = effective_min - current;
                for _ in 0..needed {
                    match self.create_container_for_pool(
                        pool,
                        in_memory_state,
                        container_scheduler,
                        true,
                    ) {
                        Ok(Some(u)) => {
                            container_scheduler.apply_container_update(&u);
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
                if idle < buffer && (active + idle) < max {
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
                            }
                        }
                        Ok(None) => {
                            // No resources available, continue to next pool
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
        let mut deficits = ResourceProfileHistogram::default();

        for pool in &pools {
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
                let trim_update = self.trim_idle_containers(pool, excess, container_scheduler)?;
                update.extend(trim_update);
            }

            // Compute deficit for this pool (uses buffer=0 when None for deficit reporting)
            let buffer = pool.buffer_containers.unwrap_or(0);
            let deficit_target = (active + buffer).max(min).min(max);
            if current_total < deficit_target {
                let deficit = deficit_target - current_total;
                let profile = ResourceProfile::from_container_resources(&pool.resources);
                deficits.increment_by(profile, deficit as u64);
            }
        }

        update.pool_deficits = Some(deficits);

        Ok(update)
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
        is_critical: bool,
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

        let containers = if pool.id.is_function_pool() {
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
