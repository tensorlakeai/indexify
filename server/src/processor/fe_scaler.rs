//! FE Autoscaler - computes scaling decisions based on pending runs and FE
//! limits

use std::collections::HashMap;

use anyhow::Result;
use tracing::debug;

use crate::{
    data_model::{ExecutorId, FunctionURI},
    processor::resource_placement::{ResourcePlacementIndex, TrackedFE},
    state_store::in_memory_state::InMemoryState,
};

/// Scaling decision for one function on one executor
#[derive(Debug, Clone)]
pub struct FunctionScalingDecision {
    pub fn_uri: FunctionURI,
    pub fes_to_create: u32,
    pub fes_to_vacuum: Vec<TrackedFE>,
}

/// Complete scaling plan for an executor
#[derive(Debug, Default)]
pub struct FEScalingPlan {
    pub executor_id: ExecutorId,
    pub function_decisions: Vec<FunctionScalingDecision>,
    pub idle_fes_for_resources: Vec<TrackedFE>,
}

pub struct FEScaler;

impl FEScaler {
    pub fn compute_plan(state: &InMemoryState, executor_id: &ExecutorId) -> Result<FEScalingPlan> {
        let mut plan = FEScalingPlan {
            executor_id: executor_id.clone(),
            ..Default::default()
        };

        // Get executor to check allowlist
        let executor = state.executors.get(executor_id);

        let index = &state.resource_placement_index;
        let functions = Self::get_functions_with_pending(index);

        for (fn_uri, pending) in functions {
            // Skip functions not allowed on this executor
            if let Some(exec) = &executor &&
                !exec.is_function_uri_allowed(&fn_uri)
            {
                continue;
            }

            let Some(config) = state.get_scaling_config(&fn_uri) else {
                continue;
            };
            let current = index.fe_count_for_function(executor_id, &fn_uri) as u32;
            let desired = pending.min(config.max_fe_count).max(config.min_fe_count);

            if desired > current {
                plan.function_decisions.push(FunctionScalingDecision {
                    fn_uri,
                    fes_to_create: desired - current,
                    fes_to_vacuum: vec![],
                });
            } else if desired < current {
                let excess = current - desired;
                let idle = index.get_idle_fes_for_function(executor_id, &fn_uri);
                plan.function_decisions.push(FunctionScalingDecision {
                    fn_uri,
                    fes_to_create: 0,
                    fes_to_vacuum: idle.into_iter().take(excess as usize).collect(),
                });
            }
        }

        // Find idle FEs from other functions for resource reclamation
        let to_create: u32 = plan
            .function_decisions
            .iter()
            .map(|d| d.fes_to_create)
            .sum();
        if to_create > 0 {
            let vacuuming: std::collections::HashSet<_> = plan
                .function_decisions
                .iter()
                .flat_map(|d| d.fes_to_vacuum.iter().map(|fe| fe.fe_id.clone()))
                .collect();

            plan.idle_fes_for_resources = index
                .get_idle_fes(executor_id)
                .into_iter()
                .filter(|fe| !vacuuming.contains(&fe.fe_id))
                .filter(|fe| !index.has_pending_runs_for_function(&fe.fn_uri))
                .collect();

            debug!(
                to_create = to_create,
                idle_available = plan.idle_fes_for_resources.len(),
                "idle FEs for reclamation"
            );
        }

        Ok(plan)
    }

    fn get_functions_with_pending(index: &ResourcePlacementIndex) -> HashMap<FunctionURI, u32> {
        index
            .get_all_function_uris_with_pending()
            .into_iter()
            .filter_map(|uri| {
                let count = index.pending_count_for_function(&uri);
                if count > 0 {
                    Some((uri, count as u32))
                } else {
                    None
                }
            })
            .collect()
    }
}
