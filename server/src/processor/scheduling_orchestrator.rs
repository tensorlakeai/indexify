//! Scheduling Orchestrator - coordinates FE scaling and task allocation
//!
//! This module provides high-level orchestration methods that combine
//! FE management, scaling decisions, and task allocation into linear,
//! easy-to-follow flows.

use std::time::Instant;

use anyhow::Result;
use opentelemetry::metrics::Histogram;
use tracing::debug;

use crate::{
    data_model::{ExecutorId, FunctionURI},
    processor::{
        fe_scaler::FEScaler,
        function_executor_manager::FunctionExecutorManager,
        function_run_processor::FunctionRunProcessor,
    },
    state_store::{in_memory_state::InMemoryState, requests::SchedulerUpdateRequest},
};

/// Orchestrates scheduling operations in a linear, readable flow
pub struct SchedulingOrchestrator<'a> {
    fe_manager: &'a FunctionExecutorManager,
    task_allocator: FunctionRunProcessor<'a>,
    allocate_latency: &'a Histogram<f64>,
    spatial_query_latency: &'a Histogram<f64>,
}

impl<'a> SchedulingOrchestrator<'a> {
    pub fn new(
        fe_manager: &'a FunctionExecutorManager,
        allocate_latency: &'a Histogram<f64>,
        spatial_query_latency: &'a Histogram<f64>,
        clock: u64,
    ) -> Self {
        Self {
            fe_manager,
            task_allocator: FunctionRunProcessor::new(clock, fe_manager),
            allocate_latency,
            spatial_query_latency,
        }
    }

    /// Access the task allocator for direct allocation operations
    pub fn task_allocator(&self) -> &FunctionRunProcessor<'a> {
        &self.task_allocator
    }

    /// Handle allocation completion - the FE just finished work and has
    /// capacity
    ///
    /// Flow:
    /// 1. Process completed allocation (create child tasks or mark request
    ///    complete)
    /// 2. Try to allocate pending runs of the SAME function (they can use this
    ///    FE)
    /// 3. If no same-function work: scale down idle FEs and find work for other
    ///    functions
    pub fn handle_freed_capacity(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
        fn_uri: &FunctionURI,
        initial_update: SchedulerUpdateRequest,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = initial_update;

        // Step 1: Allocate child runs created by the completed allocation
        let child_runs = update.unallocated_function_runs();
        if !child_runs.is_empty() {
            debug!(child_count = child_runs.len(), "allocating child runs");
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                child_runs,
                self.allocate_latency,
            )?);
        }

        // Step 2: Check for pending runs of the SAME function - O(1) lookup
        let pending_same_fn = state.find_pending_runs_for_function(fn_uri, 100);
        if !pending_same_fn.is_empty() {
            debug!(
                fn_uri = %fn_uri,
                pending_count = pending_same_fn.len(),
                "allocating pending runs for same function"
            );
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                pending_same_fn,
                self.allocate_latency,
            )?);
            return Ok(update);
        }

        // Step 3: No same-function work - try to reuse capacity for other functions
        update.extend(self.try_reuse_capacity_for_other_functions(state, executor_id)?);

        Ok(update)
    }

    /// Handle executor becoming ready (new registration or heartbeat)
    ///
    /// Flow:
    /// 1. Reconcile FE state with executor's reported state
    /// 2. Scale up/down FEs based on pending work
    /// 3. Find and allocate pending runs that fit this executor
    pub fn handle_executor_ready(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        // Step 1: Reconcile FE state
        let mut update = self
            .fe_manager
            .reconcile_executor_state(state, executor_id)?;

        // Step 2: Update capacity index and apply scaling plan
        state.update_executor_capacity_in_index(executor_id);
        if let Ok(plan) = FEScaler::compute_plan(state, executor_id)
            && let Ok(scaling_update) = self.fe_manager.apply_scaling_plan(state, &plan) {
                update.extend(scaling_update);
            }

        // Step 3: Find and allocate pending runs
        let query_start = Instant::now();
        let placeable_runs = state.find_placeable_runs_for_executor(executor_id, 100);
        self.spatial_query_latency
            .record(query_start.elapsed().as_secs_f64(), &[]);

        debug!(
            executor_id = executor_id.get(),
            placeable_count = placeable_runs.len(),
            "found placeable runs for executor"
        );

        update.extend(self.task_allocator.allocate_function_runs(
            state,
            placeable_runs,
            self.allocate_latency,
        )?);

        Ok(update)
    }

    /// Handle executor removal - reschedule its work to other executors
    ///
    /// Flow:
    /// 1. Deregister executor and mark its allocations as failed
    /// 2. Retry failed allocations on other executors
    pub fn handle_executor_removed(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        // Step 1: Deregister and collect retryable runs
        let mut update = self.fe_manager.deregister_executor(state, executor_id)?;

        // Step 2: Retry on other executors
        let retryable_runs = update.unallocated_function_runs();
        if !retryable_runs.is_empty() {
            debug!(
                retry_count = retryable_runs.len(),
                "retrying runs after executor removal"
            );
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                retryable_runs,
                self.allocate_latency,
            )?);
        }

        Ok(update)
    }

    /// Try to reuse freed capacity for functions other than the one that just
    /// completed
    ///
    /// Flow:
    /// 1. Update executor capacity in index
    /// 2. Compute scaling plan (may vacuum idle FEs to free resources)
    /// 3. Find runs from other functions that fit
    fn try_reuse_capacity_for_other_functions(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Update capacity index
        state.update_executor_capacity_in_index(executor_id);

        // Step 2: Apply scaling plan (vacuum idle FEs if needed)
        if let Ok(plan) = FEScaler::compute_plan(state, executor_id)
            && let Ok(scaling_update) = self.fe_manager.apply_scaling_plan(state, &plan) {
                update.extend(scaling_update);
            }

        // Step 3: Find runs from other functions
        let query_start = Instant::now();
        let placeable_runs = state.find_placeable_runs_for_executor(executor_id, 50);
        self.spatial_query_latency
            .record(query_start.elapsed().as_secs_f64(), &[]);

        if !placeable_runs.is_empty() {
            debug!(
                executor_id = executor_id.get(),
                placeable_count = placeable_runs.len(),
                "found runs from other functions"
            );
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                placeable_runs,
                self.allocate_latency,
            )?);
        }

        Ok(update)
    }
}
