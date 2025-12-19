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
    pub fn handle_executor_ready(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = self
            .fe_manager
            .reconcile_executor_state(state, executor_id)?;

        // Ensure min FEs for functions in this executor's allowlist
        // O(A) where A = allowlist size
        update.extend(self.fe_manager.ensure_min_fes(state, executor_id)?);

        // Update capacity and find placeable runs via spatial index
        state.update_executor_capacity_in_index(executor_id);
        let query_start = Instant::now();
        let runs = state.find_placeable_runs_for_executor(executor_id, 100);
        self.spatial_query_latency
            .record(query_start.elapsed().as_secs_f64(), &[]);

        if !runs.is_empty() {
            debug!(
                executor_id = executor_id.get(),
                count = runs.len(),
                "allocating runs on executor ready"
            );
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                runs,
                self.allocate_latency,
            )?);
        }

        Ok(update)
    }

    /// Handle executor removal - reschedule its work to other executors
    pub fn handle_executor_removed(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = self.fe_manager.deregister_executor(state, executor_id)?;
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

    /// Try to reuse freed capacity for other functions
    fn try_reuse_capacity_for_other_functions(
        &self,
        state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Update capacity index and find placeable runs
        state.update_executor_capacity_in_index(executor_id);

        let query_start = Instant::now();
        let runs = state.find_placeable_runs_for_executor(executor_id, 50);
        self.spatial_query_latency
            .record(query_start.elapsed().as_secs_f64(), &[]);

        if !runs.is_empty() {
            debug!(
                executor_id = executor_id.get(),
                count = runs.len(),
                "allocating runs from other functions"
            );
            update.extend(self.task_allocator.allocate_function_runs(
                state,
                runs,
                self.allocate_latency,
            )?);
        }

        Ok(update)
    }
}
