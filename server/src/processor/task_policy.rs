use if_chain::if_chain;

use crate::data_model::{
    Allocation,
    ComputeGraphVersion,
    Task,
    TaskFailureReason,
    TaskOutcome,
    TaskStatus,
};

/// Determines task retry policy based on failure reasons and retry
/// configuration
pub struct TaskRetryPolicy;

impl TaskRetryPolicy {
    /// Determines if a task should be retried based on an allocation failure
    /// reason, updating the task accordingly.
    fn handle_allocation_failure(
        task: &mut Task,
        alloc_failure_reason: TaskFailureReason,
        compute_graph_version: &ComputeGraphVersion,
    ) {
        let uses_attempt = alloc_failure_reason.should_count_against_task_retry_attempts();

        if_chain! {
            if let Some(max_retries) = compute_graph_version.task_max_retries(task);
            if alloc_failure_reason.is_retriable();
        if task.attempt_number < max_retries || !uses_attempt;
            then {
                // Task can be retried
                task.status = TaskStatus::Pending;
                if uses_attempt {
                    task.attempt_number += 1;
                }
            }
            else {
                // Task cannot be retried - either no max retries, not retriable, or exhausted attempts.
                task.status = TaskStatus::Completed;
                task.outcome = TaskOutcome::Failure(alloc_failure_reason);
            }
        }
    }

    /// Determines if a task should be retried based on allocation
    /// outcome, leaving the task with the appropriate status.
    /// The task must be running the allocation to ensure idempotency.
    pub fn handle_allocation_outcome(
        task: &mut Task,
        allocation: &Allocation,
        compute_graph_version: &ComputeGraphVersion,
    ) {
        match allocation.outcome {
            TaskOutcome::Success => {
                task.status = TaskStatus::Completed;
                task.outcome = allocation.outcome;
            }
            TaskOutcome::Failure(failure_reason) => {
                // Handle allocation failure
                Self::handle_allocation_failure(task, failure_reason, compute_graph_version);
            }
            TaskOutcome::Unknown => {
                // For unknown outcomes, set to pending to allow retry
                task.status = TaskStatus::Pending;
            }
        }
    }
}
