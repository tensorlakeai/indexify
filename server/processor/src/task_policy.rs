use data_model::{ComputeGraphVersion, Task, TaskFailureReason, TaskOutcome, TaskStatus};
use if_chain::if_chain;

/// Determines task retry policy based on failure reasons and retry
/// configuration
pub struct TaskRetryPolicy;

impl TaskRetryPolicy {
    /// Determines if a task should be retried based on a function executor
    /// termination, updating the task accordingly
    pub fn handle_function_executor_termination(
        task: &mut Task,
        task_failure_reason: TaskFailureReason,
        failed_alloc_ids: &[String],
        alloc_id: &str,
        compute_graph_version: &ComputeGraphVersion,
    ) {
        // Check if this allocation was blamed for the failure
        if failed_alloc_ids.contains(&alloc_id.to_string()) {
            // Use the standard allocation failure handling
            Self::handle_allocation_failure(task, task_failure_reason, compute_graph_version);
        } else {
            // This allocation wasn't blamed for the failure, allow retry
            task.status = TaskStatus::Pending;
        }
    }

    /// Determines if a task should be retried based on an allocation failure
    /// reason, updating the task accordingly
    pub fn handle_allocation_failure(
        task: &mut Task,
        failure_reason: TaskFailureReason,
        compute_graph_version: &ComputeGraphVersion,
    ) {
        let uses_attempt = failure_reason.should_count_against_task_retry_attempts();

        if_chain! {
            if let Some(max_retries) = compute_graph_version.task_max_retries(task);
            if failure_reason.is_retriable();
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
                task.outcome = TaskOutcome::Failure(failure_reason);
            }
        }
    }

    /// Determines if a task should be retried based on allocation
    /// outcome, leaving the task with the appropriate status
    pub fn handle_allocation_outcome(
        task: &mut Task,
        outcome: &TaskOutcome,
        compute_graph_version: &ComputeGraphVersion,
    ) {
        match outcome {
            TaskOutcome::Success => {
                task.status = TaskStatus::Completed;
                task.outcome = *outcome;
            }
            TaskOutcome::Failure(failure_reason) => {
                // Handle allocation failure
                Self::handle_allocation_failure(task, *failure_reason, compute_graph_version);
            }
            TaskOutcome::Unknown => {
                // For unknown outcomes, set to pending to allow retry
                task.status = TaskStatus::Pending;
            }
        }
    }
}
