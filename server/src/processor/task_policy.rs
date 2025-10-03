use if_chain::if_chain;

use crate::data_model::{
    Allocation,
    ApplicationVersion,
    FunctionRun,
    FunctionRunFailureReason,
    FunctionRunOutcome,
    FunctionRunStatus,
};

/// Determines task retry policy based on failure reasons and retry
/// configuration
pub struct TaskRetryPolicy;

impl TaskRetryPolicy {
    /// Determines if a task should be retried based on an allocation failure
    /// reason, updating the task accordingly.
    fn handle_allocation_failure(
        run: &mut FunctionRun,
        alloc_failure_reason: FunctionRunFailureReason,
        application_version: &ApplicationVersion,
    ) {
        let uses_attempt = alloc_failure_reason.should_count_against_task_retry_attempts();

        if_chain! {
            if let Some(max_retries) = application_version.task_max_retries(run);
            if alloc_failure_reason.is_retriable();
        if run.attempt_number < max_retries || !uses_attempt;
            then {
                // Task can be retried
                run.status = FunctionRunStatus::Pending;
                if uses_attempt {
                    run.attempt_number += 1;
                }
            }
            else {
                // Task cannot be retried - either no max retries, not retriable, or exhausted attempts.
                run.status = FunctionRunStatus::Completed;
                run.outcome = Some(FunctionRunOutcome::Failure(alloc_failure_reason));
            }
        }
    }

    /// Determines if a task should be retried based on allocation
    /// outcome, leaving the task with the appropriate status.
    /// The task must be running the allocation to ensure idempotency.
    pub fn handle_allocation_outcome(
        run: &mut FunctionRun,
        allocation: &Allocation,
        application_version: &ApplicationVersion,
    ) {
        match allocation.outcome {
            FunctionRunOutcome::Success => {
                run.status = FunctionRunStatus::Completed;
                run.outcome = Some(allocation.outcome);
            }
            FunctionRunOutcome::Failure(failure_reason) => {
                // Handle allocation failure
                Self::handle_allocation_failure(run, failure_reason, application_version);
            }
            _ => {}
        }
    }
}
