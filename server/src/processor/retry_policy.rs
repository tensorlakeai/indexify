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
pub struct FunctionRunRetryPolicy;

impl FunctionRunRetryPolicy {
    /// Determines if a task should be retried based on an allocation failure
    /// reason, updating the task accordingly.
    fn handle_allocation_failure(
        run: &mut FunctionRun,
        alloc_failure_reason: FunctionRunFailureReason,
        application_version: &ApplicationVersion,
    ) {
        let uses_attempt = alloc_failure_reason.should_count_against_function_run_retry_attempts();
        if let Some(max_retries) = application_version.function_run_max_retries(run) {
            if alloc_failure_reason.is_retriable() {
                if run.attempt_number < max_retries {
                    run.status = FunctionRunStatus::Pending;
                    if uses_attempt {
                        run.attempt_number += 1;
                    }
                    return;
                }
            }
        } else {
            run.status = FunctionRunStatus::Completed;
            run.outcome = Some(FunctionRunOutcome::Failure(alloc_failure_reason));
            return;
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
