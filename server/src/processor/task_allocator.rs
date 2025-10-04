use anyhow::Result;
use tracing::{debug, info_span, warn};

use crate::{
    data_model::{
        AllocationBuilder,
        ApplicationInvocationCtx,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        RunningFunctionRunStatus,
    },
    processor::function_executor_manager::FunctionExecutorManager,
    state_store::{
        self,
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct FunctionRunProcessor<'a> {
    clock: u64,
    fe_manager: &'a FunctionExecutorManager,
}

impl<'a> FunctionRunProcessor<'a> {
    pub fn new(clock: u64, fe_manager: &'a FunctionExecutorManager) -> Self {
        Self { clock, fe_manager }
    }

    /// Allocate attempts to allocate unallocated tasks to function executors.
    #[tracing::instrument(skip(self, in_memory_state))]
    pub fn allocate(&self, in_memory_state: &mut InMemoryState) -> Result<SchedulerUpdateRequest> {
        // Step 1: Fetch unallocated tasks
        let function_runs = in_memory_state.unallocated_function_runs();

        // Step 2: Allocate tasks
        let mut update = SchedulerUpdateRequest::default();

        for function_run in function_runs {
            let Some(mut ctx) = in_memory_state
                .invocation_ctx
                .get(&function_run.clone().into())
                .cloned()
            else {
                continue;
            };

            match self.create_allocation(in_memory_state, &function_run, &mut ctx) {
                Ok(allocation_update) => {
                    update.extend(allocation_update);
                }
                Err(err) => {
                    // Check if this is a state store error we can handle gracefully
                    if let Some(state_store_error) =
                        err.downcast_ref::<state_store::in_memory_state::Error>()
                    {
                        warn!(
                            function_call_id = function_run.id.to_string(),
                            namespace = function_run.namespace,
                            application = function_run.application,
                            application_version = state_store_error.version(),
                            "fb" = state_store_error.function_name(),
                            error = %state_store_error,
                            "Unable to allocate task"
                        );

                        // Check if this is a ConstraintUnsatisfiable error; if it is, we want to
                        // fail the task and invocation.
                        //
                        // TODO: Turn this into a check at server startup.
                        if matches!(
                            state_store_error,
                            state_store::in_memory_state::Error::ConstraintUnsatisfiable { .. }
                        ) {
                            // Fail the task
                            let mut failed_function_run = function_run.clone();
                            failed_function_run.status = FunctionRunStatus::Completed;
                            failed_function_run.outcome = Some(FunctionRunOutcome::Failure(
                                FunctionRunFailureReason::ConstraintUnsatisfiable,
                            ));

                            // Add the failed function run to the update
                            ctx.outcome = Some(
                                crate::data_model::ApplicationRequestOutcome::Failure(
                                    crate::data_model::ApplicationInvocationFailureReason::ConstraintUnsatisfiable
                                )
                            );
                            update.add_function_run(failed_function_run.clone(), &mut ctx);
                        }

                        continue;
                    }
                    // For any other error, return it
                    return Err(err);
                }
            }
        }

        Ok(update)
    }

    fn create_allocation(
        &self,
        in_memory_state: &mut InMemoryState,
        function_run: &FunctionRun,
        ctx: &mut ApplicationInvocationCtx,
    ) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            "create_allocation",
            namespace = function_run.namespace,
            function_call_id = function_run.id.to_string(),
            request_id = function_run.request_id,
            application = function_run.application,
            "fn" = function_run.name,
            application_version = function_run.application_version.to_string(),
        );
        let _guard = span.enter();

        let mut update = SchedulerUpdateRequest::default();

        // Use FunctionExecutorManager to handle function executor selection/creation
        let (selected_target, fe_update) = self
            .fe_manager
            .select_or_create_function_executor(in_memory_state, function_run)?;
        update.extend(fe_update);

        let Some(target) = selected_target else {
            return Ok(update);
        };
        let allocation = AllocationBuilder::default()
            .namespace(function_run.namespace.clone())
            .application(function_run.application.clone())
            .function(function_run.name.clone())
            .invocation_id(function_run.request_id.clone())
            .function_call_id(function_run.id.clone())
            .call_metadata(function_run.call_metadata.clone())
            .input_args(function_run.input_args.clone())
            .target(target)
            .attempt_number(function_run.attempt_number)
            .outcome(FunctionRunOutcome::Unknown)
            .build()?;

        debug!(allocation_id = %allocation.id, "created allocation");

        let mut updated_function_run = function_run.clone();
        updated_function_run.status = FunctionRunStatus::Running(RunningFunctionRunStatus {
            allocation_id: allocation.id.clone(),
        });

        update.add_function_run(updated_function_run.clone(), ctx);
        update.new_allocations.push(allocation);
        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(update.clone()), vec![])),
            "task_allocator",
        )?;
        Ok(update)
    }
}
