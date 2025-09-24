use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    vec,
};

use anyhow::Result;
use bytes::Bytes;
use tracing::{error, trace, warn};

use crate::{
    data_model::{
        AllocationOutputIngestedEvent,
        ComputeOp,
        FunctionArgs,
        FunctionCall,
        FunctionCallId,
        FunctionRun,
        GraphInvocationCtx,
        GraphInvocationError,
        GraphInvocationFailureReason,
        GraphInvocationOutcome,
        InputArgs,
        ReduceOperation,
        RunningTaskStatus,
        TaskOutcome,
        TaskStatus,
    },
    processor::task_policy::TaskRetryPolicy,
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
        IndexifyState,
    },
};

pub struct TaskCreator {
    indexify_state: Arc<IndexifyState>,
    clock: u64,
}

impl TaskCreator {
    pub fn new(indexify_state: Arc<IndexifyState>, clock: u64) -> Self {
        Self {
            indexify_state,
            clock,
        }
    }
}

impl TaskCreator {
    #[tracing::instrument(skip(self, in_memory_state, alloc_finished_event))]
    pub async fn handle_allocation_ingestion(
        &self,
        in_memory_state: &mut InMemoryState,
        alloc_finished_event: &AllocationOutputIngestedEvent,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(mut invocation_ctx) = in_memory_state
            .invocation_ctx
            .get(
                &GraphInvocationCtx::key_from(
                    &alloc_finished_event.namespace,
                    &alloc_finished_event.compute_graph,
                    &alloc_finished_event.invocation_id,
                )
                .into(),
            )
            .cloned()
        else {
            trace!("no invocation ctx, stopping scheduling of child tasks");
            return Ok(SchedulerUpdateRequest::default());
        };

        if invocation_ctx.outcome.is_some() {
            trace!("invocation already completed, stopping scheduling of child tasks");
            return Ok(SchedulerUpdateRequest::default());
        }

        let Some(mut function_run) = invocation_ctx
            .function_runs
            .get(&alloc_finished_event.function_call_id)
            .cloned()
        else {
            error!(
                function_call_id = alloc_finished_event.function_call_id.to_string(),
                invocation_id = alloc_finished_event.invocation_id,
                namespace = alloc_finished_event.namespace,
                graph = alloc_finished_event.compute_graph,
                fn = alloc_finished_event.compute_fn,
                "function run not found, stopping scheduling of child tasks",
            );
            return Ok(SchedulerUpdateRequest::default());
        };
        function_run.output = alloc_finished_event.data_payload.clone();
        match &alloc_finished_event.graph_updates {
            Some(graph_updates) => {
                function_run.child_function_call =
                    Some(graph_updates.output_function_call_id.clone());
            }
            None => {}
        }

        // If allocation_key is not None, then the output is coming from an allocation,
        // not from cache.
        let Some(allocation) = self
            .indexify_state
            .reader()
            .get_allocation(&alloc_finished_event.allocation_key)?
        else {
            error!(
                allocation_key = alloc_finished_event.allocation_key,
                "allocation not found, stopping scheduling of child tasks",
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        let mut scheduler_update =
            propagate_output_to_consumers(&mut invocation_ctx, &function_run)?;
        let Some(cg_version) = in_memory_state
            .get_existing_compute_graph_version(&function_run)
            .cloned()
        else {
            warn!(
                function_run.id = function_run.id.to_string(),
                function_run.request_id = function_run.request_id,
                function_run.namespace = function_run.namespace,
                function_run.application = function_run.application,
                function_run.graph_version = function_run.graph_version.0,
                "compute graph version not found, stopping scheduling of child tasks",
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        // Idempotency: we only act on this alloc's task if the task is currently
        // running this alloc. This is because we handle allocation failures
        // on FE termination and alloc output ingestion paths.
        if function_run.status !=
            TaskStatus::Running(RunningTaskStatus {
                allocation_id: allocation.id.clone(),
            })
        {
            return Ok(SchedulerUpdateRequest::default());
        }

        TaskRetryPolicy::handle_allocation_outcome(&mut function_run, &allocation, &cg_version);
        scheduler_update.add_function_run(function_run.clone(), &mut invocation_ctx);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(scheduler_update.clone()), vec![])),
            "task_creator",
        )?;

        // If task is pending (being retried), return early
        if function_run.status == TaskStatus::Pending {
            return Ok(scheduler_update);
        }

        if let TaskOutcome::Failure(failure_reason) = allocation.outcome {
            trace!("task failed, stopping scheduling of child tasks");
            function_run.status = TaskStatus::Completed;
            function_run.outcome = Some(allocation.outcome);
            if let Some(invocation_error_payload) = &alloc_finished_event.request_exception {
                invocation_ctx.request_error = Some(GraphInvocationError {
                    function_name: function_run.name.clone(),
                    payload: invocation_error_payload.clone(),
                });
            }
            invocation_ctx.outcome = Some(GraphInvocationOutcome::Failure(failure_reason.into()));
            let mut scheduler_update = SchedulerUpdateRequest::default();
            scheduler_update.add_function_run(function_run.clone(), &mut invocation_ctx);
            return Ok(scheduler_update);
        }

        // Update the invocation ctx with the new function calls
        if let Some(graph_updates) = &alloc_finished_event.graph_updates {
            for function_call in &graph_updates.graph_updates {
                match function_call {
                    ComputeOp::FunctionCall(function_call) => {
                        invocation_ctx.function_calls.insert(
                            function_call.function_call_id.clone(),
                            function_call.clone(),
                        );
                    }
                    _ => {}
                }
            }
            for function_call in &graph_updates.graph_updates {
                match function_call {
                    ComputeOp::Reduce(reduce_op) => {
                        let mut reducer_collection = VecDeque::from(reduce_op.collection.clone());
                        let Some(first_arg) = reducer_collection.pop_front() else {
                            error!(
                                request_id = invocation_ctx.request_id,
                                "reducer collection is empty"
                            );
                            invocation_ctx.outcome = Some(GraphInvocationOutcome::Failure(
                                GraphInvocationFailureReason::FunctionError,
                            ));
                            return Ok(scheduler_update);
                        };

                        let second_arg = reducer_collection.pop_front();
                        if let Some(second_arg) = second_arg {
                            let mut last_function_call = create_function_call_from_reduce_op1(
                                reduce_op,
                                &first_arg,
                                &second_arg,
                            );
                            scheduler_update
                                .add_function_call(last_function_call.clone(), &mut invocation_ctx);
                            for arg in reducer_collection {
                                let function_call = create_function_call_from_reduce_op1(
                                    reduce_op,
                                    &arg,
                                    &FunctionArgs::FunctionRunOutput(
                                        last_function_call.function_call_id.clone(),
                                    ),
                                );
                                scheduler_update
                                    .add_function_call(function_call.clone(), &mut invocation_ctx);
                                last_function_call = function_call.clone();
                            }
                            // the last function call is assigned the id of the reduce op
                            // this enables dependent function calls to resolve when the reduce op's
                            // last function is resolved and it's also
                            // the final output of the reduce op
                            last_function_call.function_call_id =
                                reduce_op.function_call_id.clone();
                            scheduler_update
                                .add_function_call(last_function_call.clone(), &mut invocation_ctx);
                        } else {
                            match &first_arg {
                                FunctionArgs::DataPayload(data_payload) => {
                                    let function_call = FunctionCall {
                                        function_call_id: reduce_op.function_call_id.clone(),
                                        inputs: vec![],
                                        fn_name: "reducer".into(),
                                        call_metadata: Bytes::new(),
                                    };
                                    invocation_ctx.function_calls.insert(
                                        reduce_op.function_call_id.clone(),
                                        function_call.clone(),
                                    );
                                    let mut function_run = cg_version
                                        .create_function_run(
                                            &function_call,
                                            vec![],
                                            &invocation_ctx.request_id,
                                        )
                                        .unwrap();
                                    function_run.output = Some(data_payload.clone());
                                    function_run.status = TaskStatus::Completed;
                                    function_run.outcome = Some(TaskOutcome::Success);
                                    scheduler_update.add_function_run(
                                        function_run.clone(),
                                        &mut invocation_ctx,
                                    );
                                    scheduler_update.extend(propagate_output_to_consumers(
                                        &mut invocation_ctx,
                                        &function_run,
                                    )?);
                                }
                                FunctionArgs::FunctionRunOutput(function_call_id) => {
                                    let mut function_call = invocation_ctx
                                        .function_calls
                                        .get(function_call_id)
                                        .cloned()
                                        .unwrap();
                                    invocation_ctx.function_calls.remove(function_call_id);
                                    function_call.function_call_id =
                                        reduce_op.function_call_id.clone();
                                    scheduler_update.add_function_call(
                                        function_call.clone(),
                                        &mut invocation_ctx,
                                    );
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let function_call_ids = invocation_ctx
            .function_calls
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let function_run_ids = invocation_ctx
            .function_runs
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        if function_call_ids.len() == function_run_ids.len() {
            invocation_ctx.outcome = Some(GraphInvocationOutcome::Success);
            scheduler_update.add_function_run(function_run.clone(), &mut invocation_ctx);
            return Ok(scheduler_update);
        }
        let pending_function_calls = function_call_ids
            .difference(&function_run_ids)
            .cloned()
            .collect::<HashSet<_>>();
        for function_call_id in pending_function_calls {
            let function_call = invocation_ctx
                .function_calls
                .get(&function_call_id)
                .unwrap();
            let mut input_args = vec![];
            let mut schedulable = true;
            for arg in function_call.inputs.clone() {
                match arg {
                    FunctionArgs::DataPayload(data_payload) => {
                        input_args.push(InputArgs {
                            function_call_id: None,
                            data_payload: data_payload.clone(),
                        });
                    }
                    FunctionArgs::FunctionRunOutput(function_call_id) => {
                        let Some(function_run) =
                            invocation_ctx.function_runs.get(&function_call_id)
                        else {
                            schedulable = false;
                            break;
                        };
                        if let Some(output) = &function_run.output {
                            input_args.push(InputArgs {
                                function_call_id: Some(function_call_id.clone()),
                                data_payload: output.clone(),
                            });
                        } else {
                            schedulable = false;
                            break;
                        }
                    }
                }
            }
            if !schedulable {
                continue;
            }
            let function_run = cg_version
                .create_function_run(function_call, input_args, &invocation_ctx.request_id)
                .unwrap();
            scheduler_update.add_function_run(function_run.clone(), &mut invocation_ctx);
        }

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(scheduler_update.clone()), vec![])),
            "task_creator",
        )?;
        Ok(scheduler_update)
    }
}

fn create_function_call_from_reduce_op1(
    reduce_op: &ReduceOperation,
    first_arg: &FunctionArgs,
    second_arg: &FunctionArgs,
) -> FunctionCall {
    let mut inputs = vec![];
    inputs.push(first_arg.clone());
    inputs.push(second_arg.clone());
    FunctionCall {
        function_call_id: FunctionCallId(nanoid::nanoid!()),
        inputs,
        fn_name: reduce_op.fn_name.clone(),
        call_metadata: reduce_op.call_metadata.clone(),
    }
}

fn propagate_output_to_consumers(
    invocation_ctx: &mut GraphInvocationCtx,
    function_run: &FunctionRun,
) -> Result<SchedulerUpdateRequest> {
    let mut scheduler_update = SchedulerUpdateRequest::default();
    let invocation_ctx_key = invocation_ctx.key().clone();
    if !function_run.output.is_some() {
        return Ok(scheduler_update);
    }
    let mut function_run_output_to_propagate = Some(function_run.clone());
    let mut updated_run = true;
    while updated_run {
        updated_run = false;
        // 1. Go through all the function runs
        for fn_run in invocation_ctx.function_runs.values_mut() {
            // 2. See if the function run is linked to a child function call for it's output
            if let Some(child_function_call_id) = &fn_run.child_function_call {
                let Some(function_run) = function_run_output_to_propagate.clone() else {
                    return Ok(scheduler_update);
                };
                // 3. If the function run that just finished is linked to this function run
                // assign the output to the function run
                if child_function_call_id == &function_run.id {
                    fn_run.output = function_run.output.clone();
                    scheduler_update
                        .updated_function_runs
                        .entry(invocation_ctx_key.clone())
                        .or_insert(HashSet::new())
                        .insert(fn_run.id.clone());
                    // 4. Now that this function run has an output, figure out which
                    // function run this function run is linked to
                    function_run_output_to_propagate = Some(fn_run.clone());

                    // 5. Since we need to go through this whole process again, set updated_run to
                    //    true
                    updated_run = true;
                    break;
                }
            }
        }
    }
    return Ok(scheduler_update);
}
