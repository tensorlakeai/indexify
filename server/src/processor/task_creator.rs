use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    vec,
};

use anyhow::Result;
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
        GraphInvocationCtxBuilder,
        GraphInvocationError,
        GraphInvocationFailureReason,
        GraphInvocationOutcome,
        InputArgs,
        InvokeFunctionEvent,
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
    utils::get_epoch_time_in_ms,
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
    #[tracing::instrument(skip(self, in_memory_state, invoke_function_event))]
    pub async fn handle_invoke_function(
        &self,
        in_memory_state: &mut InMemoryState,
        invoke_function_event: InvokeFunctionEvent,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        let cg_key = GraphInvocationCtx::key_from(
            &invoke_function_event.namespace,
            &invoke_function_event.application,
            &invoke_function_event.parent_request_id,
        );

        let Some(mut parent_request_ctx) =
            in_memory_state.invocation_ctx.get(&cg_key.into()).cloned()
        else {
            return Err(anyhow::anyhow!("parent request context not found"));
        };
        // If the parent request is over we don't need to create a new function run
        if parent_request_ctx.outcome.is_some() {
            return Ok(scheduler_update);
        }
        let function_call_id = FunctionCallId(invoke_function_event.request_id.clone());

        // if the request id is already present, then we don't need to create a new
        // function run
        if parent_request_ctx
            .function_calls
            .contains_key(&function_call_id)
        {
            return Ok(scheduler_update);
        }

        let function_call = FunctionCall {
            function_call_id,
            inputs: invoke_function_event
                .data_payloads
                .iter()
                .map(|dp| FunctionArgs::DataPayload(dp.clone()))
                .collect(),
            fn_name: invoke_function_event.function_name.clone(),
            call_metadata: invoke_function_event.call_metadata.clone(),
        };

        let function_run = self
            .indexify_state
            .reader()
            .get_compute_graph_version(
                &invoke_function_event.namespace,
                &invoke_function_event.application,
                &parent_request_ctx.graph_version,
            )
            .map_err(|e| anyhow::anyhow!("failed to get compute graph version: {e}"))?
            .ok_or(anyhow::anyhow!("compute graph version not found"))?
            .create_function_run(
                &function_call,
                invoke_function_event
                    .data_payloads
                    .iter()
                    .map(|arg| InputArgs {
                        function_call_id: None,
                        data_payload: arg.clone(),
                    })
                    .collect::<Vec<_>>(),
                &invoke_function_event.request_id,
            )
            .map_err(|e| anyhow::anyhow!("failed to create function run: {e}"))?;

        let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
            .namespace(invoke_function_event.namespace.to_string())
            .compute_graph_name(invoke_function_event.application.to_string())
            .graph_version(parent_request_ctx.graph_version.clone())
            .request_id(invoke_function_event.request_id.clone())
            .created_at(get_epoch_time_in_ms())
            .parent_request_id(Some(invoke_function_event.parent_request_id.clone()))
            .function_runs(HashMap::from([(function_run.id.clone(), function_run)]))
            .function_calls(HashMap::from([(
                function_call.function_call_id.clone(),
                function_call,
            )]))
            .source_function_call_id(Some(
                invoke_function_event
                    .source_function_call_id
                    .0
                    .as_str()
                    .into(),
            ))
            .build()
            .map_err(|e| anyhow::anyhow!("failed to create graph invocation ctx: {e}"))?;

        parent_request_ctx
            .child_requests
            .insert(invoke_function_event.request_id.clone());

        scheduler_update.add_invocation_state(&parent_request_ctx);
        scheduler_update.add_invocation_state(&graph_invocation_ctx);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(scheduler_update.clone()), vec![])),
            "task_creator",
        )?;
        return Ok(scheduler_update);
    }

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

        let mut scheduler_update = SchedulerUpdateRequest::default();
        function_run.output = alloc_finished_event.data_payload.clone();
        if let Some(graph_updates) = &alloc_finished_event.graph_updates {
            function_run.child_function_call = Some(graph_updates.output_function_call_id.clone());
        }
        scheduler_update.add_function_run(function_run.clone(), &mut invocation_ctx);
        scheduler_update.extend(propagate_output_to_consumers(
            &mut invocation_ctx,
            &function_run,
        )?);

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
                if let ComputeOp::FunctionCall(function_call) = function_call {
                    invocation_ctx.function_calls.insert(
                        function_call.function_call_id.clone(),
                        function_call.clone(),
                    );
                }
            }
            for function_call in &graph_updates.graph_updates {
                if let ComputeOp::Reduce(reduce_op) = function_call {
                    let mut reducer_collection = VecDeque::from(reduce_op.collection.clone());
                    let first_arg = reducer_collection.pop_front();
                    let Some(first_arg) = first_arg else {
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
                    let Some(second_arg) = second_arg else {
                        error!(
                            request_id = invocation_ctx.request_id,
                            "reducer collection has < 2 items"
                        );
                        invocation_ctx.outcome = Some(GraphInvocationOutcome::Failure(
                            GraphInvocationFailureReason::FunctionError,
                        ));
                        return Ok(scheduler_update);
                    };

                    let mut last_function_call =
                        create_function_call_from_reduce_op(reduce_op, first_arg, second_arg);
                    scheduler_update
                        .add_function_call(last_function_call.clone(), &mut invocation_ctx);
                    // Ordering of arguments is important. When we reduce "a, b, c, d"
                    // we want to do reduce(reduce(reduce(a, b), c), d).
                    // So the reduce calls are in order of collection.
                    for arg in reducer_collection {
                        let function_call = create_function_call_from_reduce_op(
                            reduce_op,
                            FunctionArgs::FunctionRunOutput(
                                last_function_call.function_call_id.clone(),
                            ),
                            arg,
                        );
                        scheduler_update
                            .add_function_call(function_call.clone(), &mut invocation_ctx);
                        last_function_call = function_call.clone();
                    }
                    // Change the function call ID of the last reducer function call to
                    // be the reduce operation's function call ID.
                    // Alternatively, we could create a new reducer function run that
                    // consumes the output of the last function call using function_call_id
                    // field.
                    invocation_ctx
                        .function_calls
                        .remove(&last_function_call.function_call_id);
                    last_function_call.function_call_id = reduce_op.function_call_id.clone();
                    scheduler_update
                        .add_function_call(last_function_call.clone(), &mut invocation_ctx);
                }
            }
        }

        // At this point all new function calls are created but their new function runs
        // are not. If no new function runs need to be created and all existing
        // function runs are completed then the invocation is completed
        // successfully because if a function run failed earlier, the invocation
        // will be marked as failed already.
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
            let all_function_runs_finished = invocation_ctx
                .function_runs
                .values()
                .all(|function_run| matches!(function_run.status, TaskStatus::Completed));
            if all_function_runs_finished {
                invocation_ctx.outcome = Some(GraphInvocationOutcome::Success);
                scheduler_update.add_invocation_state(&invocation_ctx);
                return Ok(scheduler_update);
            }
        }

        // Create a function run for each function call that has all the input data
        // payloads available.
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
                            // Function run is not created yet - it's output can't be available.
                            schedulable = false;
                            break;
                        };
                        if let Some(output) = &function_run.output {
                            input_args.push(InputArgs {
                                function_call_id: Some(function_call_id.clone()),
                                data_payload: output.clone(),
                            });
                        } else {
                            // Function run is created and might be running already but not finished
                            // as it has no output yet.
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

fn create_function_call_from_reduce_op(
    reduce_op: &ReduceOperation,
    first_arg: FunctionArgs,
    second_arg: FunctionArgs,
) -> FunctionCall {
    let inputs = vec![first_arg, second_arg];
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
    if function_run.output.is_none() {
        return Ok(scheduler_update);
    }
    let mut finished_function_run = Some(function_run.clone());
    let mut run_was_updated = true;
    while run_was_updated {
        run_was_updated = false;
        // 1. Go through all the function runs
        for fn_run in invocation_ctx.function_runs.values_mut() {
            // 2. See if the function run is linked to a child function call for it's output
            if let Some(child_function_call_id) = &fn_run.child_function_call {
                let Some(function_run_to_propagate) = finished_function_run.clone() else {
                    return Ok(scheduler_update);
                };
                // 3. If the function run that just finished is linked to this function run
                // assign the output to the function run
                if child_function_call_id == &function_run_to_propagate.id {
                    fn_run.output = function_run_to_propagate.output.clone();
                    scheduler_update
                        .updated_function_runs
                        .entry(invocation_ctx_key.clone())
                        .or_insert(HashSet::new())
                        .insert(fn_run.id.clone());
                    // 4. Now that this function run has an output, figure out which
                    // function run this function run is linked to
                    finished_function_run = Some(fn_run.clone());

                    // 5. Since we need to go through this whole process again, set updated_run to
                    //    true
                    run_was_updated = true;
                    break;
                }
            }
        }
    }
    Ok(scheduler_update)
}
