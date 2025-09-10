use std::{
    collections::{HashMap, HashSet}, process::exit, sync::Arc, vec
};

use anyhow::Result;
use tracing::{error, trace};

use crate::{
    data_model::{
        AllocationOutputIngestedEvent,
        ComputeGraphVersion,
        ComputeOp,
        FunctionArgs,
        FunctionCall,
        FunctionCallId,
        FunctionRun,
        GraphInvocationCtx,
        GraphInvocationError,
        GraphInvocationOutcome,
        InputArgs,
        RunningTaskStatus,
        TaskOutcome,
        TaskStatus,
    },
    processor::{task_cache::TaskCache, task_policy::TaskRetryPolicy},
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
        IndexifyState,
    },
};

pub struct TaskCreator {
    indexify_state: Arc<IndexifyState>,
    clock: u64,
    task_cache: Arc<TaskCache>,
}

impl TaskCreator {
    pub fn new(indexify_state: Arc<IndexifyState>, clock: u64, task_cache: Arc<TaskCache>) -> Self {
        Self {
            indexify_state,
            clock,
            task_cache,
        }
    }
}

impl TaskCreator {
    #[tracing::instrument(skip(self, in_memory_state, task_finished_event))]
    pub async fn handle_allocation_ingestion(
        &self,
        in_memory_state: &mut InMemoryState,
        task_finished_event: &AllocationOutputIngestedEvent,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(mut invocation_ctx) = in_memory_state
            .invocation_ctx
            .get(
                &GraphInvocationCtx::key_from(
                    &task_finished_event.namespace,
                    &task_finished_event.compute_graph,
                    &task_finished_event.invocation_id,
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
            .get(&task_finished_event.function_call_id)
            .cloned()
        else {
            error!(
                function_call_id = task_finished_event.function_call_id.to_string(),
                invocation_id = task_finished_event.invocation_id,
                namespace = task_finished_event.namespace,
                graph = task_finished_event.compute_graph,
                "function" = task_finished_event.compute_fn,
                "function run not found, stopping scheduling of child tasks",
            );
            return Ok(SchedulerUpdateRequest::default());
        };
        function_run.output = task_finished_event.data_payload.clone();

        // If allocation_key is not None, then the output is coming from an allocation,
        // not from cache.
        let Some(allocation) = self
            .indexify_state
            .reader()
            .get_allocation(&task_finished_event.allocation_key)?
        else {
            error!(
                allocation_key = task_finished_event.allocation_key,
                "allocation not found, stopping scheduling of child tasks",
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        let mut scheduler_update = SchedulerUpdateRequest::default();
        let cg_version = in_memory_state
            .get_existing_compute_graph_version(&function_run)?
            .clone();

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
        invocation_ctx
            .function_runs
            .insert(function_run.id.clone(), function_run.clone());
        scheduler_update.updated_function_runs = HashMap::from([(
            invocation_ctx.key(),
            HashSet::from([function_run.id.clone()]),
        )]);
        scheduler_update
            .updated_invocations_states
            .insert(invocation_ctx.key(), *invocation_ctx.clone());

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
            println!("task failed, stopping scheduling of child tasks");
            trace!("task failed, stopping scheduling of child tasks");
            function_run.status = TaskStatus::Completed;
            function_run.outcome = Some(allocation.outcome);
            if let Some(invocation_error_payload) = &task_finished_event.request_exception {
                invocation_ctx.request_error = Some(GraphInvocationError {
                    function_name: function_run.name.clone(),
                    payload: invocation_error_payload.clone(),
                });
            }
            invocation_ctx
                .function_runs
                .insert(function_run.id.clone(), function_run.clone());
            invocation_ctx.outcome = Some(GraphInvocationOutcome::Failure(failure_reason.into()));
            return Ok(SchedulerUpdateRequest {
                updated_function_runs: HashMap::from([(
                    invocation_ctx.key(),
                    HashSet::from([function_run.id.clone()]),
                )]),
                updated_invocations_states: HashMap::from([(
                    invocation_ctx.key(),
                    (*invocation_ctx).clone(),
                )]),
                ..Default::default()
            });
        }

        // Update the invocation ctx with the new function calls
        for function_call in &task_finished_event.new_function_calls {
            match function_call {
                ComputeOp::FunctionCall(function_call) => {
                    invocation_ctx.function_calls.insert(
                        function_call.function_call_id.clone(),
                        function_call.clone(),
                    );
                }
                ComputeOp::Reduce(reduce_op) => {
                    for arg in &reduce_op.collection {
                        match arg {
                            FunctionArgs::DataPayload(data_payload) => {
                                let function_call = FunctionCall {
                                    function_call_id: FunctionCallId(nanoid::nanoid!()),
                                    inputs: vec![FunctionArgs::DataPayload(data_payload.clone())],
                                    fn_name: reduce_op.fn_name.clone(),
                                    call_metadata: reduce_op.call_metadata.clone(),
                                };
                                invocation_ctx
                                    .function_calls
                                    .insert(function_call.function_call_id.clone(), function_call);
                            }
                            FunctionArgs::FunctionRunOutput(function_call_id) => {
                                let function_call = FunctionCall {
                                    function_call_id: function_call_id.clone(),
                                    inputs: vec![FunctionArgs::FunctionRunOutput(
                                        function_call_id.clone(),
                                    )],
                                    fn_name: reduce_op.fn_name.clone(),
                                    call_metadata: reduce_op.call_metadata.clone(),
                                };
                                invocation_ctx
                                    .function_calls
                                    .insert(function_call.function_call_id.clone(), function_call);
                            }
                        }
                    }
                }
            }
        }

        let function_call_ids = invocation_ctx
            .function_calls
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        for function_call_id in &function_call_ids {
            let function_call = invocation_ctx
                .function_calls
                .get(&function_call_id)
                .unwrap();
        }
        let function_run_ids = invocation_ctx
            .function_runs
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        for function_run_id in &function_run_ids {
            let function_run = invocation_ctx.function_runs.get(&function_run_id).unwrap();
        }
        if function_call_ids.len() == function_run_ids.len() {
            invocation_ctx.outcome = Some(GraphInvocationOutcome::Success);
            scheduler_update
                .updated_invocations_states
                .insert(invocation_ctx.key(), *invocation_ctx.clone());
            return Ok(scheduler_update);
        }
        let ready_function_calls = function_call_ids
            .difference(&function_run_ids)
            .cloned()
            .collect::<HashSet<_>>();
        for function_call_id in &ready_function_calls {
            let function_call = invocation_ctx
                .function_calls
                .get(&function_call_id)
                .unwrap();
        }
        for function_call_id in ready_function_calls {
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
            invocation_ctx
                .function_runs
                .insert(function_run.id.clone(), function_run.clone());
            scheduler_update
                .updated_function_runs
                .entry(invocation_ctx.key())
                .or_insert(HashSet::new())
                .insert(function_run.id.clone());
            scheduler_update
                .updated_invocations_states
                .insert(invocation_ctx.key(), *invocation_ctx.clone());
        }

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate((Box::new(scheduler_update.clone()), vec![])),
            "task_creator",
        )?;
        Ok(scheduler_update)
    }
}
