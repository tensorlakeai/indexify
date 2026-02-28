use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    vec,
};

use anyhow::Result;
use sha2::{Digest, Sha256};
use tracing::{error, info, trace, warn};

use crate::{
    data_model::{
        Allocation,
        AllocationOutputIngestedEvent,
        ApplicationVersion,
        ComputeOp,
        FunctionArgs,
        FunctionCall,
        FunctionCallEvent,
        FunctionCallId,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        GraphUpdates,
        InputArgs,
        ReduceOperation,
        RequestCtx,
        RequestError,
        RequestFailureReason,
        RequestOutcome,
        RunningFunctionRunStatus,
    },
    processor::{container_scheduler::ContainerScheduler, retry_policy::FunctionRunRetryPolicy},
    state_store::{
        IndexifyState,
        in_memory_state::InMemoryState,
        requests::SchedulerUpdateRequest,
    },
};

pub struct FunctionRunCreator {
    indexify_state: Arc<IndexifyState>,
}

impl FunctionRunCreator {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }
}

impl FunctionRunCreator {
    #[tracing::instrument(skip(self, in_memory_state, function_call_event))]
    pub async fn handle_blocking_function_call(
        &self,
        in_memory_state: &InMemoryState,
        function_call_event: &FunctionCallEvent,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(mut request_ctx) = in_memory_state
            .request_ctx
            .get(
                &RequestCtx::key_from(
                    &function_call_event.namespace,
                    &function_call_event.application,
                    &function_call_event.request_id,
                )
                .into(),
            )
            .cloned()
        else {
            trace!("no request ctx, stopping scheduling of child function runs");
            return Ok(SchedulerUpdateRequest::default());
        };

        if request_ctx.outcome.is_some() {
            trace!("request already completed, stopping scheduling of child function runs");
            return Ok(SchedulerUpdateRequest::default());
        };

        if !request_ctx
            .function_calls
            .contains_key(&function_call_event.source_function_call_id)
        {
            trace!("source function call not found, stopping scheduling of child function runs");
            return Ok(SchedulerUpdateRequest::default());
        }

        let Some(application_version) = in_memory_state.application_version(
            &request_ctx.namespace,
            &request_ctx.application_name,
            &request_ctx.application_version,
        ) else {
            error!(
                namespace = request_ctx.namespace,
                app = request_ctx.application_name,
                app_version = request_ctx.application_version,
                "application version not found",
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        let mut scheduler_update = SchedulerUpdateRequest::default();
        scheduler_update.extend(
            self.create_function_calls(&mut request_ctx, &function_call_event.graph_updates)?,
        );
        let pending_function_calls = request_ctx.pending_function_calls();
        info!(
            request_id = %function_call_event.request_id,
            output_function_call_id = %function_call_event.graph_updates.output_function_call_id,
            num_function_calls = request_ctx.function_calls.len(),
            num_function_runs = request_ctx.function_runs.len(),
            num_pending = pending_function_calls.len(),
            "handle_blocking_function_call: created function calls, scheduling runs"
        );
        scheduler_update.extend(self.create_function_runs(
            &mut request_ctx,
            pending_function_calls,
            application_version,
        )?);
        scheduler_update.add_request_state(&request_ctx);
        Ok(scheduler_update)
    }

    #[tracing::instrument(skip(self, in_memory_state, container_scheduler, alloc_finished_event))]
    pub async fn handle_allocation_ingestion(
        &self,
        in_memory_state: &mut InMemoryState,
        container_scheduler: &mut ContainerScheduler,
        alloc_finished_event: &AllocationOutputIngestedEvent,
    ) -> Result<SchedulerUpdateRequest> {
        info!(
            namespace = alloc_finished_event.namespace,
            application = alloc_finished_event.application,
            request_id = alloc_finished_event.request_id,
            fn = alloc_finished_event.function,
            fn_run_id = alloc_finished_event.function_call_id.to_string(),
            allocation_id = alloc_finished_event.allocation_id.to_string(),
            outcome=alloc_finished_event.allocation_outcome.to_string(),
            executor_id=alloc_finished_event.allocation_target.executor_id.get(),
            "handling allocation ingestion",
        );
        let mut scheduler_update = SchedulerUpdateRequest::default();
        if let Some(fc) = container_scheduler
            .function_containers
            .get_mut(&alloc_finished_event.allocation_target.container_id)
        {
            fc.allocations.remove(&alloc_finished_event.allocation_id);
            if fc.allocations.is_empty() {
                fc.idle_since = Some(tokio::time::Instant::now());
            }
            scheduler_update.containers.insert(
                alloc_finished_event.allocation_target.container_id.clone(),
                fc.clone(),
            );
        }
        container_scheduler.apply_container_update(&scheduler_update);

        let Some(mut request_ctx) = in_memory_state
            .request_ctx
            .get(
                &RequestCtx::key_from(
                    &alloc_finished_event.namespace,
                    &alloc_finished_event.application,
                    &alloc_finished_event.request_id,
                )
                .into(),
            )
            .cloned()
        else {
            trace!("no request ctx, stopping scheduling of child function runs");
            return Ok(scheduler_update);
        };

        if request_ctx.outcome.is_some() {
            trace!("request already completed, stopping scheduling of child function runs");
            return Ok(scheduler_update);
        }

        let Some(mut function_run) = request_ctx
            .function_runs
            .get(&alloc_finished_event.function_call_id)
            .cloned()
        else {
            error!(
                fn_call_id = %alloc_finished_event.function_call_id,
                request_id = %alloc_finished_event.request_id,
                namespace = %alloc_finished_event.namespace,
                app = %alloc_finished_event.application,
                fn = %alloc_finished_event.function,
                "function run not found, stopping scheduling of child function runs",
            );
            return Ok(scheduler_update);
        };

        let allocation_key = Allocation::key_from(
            &alloc_finished_event.namespace,
            &alloc_finished_event.application,
            &alloc_finished_event.request_id,
            &alloc_finished_event.allocation_id,
        );
        let Some(mut allocation) = self
            .indexify_state
            .reader()
            .get_allocation(&allocation_key)
            .await?
        else {
            warn!(
                allocation_id = %alloc_finished_event.allocation_id,
                request_id = %alloc_finished_event.request_id,
                namespace = %alloc_finished_event.namespace,
                app = %alloc_finished_event.application,
                "allocation not found in DB, skipping"
            );
            return Ok(scheduler_update);
        };

        if allocation.is_terminal() {
            warn!(
                allocation_id = %allocation.id,
                request_id = %allocation.request_id,
                namespace = %allocation.namespace,
                app = %allocation.application,
                "allocation already terminal, skipping duplicate finished event"
            );
            return Ok(scheduler_update);
        }

        // Apply executor-reported outcome to the allocation
        allocation.outcome = alloc_finished_event.allocation_outcome.clone();
        allocation.execution_duration_ms = alloc_finished_event.execution_duration_ms;

        // Idempotency: we only act on this alloc's task if the task is currently
        // running this alloc. This is because we handle allocation failures
        // on FE termination and alloc output ingestion paths.
        if function_run.status !=
            FunctionRunStatus::Running(RunningFunctionRunStatus {
                allocation_id: allocation.id.clone(),
            })
        {
            return Ok(scheduler_update);
        }

        scheduler_update
            .updated_allocations
            .push(allocation.clone());
        function_run.output = alloc_finished_event.data_payload.clone();
        if let Some(graph_updates) = &alloc_finished_event.graph_updates {
            function_run.child_function_call = Some(graph_updates.output_function_call_id.clone());
        }
        scheduler_update.add_function_run(function_run.clone(), &mut request_ctx);
        scheduler_update.extend(propagate_output_to_consumers(
            &mut request_ctx,
            &function_run,
        )?);

        let Some(application_version) = in_memory_state
            .get_existing_application_version(&function_run)
            .cloned()
        else {
            warn!(
                fn_call_id = %function_run.id,
                request_id = %function_run.request_id,
                namespace = %function_run.namespace,
                app = %function_run.application,
                app_version = %function_run.version,
                "application version not found, stopping scheduling of child function runs",
            );
            scheduler_update.add_request_state(&request_ctx);
            return Ok(scheduler_update);
        };

        FunctionRunRetryPolicy::handle_allocation_outcome(
            &mut function_run,
            &allocation,
            &application_version,
        );
        // Defer outcome when output comes from a child function call tree.
        // The outcome will be set atomically with the output in
        // propagate_output_to_consumers when the child completes.
        if function_run.child_function_call.is_some() && function_run.output.is_none() {
            function_run.outcome = None;
        }
        scheduler_update.add_function_run(function_run.clone(), &mut request_ctx);

        scheduler_update.add_request_state(&request_ctx);

        // If task is pending (being retried), return early
        if function_run.status == FunctionRunStatus::Pending {
            return Ok(scheduler_update);
        }

        if let FunctionRunOutcome::Failure(failure_reason) = &allocation.outcome {
            function_run.status = FunctionRunStatus::Completed;
            function_run.outcome = Some(allocation.outcome.clone());
            if let Some(request_error_payload) = &alloc_finished_event.request_exception {
                request_ctx.request_error = Some(RequestError {
                    function_name: function_run.name.clone(),
                    payload: request_error_payload.clone(),
                });
                function_run.request_error = Some(request_error_payload.clone());
            }
            request_ctx.outcome = Some(RequestOutcome::Failure(failure_reason.into()));
            scheduler_update.add_function_run(function_run.clone(), &mut request_ctx);

            // Mark the other function runs which are still running as cancelled
            for function_run in request_ctx.function_runs.clone().values_mut() {
                if function_run.status != FunctionRunStatus::Completed {
                    function_run.status = FunctionRunStatus::Completed;
                    function_run.outcome = Some(FunctionRunOutcome::Failure(
                        FunctionRunFailureReason::FunctionRunCancelled,
                    ));
                    scheduler_update.add_function_run(function_run.clone(), &mut request_ctx);
                }
            }
            scheduler_update.add_request_state(&request_ctx);
            return Ok(scheduler_update);
        }

        // Update the request ctx with the new function calls
        if let Some(graph_updates) = &alloc_finished_event.graph_updates {
            scheduler_update.extend(self.create_function_calls(&mut request_ctx, graph_updates)?);
        }

        // At this point all new function calls are created but their new function runs
        // are not. If no new function runs need to be created and all existing
        // function runs are completed then the request is completed
        // successfully because if a function run failed earlier, the request
        // will be marked as failed already.
        if request_ctx.is_request_completed() {
            request_ctx.outcome = Some(RequestOutcome::Success);
            scheduler_update.add_request_state(&request_ctx);
            return Ok(scheduler_update);
        }

        // Create a function run for each function call that has all the input data
        // payloads available.
        let pending_function_calls = request_ctx.pending_function_calls();
        if !pending_function_calls.is_empty() {
            info!(
                request_id = %alloc_finished_event.request_id,
                fn_run_completed = %alloc_finished_event.function_call_id,
                num_function_calls = request_ctx.function_calls.len(),
                num_function_runs = request_ctx.function_runs.len(),
                num_pending = pending_function_calls.len(),
                "handle_allocation_ingestion: checking pending function calls after ingestion"
            );
        }
        scheduler_update.extend(self.create_function_runs(
            &mut request_ctx,
            pending_function_calls,
            &application_version,
        )?);
        scheduler_update.add_request_state(&request_ctx);
        Ok(scheduler_update)
    }

    fn create_function_calls(
        &self,
        request_ctx: &mut RequestCtx,
        update: &GraphUpdates,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        for function_call in &update.graph_updates {
            if let ComputeOp::FunctionCall(function_call) = function_call {
                scheduler_update.add_function_call(function_call.clone(), request_ctx);
            }
        }
        for function_call in &update.graph_updates {
            if let ComputeOp::Reduce(reduce_op) = function_call {
                let mut reducer_collection = VecDeque::from(reduce_op.collection.clone());
                let first_arg = reducer_collection.pop_front();
                let Some(first_arg) = first_arg else {
                    error!(
                        request_id = request_ctx.request_id,
                        "reducer collection is empty"
                    );
                    request_ctx.outcome =
                        Some(RequestOutcome::Failure(RequestFailureReason::FunctionError));
                    return Ok(scheduler_update);
                };

                let second_arg = reducer_collection.pop_front();
                let Some(second_arg) = second_arg else {
                    error!(
                        request_id = request_ctx.request_id,
                        "reducer collection has < 2 items"
                    );
                    request_ctx.outcome =
                        Some(RequestOutcome::Failure(RequestFailureReason::FunctionError));
                    return Ok(scheduler_update);
                };

                let mut last_function_call =
                    create_function_call_from_reduce_op(reduce_op, first_arg, second_arg, None, 0);
                scheduler_update.add_function_call(last_function_call.clone(), request_ctx);
                // Ordering of arguments is important. When we reduce "a, b, c, d"
                // we want to do reduce(reduce(reduce(a, b), c), d).
                // So the reduce calls are in order of collection.
                let mut position = 1;
                for arg in reducer_collection {
                    let function_call = create_function_call_from_reduce_op(
                        reduce_op,
                        FunctionArgs::FunctionRunOutput(
                            last_function_call.function_call_id.clone(),
                        ),
                        arg,
                        None,
                        position,
                    );
                    scheduler_update.add_function_call(function_call.clone(), request_ctx);
                    last_function_call = function_call.clone();
                    position += 1;
                }
                // Change the function call ID of the last reducer function call to
                // be the reduce operation's function call ID.
                // Alternatively, we could create a new reducer function run that
                // consumes the output of the last function call using function_call_id
                // field.
                request_ctx
                    .function_calls
                    .remove(&last_function_call.function_call_id);
                last_function_call.function_call_id = reduce_op.function_call_id.clone();
                scheduler_update.add_function_call(last_function_call.clone(), request_ctx);
            }
        }
        Ok(scheduler_update)
    }

    fn create_function_runs(
        &self,
        request_ctx: &mut RequestCtx,
        pending_function_calls: HashSet<FunctionCallId>,
        application_version: &ApplicationVersion,
    ) -> Result<SchedulerUpdateRequest> {
        let mut scheduler_update = SchedulerUpdateRequest::default();
        for function_call_id in &pending_function_calls {
            let function_call = request_ctx.function_calls.get(function_call_id).unwrap();
            let mut input_args = vec![];
            let mut schedulable = true;
            let mut blocking_reason = String::new();
            for (i, arg) in function_call.inputs.clone().into_iter().enumerate() {
                match arg {
                    FunctionArgs::DataPayload(data_payload) => {
                        input_args.push(InputArgs {
                            function_call_id: None,
                            data_payload: data_payload.clone(),
                        });
                    }
                    FunctionArgs::FunctionRunOutput(dep_function_call_id) => {
                        let Some(function_run) =
                            request_ctx.function_runs.get(&dep_function_call_id)
                        else {
                            // Function run is not created yet - it's output can't be available.
                            blocking_reason =
                                format!("input[{}]: fn_run {} not found", i, dep_function_call_id);
                            schedulable = false;
                            break;
                        };
                        if let Some(output) = &function_run.output {
                            input_args.push(InputArgs {
                                function_call_id: Some(dep_function_call_id.clone()),
                                data_payload: output.clone(),
                            });
                        } else {
                            // Function run is created and might be running already but not finished
                            // as it has no output yet.
                            blocking_reason = format!(
                                "input[{}]: fn_run {} has no output yet",
                                i, dep_function_call_id
                            );
                            schedulable = false;
                            break;
                        }
                    }
                }
            }
            if !schedulable {
                info!(
                    request_id = %request_ctx.request_id,
                    fn_call_id = %function_call_id,
                    fn_name = %function_call.fn_name,
                    num_inputs = function_call.inputs.len(),
                    blocking_reason = %blocking_reason,
                    "create_function_runs: function call NOT schedulable"
                );
                continue;
            }
            info!(
                request_id = %request_ctx.request_id,
                fn_call_id = %function_call_id,
                fn_name = %function_call.fn_name,
                num_inputs = function_call.inputs.len(),
                "create_function_runs: function call IS schedulable, creating run"
            );
            let function_run = application_version
                .create_function_run(function_call, input_args, &request_ctx.request_id)
                .unwrap();
            scheduler_update.add_function_run(function_run.clone(), request_ctx);
        }
        Ok(scheduler_update)
    }
}

fn create_function_call_from_reduce_op(
    reduce_op: &ReduceOperation,
    first_arg: FunctionArgs,
    second_arg: FunctionArgs,
    parent_function_call_id: Option<FunctionCallId>,
    position: usize,
) -> FunctionCall {
    let inputs = vec![first_arg, second_arg];
    // Generate a deterministic ID from the reduce operation's function_call_id
    // and position in the chain. This makes call_function RPCs idempotent —
    // re-sending the same reduce operation produces the same function call IDs
    // instead of creating duplicates with random nanoid IDs.
    let mut hasher = Sha256::new();
    hasher.update(reduce_op.function_call_id.0.as_bytes());
    hasher.update(b":reduce:");
    hasher.update(position.to_string().as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    FunctionCall {
        function_call_id: FunctionCallId(hash),
        inputs,
        fn_name: reduce_op.fn_name.clone(),
        call_metadata: reduce_op.call_metadata.clone(),
        parent_function_call_id,
    }
}

fn propagate_output_to_consumers(
    request_ctx: &mut RequestCtx,
    function_run: &FunctionRun,
) -> Result<SchedulerUpdateRequest> {
    let mut scheduler_update = SchedulerUpdateRequest::default();
    let request_ctx_key = request_ctx.key().clone();
    if function_run.output.is_none() {
        return Ok(scheduler_update);
    }

    // Build reverse index: child_call_id → parent_run_id.
    // A run with child_function_call = X is waiting for the run whose id = X.
    // So when run X finishes, we look up which parent is waiting for it.
    let child_to_parent: HashMap<FunctionCallId, FunctionCallId> = request_ctx
        .function_runs
        .iter()
        .filter_map(|(_, run)| {
            run.child_function_call
                .as_ref()
                .map(|child_id| (child_id.clone(), run.id.clone()))
        })
        .collect();

    // Walk the chain using O(1) lookups instead of O(N) scans.
    let mut current_id = function_run.id.clone();
    let mut current_output = function_run.output.clone();
    while let Some(parent_id) = child_to_parent.get(&current_id) {
        let Some(parent_run) = request_ctx.function_runs.get_mut(parent_id) else {
            break;
        };
        parent_run.output = current_output;
        parent_run.outcome = Some(FunctionRunOutcome::Success);
        parent_run.status = FunctionRunStatus::Completed;
        scheduler_update
            .updated_function_runs
            .entry(request_ctx_key.clone())
            .or_insert(imbl::HashSet::new())
            .insert(parent_run.id.clone());
        current_id = parent_run.id.clone();
        current_output = parent_run.output.clone();
    }
    Ok(scheduler_update)
}
