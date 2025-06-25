use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    AllocationOutputIngestedEvent,
    ChangeType,
    ComputeGraphVersion,
    GraphInvocationCtx,
    GraphInvocationError,
    GraphInvocationFailureReason,
    GraphInvocationOutcome,
    InvokeComputeGraphEvent,
    ReduceTask,
    Task,
    TaskOutcome,
    TaskStatus,
};
use state_store::{
    in_memory_state::InMemoryState,
    requests::{ReductionTasks, RequestPayload, SchedulerUpdateRequest},
    IndexifyState,
};
use tracing::{error, info, trace};

#[derive(Debug, Default)]
pub struct TaskCreationResult {
    pub tasks: Vec<Task>,
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
    pub invocation_ctx: Option<GraphInvocationCtx>,
}

pub struct TaskCreator {
    indexify_state: Arc<IndexifyState>,
    in_memory_state: Arc<RwLock<InMemoryState>>,
    clock: u64,
}

impl TaskCreator {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        in_memory_state: Arc<RwLock<InMemoryState>>,
        clock: u64,
    ) -> Self {
        Self {
            indexify_state,
            in_memory_state,
            clock,
        }
    }
}

impl TaskCreator {
    #[tracing::instrument(skip(self))]
    pub async fn invoke(&mut self, change: &ChangeType) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::AllocationOutputsIngested(ev) => {
                let scheduler_update = self.handle_allocation_ingestion(ev).await?;
                self.in_memory_state.write().unwrap().update_state(
                    self.clock,
                    &RequestPayload::SchedulerUpdate(Box::new(scheduler_update.clone())),
                    "task_creator",
                )?;
                Ok(scheduler_update)
            }
            ChangeType::InvokeComputeGraph(ev) => {
                let result = self.handle_invoke_compute_graph(ev.clone()).await?;
                let scheduler_update = SchedulerUpdateRequest {
                    updated_tasks: result
                        .tasks
                        .into_iter()
                        .map(|t| (t.id.clone(), t))
                        .collect(),
                    updated_invocations_states: result
                        .invocation_ctx
                        .map(|ctx| ctx.clone())
                        .into_iter()
                        .collect(),
                    reduction_tasks: ReductionTasks {
                        new_reduction_tasks: result.new_reduction_tasks,
                        processed_reduction_tasks: result.processed_reduction_tasks,
                    },
                    ..Default::default()
                };
                self.in_memory_state.write().unwrap().update_state(
                    self.clock,
                    &RequestPayload::SchedulerUpdate(Box::new(scheduler_update.clone())),
                    "task_creator",
                )?;
                Ok(scheduler_update)
            }
            _ => {
                error!(
                    "TaskCreator received an unexpected change type: {:?}",
                    change
                );
                return Err(anyhow!(
                    "TaskCreator received an unexpected change type: {:?}",
                    change
                ));
            }
        }
    }

    #[tracing::instrument(skip(self, task_finished_event))]
    pub async fn handle_allocation_ingestion(
        &self,
        task_finished_event: &AllocationOutputIngestedEvent,
    ) -> Result<SchedulerUpdateRequest> {
        let mut in_memory_state = self.in_memory_state.write().unwrap();
        let Some(invocation_ctx) = in_memory_state
            .invocation_ctx
            .get(&GraphInvocationCtx::key_from(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.invocation_id,
            ))
            .cloned()
        else {
            trace!("no invocation ctx, stopping scheduling of child tasks");
            return Ok(SchedulerUpdateRequest::default());
        };

        if invocation_ctx.completed {
            trace!("invocation already completed, stopping scheduling of child tasks");
            return Ok(SchedulerUpdateRequest::default());
        }

        let Some(mut task) = in_memory_state
            .tasks
            .get(&Task::key_from(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.invocation_id,
                &task_finished_event.compute_fn,
                &task_finished_event.task_id.to_string(),
            ))
            .cloned()
        else {
            error!(
                task_id = task_finished_event.task_id.to_string(),
                invocation_id = task_finished_event.invocation_id.to_string(),
                namespace = task_finished_event.namespace,
                graph = task_finished_event.compute_graph,
                "fn" = task_finished_event.compute_fn,
                "task not found for task finished event",
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        // We have already handled updating this task through the path of FE failures.
        // However, if there was a cache hit, the task would have been updated to
        // completed without any allocation ingestion. So we have to proceed and
        // create new tasks.
        if (task.status == TaskStatus::Pending || task.status == TaskStatus::Completed) &&
            !task.cache_hit
        {
            return Ok(SchedulerUpdateRequest::default());
        }

        let compute_graph_version = in_memory_state
            .compute_graph_versions
            .get(&task.key_compute_graph_version());
        if compute_graph_version.is_none() {
            error!(
                task_id = task.id.to_string(),
                invocation_id = task.invocation_id.to_string(),
                namespace = task.namespace,
                graph = task.compute_graph_name,
                "fn" = task.compute_fn_name,
                graph_version = task.graph_version.0,
                "compute graph version not found",
            );
            return Ok(SchedulerUpdateRequest::default());
        }
        let compute_graph_version = compute_graph_version
            .ok_or(anyhow!(
                "compute graph version not found: {:?} {:?} {:?}",
                task.namespace,
                task.compute_graph_name,
                task.graph_version.0
            ))?
            .clone();

        let mut scheduler_update = SchedulerUpdateRequest::default();
        if let Some(allocation_key) = &task_finished_event.allocation_key {
            let Some(allocation) = self
                .indexify_state
                .reader()
                .get_allocation(&allocation_key)?
            else {
                error!(
                    allocation_key = allocation_key,
                    "allocation not found, stopping scheduling of child tasks",
                );
                return Ok(SchedulerUpdateRequest::default());
            };

            if allocation.outcome == TaskOutcome::Failure &&
                compute_graph_version.should_retry_task(&task) &&
                allocation.failure_reason.is_retriable()
            {
                task.status = TaskStatus::Pending;
                task.attempt_number += 1;
                scheduler_update.updated_tasks = HashMap::from([(task.id.clone(), *task.clone())]);
                return Ok(scheduler_update);
            }
            task.status = TaskStatus::Completed;
            task.outcome = allocation.outcome;
            task.failure_reason = allocation.failure_reason;
            scheduler_update.updated_tasks = HashMap::from([(task.id.clone(), *task.clone())]);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(scheduler_update.clone())),
                "task_creator",
            )?;
        }
        let task_creation_result = self
            .handle_task_finished(
                &mut in_memory_state,
                *invocation_ctx.clone(),
                *task.clone(),
                *compute_graph_version.clone(),
                task_finished_event.node_output_key.clone(),
            )
            .await?;
        let task_creator_update = SchedulerUpdateRequest {
            updated_tasks: task_creation_result
                .tasks
                .into_iter()
                .map(|t| (t.id.clone(), t))
                .collect(),
            updated_invocations_states: task_creation_result
                .invocation_ctx
                .map(|ctx| ctx.clone())
                .into_iter()
                .collect(),
            reduction_tasks: ReductionTasks {
                new_reduction_tasks: task_creation_result.new_reduction_tasks,
                processed_reduction_tasks: task_creation_result.processed_reduction_tasks,
            },
            ..Default::default()
        };
        scheduler_update.extend(task_creator_update);
        Ok(scheduler_update)
    }

    #[tracing::instrument(skip(self, event))]
    pub async fn handle_invoke_compute_graph(
        &self,
        event: InvokeComputeGraphEvent,
    ) -> Result<TaskCreationResult> {
        let invocation_ctx = self
            .in_memory_state
            .read()
            .unwrap()
            .invocation_ctx
            .get(&GraphInvocationCtx::key_from(
                &event.namespace,
                &event.compute_graph,
                &event.invocation_id,
            ))
            .cloned();
        if invocation_ctx.is_none() {
            trace!("no invocation ctx, stopping invocation of compute graph");
            return Ok(TaskCreationResult::default());
        }
        let mut invocation_ctx = invocation_ctx
            .ok_or(anyhow!(
                "invocation context not found for invocation_id {}",
                event.invocation_id
            ))?
            .clone();

        let compute_graph_version = self
            .in_memory_state
            .read()
            .unwrap()
            .compute_graph_versions
            .get(&ComputeGraphVersion::key_from(
                &event.namespace,
                &event.compute_graph,
                &invocation_ctx.graph_version,
            ))
            .cloned();
        if compute_graph_version.is_none() {
            error!(
                invocation_id = event.invocation_id.to_string(),
                namespace = event.namespace,
                compute_graph = event.compute_graph,
                graph_version = invocation_ctx.graph_version.0,
                "compute graph version not found",
            );
            return Ok(TaskCreationResult::default());
        }
        let compute_graph_version = compute_graph_version.ok_or(anyhow!(
            "compute graph version not found: {:?} {:?} {:?}",
            event.namespace,
            event.compute_graph,
            invocation_ctx.graph_version,
        ))?;

        let Some(input) = self.indexify_state.reader().get_graph_input(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
        )?
        else {
            return Err(anyhow!(
                "input not found for invocation_id {}",
                event.invocation_id
            ));
        };

        // Create a task for the compute graph
        let task = compute_graph_version.start_fn.create_task(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            input,
            None,
            &compute_graph_version.version,
        )?;
        invocation_ctx.create_tasks(&vec![task.clone()], &vec![]);

        trace!(
            task_key = task.key(),
            "Creating a standard task to start compute graph"
        );
        Ok(TaskCreationResult {
            tasks: vec![task],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(*invocation_ctx.clone()),
        })
    }

    #[tracing::instrument(skip(
        self,
        in_memory_state,
        invocation_ctx,
        task,
        compute_graph_version,
        node_output_key
    ))]
    pub async fn handle_task_finished(
        &self,
        in_memory_state: &mut InMemoryState,
        invocation_ctx: GraphInvocationCtx,
        task: Task,
        compute_graph_version: ComputeGraphVersion,
        node_output_key: String,
    ) -> Result<TaskCreationResult> {
        trace!("invocation context: {:?}", invocation_ctx);
        let mut invocation_ctx = invocation_ctx.clone();
        invocation_ctx.update_analytics(&task);

        let node_output = self
            .indexify_state
            .reader()
            .get_node_output_by_key(&node_output_key)?;

        if task.outcome == TaskOutcome::Failure {
            trace!("task failed, stopping scheduling of child tasks");
            if let Some(node_output) = &node_output {
                if let Some(invocation_error_payload) = node_output.invocation_error_payload.clone()
                {
                    invocation_ctx.invocation_error = Some(GraphInvocationError {
                        function_name: task.compute_fn_name.clone(),
                        payload: invocation_error_payload,
                    });
                }
            }
            invocation_ctx.failure_reason = task.failure_reason.into();
            invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Failure);
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
                ..Default::default()
            });
        }

        let Some(node_output) = node_output else {
            // Handle the case when the task hasn't produced any output
            if invocation_ctx.all_tasks_completed() {
                invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Success);
            }
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
                ..Default::default()
            });
        };
        let mut new_tasks = vec![];

        let Some(compute_node) = compute_graph_version.nodes.get(&task.compute_fn_name) else {
            error!(
                task_id = task.id.to_string(),
                invocation_id = task.invocation_id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                "fn" = task.compute_fn_name,
                "compute node not found",
            );
            return Ok(TaskCreationResult::default());
        };
        if compute_node.reducer {
            let reduction_task = in_memory_state.next_reduction_task(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
                &task.compute_fn_name,
            );
            if let Some(reduction_task) = reduction_task {
                let new_task = compute_node.create_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                    reduction_task.input.clone(),
                    Some(node_output.payloads.first().unwrap().clone()),
                    &invocation_ctx.graph_version,
                )?;
                new_tasks.push(new_task.clone());
                invocation_ctx.create_tasks(&vec![new_task.clone()], &vec![]);
                invocation_ctx.complete_reducer_task(&task.compute_fn_name);
                return Ok(TaskCreationResult {
                    tasks: new_tasks,
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![reduction_task.key()],
                    invocation_ctx: Some(invocation_ctx),
                });
            }
        }

        if node_output.next_functions.is_empty() {
            trace!(
                "No more edges to schedule tasks for, waiting for outstanding tasks to finalize"
            );
            if invocation_ctx.all_tasks_completed() {
                invocation_ctx.complete_invocation(false, GraphInvocationOutcome::Success);
            }
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
                ..Default::default()
            });
        };

        // Create new tasks for the edges of the node of the current task.
        let mut new_reduction_tasks = vec![];
        for edge in node_output.next_functions.iter() {
            let edge_compute_node = compute_graph_version.nodes.get(edge);
            let Some(edge_compute_node) = edge_compute_node else {
                // This can happen e.g. if the compute graph was updated in non backward
                // compatible way while the invocation was running.
                info!(edge = edge, "Edge function not found, failing invocation",);
                invocation_ctx.failure_reason = GraphInvocationFailureReason::NextFunctionNotFound;
                invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Failure);
                return Ok(TaskCreationResult {
                    invocation_ctx: Some(invocation_ctx),
                    ..Default::default()
                });
            };

            for output in &node_output.payloads {
                if edge_compute_node.reducer {
                    // 1. Create a new reduction task with the output of the current task
                    // 2. If there are no more outstanding tasks of the current node, then create a
                    //    new task for the reducer
                    // and remove the reducer task for which we created the new task.

                    let reduction_task = ReduceTask {
                        namespace: task.namespace.clone(),
                        compute_graph_name: task.compute_graph_name.clone(),
                        invocation_id: task.invocation_id.clone(),
                        compute_fn_name: edge_compute_node.name.clone(),
                        input: output.clone(),
                        id: nanoid::nanoid!(),
                    };
                    new_reduction_tasks.push(reduction_task);
                    continue;
                }
                let new_task = edge_compute_node.create_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                    output.clone(),
                    None,
                    &invocation_ctx.graph_version,
                )?;
                info!(
                    task_key = new_task.key(),
                    "fn" = new_task.compute_fn_name,
                    graph = new_task.compute_graph_name,
                    invocation_id = new_task.invocation_id,
                    "Creating a standard task",
                );
                new_tasks.push(new_task);
            }
        }

        invocation_ctx.create_tasks(&new_tasks, &new_reduction_tasks);
        if !new_reduction_tasks.is_empty() && invocation_ctx.tasks_completed(&task.compute_fn_name)
        {
            let reducer_task = new_reduction_tasks.pop().unwrap();
            let compute_node = compute_graph_version
                .nodes
                .get(&reducer_task.compute_fn_name)
                .unwrap();
            let new_task = compute_node.create_task(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
                reducer_task.input.clone(),
                None,
                &invocation_ctx.graph_version,
            )?;
            new_tasks.push(new_task.clone());
            invocation_ctx.create_tasks(&vec![new_task.clone()], &vec![]);
            invocation_ctx.complete_reducer_task(&reducer_task.compute_fn_name);
        }

        Ok(TaskCreationResult {
            tasks: new_tasks,
            new_reduction_tasks,
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(invocation_ctx),
        })
    }
}
