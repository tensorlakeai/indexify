use std::{ops::Deref, sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    ComputeGraphVersion,
    GraphInvocationCtx,
    GraphInvocationOutcome,
    InvokeComputeGraphEvent,
    Node,
    OutputPayload,
    ReduceTask,
    Task,
    TaskOutcome,
    TaskOutputsIngestedEvent,
};
use state_store::{
    in_memory_state::{InMemoryState, UnallocatedTaskId},
    requests::{ReductionTasks, SchedulerUpdateRequest},
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
    pub in_memory_state: Box<InMemoryState>,
}

impl TaskCreator {
    pub fn new(indexify_state: Arc<IndexifyState>, in_memory_state: Box<InMemoryState>) -> Self {
        Self {
            indexify_state,
            in_memory_state,
        }
    }
}

impl TaskCreator {
    #[tracing::instrument(skip(self))]
    pub async fn invoke(&mut self, change: &ChangeType) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::TaskOutputsIngested(ev) => {
                let result = self.handle_task_finished_inner(ev).await?;
                result.tasks.iter().for_each(|t| {
                    self.in_memory_state
                        .tasks
                        .insert(t.key(), Box::new(t.clone()));
                    self.in_memory_state
                        .unallocated_tasks
                        .insert(UnallocatedTaskId::new(&t));
                });
                if let Some(ctx) = result.invocation_ctx.clone() {
                    self.in_memory_state
                        .invocation_ctx
                        .insert(ctx.key(), Box::new(ctx));
                }
                return Ok(SchedulerUpdateRequest {
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
                });
            }
            ChangeType::InvokeComputeGraph(ev) => {
                let result = self.handle_invoke_compute_graph(ev.clone()).await?;
                result.tasks.iter().for_each(|t| {
                    self.in_memory_state
                        .tasks
                        .insert(t.key(), Box::new(t.clone()));
                    self.in_memory_state
                        .unallocated_tasks
                        .insert(UnallocatedTaskId::new(&t));
                });
                if let Some(ctx) = result.invocation_ctx.clone() {
                    self.in_memory_state
                        .invocation_ctx
                        .insert(ctx.key(), Box::new(ctx));
                }
                return Ok(SchedulerUpdateRequest {
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
                });
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
    pub async fn handle_task_finished_inner(
        &self,
        task_finished_event: &TaskOutputsIngestedEvent,
    ) -> Result<TaskCreationResult> {
        let invocation_ctx =
            self.in_memory_state
                .invocation_ctx
                .get(&GraphInvocationCtx::key_from(
                    &task_finished_event.namespace,
                    &task_finished_event.compute_graph,
                    &task_finished_event.invocation_id,
                ));

        let Some(invocation_ctx) = invocation_ctx else {
            trace!("no invocation ctx, stopping scheduling of child tasks");
            return Ok(TaskCreationResult::default());
        };

        if invocation_ctx.completed {
            trace!("invocation already completed, stopping scheduling of child tasks");
            return Ok(TaskCreationResult::default());
        }

        let task = self.in_memory_state.tasks.get(&Task::key_from(
            &task_finished_event.namespace,
            &task_finished_event.compute_graph,
            &task_finished_event.invocation_id,
            &task_finished_event.compute_fn,
            &task_finished_event.task_id.to_string(),
        ));
        let Some(task) = task else {
            error!(
                task_id = task_finished_event.task_id.to_string(),
                invocation_id = task_finished_event.invocation_id.to_string(),
                namespace = task_finished_event.namespace,
                compute_graph = task_finished_event.compute_graph,
                compute_fn = task_finished_event.compute_fn,
                "task not found for task finished event",
            );
            return Ok(TaskCreationResult::default());
        };

        let compute_graph_version = self
            .in_memory_state
            .compute_graph_versions
            .get(&task.key_compute_graph_version());
        if compute_graph_version.is_none() {
            error!(
                task_id = task.id.to_string(),
                invocation_id = task.invocation_id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                compute_graph_version = task.graph_version.0,
                "compute graph version not found",
            );
            return Ok(TaskCreationResult::default());
        }
        let compute_graph_version = compute_graph_version.ok_or(anyhow!(
            "compute graph version not found: {:?} {:?} {:?}",
            task.namespace,
            task.compute_graph_name,
            task.graph_version.0
        ))?;
        self.handle_task_finished(
            invocation_ctx.deref().clone(),
            task.clone().deref().clone(),
            *compute_graph_version.clone(),
        )
        .await
    }

    #[tracing::instrument(skip(self, event))]
    pub async fn handle_invoke_compute_graph(
        &self,
        event: InvokeComputeGraphEvent,
    ) -> Result<TaskCreationResult> {
        let invocation_ctx =
            self.in_memory_state
                .invocation_ctx
                .get(&GraphInvocationCtx::key_from(
                    &event.namespace,
                    &event.compute_graph,
                    &event.invocation_id,
                ));
        if invocation_ctx.is_none() {
            trace!("no invocation ctx, stopping invocation of compute graph");
            return Ok(TaskCreationResult::default());
        }
        let mut invocation_ctx = invocation_ctx
            .ok_or(anyhow!(
                "invocation context not found for invocation_id {}",
                event.invocation_id
            ))?
            .deref()
            .clone();

        let compute_graph_version =
            self.in_memory_state
                .compute_graph_versions
                .get(&ComputeGraphVersion::key_from(
                    &event.namespace,
                    &event.compute_graph,
                    &invocation_ctx.graph_version,
                ));
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

        // Create a task for the compute graph
        let task = compute_graph_version.start_fn.create_task(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            &event.invocation_id,
            None,
            &compute_graph_version.version,
        )?;
        invocation_ctx.create_tasks(&vec![task.clone()]);

        trace!(
            task_key = task.key(),
            "Creating a standard task to start compute graph"
        );
        Ok(TaskCreationResult {
            tasks: vec![task],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(invocation_ctx),
        })
    }

    #[tracing::instrument(skip(self, invocation_ctx, task))]
    pub async fn handle_task_finished(
        &self,
        invocation_ctx: GraphInvocationCtx,
        task: Task,
        compute_graph_version: ComputeGraphVersion,
    ) -> Result<TaskCreationResult> {
        trace!("invocation context: {:?}", invocation_ctx);
        let mut invocation_ctx = invocation_ctx.clone();
        invocation_ctx.update_analytics(&task);

        if task.outcome == TaskOutcome::Failure {
            trace!("task failed, stopping scheduling of child tasks");
            invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Failure);
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
                ..Default::default()
            });
        }
        let outputs = self
            .indexify_state
            .reader()
            .get_task_outputs(&task.namespace, &task.id.to_string())?;
        let mut new_tasks = vec![];

        // Check if the task has a router output and create new tasks for the router
        // edges.
        {
            let mut router_edges = vec![];
            for output in &outputs {
                if let OutputPayload::Router(router_output) = &output.payload {
                    for edge in &router_output.edges {
                        router_edges.push(edge);
                    }
                }
            }
            if !router_edges.is_empty() {
                for edge in router_edges {
                    let compute_fn = compute_graph_version
                        .nodes
                        .get(edge)
                        .ok_or(anyhow!("compute node not found: {:?}", edge))?;
                    let new_task = compute_fn.create_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &task.input_node_output_key,
                        None,
                        &invocation_ctx.graph_version,
                    )?;
                    new_tasks.push(new_task);
                }
                trace!(
                    task_keys = ?new_tasks.iter().map(|t| t.key()).collect::<Vec<String>>(),
                    "Creating a router edge task",
                );
                invocation_ctx.create_tasks(&new_tasks);

                return Ok(TaskCreationResult {
                    tasks: new_tasks,
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                    invocation_ctx: Some(invocation_ctx),
                });
            }
        }

        // When a reducer task finishes, check for more queued reduction tasks to create
        // to ensure sequential execution.
        if let Some(compute_node) = compute_graph_version.nodes.get(&task.compute_fn_name) {
            if let Node::Compute(compute_fn) = compute_node {
                if compute_fn.reducer {
                    if let Some(task_analytics) =
                        invocation_ctx.get_task_analytics(&task.compute_fn_name)
                    {
                        // Do nothing if there is a pending reducer task for this compute node.
                        //
                        // This protects against the case where a reducer task finished before the
                        // next output and another one was created without
                        // queuing.
                        if task_analytics.pending_tasks > 0 {
                            trace!(
                                compute_fn_name = compute_fn.name,
                                "Waiting for pending reducer tasks to finish before unqueuing"
                            );
                            return Ok(TaskCreationResult {
                                invocation_ctx: Some(invocation_ctx),
                                ..Default::default()
                            });
                        }
                    }
                    let reduction_task = self.in_memory_state.next_reduction_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &compute_fn.name,
                    );
                    if let Some(reduction_task) = reduction_task {
                        // Create a new task for the queued reduction_task
                        let output = outputs.first().unwrap();
                        let new_task = compute_node.create_task(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            &reduction_task.task_output_key,
                            Some(output.id.clone()),
                            &invocation_ctx.graph_version,
                        )?;
                        trace!(
                            task_keys = ?new_tasks.iter().map(|t| t.key()).collect::<Vec<String>>(),
                            compute_fn_name = new_task.compute_fn_name,
                            "Creating a reduction task from queue",
                        );
                        invocation_ctx.create_tasks(&vec![new_task.clone()]);
                        return Ok(TaskCreationResult {
                            tasks: vec![new_task],
                            new_reduction_tasks: vec![],
                            processed_reduction_tasks: vec![reduction_task.key()],
                            invocation_ctx: Some(invocation_ctx),
                        });
                    }
                    trace!(
                        computed_fn_name = compute_fn.name,
                        "No queued reduction tasks to create",
                    );

                    // Prevent proceeding to edges too early if there are parent tasks that are
                    // still pending or that have failed.
                    if compute_graph_version
                        .get_compute_parent_nodes(compute_node.name())
                        .iter()
                        .any(|parent_node| {
                            if let Some(parent_task_analytics) =
                                invocation_ctx.get_task_analytics(parent_node)
                            {
                                parent_task_analytics.pending_tasks > 0 ||
                                    parent_task_analytics.failed_tasks > 0
                            } else {
                                false
                            }
                        })
                    {
                        trace!(
                            compute_fn_name = compute_fn.name,
                            "Waiting for parent tasks to finish before starting a new reducer task"
                        );
                        return Ok(TaskCreationResult {
                            invocation_ctx: Some(invocation_ctx),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        // Find the edges of the function
        let edges = compute_graph_version.edges.get(&task.compute_fn_name);

        // If there are no edges, check if the invocation should be finished.
        if edges.is_none() {
            trace!(
                "No more edges to schedule tasks for, waiting for outstanding tasks to finalize"
            );
            invocation_ctx.complete_invocation(false, GraphInvocationOutcome::Success);
            return Ok(TaskCreationResult {
                invocation_ctx: Some(invocation_ctx),
                ..Default::default()
            });
        }

        // Create new tasks for the edges of the node of the current task.
        let mut new_reduction_tasks = vec![];
        let edges = edges.unwrap();
        for edge in edges {
            let compute_node = compute_graph_version
                .nodes
                .get(edge)
                .ok_or(anyhow!("compute node not found: {:?}", edge))?;

            for output in &outputs {
                if compute_node.reducer() {
                    if let Some(task_analytics) =
                        invocation_ctx.get_task_analytics(&task.compute_fn_name)
                    {
                        // Do not schedule more tasks if the parent node of the reducer has failing
                        // tasks.
                        //
                        // This protects against continuing the invocation with partial reduced
                        // results which would lead to incorrect graph
                        // outputs.
                        if task_analytics.failed_tasks > 0 {
                            trace!(
                                compute_fn_name = task.compute_fn_name,
                                "Reducer parent node has failing tasks, not scheduling more tasks"
                            );
                            return Ok(TaskCreationResult::default());
                        }
                    }

                    let task_analytics_edge = invocation_ctx.get_task_analytics(&edge);
                    trace!(
                        compute_fn_name = compute_node.name(),
                        "task_analytics_edge: {:?}",
                        task_analytics_edge,
                    );
                    let (
                        outstanding_tasks_for_node,
                        successful_tasks_for_node,
                        failed_tasks_for_node,
                    ) = match task_analytics_edge {
                        Some(task_analytics) => (
                            task_analytics.pending_tasks,
                            task_analytics.successful_tasks,
                            task_analytics.failed_tasks,
                        ),
                        None => {
                            error!("task analytics not found for edge : {:?}", edge);
                            (0, 0, 0)
                        }
                    };

                    // If a previous reducer task failed, we need to stop queuing new tasks and
                    // finalize the invocation if we are finalizing the last task.
                    if failed_tasks_for_node > 0 {
                        info!(
                            compute_fn_name = compute_node.name(),
                            "Found previously failed reducer task, stopping reducers",
                        );
                        invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Failure);
                        return Ok(TaskCreationResult::default());
                    }

                    // In order to ensure sequential execution of reducer tasks, we queue a
                    // reduction task for this output if there are still outstanding
                    // tasks for the node or if we are going to create a new task for the node.
                    if new_tasks.len() > 0 || outstanding_tasks_for_node > 0 {
                        let new_task = compute_node.reducer_task(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            &task.id.to_string(),
                            &output.key(&task.invocation_id),
                        );
                        trace!(
                            compute_fn_name = compute_node.name(),
                            "Creating a queued reduction task",
                        );
                        new_reduction_tasks.push(new_task);
                        continue;
                    }

                    // If a previous reducer task finished previously, we need to create
                    // a new reducer task here without queuing it.
                    //
                    // To do so, we need to find the previous reducer task to reuse its output.
                    if successful_tasks_for_node > 0 {
                        let prev_reducer_tasks = self.in_memory_state.get_tasks_by_fn(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            &compute_node.name(),
                        );
                        if prev_reducer_tasks.is_empty() {
                            return Err(anyhow!(
                                "Previous reducer task not found, should never happen: {:?}",
                                compute_node.name()
                            ));
                        }

                        let prev_reducer_task = prev_reducer_tasks.first().unwrap();

                        let prev_reducer_outputs = self.indexify_state.reader().get_task_outputs(
                            &prev_reducer_task.namespace,
                            &prev_reducer_task.id.to_string(),
                        )?;

                        if prev_reducer_outputs.is_empty() {
                            return Err(anyhow!(
                            "No outputs found for previous reducer task, should never happen: {:?}",
                            prev_reducer_task.key(),
                        ));
                        }
                        let prev_reducer_output = prev_reducer_outputs.first().unwrap();

                        let output = outputs.first().unwrap();
                        let new_task = compute_node.create_task(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            &output.key(&task.invocation_id),
                            Some(prev_reducer_output.id.clone()),
                            &invocation_ctx.graph_version,
                        )?;
                        trace!(
                            task_key = new_task.key(),
                            compute_fn_name = new_task.compute_fn_name,
                            "Creating a reduction task",
                        );
                        new_tasks.push(new_task);

                        continue;
                    }
                }

                let new_task = compute_node.create_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                    &output.key(&task.invocation_id),
                    None,
                    &invocation_ctx.graph_version,
                )?;
                trace!(
                    task_key = new_task.key(),
                    compute_fn_name = new_task.compute_fn_name,
                    "Creating a standard task",
                );
                new_tasks.push(new_task);
            }
        }

        invocation_ctx.create_tasks(&new_tasks);
        Ok(TaskCreationResult {
            tasks: new_tasks,
            new_reduction_tasks,
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(invocation_ctx),
        })
    }
}
