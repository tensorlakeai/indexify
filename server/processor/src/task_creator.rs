use std::{
    sync::{Arc, RwLock},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    ComputeGraphVersion,
    GraphInvocationCtx,
    GraphInvocationOutcome,
    InvokeComputeGraphEvent,
    ReduceTask,
    Task,
    TaskOutcome,
    TaskOutputsIngestedEvent,
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
            ChangeType::TaskOutputsIngested(ev) => {
                let result = self.handle_task_finished_inner(ev).await?;
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
    pub async fn handle_task_finished_inner(
        &self,
        task_finished_event: &TaskOutputsIngestedEvent,
    ) -> Result<TaskCreationResult> {
        let in_memory_state = self.in_memory_state.read().unwrap();
        let invocation_ctx = in_memory_state
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

        let task = in_memory_state.tasks.get(&Task::key_from(
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

        let compute_graph_version = in_memory_state
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
            *invocation_ctx.clone(),
            *task.clone(),
            *compute_graph_version.clone(),
            task_finished_event.node_output_key.clone(),
        )
        .await
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
        invocation_ctx,
        task,
        compute_graph_version,
        node_output_key
    ))]
    pub async fn handle_task_finished(
        &self,
        invocation_ctx: GraphInvocationCtx,
        task: Task,
        compute_graph_version: ComputeGraphVersion,
        node_output_key: String,
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
        let node_output = self
            .indexify_state
            .reader()
            .get_node_output_by_key(&node_output_key)?;
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

        // Check if the task has a router output and create new tasks for the router
        // edges.
        {
            let mut router_edges = vec![];
            router_edges.extend(node_output.edges.iter().map(|edge| edge.to_string()));
            if !router_edges.is_empty() {
                for edge in router_edges {
                    let compute_fn = compute_graph_version
                        .nodes
                        .get(&edge)
                        .ok_or(anyhow!("compute node not found: {:?}", edge))?;
                    for payload in node_output.payloads.iter() {
                        let new_task = compute_fn.create_task(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            payload.clone(),
                            None,
                            &invocation_ctx.graph_version,
                        )?;
                        new_tasks.push(new_task);
                    }
                }
                trace!(
                    task_keys = ?new_tasks.iter().map(|t| t.key()).collect::<Vec<String>>(),
                    "Creating a router edge task",
                );
                invocation_ctx.create_tasks(&new_tasks, &vec![]);

                return Ok(TaskCreationResult {
                    tasks: new_tasks,
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                    invocation_ctx: Some(invocation_ctx),
                });
            }
        }

        let Some(compute_node) = compute_graph_version.nodes.get(&task.compute_fn_name) else {
            error!(
                task_id = task.id.to_string(),
                invocation_id = task.invocation_id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                "compute node not found",
            );
            return Ok(TaskCreationResult::default());
        };
        if compute_node.reducer() {
            let reduction_task = self.in_memory_state.read().unwrap().next_reduction_task(
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
                println!(
                    "DIPTANU BEFORE REDUCER invocation_ctx: {:?}",
                    invocation_ctx
                );
                invocation_ctx.complete_reducer_task(&task.compute_fn_name);
                println!("DIPTANU AFTER REDUCER invocation_ctx: {:?}", invocation_ctx);
                return Ok(TaskCreationResult {
                    tasks: new_tasks,
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![reduction_task.key()],
                    invocation_ctx: Some(invocation_ctx),
                });
            }
        }

        // Find the edges of the function
        let Some(edges) = compute_graph_version.edges.get(&task.compute_fn_name) else {
            // If there are no edges, check if the invocation should be finished.
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
        for edge in edges.iter() {
            let edge_compute_node = compute_graph_version
                .nodes
                .get(edge)
                .ok_or(anyhow!("compute node not found: {:?}", edge))?;

            for output in &node_output.payloads {
                if edge_compute_node.reducer() {
                    // 1. Create a new reduction task with the output of the current task
                    // 2. If there are no more outstanding tasks of the current node, then create a
                    //    new task for the reducer
                    // and remove the reducer task for which we created the new task.

                    let reduction_task = ReduceTask {
                        namespace: task.namespace.clone(),
                        compute_graph_name: task.compute_graph_name.clone(),
                        invocation_id: task.invocation_id.clone(),
                        compute_fn_name: edge_compute_node.name().to_string(),
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
                    compute_fn_name = new_task.compute_fn_name,
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

        if edges.is_empty() && invocation_ctx.all_tasks_completed() {
            invocation_ctx.complete_invocation(true, GraphInvocationOutcome::Success);
        }

        Ok(TaskCreationResult {
            tasks: new_tasks,
            new_reduction_tasks,
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(invocation_ctx),
        })
    }
}
