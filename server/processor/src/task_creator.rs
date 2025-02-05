use std::{sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{
    ComputeGraphVersion,
    GraphInvocationCtx,
    GraphInvocationCtxBuilder,
    InvokeComputeGraphEvent,
    Node,
    OutputPayload,
    ReduceTask,
    Task,
    TaskAnalytics,
    TaskFinalizedEvent,
    TaskOutcome,
};
use state_store::{in_memory_state, IndexifyState};
use tracing::{error, info, trace};

#[derive(Debug)]
pub struct TaskCreationResult {
    pub namespace: String,
    pub compute_graph: String,
    pub tasks: Vec<Task>,
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
    pub invocation_id: String,
    pub invocation_ctx: Option<GraphInvocationCtx>,
}

impl TaskCreationResult {
    pub fn no_tasks(namespace: &str, compute_graph: &str, invocation_id: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            compute_graph: compute_graph.to_string(),
            invocation_id: invocation_id.to_string(),
            tasks: vec![],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_ctx: None,
        }
    }
}

pub struct TaskCreator {
    indexify_state: Arc<IndexifyState>,
}

impl TaskCreator {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }
}

impl TaskCreator {
    pub async fn handle_task_finished_inner(
        &self,
        task_finished_event: &TaskFinalizedEvent,
    ) -> Result<TaskCreationResult> {
        let in_memory_state = self.indexify_state.in_memory_state().await;
        let task = &in_memory_state
            .tasks
            .get(&Task::key_from(&task_finished_event.namespace, &task_finished_event.compute_graph, &task_finished_event.invocation_id, &task_finished_event.compute_fn, &task_finished_event.task_id.to_string()));
        if task.is_none() {
            error!(
                "task not found for task finished event: {}",
                task_finished_event.task_id
            );
            return Ok(TaskCreationResult::no_tasks(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.invocation_id,
            ));
        }
        let task = task.ok_or(anyhow!("task not found: {}", task_finished_event.task_id))?;

        let compute_graph_version = in_memory_state.compute_graph_versions.get(&format!(
            "{}|{}|{}",
            task.namespace, task.compute_graph_name, task.graph_version.0,
        ));

        if compute_graph_version.is_none() {
            error!(
                "compute graph version not found: {:?} {:?}",
                task.namespace, task.compute_graph_name
            );
            return Ok(TaskCreationResult::no_tasks(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            ));
        }
        let compute_graph_version = compute_graph_version.ok_or(anyhow!(
            "compute graph version not found: {:?} {:?}",
            task.namespace,
            task.compute_graph_name
        ))?;
        self.handle_task_finished(task, compute_graph_version, &in_memory_state)
            .await
    }

    pub async fn handle_invoke_compute_graph(
        &self,
        event: InvokeComputeGraphEvent,
        in_memory_state: &in_memory_state::InMemoryState,
    ) -> Result<TaskCreationResult> {
        let compute_graph = in_memory_state
            .compute_graphs
            .get(&format!("{}|{}", event.namespace, event.compute_graph));
        if compute_graph.is_none() {
            error!(
                "compute graph not found: {:?} {:?}",
                event.namespace, event.compute_graph
            );
            return Ok(TaskCreationResult::no_tasks(
                &event.namespace,
                &event.compute_graph,
                &event.invocation_id,
            ));
        }
        let compute_graph = compute_graph.unwrap();
        let compute_graph_version = compute_graph.into_version();
        // Create a task for the compute graph
        let task = compute_graph_version.start_fn.create_task(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            &event.invocation_id,
            None,
            &compute_graph_version.version,
        )?;
        info!(
            task_key = task.key(),
            "Creating a standard task to start compute graph"
        );
        let mut graph_ctx = GraphInvocationCtxBuilder::default()
            .graph_version(compute_graph.version.clone())
            .invocation_id(event.invocation_id.clone())
            .namespace(event.namespace.clone())
            .outstanding_tasks(1)
            .compute_graph_name(event.compute_graph.clone())
            .build(compute_graph.clone())?;
        graph_ctx.fn_task_analytics.insert(
            task.compute_fn_name.clone(),
            TaskAnalytics {
                pending_tasks: 1,
                successful_tasks: 0,
                failed_tasks: 0,
            },
        );
        info!("hereree");
        Ok(TaskCreationResult {
            namespace: event.namespace.clone(),
            compute_graph: event.compute_graph.clone(),
            invocation_id: event.invocation_id.clone(),
            tasks: vec![task],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(graph_ctx),
        })
    }

    pub async fn handle_task_finished(
        &self,
        task: &Task,
        compute_graph_version: &ComputeGraphVersion,
        in_memory_state: &in_memory_state::InMemoryState,
    ) -> Result<TaskCreationResult> {
        let invocation_ctx = in_memory_state.invocation_ctx.get(&format!(
            "{}|{}|{}",
            task.namespace, task.compute_graph_name, task.invocation_id
        ));
        if invocation_ctx.is_none() {
            error!("no invocation ctx, stopping scheduling of child tasks");
            return Ok(TaskCreationResult::no_tasks(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            ));
        }
        let mut invocation_ctx = invocation_ctx
            .ok_or(anyhow!(
                "invocation context not found for invocation_id {}",
                task.invocation_id
            ))?
            .clone();

        if task.outcome == TaskOutcome::Failure {
            trace!("task failed, stopping scheduling of child tasks");
            return Ok(TaskCreationResult::no_tasks(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            ));
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
                    invocation_ctx
                        .fn_task_analytics
                        .entry(compute_fn.name().to_string())
                        .or_default()
                        .pending();
                    new_tasks.push(new_task);
                }
                trace!(
                    task_keys = ?new_tasks.iter().map(|t| t.key()).collect::<Vec<String>>(),
                    "Creating a router edge task",
                );
                invocation_ctx.outstanding_tasks += new_tasks.len() as u64;
                return Ok(TaskCreationResult {
                    namespace: task.namespace.clone(),
                    compute_graph: task.compute_graph_name.clone(),
                    invocation_id: task.invocation_id.clone(),
                    tasks: new_tasks,
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                    invocation_ctx: Some(invocation_ctx.clone()),
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
                            return Ok(TaskCreationResult::no_tasks(
                                &task.namespace,
                                &task.compute_graph_name,
                                &task.invocation_id,
                            ));
                        }
                    }
                    let reduction_task = in_memory_state.next_queued_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &task.compute_fn_name,
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
                        invocation_ctx.outstanding_tasks += 1;
                        invocation_ctx
                            .fn_task_analytics
                            .entry(compute_node.name().to_string())
                            .or_default()
                            .pending();
                        return Ok(TaskCreationResult {
                            namespace: task.namespace.clone(),
                            compute_graph: task.compute_graph_name.clone(),
                            invocation_id: task.invocation_id.clone(),
                            tasks: vec![new_task],
                            new_reduction_tasks: vec![],
                            processed_reduction_tasks: vec![reduction_task.key()],
                            invocation_ctx: Some(invocation_ctx.clone()),
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
                        return Ok(TaskCreationResult::no_tasks(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                        ));
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
            return Ok(TaskCreationResult::no_tasks(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            ));
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
                            return Ok(TaskCreationResult::no_tasks(
                                &task.namespace,
                                &task.compute_graph_name,
                                &task.invocation_id,
                            ));
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
                        return Ok(TaskCreationResult::no_tasks(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                        ));
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
                        let (prev_reducer_tasks, _) = self.indexify_state.reader().get_task_by_fn(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            compute_node.name(),
                            None,
                            Some(1),
                        )?;

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

        trace!("tasks: {:?}", new_tasks.len());
        invocation_ctx.outstanding_tasks += new_tasks.len() as u64;
        for task in &new_tasks {
            invocation_ctx
                .fn_task_analytics
                .entry(task.compute_fn_name.clone())
                .or_default()
                .pending();
        }
        if new_tasks.is_empty() && invocation_ctx.outstanding_tasks == 0 {
            invocation_ctx.completed = true;
        }
        Ok(TaskCreationResult {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            invocation_id: task.invocation_id.clone(),
            tasks: new_tasks,
            new_reduction_tasks,
            processed_reduction_tasks: vec![],
            invocation_ctx: Some(invocation_ctx.clone()),
        })
    }
}
