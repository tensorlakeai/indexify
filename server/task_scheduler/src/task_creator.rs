use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{
    ComputeGraph,
    GraphInvocationCtx,
    InvokeComputeGraphEvent,
    Node,
    OutputPayload,
    Task,
    TaskOutcome,
};
use state_store::{state_machine::IndexifyObjectsColumns, IndexifyState};
use tracing::{error, info, instrument, trace, Level};

use crate::TaskCreationResult;

#[instrument(ret(level = Level::DEBUG), skip(indexify_state, event), fields(namespace = event.namespace, compute_graph = event.compute_graph, invocation_id = event.invocation_id))]
pub async fn handle_invoke_compute_graph(
    indexify_state: Arc<IndexifyState>,
    event: InvokeComputeGraphEvent,
) -> Result<TaskCreationResult> {
    trace!(
        "Handling invoke compute graph event: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}",
        event.namespace, event.compute_graph, event.invocation_id
    );

    let compute_graph = indexify_state
        .reader()
        .get_compute_graph(&event.namespace, &event.compute_graph)?;
    if compute_graph.is_none() {
        error!(
            "compute graph not found: {:?} {:?}",
            event.namespace, event.compute_graph
        );
        return Ok(TaskCreationResult {
            tasks: vec![],
            namespace: event.namespace.clone(),
            invocation_id: event.invocation_id.clone(),
            compute_graph: event.compute_graph.clone(),
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_finished: false,
        });
    }
    let compute_graph = compute_graph.unwrap();
    // Create a task for the compute graph
    let task = compute_graph.start_fn.create_task(
        &event.namespace,
        &event.compute_graph,
        &event.invocation_id,
        &event.invocation_id,
        None,
        compute_graph.version,
    )?;
    trace!(
        "Created task for compute graph: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, task_id = {:?}",
        event.namespace, event.compute_graph, event.invocation_id, task.id
    );
    Ok(TaskCreationResult {
        namespace: event.namespace.clone(),
        compute_graph: event.compute_graph.clone(),
        invocation_id: event.invocation_id.clone(),
        tasks: vec![task],
        new_reduction_tasks: vec![],
        processed_reduction_tasks: vec![],
        invocation_finished: false,
    })
}

#[instrument(skip(indexify_state, task, compute_graph), fields(namespace = task.namespace, compute_graph = task.compute_graph_name, invocation_id = task.invocation_id, compute_fn_name = task.compute_fn_name, task_id = task.id.to_string()))]
pub async fn handle_task_finished(
    indexify_state: Arc<IndexifyState>,
    task: Task,
    compute_graph: ComputeGraph,
) -> Result<TaskCreationResult> {
    trace!(
        "Handling task finished: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, task_id = {:?}",
        task.namespace, task.compute_graph_name, task.invocation_id, task.id
    );

    let txn = indexify_state.db.transaction();

    let key = GraphInvocationCtx::key_from(
        task.namespace.as_str(),
        task.compute_graph_name.as_str(),
        task.invocation_id.as_str(),
    );
    let _value = txn.get_for_update_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
        &key,
        true,
    )?;

    let _value = txn.get_for_update_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&indexify_state.db),
        &task.key(),
        true,
    )?;
    let invocation_ctx = indexify_state
        .reader()
        .invocation_ctx(
            &task.namespace,
            &task.compute_graph_name,
            &task.invocation_id,
        )
        .map_err(|e| {
            anyhow!(
                "error getting invocation context for invocation {}: {:?}",
                task.invocation_id,
                e
            )
        })?;
    if invocation_ctx.is_none() {
        return Ok(TaskCreationResult::no_tasks(
            &task.namespace,
            &task.compute_graph_name,
            &task.invocation_id,
        ));
    }
    let invocation_ctx = invocation_ctx.ok_or(anyhow!(
        "invocation context not found for invocation_id {}",
        task.invocation_id
    ))?;

    trace!("GraphInvocationCtx: {:?}", invocation_ctx);

    if task.outcome == TaskOutcome::Failure {
        let mut invocation_finished = false;
        if invocation_ctx.outstanding_tasks == 0 {
            invocation_finished = true;
        }
        info!(
            "Task failed, graph invocation: {:?} {}",
            task.compute_graph_name, invocation_finished
        );
        return Ok(TaskCreationResult::no_tasks(
            &task.namespace,
            &task.compute_graph_name,
            &task.invocation_id,
        ));
    }
    let mut new_tasks = vec![];
    let outputs = indexify_state
        .reader()
        .get_task_outputs(&task.namespace, &task.id.to_string())?;
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
            let compute_fn = compute_graph
                .nodes
                .get(edge)
                .ok_or(anyhow!("compute node not found: {:?}", edge))?;
            let new_task = compute_fn.create_task(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
                &task.input_node_output_key,
                None,
                invocation_ctx.graph_version,
            )?;
            new_tasks.push(new_task);
        }
        trace!(
            "Created new tasks for router edges: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, task_ids = {:?}",
            task.namespace, task.compute_graph_name, task.invocation_id, new_tasks.iter().map(|t| t.id.clone()).collect::<Vec<_>>()
        );
        return Ok(TaskCreationResult {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            invocation_id: task.invocation_id.clone(),
            tasks: new_tasks,
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_finished: false,
        });
    }

    // Check for pending reduction tasks to create
    if let Some(compute_node) = compute_graph.nodes.get(&task.compute_fn_name) {
        if let Node::Compute(compute_fn) = compute_node {
            if compute_fn.reducer {
                if let Some(task_analytics) =
                    invocation_ctx.get_task_analytics(&task.compute_fn_name)
                {
                    if task_analytics.pending_tasks > 0 {
                        trace!(
                            "Waiting for pending reducer tasks to finish before queuing new ones"
                        );
                        return Ok(TaskCreationResult {
                            namespace: task.namespace.clone(),
                            compute_graph: task.compute_graph_name.clone(),
                            invocation_id: task.invocation_id.clone(),
                            tasks: vec![],
                            new_reduction_tasks: vec![],
                            processed_reduction_tasks: vec![],
                            invocation_finished: false,
                        });
                    }
                }
                let reduction_task = indexify_state
                    .reader()
                    .next_reduction_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &compute_fn.name,
                    )
                    .map_err(|e| anyhow!("error getting next reduction task: {:?}", e))?;
                if let Some(reduction_task) = reduction_task {
                    // Create a new task for the queued reduction_task
                    let output = outputs.first().unwrap();
                    let new_task = compute_node.create_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &reduction_task.task_output_key,
                        Some(output.id.clone()),
                        invocation_ctx.graph_version,
                    )?;
                    trace!(
                        "Created new task for reduction task: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, task_id = {:?}, compute_fn_name = {:?}",
                        task.namespace, task.compute_graph_name, task.invocation_id, new_task.id, new_task.compute_fn_name
                    );
                    return Ok(TaskCreationResult {
                        namespace: task.namespace.clone(),
                        compute_graph: task.compute_graph_name.clone(),
                        invocation_id: task.invocation_id.clone(),
                        tasks: vec![new_task],
                        new_reduction_tasks: vec![],
                        processed_reduction_tasks: vec![reduction_task.key()],
                        invocation_finished: false,
                    });
                }
                trace!(
                        "No reduction tasks to create: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, compute_fn_name = {:?}",
                        task.namespace, task.compute_graph_name, task.invocation_id, task.compute_fn_name
                    );

                // Prevent early finalization of the invocation if a reduction task finished
                // before other parent output have been generated.
                if compute_graph
                    .get_compute_parent_nodes(compute_node.name())
                    .iter()
                    .any(|parent_node| {
                        if let Some(parent_task_analytics) =
                            invocation_ctx.get_task_analytics(parent_node)
                        {
                            parent_task_analytics.pending_tasks > 0
                        } else {
                            false
                        }
                    })
                {
                    trace!(
                        "Waiting for parent tasks to finish before start child tasks of reducer"
                    );
                    return Ok(TaskCreationResult {
                        namespace: task.namespace.clone(),
                        compute_graph: task.compute_graph_name.clone(),
                        invocation_id: task.invocation_id.clone(),
                        tasks: vec![],
                        new_reduction_tasks: vec![],
                        processed_reduction_tasks: vec![],
                        invocation_finished: false,
                    });
                }
            }
        }
    }

    // Find the edges of the function
    let edges = compute_graph.edges.get(&task.compute_fn_name);
    if edges.is_none() {
        let invocation_finished = if invocation_ctx.outstanding_tasks == 0 {
            true
        } else {
            false
        };
        info!(
            "compute graph {} invocation finished: {:?}",
            task.compute_graph_name, invocation_finished
        );
        return Ok(TaskCreationResult {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            invocation_id: task.invocation_id.clone(),
            tasks: vec![],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_finished,
        });
    }
    let mut new_reduction_tasks = vec![];
    let edges = edges.unwrap();
    for edge in edges {
        for output in &outputs {
            let compute_node = compute_graph
                .nodes
                .get(edge)
                .ok_or(anyhow!("compute node not found: {:?}", edge))?;

            let task_analytics_edge = invocation_ctx.get_task_analytics(&edge);
            trace!(
                compute_fn_name = compute_node.name(),
                "task_analytics_edge: {:?}",
                task_analytics_edge,
            );
            let (outstanding_tasks_for_node, successfull_tasks_for_node) = match task_analytics_edge
            {
                Some(task_analytics) => (
                    task_analytics.pending_tasks,
                    task_analytics.successful_tasks,
                ),
                None => {
                    error!("task analytics not found for edge : {:?}", edge);
                    (0, 0)
                }
            };
            // hypothesis: if a previous reducer task finished previously, we need to create
            // a new reducer task here BUT if we create a new reduction task and
            // not a task, it will not get scheduled.
            if compute_node.reducer() {
                if new_tasks.len() > 0 || outstanding_tasks_for_node > 0 {
                    let new_task = compute_node.reducer_task(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        &task.id.to_string(),
                        &output.key(&task.invocation_id),
                    );
                    trace!(
                        "Created new reduction task: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, compute_fn_name = {:?}, new_tasks_len = {:?}, outstanding_tasks_for_node = {:?}, output_len = {:?}",
                        new_task.namespace, new_task.compute_graph_name, new_task.invocation_id, new_task.compute_fn_name, new_tasks.len(), outstanding_tasks_for_node, outputs.len()
                    );
                    new_reduction_tasks.push(new_task);
                    continue;
                }
                if successfull_tasks_for_node > 0 {
                    let (prev_reducer_tasks, _) = indexify_state.reader().get_task_by_fn(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                        compute_node.name(),
                        None,
                        Some(1),
                    )?;

                    if !prev_reducer_tasks.is_empty() {
                        let prev_reducer_task = prev_reducer_tasks.first().unwrap();
                        let prev_reducer_outputs = indexify_state.reader().get_task_outputs(
                            &prev_reducer_task.namespace,
                            &prev_reducer_task.id.to_string(),
                        )?;

                        if prev_reducer_outputs.is_empty() {
                            error!(
                                "No outputs found for previous reducer task: {:?}",
                                prev_reducer_task.id
                            );
                            return Err(anyhow!(
                                "No outputs found for previous reducer task, should never happen: {:?}",
                                prev_reducer_task.key(),
                            ));
                        }
                        let prev_reducer_output = prev_reducer_outputs.first().unwrap();
                        // A reducer task has already finished, so we need to create a new task

                        // we cannot start a normal task
                        // TODO: Handle the case where a failure happened in a reducer task

                        // Create a new task for the queued reduction_task
                        let output = outputs.first().unwrap();
                        let new_task = compute_node.create_task(
                            &task.namespace,
                            &task.compute_graph_name,
                            &task.invocation_id,
                            &output.key(&task.invocation_id),
                            Some(prev_reducer_output.id.clone()),
                            invocation_ctx.graph_version,
                        )?;
                        trace!(
                            "Created new task for reduction task (resume reduce): namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, task_id = {:?}, compute_fn_name = {:?}",
                            task.namespace, task.compute_graph_name, task.invocation_id, new_task.id, new_task.compute_fn_name
                        );
                        new_tasks.push(new_task);
                    } else {
                        // TODO: consider returning an error.
                        error!(
                            "Previous reducer task not found for compute node: {:?}",
                            compute_node.name()
                        );
                    }
                    continue;
                }
            }
            let new_task = compute_node.create_task(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
                &output.key(&task.invocation_id),
                None,
                invocation_ctx.graph_version,
            )?;
            trace!(
            "Created new task for output: namespace = {:?}, compute_graph = {:?}, invocation_id = {:?}, compute_fn_name = {:?}, new_tasks_len = {:?}, outstanding_tasks_for_node = {:?}, output_len = {:?}",
                     new_task.namespace, new_task.compute_graph_name, new_task.invocation_id, new_task.compute_fn_name, new_tasks.len(), outstanding_tasks_for_node, outputs.len()
            );
            new_tasks.push(new_task);
        }
    }
    Ok(TaskCreationResult {
        namespace: task.namespace.clone(),
        compute_graph: task.compute_graph_name.clone(),
        invocation_id: task.invocation_id.clone(),
        tasks: new_tasks,
        new_reduction_tasks,
        processed_reduction_tasks: vec![],
        invocation_finished: false,
    })
}
