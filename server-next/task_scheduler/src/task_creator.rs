use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ComputeGraph, InvokeComputeGraphEvent, Node, OutputPayload, Task, TaskOutcome};
use state_store::IndexifyState;
use tracing::{error, info};

use crate::TaskCreationResult;

pub async fn handle_invoke_compute_graph(
    indexify_state: Arc<IndexifyState>,
    event: InvokeComputeGraphEvent,
) -> Result<TaskCreationResult> {
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
    // Crate a task for the compute graph
    let task = compute_graph.start_fn.create_task(
        &event.namespace,
        &event.compute_graph,
        &event.invocation_id,
        &event.invocation_id,
        None,
    )?;
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

pub async fn handle_task_finished(
    indexify_state: Arc<IndexifyState>,
    task: Task,
    compute_graph: ComputeGraph,
) -> Result<TaskCreationResult> {
    let invocation_ctx = indexify_state.reader().invocation_ctx(
        &task.namespace,
        &task.compute_graph_name,
        &task.invocation_id,
    )?;

    if task.outcome == TaskOutcome::Failure {
        let mut invocation_finished = false;
        if invocation_ctx.outstanding_tasks == 0 {
            invocation_finished = true;
        }

        info!(
            "Task failed, graph invocation: {:?} {}",
            task.compute_graph_name, invocation_finished
        );

        return Ok(TaskCreationResult {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            invocation_id: task.invocation_id.clone(),
            tasks: vec![],
            invocation_finished,
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
        });
    }
    let mut new_tasks = vec![];
    let mut new_reduction_tasks = vec![];
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
            )?;
            new_tasks.push(new_task);
        }
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

    if let Some(compute_node) = compute_graph.nodes.get(&task.compute_fn_name) {
        if let Node::Compute(compute_fn) = compute_node {
            if compute_fn.reducer {
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
                    )?;

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
    let edges = edges.unwrap();
    for edge in edges {
        for output in &outputs {
            let compute_node = compute_graph
                .nodes
                .get(edge)
                .ok_or(anyhow!("compute node not found: {:?}", edge))?;
            let task_analytics_edge = indexify_state
                .reader()
                .task_analytics(&task.namespace, &task.compute_graph_name, &task.invocation_id, &edge)?;
            let outstanding_reduction_tasks = match task_analytics_edge {
                Some(task_analytics) => task_analytics.pending_tasks,
                None => {
                    error!("task analytics not found for edge : {:?}", edge);
                    0
                },
            };
            if compute_node.reducer() && (new_tasks.len() > 0 || outstanding_reduction_tasks > 0) {
                let new_task = compute_node.reducer_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                    &task.id.to_string(),
                    &output.key(&task.invocation_id),
                );
                new_reduction_tasks.push(new_task);
                continue;
            }
            let new_task = compute_node.create_task(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
                &output.key(&task.invocation_id),
                None,
            )?;
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
