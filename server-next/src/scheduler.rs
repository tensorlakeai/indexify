use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ChangeType, InvokeComputeGraphEvent, StateChangeId, Task, TaskFinishedEvent};
use state_store::{
    requests::{CreateTaskRequest, RequestType},
    IndexifyState,
};
use tokio::{self, sync::watch::Receiver};
use tracing::{error, info};

pub struct Scheduler {
    indexify_state: Arc<IndexifyState>,
}

impl Scheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    async fn handle_invoke_compute_graph(
        &self,
        event: InvokeComputeGraphEvent,
    ) -> Result<Vec<Task>> {
        let compute_graph = self
            .indexify_state
            .reader()
            .get_compute_graph(&event.namespace, &event.compute_graph)?;
        if compute_graph.is_none() {
            error!(
                "compute graph not found: {:?} {:?}",
                event.namespace, event.compute_graph
            );
            return Ok(vec![]);
        }
        let compute_graph = compute_graph.unwrap();
        // Crate a task for the compute graph
        let task = compute_graph.start_fn.create_task(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            &event.invocation_id,
        )?;
        Ok(vec![task])
    }

    async fn handle_task_finished(
        &self,
        task_finished_event: TaskFinishedEvent,
    ) -> Result<Vec<Task>> {
        let task = self
            .indexify_state
            .reader()
            .get_task(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
                &task_finished_event.compute_fn,
                &task_finished_event.task_id,
            )?
            .ok_or(anyhow!("task not found: {:?}", task_finished_event))?;

        let compute_graph = self
            .indexify_state
            .reader()
            .get_compute_graph(
                &task_finished_event.namespace,
                &task_finished_event.compute_graph,
            )?
            .ok_or(anyhow!(
                "compute graph not found: {:?} {:?}",
                task_finished_event.namespace,
                task_finished_event.compute_graph
            ))?;
        // Find the edges
        let edges = compute_graph.edges.get(&task_finished_event.compute_fn);
        if edges.is_none() {
            // Mark the graph to be completed
            return Ok(vec![]);
        }

        // Get all the outputs of the compute fn
        let outputs = self.indexify_state.reader().get_task_outputs(
            &task_finished_event.namespace,
            &task_finished_event.compute_graph,
            &task_finished_event.compute_fn,
        )?;

        let edges = edges.unwrap();
        let mut new_tasks = vec![];
        for edge in edges {
            for output in &outputs {
                let new_task = edge.create_task(
                    &task.namespace,
                    &task.compute_graph_name,
                    &output.id,
                    &task.invocation_id,
                )?;
                new_tasks.push(new_task);
            }
        }
        Ok(new_tasks)
    }

    pub async fn run_scheduler(&self) -> Result<()> {
        let state_changes = self
            .indexify_state
            .reader()
            .get_unprocessed_state_changes()?;
        for state_change in state_changes {
            let tasks: Vec<Task> = match state_change.change_type {
                ChangeType::InvokeComputeGraph(invoke_compute_graph_event) => {
                    self.handle_invoke_compute_graph(invoke_compute_graph_event)
                        .await?
                }
                ChangeType::TaskFinished(task_finished_event) => {
                    self.handle_task_finished(task_finished_event).await?
                }
                _ => {
                    vec![]
                }
            };
            self.indexify_state
                .write(RequestType::CreateTasks(CreateTaskRequest { tasks }))
                .await?;
        }
        Ok(())
    }

    pub async fn start(
        &self,
        mut shutdown_rx: Receiver<()>,
        mut state_watcher_rx: Receiver<StateChangeId>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = state_watcher_rx.changed() => {
                       let _state_change = *state_watcher_rx.borrow_and_update();
                       if let Err(err) = self.run_scheduler().await {
                              error!("error processing and distributing work: {:?}", err);
                       }
                },
                _ = shutdown_rx.changed() => {
                    info!("scheduler shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use data_model::test_objects::tests::{self, mock_data_object};
    use state_store::{
        requests::{CreateComputeGraphRequest, InvokeComputeGraphRequest},
        IndexifyState,
    };
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_invoke_compute_graph_event_creates_tasks() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = Arc::new(IndexifyState::new(temp_dir.path().join("state"))?);
        let cg_request = CreateComputeGraphRequest {
            namespace: "test".to_string(),
            compute_graph: tests::mock_graph_a(),
        };
        indexify_state
            .write(RequestType::CreateComputeGraph(cg_request))
            .await?;
        let scheduler = Scheduler::new(indexify_state.clone());
        let mock_data_object = mock_data_object();
        let request = InvokeComputeGraphRequest {
            namespace: "test".to_string(),
            compute_graph_name: "graph_A".to_string(),
            data_object: mock_data_object.clone(),
        };
        indexify_state
            .write(RequestType::InvokeComputeGraph(request))
            .await
            .unwrap();
        scheduler.run_scheduler().await?;
        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph("test", "graph_A", &mock_data_object.id)
            .unwrap();
        assert_eq!(tasks.len(), 1);
        Ok(())
    }
}
