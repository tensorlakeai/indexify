use std::sync::Arc;

use anyhow::{Ok, Result};
use state_store::{
    requests::{
        RemoveSystemTaskRequest,
        ReplayInvocationsRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        UpdateSystemTaskRequest,
    },
    IndexifyState,
};
use tokio::{self, sync::watch::Receiver};
use tracing::{debug, error, info, info_span};

const MAX_PENDING_TASKS: usize = 10;

pub struct SystemTasksExecutor {
    state: Arc<IndexifyState>,
    rx: tokio::sync::watch::Receiver<()>,
    shutdown_rx: Receiver<()>,
}

impl SystemTasksExecutor {
    pub fn new(state: Arc<IndexifyState>, shutdown_rx: Receiver<()>) -> Self {
        let rx = state.get_system_tasks_watcher();
        Self {
            state,
            rx,
            shutdown_rx,
        }
    }

    pub async fn start(&self) {
        let mut rx = self.rx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            rx.borrow_and_update();
            if let Err(err) = self.run().await {
                error!("error processing system tasks work: {:?}", err);
            }
            tokio::select! {
                _ = rx.changed() => {},
                _ = shutdown_rx.changed() => {
                    info!("system tasks executor shutting down");
                    break;
                }
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("running system tasks executor");

        // TODO: support concurrent running system tasks
        let (tasks, _) = self.state.reader().get_system_tasks(Some(1))?;

        if let Some(task) = tasks.first() {
            let task_span = info_span!("system_task", task = task.key(), "type" = "replay");
            let _span_guard = task_span.enter();

            // Check if first current system task can be completed.
            if task.waiting_for_running_invocations {
                self.handle_completion(&task.namespace, &task.compute_graph_name)
                    .await?;
                return Ok(());
            }

            let pending_tasks = self.state.reader().get_pending_system_tasks()?;
            if pending_tasks >= MAX_PENDING_TASKS {
                debug!(pending_tasks = pending_tasks, "max pending tasks reached");
                return Ok(());
            }

            let all_queued = self.queue_invocations(task, pending_tasks).await?;
            // handle completion right away if all invocations are completed
            if all_queued {
                self.handle_completion(&task.namespace, &task.compute_graph_name)
                    .await?
            }
        } else {
            debug!("no system tasks to process");
        }

        Ok(())
    }

    async fn queue_invocations(
        &self,
        task: &data_model::SystemTask,
        pending_tasks: usize,
    ) -> Result<bool> {
        let (invocations, restart_key) = self.state.reader().list_invocations(
            &task.namespace,
            &task.compute_graph_name,
            task.restart_key.as_deref(),
            Some(MAX_PENDING_TASKS - pending_tasks),
        )?;

        info!(queuing = invocations.len(), "queueing invocations");

        let replay_req = ReplayInvocationsRequest {
                    namespace: task.namespace.clone(),
                    compute_graph_name: task.compute_graph_name.clone(),
                    graph_version: task.graph_version.clone(),
                    invocation_ids: invocations.iter().map(|i| i.id.clone()).collect(),
                    restart_key: restart_key.clone(),
                };
        self.state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::ReplayInvocations(replay_req),
                processed_state_changes: vec![],
            })
            .await?;

        Ok(restart_key.is_none())
    }

    async fn handle_completion(&self, namespace: &str, compute_graph_name: &str) -> Result<()> {
        if let Some(task) = self
            .state
            .reader()
            .get_system_task(namespace, compute_graph_name)?
        {
            if task.num_running_invocations == 0 {
                // remove the task if reached the end of invocations column
                self.state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::RemoveSystemTask(RemoveSystemTaskRequest {
                            namespace: task.namespace.clone(),
                            compute_graph_name: task.compute_graph_name.clone(),
                        }),
                        processed_state_changes: vec![],
                    })
                    .await?;
            } else {
                info!(
                    running_invocations = task.num_running_invocations,
                    "waiting for all invocations to finish before completing the task",
                );
                // Mark task as completing so that it gets removed on last finished invocation.
                if !task.waiting_for_running_invocations {
                    self.state
                        .write(StateMachineUpdateRequest {
                            payload: RequestPayload::UpdateSystemTask(UpdateSystemTaskRequest {
                                namespace: task.namespace.clone(),
                                compute_graph_name: task.compute_graph_name.clone(),
                                waiting_for_running_invocations: true,
                            }),
                            processed_state_changes: vec![],
                        })
                        .await?;
                }
            }
        };

        Ok(())
    }
}
