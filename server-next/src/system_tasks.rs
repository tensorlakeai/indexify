use std::sync::Arc;

use anyhow::Result;
use state_store::IndexifyState;

pub struct SystemTasksExecutor {
    state: Arc<IndexifyState>,
    rx: tokio::sync::watch::Receiver<()>,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
}

const MAX_PENDING_TASKS: usize = 10;

impl SystemTasksExecutor {
    pub fn new(state: Arc<IndexifyState>, shutdown_rx: tokio::sync::watch::Receiver<()>) -> Self {
        let rx = state.get_system_tasks_watcher();
        Self {
            state,
            rx,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let pending_tasks = self.state.reader().get_pending_system_tasks()?;
        let (tasks, _) = self.state.reader().get_system_tasks(Some(1))?;
        if tasks.is_empty() || pending_tasks >= MAX_PENDING_TASKS {
            tokio::select! {
                _ = self.rx.changed() => {
                    println!("GC signal received.");
                    self.rx.borrow_and_update();
                }
                _ = self.shutdown_rx.changed() => {
                    println!("shutdown signal received.");
                    return Ok(());
                }
            }
        } else {
            let task = tasks.first().unwrap();
            tracing::info!("Executing task {:?}", task);
            let (invocations, restart_key) = self.state.reader().list_invocations(
                &task.namespace,
                &task.compute_graph_name,
                task.restart_key.as_deref(),
                Some(MAX_PENDING_TASKS - pending_tasks),
            )?;
            for invocation in invocations {
                tracing::info!("Executing invocation {:?}", invocation);
                self.state
                    .write(state_store::requests::StateMachineUpdateRequest {
                        payload: state_store::requests::RequestPayload::RerunInvocation(
                            state_store::requests::RerunInvocationRequest {
                                namespace: task.namespace.clone(),
                                compute_graph_name: task.compute_graph_name.clone(),
                                graph_version: task.graph_version.clone(),
                                invocation_id: invocation.id.clone(),
                            },
                        ),
                        state_changes_processed: vec![],
                    })
                    .await?;
            }
            match restart_key {
                Some(restart_key) => {
                    // Update task with new restart key for subsequent invocations
                    tracing::info!(
                        "updating system task for graph {} for restart",
                        task.compute_graph_name
                    );
                    self.state
                        .write(state_store::requests::StateMachineUpdateRequest {
                            payload: state_store::requests::RequestPayload::UpdateSystemTask(
                                state_store::requests::UpdateSystemTaskRequest {
                                    namespace: task.namespace.clone(),
                                    compute_graph_name: task.compute_graph_name.clone(),
                                    restart_key,
                                },
                            ),
                            state_changes_processed: vec![],
                        })
                        .await?;
                }
                None => {
                    tracing::info!(
                        "system task for graph {} completed",
                        task.compute_graph_name
                    );
                    // remove the task if reached the end of invocations column
                    self.state
                        .write(state_store::requests::StateMachineUpdateRequest {
                            payload: state_store::requests::RequestPayload::RemoveSystemTask(
                                state_store::requests::RemoveSystemTaskRequest {
                                    namespace: task.namespace.clone(),
                                    compute_graph_name: task.compute_graph_name.clone(),
                                },
                            ),
                            state_changes_processed: vec![],
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use data_model::{
        test_objects::tests::{
            mock_graph_a,
            mock_invocation_payload,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        DataPayload,
        ExecutorId,
        InvocationPayload,
        InvocationPayloadBuilder,
        NodeOutput,
        NodeOutputBuilder,
        OutputPayload,
        TaskId,
        TaskOutcome,
    };
    use rand::Rng;
    use state_store::requests::{
        CreateComputeGraphRequest,
        FinalizeTaskRequest,
        InvokeComputeGraphRequest,
        RequestPayload,
        RerunComputeGraphRequest,
        StateMachineUpdateRequest,
    };
    use tracing_subscriber::{layer::SubscriberExt, Layer};
    use uuid::Uuid;

    use super::*;
    use crate::scheduler::Scheduler;

    fn generate_random_hash() -> String {
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }

    fn mock_node_fn_output(invocation_id: &str, graph: &str, compute_fn_name: &str) -> NodeOutput {
        NodeOutputBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .graph_version(Default::default())
            .compute_fn_name(compute_fn_name.to_string())
            .compute_graph_name(graph.to_string())
            .invocation_id(invocation_id.to_string())
            .payload(OutputPayload::Fn(DataPayload {
                sha256_hash: generate_random_hash(),
                path: Uuid::new_v4().to_string(),
                size: 12,
            }))
            .build()
            .unwrap()
    }

    fn make_finalize_request(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn_name: &str,
        task_id: &TaskId,
    ) -> FinalizeTaskRequest {
        FinalizeTaskRequest {
            namespace: namespace.to_string(),
            compute_graph: compute_graph.to_string(),
            compute_fn: compute_fn_name.to_string(),
            invocation_id: invocation_id.to_string(),
            task_id: task_id.clone(),
            node_outputs: vec![mock_node_fn_output(
                invocation_id,
                compute_graph,
                compute_fn_name,
            )],
            task_outcome: TaskOutcome::Success,
            executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
            diagnostics: None,
        }
    }

    #[tokio::test]
    async fn test_graph_rerun() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let state = IndexifyState::new(temp_dir.path().join("state"))
            .await
            .unwrap();
        let shutdown_rx = tokio::sync::watch::channel(()).1;
        let scheduler = Scheduler::new(state.clone());
        let mut executor = SystemTasksExecutor::new(state.clone(), shutdown_rx);

        let graph = mock_graph_a();
        let cg_request = CreateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await
            .unwrap();
        let invocation_payload = mock_invocation_payload();
        let request = InvokeComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
            invocation_payload: invocation_payload.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::InvokeComputeGraph(request),
                state_changes_processed: vec![],
            })
            .await
            .unwrap();

        scheduler.run_scheduler().await?;

        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];

        let request = make_finalize_request(
            &graph.namespace,
            &graph.name,
            &invocation_payload.id,
            &task.compute_fn_name,
            &task.id,
        );
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::FinalizeTask(request),
                state_changes_processed: vec![],
            })
            .await?;
        scheduler.run_scheduler().await?;
        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3);
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        assert_eq!(incomplete_tasks.clone().count(), 2);

        for task in incomplete_tasks {
            let request = make_finalize_request(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                &task.compute_fn_name,
                &task.id,
            );
            tracing::info!("complete task {:?}", task);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;
        }
        scheduler.run_scheduler().await?;
        assert_eq!(tasks.len(), 3);
        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        assert_eq!(incomplete_tasks.clone().count(), 0);

        scheduler.run_scheduler().await?;

        let state_changes = state.reader().get_unprocessed_state_changes()?;
        assert_eq!(state_changes.len(), 0);

        let graph_ctx =
            state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?;
        assert_eq!(graph_ctx.outstanding_tasks, 0);

        let request = RequestPayload::RerunComputeGraph(RerunComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        state
            .write(StateMachineUpdateRequest {
                payload: request,
                state_changes_processed: vec![],
            })
            .await?;

        let system_tasks = state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        executor.run().await?;

        // Since graph version is the same it should generate new tasks
        let state_changes = state.reader().get_unprocessed_state_changes()?;
        assert_eq!(state_changes.len(), 0);

        let system_tasks = state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 0);

        // Update graph so version is incremented
        let mut graph = graph;
        graph.code.sha256_hash = generate_random_hash();

        let cg_request = CreateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await
            .unwrap();

        let (graphs, _) = state
            .reader()
            .list_compute_graphs(&graph.namespace, None, None)?;
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].version, graph.version.next());

        let graph = graphs[0].clone();

        let request = RequestPayload::RerunComputeGraph(RerunComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        state
            .write(StateMachineUpdateRequest {
                payload: request,
                state_changes_processed: vec![],
            })
            .await?;

        let system_tasks = state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        executor.run().await?;

        // task should be deleted since all entries are processed
        let system_tasks = state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 0);

        // Since graph version is different new changes should be generated
        let state_changes = state.reader().get_unprocessed_state_changes()?;
        assert_eq!(state_changes.len(), 1);

        // Number of pending system tasks should be incremented
        let num_pending_tasks = state.reader().get_pending_system_tasks()?;
        assert_eq!(num_pending_tasks, 1);

        scheduler.run_scheduler().await?;

        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        assert_eq!(incomplete_tasks.clone().count(), 1);

        for task in incomplete_tasks {
            let request = make_finalize_request(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                &task.compute_fn_name,
                &task.id,
            );
            tracing::info!("complete task {:?} req {:?}", task, request);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;
        }

        scheduler.run_scheduler().await?;
        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        assert_eq!(incomplete_tasks.clone().count(), 2);

        for task in incomplete_tasks {
            let request = make_finalize_request(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                &task.compute_fn_name,
                &task.id,
            );
            tracing::info!("complete task {:?}", task);
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;
        }
        scheduler.run_scheduler().await?;
        let tasks = state
            .reader()
            .list_tasks_by_compute_graph(
                &graph.namespace,
                &graph.name,
                &invocation_payload.id,
                None,
                None,
            )
            .unwrap()
            .0;
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        assert_eq!(incomplete_tasks.clone().count(), 0);

        scheduler.run_scheduler().await?;

        let state_changes = state.reader().get_unprocessed_state_changes()?;
        assert_eq!(state_changes.len(), 0);

        // Number of pending system tasks should be decremented after graph completion
        let num_pending_tasks = state.reader().get_pending_system_tasks()?;
        assert_eq!(num_pending_tasks, 0);

        Ok(())
    }

    fn generate_invocation_payload(namespace: &str, graph: &str) -> InvocationPayload {
        InvocationPayloadBuilder::default()
            .namespace(namespace.to_string())
            .compute_graph_name(graph.to_string())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: generate_random_hash(),
            })
            .build()
            .unwrap()
    }

    async fn finalize_incomplete_tasks(
        state: &IndexifyState,
        namespace: &str,
    ) -> Result<(), anyhow::Error> {
        let tasks = state
            .reader()
            .list_tasks_by_namespace(namespace, None, None)
            .unwrap()
            .0;
        let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
        for task in incomplete_tasks {
            let request = make_finalize_request(
                &task.namespace,
                &task.compute_graph_name,
                task.invocation_id.as_str(),
                &task.compute_fn_name,
                &task.id,
            );
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await?;
        }

        Ok(())
    }

    // test creating more tasks than MAX_PENDING_TASKS
    // tasks in progress should stays at or below MAX_PENDING_TASKS
    // all tasks should complete eventually
    #[tokio::test]
    async fn test_graph_flow_control_rerun() -> Result<()> {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let state = IndexifyState::new(temp_dir.path().join("state"))
            .await
            .unwrap();
        let shutdown_rx = tokio::sync::watch::channel(()).1;
        let scheduler = Scheduler::new(state.clone());
        let mut executor = SystemTasksExecutor::new(state.clone(), shutdown_rx);

        let graph = mock_graph_a();
        let cg_request = CreateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await
            .unwrap();

        for _ in 0..MAX_PENDING_TASKS * 3 {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: generate_invocation_payload(&graph.namespace, &graph.name),
            };
            state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();
        }

        scheduler.run_scheduler().await?;

        loop {
            finalize_incomplete_tasks(&state, &graph.namespace).await?;

            scheduler.run_scheduler().await?;
            scheduler.run_scheduler().await?;
            let tasks = state
                .reader()
                .list_tasks_by_namespace(&graph.namespace, None, None)
                .unwrap()
                .0;
            let incomplete_tasks = tasks.iter().filter(|t| t.outcome == TaskOutcome::Unknown);
            let state_changes = state.reader().get_unprocessed_state_changes()?;
            if state_changes.len() == 0 && incomplete_tasks.count() == 0 {
                break;
            }
        }

        // Update graph so version is incremented
        let mut graph = graph;
        graph.code.sha256_hash = generate_random_hash();

        let cg_request = CreateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(cg_request),
                state_changes_processed: vec![],
            })
            .await
            .unwrap();

        let (graphs, _) = state
            .reader()
            .list_compute_graphs(&graph.namespace, None, None)?;
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].version, graph.version.next());

        let graph = graphs[0].clone();

        let request = RequestPayload::RerunComputeGraph(RerunComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        state
            .write(StateMachineUpdateRequest {
                payload: request,
                state_changes_processed: vec![],
            })
            .await?;

        let system_tasks = state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        loop {
            executor.run().await?;

            let num_pending_tasks = state.reader().get_pending_system_tasks()?;
            tracing::info!("num pending tasks {:?}", num_pending_tasks);
            assert!(num_pending_tasks <= MAX_PENDING_TASKS);

            scheduler.run_scheduler().await?;

            finalize_incomplete_tasks(&state, &graph.namespace).await?;

            let tasks = state
                .reader()
                .list_tasks_by_namespace(&graph.namespace, None, None)
                .unwrap()
                .0;
            let num_incomplete_tasks = tasks
                .iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .count();

            let system_tasks = state.reader().get_system_tasks(None).unwrap().0;

            let state_changes = state.reader().get_unprocessed_state_changes()?;
            if state_changes.len() == 0 && num_incomplete_tasks == 0 && system_tasks.len() == 0 {
                break;
            }
        }

        // Verify that all outputs are initialized with correct graph version
        let invocations = state
            .reader()
            .list_invocations(&graph.namespace, &graph.name, None, None)?
            .0;
        for invocation in invocations {
            let outputs = state
                .reader()
                .list_outputs_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation.id,
                    None,
                    None,
                )?
                .0;
            assert!(outputs.iter().all(|o| o.graph_version == graph.version));
        }

        Ok(())
    }
}
