#[cfg(test)]
mod tests {
    use anyhow::Result;
    use data_model::{
        test_objects::tests::{
            mock_graph_a,
            mock_invocation_payload,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        DataPayload,
        ExecutorId,
        GraphVersion,
        InvocationPayload,
        InvocationPayloadBuilder,
        NodeOutput,
        NodeOutputBuilder,
        OutputPayload,
        TaskId,
        TaskOutcome,
    };
    use rand::Rng;
    use state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            FinalizeTaskRequest,
            InvokeComputeGraphRequest,
            ReplayComputeGraphRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        IndexifyState,
    };
    use tracing::info;
    use uuid::Uuid;

    use crate::{
        service::Service,
        testing::{self},
    };

    fn generate_random_hash() -> String {
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }

    fn mock_node_fn_output(invocation_id: &str, graph: &str, compute_fn_name: &str) -> NodeOutput {
        NodeOutputBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
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
    async fn test_graph_replay() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            system_tasks_executor,
            ..
        } = test_srv.service.clone();

        let graph = mock_graph_a("image_hash".to_string());
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                process_state_change: None,
            })
            .await
            .unwrap();
        let invocation_payload = mock_invocation_payload();
        let request = InvokeComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
            invocation_payload: invocation_payload.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::InvokeComputeGraph(request),
                process_state_change: None,
            })
            .await
            .unwrap();

        test_srv.process_all().await?;

        let tasks = indexify_state
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
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::FinalizeTask(request),
                process_state_change: None,
            })
            .await?;
        test_srv.process_all().await?;
        let tasks = indexify_state
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
            info!("complete task {:?}", task);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    process_state_change: None,
                })
                .await?;
        }
        test_srv.process_all().await?;
        assert_eq!(tasks.len(), 3);
        let tasks = indexify_state
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

        test_srv.process_all().await?;

        let state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes_all_processors()?;
        assert_eq!(state_changes.len(), 0);

        let graph_ctx = indexify_state.reader().invocation_ctx(
            &graph.namespace,
            &graph.name,
            &invocation_payload.id,
        )?;
        assert_eq!(graph_ctx.unwrap().outstanding_tasks, 0);

        let request = RequestPayload::ReplayComputeGraph(ReplayComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: request,
                process_state_change: None,
            })
            .await?;

        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        system_tasks_executor.lock().await.run().await?;

        // Since graph version is the same it should generate new tasks
        let state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes_all_processors()?;
        assert_eq!(state_changes.len(), 0);

        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 0);

        // Update graph version
        let mut graph = graph;
        graph.version = GraphVersion::from("2");

        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                process_state_change: None,
            })
            .await
            .unwrap();

        let (graphs, _) =
            indexify_state
                .reader()
                .list_compute_graphs(&graph.namespace, None, None)?;
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].version, GraphVersion::from("2"));

        let graph = graphs[0].clone();

        let request = RequestPayload::ReplayComputeGraph(ReplayComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: request,
                process_state_change: None,
            })
            .await?;

        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        system_tasks_executor.lock().await.run().await?;

        // task should still exist since there are still invocations to process
        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);

        // Since graph version is different new changes should be generated
        let state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes_all_processors()?;
        assert_eq!(state_changes.len(), 1);

        // Number of pending system tasks should be incremented
        let num_pending_tasks = indexify_state.reader().get_pending_system_tasks()?;
        assert_eq!(num_pending_tasks, 1);

        test_srv.process_all().await?;

        let tasks = indexify_state
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
            info!("complete task {:?} req {:?}", task, request);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    process_state_change: None,
                })
                .await?;
        }

        test_srv.process_all().await?;
        let tasks = indexify_state
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
            info!("complete task {:?}", task);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    process_state_change: None,
                })
                .await?;
        }
        test_srv.process_all().await?;
        let tasks = indexify_state
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

        test_srv.process_all().await?;

        let state_changes = indexify_state
            .reader()
            .get_unprocessed_state_changes_all_processors()?;
        assert_eq!(state_changes.len(), 0);

        // Number of pending system tasks should be decremented after graph completion
        let num_pending_tasks = indexify_state.reader().get_pending_system_tasks()?;
        assert_eq!(num_pending_tasks, 0);

        system_tasks_executor.lock().await.run().await?;

        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(
            system_tasks.len(),
            0,
            "task should not exist anymore since all invocations are processed"
        );

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
            .encoding("application/octet-stream".to_string())
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
                    process_state_change: None,
                })
                .await?;
        }

        Ok(())
    }

    // test creating more tasks than MAX_PENDING_TASKS
    // tasks in progress should stays at or below MAX_PENDING_TASKS
    // all tasks should complete eventually
    #[tokio::test]
    async fn test_graph_flow_control_replay() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            system_tasks_executor,
            ..
        } = test_srv.service.clone();

        let graph = mock_graph_a("image_hash".to_string());
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                process_state_change: None,
            })
            .await
            .unwrap();

        for _ in 0..10 * 3 {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: generate_invocation_payload(&graph.namespace, &graph.name),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    process_state_change: None,
                })
                .await
                .unwrap();
        }

        test_srv.process_all().await?;

        loop {
            finalize_incomplete_tasks(&indexify_state, &graph.namespace).await?;

            test_srv.process_all().await?;
            let tasks = indexify_state
                .reader()
                .list_tasks_by_namespace(&graph.namespace, None, None)
                .unwrap()
                .0;
            let incomplete_tasks = tasks
                .iter()
                .filter(|t: &&data_model::Task| t.outcome == TaskOutcome::Unknown);
            let state_changes = indexify_state
                .reader()
                .get_unprocessed_state_changes_all_processors()?;
            if state_changes.is_empty() && incomplete_tasks.count() == 0 {
                break;
            }
        }

        // Update graph version
        let mut graph = graph;
        graph.version = GraphVersion::from("2");

        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                process_state_change: None,
            })
            .await
            .unwrap();

        let (graphs, _) =
            indexify_state
                .reader()
                .list_compute_graphs(&graph.namespace, None, None)?;
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].version, GraphVersion::from("2"));

        let graph = graphs[0].clone();

        let request = RequestPayload::ReplayComputeGraph(ReplayComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph_name: graph.name.clone(),
        });
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: request,
                process_state_change: None,
            })
            .await?;

        let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;
        assert_eq!(system_tasks.len(), 1);
        let system_task = &system_tasks[0];
        assert_eq!(system_task.namespace, graph.namespace);
        assert_eq!(system_task.compute_graph_name, graph.name);

        loop {
            system_tasks_executor.lock().await.run().await?;

            let num_pending_tasks = indexify_state.reader().get_pending_system_tasks()?;
            info!("num pending tasks {:?}", num_pending_tasks);
            assert!(num_pending_tasks <= 10);

            test_srv.process_all().await?;

            finalize_incomplete_tasks(&indexify_state, &graph.namespace).await?;

            let tasks = indexify_state
                .reader()
                .list_tasks_by_namespace(&graph.namespace, None, None)
                .unwrap()
                .0;
            let num_incomplete_tasks = tasks
                .iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .count();

            let system_tasks = indexify_state.reader().get_system_tasks(None).unwrap().0;

            let state_changes = indexify_state
                .reader()
                .get_unprocessed_state_changes_all_processors()?;
            if state_changes.is_empty() && num_incomplete_tasks == 0 && system_tasks.is_empty() {
                break;
            }
        }

        // Verify that all outputs are initialized with correct graph version
        let invocations = indexify_state
            .reader()
            .list_invocations(&graph.namespace, &graph.name, None, None)?
            .0;
        for invocation in invocations {
            let invocation_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation.id)?
                .unwrap();
            assert!(invocation_ctx.graph_version == graph.version);
        }

        Ok(())
    }
}
