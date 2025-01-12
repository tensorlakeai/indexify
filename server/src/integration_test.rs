#[cfg(test)]
mod tests {
    use std::{collections::HashMap, vec};

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{
            mock_executor,
            mock_executor_id,
            mock_invocation_payload_graph_b,
            mock_node_fn_output,
            reducer_fn,
            test_compute_fn,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        ComputeGraph,
        ComputeGraphCode,
        DataPayload,
        ExecutorId,
        GraphVersion,
        InvocationPayloadBuilder,
        Node,
        RuntimeInformation,
        StateChange,
        Task,
        TaskId,
        TaskOutcome,
    };
    use state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            DeregisterExecutorRequest,
            FinalizeTaskRequest,
            InvokeComputeGraphRequest,
            RegisterExecutorRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        serializer::{JsonEncode, JsonEncoder},
        state_machine::IndexifyObjectsColumns,
        test_state_store,
    };

    use crate::{service::Service, testing};

    #[tokio::test]
    async fn test_invoke_compute_graph_event_creates_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_ns().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes()
            .unwrap();
        // Processes the invoke cg event and creates a task created event
        assert_eq!(unprocessed_state_changes.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn create_tasks_when_after_fn_finishes() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        // Should have 1 unprocessed state - Task Created 
        let unprocessed_state_changes = indexify_state.reader().unprocessed_state_changes()?;
        assert_eq!(1, unprocessed_state_changes.len(), "{:?}", unprocessed_state_changes);
        let task = &tasks[0];
        // Finish the task and check if new tasks are created
        indexify_state.write(StateMachineUpdateRequest{
            processed_state_changes: vec![],
            payload: RequestPayload::FinalizeTask(FinalizeTaskRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: "graph_A".to_string(),
                compute_fn: "fn_a".to_string(),
                invocation_id: invocation_id.clone(),
                task_id: task.id.clone(),
                node_outputs: vec![mock_node_fn_output(
                    invocation_id.as_str(),
                    "graph_A",
                    "fn_a",
                    None,
                )],
                task_outcome: TaskOutcome::Success,
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            })
        }).await.unwrap();
        test_srv.process_ns().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3);
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes()
            .unwrap();

        // At this point there should be 2 unprocessed task created state changes - for the two new tasks
        assert_eq!(
            unprocessed_state_changes.len(),
            2,
            "unprocessed_state_changes: {:?}",
            unprocessed_state_changes
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_failed_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];

        // Finish the task and check if new tasks are created
        test_state_store::finalize_task(&indexify_state, task, 1, TaskOutcome::Failure, false)
            .await
            .unwrap();

        test_srv.process_all().await?;

        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes()
            .unwrap();

        // no more tasks since invocation failed
        assert_eq!(
            unprocessed_state_changes.len(),
            0,
            "unprocessed_state_changes: {:?}",
            unprocessed_state_changes
        );

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let task = &tasks[0];

        let invocation_ctx = indexify_state
            .reader()
            .invocation_ctx(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            )
            .unwrap();

        assert!(invocation_ctx.unwrap().completed);

        Ok(())
    }

    #[tokio::test]
    async fn test_task_remove() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_all().await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: mock_executor(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        let task = tasks.first().unwrap();
        test_state_store::finalize_task(&indexify_state, task, 1, TaskOutcome::Success, false)
            .await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 0);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_task_unassign() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_all().await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: mock_executor(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: mock_executor_id(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        test_srv.process_all().await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 0);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 1);

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: mock_executor(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all().await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&mock_executor_id(), 10)?;
        assert_eq!(executor_tasks.len(), 1);

        let unallocated_tasks = indexify_state.reader().unallocated_tasks()?;
        assert_eq!(unallocated_tasks.len(), 0);

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: mock_executor_id(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all().await?;

        let mut executor = mock_executor();
        executor.id = ExecutorId::new("2".to_string());
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: executor.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all().await?;

        let executor_tasks = indexify_state
            .reader()
            .get_tasks_by_executor(&executor.id, 10)?;
        assert_eq!(executor_tasks.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_create_tasks_for_router_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let invocation_id = test_state_store::with_router_graph(&indexify_state).await;

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)?
            .0;
        assert_eq!(tasks.len(), 1);
        let task_id = &tasks[0].id;

        // Finish the task and check if new tasks are created
        test_state_store::finalize_task_graph_b(
            &indexify_state,
            &mock_invocation_payload_graph_b().id,
            task_id,
        )
        .await
        .unwrap();

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 2);
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes()
            .unwrap();

        // has task created state change in it.
        assert_eq!(
            unprocessed_state_changes.len(),
            0,
            "{:?}",
            unprocessed_state_changes
        );

        // Now finish the router task and we should have 3 tasks
        // The last one would be for the edge which the router picks
        let task_id = &tasks[1].id;
        test_state_store::finalize_router_x(
            &indexify_state,
            &mock_invocation_payload_graph_b().id,
            task_id,
        )
        .await
        .unwrap();

        test_srv.process_all().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3, "tasks: {:?}", tasks);

        Ok(())
    }

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
            let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", "image_hash".to_string());
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                tombstoned: false,
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion::from("1"),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                expected_num,
                pending_tasks.len(),
                "pending tasks: {:#?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let tasks: Vec<Task> = indexify_state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(3, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                indexify_state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        processed_state_changes: vec![],
                    })
                    .await?;
            }
            test_srv.process_all().await?;
        }

        // complete all fn_reduce tasks
        for _ in 0..3 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let state_changes = indexify_state
                .reader()
                .unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_first_reducer_finish_before_parent_finish() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
            let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", "image_hash".to_string());
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                tombstoned: false,
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion::from("1"),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
            })
            .await?;
        test_srv.process_all().await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let tasks: Vec<Task> = indexify_state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}", pending_tasks);

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &reduce_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(2, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                indexify_state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        processed_state_changes: vec![],
                    })
                    .await?;

                // FIXME: Batching all tasks will currently break the reducing.
                test_srv.process_all().await?;
            }
        }

        for _ in 0..2 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
            let pending_task = pending_tasks.first().unwrap();
            assert_eq!(pending_task.compute_fn_name, "fn_convert");

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }
        {
            let state_changes = indexify_state
                .reader()
                .unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    // test a simple reducer graph
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_first_reducer_error_before_parent_finish() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
            let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", "image_hash".to_string());
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                tombstoned: false,
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion::from("1"),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let tasks: Vec<Task> = indexify_state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}", pending_tasks);

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            // Completing all fn_map tasks
            let request = FinalizeTaskRequest {
                namespace: invocation_payload.namespace.clone(),
                compute_graph: invocation_payload.compute_graph_name.clone(),
                compute_fn: "fn_reduce".to_string(),
                invocation_id: invocation_payload.id.clone(),
                task_id: reduce_task.id.clone(),
                node_outputs: vec![],
                task_outcome: TaskOutcome::Failure, // Failure!
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(2, "fn_map")?;

            // Completing all fn_map tasks
            for task in pending_tasks {
                let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
                indexify_state
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::FinalizeTask(request),
                        processed_state_changes: vec![],
                    })
                    .await?;

                // FIXME: Batching all tasks will currently break the reducing.
                test_srv.process_all().await?;
            }
        }

        {
            let state_changes = indexify_state
                .reader()
                .unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    // test_reducer_graph_reducer_finalize_just_before_map
    //
    // Tasks:
    // invoke -> fn_gen
    // fn_gen -> 1 output x fn_map
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce (reduce task finalize before next map tasks)
    // fn_gen -> 1 output x fn_map
    // fn_map -> fn_reduce
    // fn_map -> fn_reduce
    // fn_reduce -> fn_convert
    // fn_convert -> end
    //
    #[tokio::test]
    async fn test_reducer_graph_reducer_finalize_just_before_map() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
            let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", "image_hash".to_string());
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                tombstoned: false,
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion::from("1"),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:#?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let tasks: Vec<Task> = indexify_state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        let pending_map_tasks = check_pending_tasks(3, "fn_map")?;
        {
            let task = pending_map_tasks[0].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let task = pending_map_tasks[1].clone();
            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            // NOT running scheduler yet
        }

        // HACK: We need to finalize the reducer task before the map task, but without
        // saving it.
        let all_unprocessed_state_changes_reduce = {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                2,
                "pending tasks: {:#?}",
                pending_tasks
            );

            let reduce_task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_reduce")
                .unwrap();

            let all_unprocessed_state_changes_before = indexify_state
                .reader()
                .unprocessed_state_changes()?;

            let request = make_finalize_request("fn_reduce", &reduce_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            let all_unprocessed_state_changes_reduce: Vec<StateChange> = indexify_state
                .reader()
                .unprocessed_state_changes()?
                .iter()
                .filter(|sc| {
                    !all_unprocessed_state_changes_before
                        .iter()
                        .any(|sc_before| sc_before.id == sc.id)
                })
                .cloned()
                .collect();

            // Need to finalize without persisting the state change
            for state_change in all_unprocessed_state_changes_reduce.clone() {
                indexify_state.db.delete_cf(
                    &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&indexify_state.db),
                    state_change.key(),
                )?;
            }

            let ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(
                ctx.get_task_analytics("fn_reduce").unwrap().pending_tasks,
                0
            );
            assert_eq!(
                ctx.get_task_analytics("fn_reduce")
                    .unwrap()
                    .successful_tasks,
                1
            );

            // running scheduler for previous map
            test_srv.process_all().await?;

            all_unprocessed_state_changes_reduce
        };

        {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(pending_tasks.len(), 2, "pending tasks: {:?}", pending_tasks);

            let task = pending_tasks
                .iter()
                .find(|t| t.compute_fn_name == "fn_map")
                .unwrap();

            let request = make_finalize_request(&task.compute_fn_name, &task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            // Persisting state change
            for state_change in all_unprocessed_state_changes_reduce {
                let serialized_state_change = JsonEncoder::encode(&state_change)?;
                indexify_state.db.put_cf(
                    &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&indexify_state.db),
                    state_change.key(),
                    serialized_state_change,
                )?;
            }

            // running scheduler for reducer
            test_srv.process_all().await?;
        }

        for _ in 0..2 {
            let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_reduce", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(1, "fn_convert")?;
            let pending_task = pending_tasks.first().unwrap();

            // Completing all fn_map tasks
            let request = make_finalize_request("fn_convert", &pending_task.id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }
        {
            let state_changes = indexify_state
                .reader()
                .unprocessed_state_changes()?;
            assert_eq!(
                state_changes.len(),
                0,
                "expected no state changes: {:?}",
                state_changes
            );

            let graph_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(
                graph_ctx.outstanding_tasks, 0,
                "expected no outstanding tasks: {}",
                graph_ctx.outstanding_tasks
            );
            assert!(graph_ctx.completed);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reducer_graph_reducer_parent_errors() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let graph = {
            let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
            let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
            let fn_reduce = reducer_fn("fn_reduce");
            let fn_convert = test_compute_fn("fn_convert", "image_hash".to_string());
            ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_R".to_string(),
                tags: HashMap::new(),
                tombstoned: false,
                nodes: HashMap::from([
                    ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
                    ("fn_map".to_string(), Node::Compute(fn_map)),
                    ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
                    ("fn_convert".to_string(), Node::Compute(fn_convert)),
                ]),
                edges: HashMap::from([
                    ("fn_gen".to_string(), vec!["fn_map".to_string()]),
                    ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
                    ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
                ]),
                description: "description graph_R".to_string(),
                code: ComputeGraphCode {
                    path: "cg_path".to_string(),
                    size: 23,
                    sha256_hash: "hash123".to_string(),
                },
                version: GraphVersion::from("1"),
                created_at: 5,
                start_fn: Node::Compute(fn_gen),
                runtime_information: RuntimeInformation {
                    major_version: 3,
                    minor_version: 10,
                    sdk_version: "1.2.3".to_string(),
                },
                replaying: false,
            }
        };
        let cg_request = CreateOrUpdateComputeGraphRequest {
            namespace: graph.namespace.clone(),
            compute_graph: graph.clone(),
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
                processed_state_changes: vec![],
            })
            .await?;
        let invocation_payload = InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name(graph.name.clone())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()?;

        let make_finalize_request =
            |compute_fn_name: &str, task_id: &TaskId, num_outputs: usize| -> FinalizeTaskRequest {
                // let invocation_payload_clone = invocation_payload.clone();
                let node_outputs = (0..num_outputs)
                    .map(|_| {
                        mock_node_fn_output(
                            invocation_payload.id.as_str(),
                            invocation_payload.compute_graph_name.as_str(),
                            compute_fn_name,
                            None,
                        )
                    })
                    .collect();
                FinalizeTaskRequest {
                    namespace: invocation_payload.namespace.clone(),
                    compute_graph: invocation_payload.compute_graph_name.clone(),
                    compute_fn: compute_fn_name.to_string(),
                    invocation_id: invocation_payload.id.clone(),
                    task_id: task_id.clone(),
                    node_outputs,
                    task_outcome: TaskOutcome::Success,
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }
            };

        let check_pending_tasks = |expected_num, expected_fn_name| -> Result<Vec<Task>> {
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

            let pending_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Unknown)
                .collect();

            assert_eq!(
                pending_tasks.len(),
                expected_num,
                "pending tasks: {:?}",
                pending_tasks
            );
            pending_tasks.iter().for_each(|t| {
                assert_eq!(t.compute_fn_name, expected_fn_name);
            });

            Ok(pending_tasks)
        };

        {
            let request = InvokeComputeGraphRequest {
                namespace: graph.namespace.clone(),
                compute_graph_name: graph.name.clone(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let tasks: Vec<Task> = indexify_state
                .reader()
                .list_tasks_by_compute_graph(
                    &graph.namespace,
                    &graph.name,
                    &invocation_payload.id,
                    None,
                    None,
                )?
                .0;
            assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
            let task = &tasks.first().unwrap();
            assert_eq!(task.compute_fn_name, "fn_gen");

            let request = make_finalize_request("fn_gen", &task.id, 3);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        {
            let pending_tasks = check_pending_tasks(3, "fn_map")?;

            let request =
                make_finalize_request(&pending_tasks[0].compute_fn_name, &pending_tasks[0].id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            // FAIL second fn_map task
            let request = FinalizeTaskRequest {
                namespace: invocation_payload.namespace.clone(),
                compute_graph: invocation_payload.compute_graph_name.clone(),
                compute_fn: "fn_map".to_string(),
                invocation_id: invocation_payload.id.clone(),
                task_id: pending_tasks[1].id.clone(),
                node_outputs: vec![],
                task_outcome: TaskOutcome::Failure, // Failure!
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            let request =
                make_finalize_request(&pending_tasks[2].compute_fn_name, &pending_tasks[2].id, 1);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    processed_state_changes: vec![],
                })
                .await?;

            test_srv.process_all().await?;
        }

        // Expect no more tasks and a completed graph
        {
            let state_changes = indexify_state
                .reader()
                .unprocessed_state_changes()?;
            assert_eq!(state_changes.len(), 0);

            let graph_ctx = indexify_state
                .reader()
                .invocation_ctx(&graph.namespace, &graph.name, &invocation_payload.id)?
                .unwrap();
            assert_eq!(graph_ctx.outstanding_tasks, 0);
            assert!(graph_ctx.completed);
        }

        Ok(())
    }
}
