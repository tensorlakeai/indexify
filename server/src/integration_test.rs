#[cfg(test)]
mod tests {
    use std::vec;

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{
            mock_executor,
            mock_executor_id,
            mock_invocation_payload_graph_b,
            mock_node_fn_output,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        ExecutorId,
        Task,
        TaskOutcome,
        TaskStatus,
    };
    use futures::StreamExt;
    use state_store::{
        requests::{
            DeleteComputeGraphRequest,
            IngestTaskOutputsRequest,
            RegisterExecutorRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        task_stream,
        test_state_store,
    };

    use crate::{
        service::Service,
        testing::{self, FinalizeTaskArgs},
    };

    #[tokio::test]
    async fn test_simple_graph_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id = {
            let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

            // Should have 1 unprocessed state - one task created event
            let unprocessed_state_changes = indexify_state
                .reader()
                .unprocessed_state_changes(&None, &None)?;
            assert_eq!(
                1,
                unprocessed_state_changes.changes.len(),
                "{:?}",
                unprocessed_state_changes
            );

            test_srv.process_all_state_changes().await?;

            // Should have 0 unprocessed state.
            let unprocessed_state_changes = indexify_state
                .reader()
                .unprocessed_state_changes(&None, &None)?;
            assert_eq!(
                unprocessed_state_changes.changes.len(),
                0,
                "{:#?}",
                unprocessed_state_changes
            );

            test_srv.assert_task_states(1, 0, 1, 0).await?;

            invocation_id
        };

        // register executor
        let executor = {
            let executor = test_srv.register_executor(mock_executor_id()).await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(1, 1, 0, 0).await?;

            executor
        };

        // finalize the starting node task
        {
            let executor_tasks = executor.get_tasks().await?;
            assert_eq!(
                executor_tasks.len(),
                1,
                "Executor tasks: {:#?}",
                executor_tasks
            );
            executor
                .finalize_task(
                    executor_tasks.first().unwrap(),
                    FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(3, 2, 0, 1).await?;
        }

        // finalize the remaining tasks
        {
            let executor_tasks = executor.get_tasks().await?;
            assert_eq!(
                executor_tasks.len(),
                2,
                "fn_b and fn_c tasks: {:#?}",
                executor_tasks
            );

            for task in executor_tasks {
                executor
                    .finalize_task(
                        &task,
                        FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
                    )
                    .await?;
            }

            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            let tasks = indexify_state
                .reader()
                .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
                .unwrap()
                .0;
            assert_eq!(tasks.len(), 3, "{:#?}", tasks);
            let successful_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Success)
                .collect();
            assert_eq!(successful_tasks.len(), 3, "{:#?}", successful_tasks);

            let executor_tasks = executor.get_tasks().await?;
            assert!(
                executor_tasks.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                executor_tasks
            );

            let invocation = indexify_state
                .reader()
                .invocation_ctx(TEST_NAMESPACE, "graph_A", &invocation_id)?
                .unwrap();

            assert!(invocation.completed);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_failure() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id = {
            let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

            test_srv.process_all_state_changes().await?;

            invocation_id
        };

        // register executor
        let executor = {
            let executor = test_srv.register_executor(mock_executor_id()).await?;

            test_srv.process_all_state_changes().await?;

            executor
        };

        // finalize the starting node task with failure
        {
            let executor_tasks = executor.get_tasks().await?;
            assert_eq!(executor_tasks.len(), 1, "{:#?}", executor_tasks);

            executor
                .finalize_task(
                    executor_tasks.first().unwrap(),
                    FinalizeTaskArgs::new().task_outcome(TaskOutcome::Failure),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            let tasks = indexify_state
                .reader()
                .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_A", &invocation_id, None, None)
                .unwrap()
                .0;
            assert_eq!(tasks.len(), 1, "{:#?}", tasks);

            let successful_tasks: Vec<Task> = tasks
                .into_iter()
                .filter(|t| t.outcome == TaskOutcome::Failure)
                .collect();
            assert_eq!(successful_tasks.len(), 1, "{:#?}", successful_tasks);

            let executor_tasks = executor.get_tasks().await?;
            assert!(
                executor_tasks.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                executor_tasks
            );

            let invocation = indexify_state
                .reader()
                .invocation_ctx(TEST_NAMESPACE, "graph_A", &invocation_id)?
                .unwrap();

            assert!(invocation.completed);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_executor_task_reassignment() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let _invocation_id = {
            let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;

            test_srv.process_all_state_changes().await?;

            invocation_id
        };

        // register executor1, task assigned to it
        let executor1 = {
            let executor1 = test_srv
                .register_executor(ExecutorId::new("executor_1".to_string()))
                .await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(1, 1, 0, 0).await?;

            let executor_tasks = executor1.get_tasks().await?;
            assert_eq!(executor_tasks.len(), 1, "{:#?}", executor_tasks);

            executor1
        };

        // register executor2, no tasks assigned to it
        let executor2 = {
            let executor2 = test_srv
                .register_executor(ExecutorId::new("executor_2".to_string()))
                .await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(1, 1, 0, 0).await?;

            let executor_tasks = executor2.get_tasks().await?;
            assert!(executor_tasks.is_empty(), "{:#?}", executor_tasks);

            executor2
        };

        // when executor1 deregisters, its tasks are reassigned to executor2
        {
            executor1.deregister().await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(1, 1, 0, 0).await?;

            let executor_tasks = executor2.get_tasks().await?;
            assert_eq!(executor_tasks.len(), 1, "{:#?}", executor_tasks);
        }

        // when executor2 deregisters, its tasks are not unallocated
        {
            executor2.deregister().await?;

            test_srv.process_all_state_changes().await?;

            test_srv.assert_task_states(1, 0, 1, 0).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_tasks_for_router_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        let invocation_id = test_state_store::with_router_graph(&indexify_state).await;

        test_srv.process_all_state_changes().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)?
            .0;
        assert_eq!(tasks.len(), 1);

        // Finish the task and check if new tasks are created
        test_state_store::finalize_task_graph_b(
            &indexify_state,
            &mock_invocation_payload_graph_b().id,
            &tasks[0],
        )
        .await
        .unwrap();

        test_srv.process_all_state_changes().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 2);
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)
            .unwrap();

        // has task created state change in it.
        assert_eq!(
            unprocessed_state_changes.changes.len(),
            0,
            "{:?}",
            unprocessed_state_changes
        );

        // Now finish the router task and we should have 3 tasks
        // The last one would be for the edge which the router picks
        test_state_store::finalize_router_x(
            &indexify_state,
            &mock_invocation_payload_graph_b().id,
            &tasks[1],
        )
        .await
        .unwrap();

        test_srv.process_all_state_changes().await?;

        let tasks = indexify_state
            .reader()
            .list_tasks_by_compute_graph(TEST_NAMESPACE, "graph_B", &invocation_id, None, None)
            .unwrap()
            .0;
        assert_eq!(tasks.len(), 3, "tasks: {:?}", tasks);

        Ok(())
    }

    // // #[tokio::test]
    // // async fn test_reducer_graph() -> Result<()> {
    // //     let test_srv = testing::TestService::new().await?;
    // //     let Service { indexify_state, .. } = test_srv.service.clone();

    // //     let graph = {
    // //         let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
    // //         let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
    // //         let fn_reduce = reducer_fn("fn_reduce");
    // //         let fn_convert = test_compute_fn("fn_convert",
    // "image_hash".to_string()); //         ComputeGraph {
    // //             namespace: TEST_NAMESPACE.to_string(),
    // //             name: "graph_R".to_string(),
    // //             tags: HashMap::new(),
    // //             tombstoned: false,
    // //             nodes: HashMap::from([
    // //                 ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
    // //                 ("fn_map".to_string(), Node::Compute(fn_map)),
    // //                 ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
    // //                 ("fn_convert".to_string(), Node::Compute(fn_convert)),
    // //             ]),
    // //             edges: HashMap::from([
    // //                 ("fn_gen".to_string(), vec!["fn_map".to_string()]),
    // //                 ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
    // //                 ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
    // //             ]),
    // //             description: "description graph_R".to_string(),
    // //             code: ComputeGraphCode {
    // //                 path: "cg_path".to_string(),
    // //                 size: 23,
    // //                 sha256_hash: "hash123".to_string(),
    // //             },
    // //             version: GraphVersion::from("1"),
    // //             created_at: 5,
    // //             start_fn: Node::Compute(fn_gen),
    // //             runtime_information: RuntimeInformation {
    // //                 major_version: 3,
    // //                 minor_version: 10,
    // //                 sdk_version: "1.2.3".to_string(),
    // //             },
    // //             replaying: false,
    // //         }
    // //     };
    // //     let cg_request = CreateOrUpdateComputeGraphRequest {
    // //         namespace: graph.namespace.clone(),
    // //         compute_graph: graph.clone(),
    // //         upgrade_tasks_to_current_version: false,
    // //     };
    // //     indexify_state
    // //         .write(StateMachineUpdateRequest {
    // //             payload:
    // RequestPayload::CreateOrUpdateComputeGraph(cg_request), //
    // processed_state_changes: vec![], //         })
    // //         .await?;
    // //     let invocation_payload = InvocationPayloadBuilder::default()
    // //         .namespace(TEST_NAMESPACE.to_string())
    // //         .compute_graph_name(graph.name.clone())
    // //         .payload(DataPayload {
    // //             path: "test".to_string(),
    // //             size: 23,
    // //             sha256_hash: "hash1232".to_string(),
    // //         })
    // //         .encoding("application/octet-stream".to_string())
    // //         .build()?;

    // //     let make_finalize_request =
    // //         |compute_fn_name: &str, task: &Task, num_outputs: usize| ->
    // IngestTaskOutputsRequest { //             // let invocation_payload_clone
    // = invocation_payload.clone(); //             let node_outputs =
    // (0..num_outputs) //                 .map(|_| {
    // //                     mock_node_fn_output(
    // //                         invocation_payload.id.as_str(),
    // //                         invocation_payload.compute_graph_name.as_str(),
    // //                         compute_fn_name,
    // //                         None,
    // //                     )
    // //                 })
    // //                 .collect();
    // //             IngestTaskOutputsRequest {
    // //                 namespace: invocation_payload.namespace.clone(),
    // //                 compute_graph:
    // invocation_payload.compute_graph_name.clone(), //
    // compute_fn: compute_fn_name.to_string(), //
    // invocation_id: invocation_payload.id.clone(), //                 task:
    // task.clone(), //                 node_outputs,
    // //                 task_outcome: TaskOutcome::Success,
    // //                 executor_id:
    // ExecutorId::new(TEST_EXECUTOR_ID.to_string()), //
    // diagnostics: None, //             }
    // //         };

    // //     let check_pending_tasks = |expected_num, expected_fn_name| ->
    // Result<Vec<Task>> { //         let tasks = indexify_state
    // //             .reader()
    // //             .list_tasks_by_compute_graph(
    // //                 &graph.namespace,
    // //                 &graph.name,
    // //                 &invocation_payload.id,
    // //                 None,
    // //                 None,
    // //             )
    // //             .unwrap()
    // //             .0;

    // //         let pending_tasks: Vec<Task> = tasks
    // //             .into_iter()
    // //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    // //             .collect();

    // //         assert_eq!(
    // //             expected_num,
    // //             pending_tasks.len(),
    // //             "pending tasks: {:#?}",
    // //             pending_tasks
    // //         );
    // //         pending_tasks.iter().for_each(|t| {
    // //             assert_eq!(t.compute_fn_name, expected_fn_name);
    // //         });

    // //         Ok(pending_tasks)
    // //     };

    // //     {
    // //         let compute_graph = indexify_state
    // //             .reader()
    // //             .get_compute_graph(&graph.namespace, &graph.name)?
    // //             .unwrap();
    // //         let ctx = GraphInvocationCtxBuilder::default()
    // //             .build(compute_graph)
    // //             .unwrap();
    // //         let request = InvokeComputeGraphRequest {
    // //             namespace: graph.namespace.clone(),
    // //             compute_graph_name: graph.name.clone(),
    // //             invocation_payload: invocation_payload.clone(),
    // //             ctx,
    // //         };
    // //         indexify_state
    // //             .write(StateMachineUpdateRequest {
    // //                 payload: RequestPayload::InvokeComputeGraph(request),
    // //                 processed_state_changes: vec![],
    // //             })
    // //             .await?;

    // //         test_srv.process_all_state_changes().await?;
    // //     }

    // //     {
    // //         let tasks: Vec<Task> = indexify_state
    // //             .reader()
    // //             .list_tasks_by_compute_graph(
    // //                 &graph.namespace,
    // //                 &graph.name,
    // //                 &invocation_payload.id,
    // //                 None,
    // //                 None,
    // //             )?
    // //             .0;
    // //         assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
    // //         let task = &tasks.first().unwrap();
    // //         assert_eq!(task.compute_fn_name, "fn_gen");

    // //         let request = make_finalize_request("fn_gen", task, 3);
    // //         indexify_state
    // //             .write(StateMachineUpdateRequest {
    // //                 payload: RequestPayload::IngestTaskOutputs(request),
    // //                 processed_state_changes: vec![],
    // //             })
    // //             .await?;

    // //         test_srv.process_all_state_changes().await?;
    // //     }

    // //     {
    // //         let pending_tasks = check_pending_tasks(3, "fn_map")?;

    // //         // Completing all fn_map tasks
    // //         for task in pending_tasks {
    // //             let request = make_finalize_request(&task.compute_fn_name,
    // &task, 1); //             indexify_state
    // //                 .write(StateMachineUpdateRequest {
    // //                     payload: RequestPayload::IngestTaskOutputs(request),
    // //                     processed_state_changes: vec![],
    // //                 })
    // //                 .await?;
    // //         }
    // //         test_srv.process_all_state_changes().await?;
    // //     }

    // //     // complete all fn_reduce tasks
    // //     for _ in 0..3 {
    // //         let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
    // //         let pending_task = pending_tasks.first().unwrap();

    // //         // Completing all fn_map tasks
    // //         let request = make_finalize_request("fn_reduce", pending_task, 1);
    // //         indexify_state
    // //             .write(StateMachineUpdateRequest {
    // //                 payload: RequestPayload::IngestTaskOutputs(request),
    // //                 processed_state_changes: vec![],
    // //             })
    // //             .await?;

    // //         test_srv.process_all_state_changes().await?;
    // //     }

    // //     {
    // //         let pending_tasks = check_pending_tasks(1, "fn_convert")?;
    // //         let pending_task = pending_tasks.first().unwrap();

    // //         let request = make_finalize_request("fn_convert", pending_task,
    // 1); //         indexify_state
    // //             .write(StateMachineUpdateRequest {
    // //                 payload: RequestPayload::IngestTaskOutputs(request),
    // //                 processed_state_changes: vec![],
    // //             })
    // //             .await?;

    // //         test_srv.process_all_state_changes().await?;
    // //     }

    // //     {
    // //         let state_changes = indexify_state
    // //             .reader()
    // //             .unprocessed_state_changes(&None, &None)?;
    // //         assert_eq!(state_changes.changes.len(), 0);

    // //         let graph_ctx = indexify_state
    // //             .reader()
    // //             .invocation_ctx(&graph.namespace, &graph.name,
    // &invocation_payload.id)? //             .unwrap();
    // //         assert_eq!(graph_ctx.outstanding_tasks, 0);
    // //         assert!(graph_ctx.completed);
    // //     }

    //     Ok(())
    // }

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
    //#[tokio::test]
    // async fn test_reducer_graph_first_reducer_finish_before_parent_finish() ->
    // Result<()> {     let test_srv = testing::TestService::new().await?;
    //     let Service { indexify_state, .. } = test_srv.service.clone();

    //     let graph = {
    //         let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
    //         let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
    //         let fn_reduce = reducer_fn("fn_reduce");
    //         let fn_convert = test_compute_fn("fn_convert",
    // "image_hash".to_string());         ComputeGraph {
    //             namespace: TEST_NAMESPACE.to_string(),
    //             name: "graph_R".to_string(),
    //             tags: HashMap::new(),
    //             tombstoned: false,
    //             nodes: HashMap::from([
    //                 ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
    //                 ("fn_map".to_string(), Node::Compute(fn_map)),
    //                 ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
    //                 ("fn_convert".to_string(), Node::Compute(fn_convert)),
    //             ]),
    //             edges: HashMap::from([
    //                 ("fn_gen".to_string(), vec!["fn_map".to_string()]),
    //                 ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
    //                 ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
    //             ]),
    //             description: "description graph_R".to_string(),
    //             code: ComputeGraphCode {
    //                 path: "cg_path".to_string(),
    //                 size: 23,
    //                 sha256_hash: "hash123".to_string(),
    //             },
    //             version: GraphVersion::from("1"),
    //             created_at: 5,
    //             start_fn: Node::Compute(fn_gen),
    //             runtime_information: RuntimeInformation {
    //                 major_version: 3,
    //                 minor_version: 10,
    //                 sdk_version: "1.2.3".to_string(),
    //             },
    //             replaying: false,
    //         }
    //     };
    //     let cg_request = CreateOrUpdateComputeGraphRequest {
    //         namespace: graph.namespace.clone(),
    //         compute_graph: graph.clone(),
    //         upgrade_tasks_to_current_version: false,
    //     };
    //     indexify_state
    //         .write(StateMachineUpdateRequest {
    //             payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
    //             processed_state_changes: vec![],
    //         })
    //         .await?;
    //     test_srv.process_all_state_changes().await?;
    //     let invocation_payload = InvocationPayloadBuilder::default()
    //         .namespace(TEST_NAMESPACE.to_string())
    //         .compute_graph_name(graph.name.clone())
    //         .payload(DataPayload {
    //             path: "test".to_string(),
    //             size: 23,
    //             sha256_hash: "hash1232".to_string(),
    //         })
    //         .encoding("application/octet-stream".to_string())
    //         .build()?;

    //     let make_finalize_request =
    //         |compute_fn_name: &str, task: &Task, num_outputs: usize| -> IngestTaskOutputsRequest {
    //             // let invocation_payload_clone = invocation_payload.clone();
    //             let node_outputs = (0..num_outputs)
    //                 .map(|_| {
    //                     mock_node_fn_output(
    //                         invocation_payload.id.as_str(),
    //                         invocation_payload.compute_graph_name.as_str(),
    //                         compute_fn_name,
    //                         None,
    //                     )
    //                 })
    //                 .collect();
    //             IngestTaskOutputsRequest {
    //                 namespace: invocation_payload.namespace.clone(),
    //                 compute_graph: invocation_payload.compute_graph_name.clone(),
    //                 compute_fn: compute_fn_name.to_string(),
    //                 invocation_id: invocation_payload.id.clone(),
    //                 task: task.clone(),
    //                 node_outputs,
    //                 task_outcome: TaskOutcome::Success,
    //                 executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    //                 diagnostics: None,
    //             }
    //         };

    //     let check_pending_tasks = |expected_num, expected_fn_name| ->
    // Result<Vec<Task>> {         let tasks = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )
    //             .unwrap()
    //             .0;

    //         let pending_tasks: Vec<Task> = tasks
    //             .into_iter()
    //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    //             .collect();

    //         assert_eq!(
    //             pending_tasks.len(),
    //             expected_num,
    //             "pending tasks: {:?}",
    //             pending_tasks
    //         );
    //         pending_tasks.iter().for_each(|t| {
    //             assert_eq!(t.compute_fn_name, expected_fn_name);
    //         });

    //         Ok(pending_tasks)
    //     };

    //     {
    //         let compute_graph = indexify_state
    //             .reader()
    //             .get_compute_graph(&graph.namespace, &graph.name)?
    //             .unwrap();
    //         let ctx = GraphInvocationCtxBuilder::default()
    //         .namespace(graph.namespace.clone())
    //             .compute_graph_name(graph.name.clone())
    //             .invocation_id(invocation_payload.id.clone())
    //             .graph_version(graph.version.clone())
    //             .build(compute_graph)
    //             .unwrap();
    //         let request = InvokeComputeGraphRequest {
    //             namespace: graph.namespace.clone(),
    //             compute_graph_name: graph.name.clone(),
    //             invocation_payload: invocation_payload.clone(),
    //             ctx,
    //         };
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::InvokeComputeGraph(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let tasks: Vec<Task> = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )?
    //             .0;
    //         assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
    //         let task = &tasks.first().unwrap();
    //         assert_eq!(task.compute_fn_name, "fn_gen");

    //         let request = make_finalize_request("fn_gen", task, 3);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

    //         let task = pending_map_tasks[0].clone();
    //         let request = make_finalize_request(&task.compute_fn_name, &task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let tasks = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )
    //             .unwrap()
    //             .0;

    //         let pending_tasks: Vec<Task> = tasks
    //             .into_iter()
    //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    //             .collect();

    //         assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}",
    // pending_tasks);

    //         let reduce_task = pending_tasks
    //             .iter()
    //             .find(|t| t.compute_fn_name == "fn_reduce")
    //             .unwrap();

    //         // Completing all fn_map tasks
    //         let request = make_finalize_request("fn_reduce", reduce_task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_tasks = check_pending_tasks(2, "fn_map")?;

    //         // Completing all fn_map tasks
    //         for task in pending_tasks {
    //             let request = make_finalize_request(&task.compute_fn_name, &task,
    // 1);             indexify_state
    //                 .write(StateMachineUpdateRequest {
    //                     payload: RequestPayload::IngestTaskOutputs(request),
    //                     processed_state_changes: vec![],
    //                 })
    //                 .await?;

    //             // FIXME: Batching all tasks will currently break the reducing.
    //             test_srv.process_all_state_changes().await?;
    //         }
    //     }

    //     for _ in 0..2 {
    //         let pending_tasks = check_pending_tasks(1, "fn_reduce")?;
    //         let pending_task = pending_tasks.first().unwrap();

    //         // Completing all fn_map tasks
    //         let request = make_finalize_request("fn_reduce", pending_task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_tasks = check_pending_tasks(1, "fn_convert")?;
    //         let pending_task = pending_tasks.first().unwrap();

    //         let request = make_finalize_request("fn_convert", pending_task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;

    //         let pending_task = pending_tasks.first().unwrap();
    //         assert_eq!(pending_task.compute_fn_name, "fn_convert");

    //         // Completing all fn_map tasks
    //         let request = make_finalize_request("fn_convert", pending_task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let state_changes = indexify_state
    //             .reader()
    //             .unprocessed_state_changes(&None, &None)?;
    //         assert_eq!(state_changes.changes.len(), 0);

    //         let graph_ctx = indexify_state
    //             .reader()
    //             .invocation_ctx(&graph.namespace, &graph.name,
    // &invocation_payload.id)?             .unwrap();
    //         assert_eq!(graph_ctx.outstanding_tasks, 0);
    //         assert!(graph_ctx.completed);
    //     }

    //     Ok(())
    // }

    // // test a simple reducer graph
    // //
    // // Tasks:
    // // invoke -> fn_gen
    // // fn_gen -> 1 output x fn_map
    // // fn_map -> fn_reduce
    // // fn_gen -> 1 output x fn_map
    // // fn_gen -> 1 output x fn_map
    // // fn_map -> fn_reduce
    // // fn_map -> fn_reduce
    // // fn_reduce -> fn_convert
    // // fn_convert -> end
    // //
    // #[tokio::test]
    // async fn test_reducer_graph_first_reducer_error_before_parent_finish() ->
    // Result<()> {     let test_srv = testing::TestService::new().await?;
    //     let Service { indexify_state, .. } = test_srv.service.clone();

    //     let graph = {
    //         let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
    //         let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
    //         let fn_reduce = reducer_fn("fn_reduce");
    //         let fn_convert = test_compute_fn("fn_convert",
    // "image_hash".to_string());         ComputeGraph {
    //             namespace: TEST_NAMESPACE.to_string(),
    //             name: "graph_R".to_string(),
    //             tags: HashMap::new(),
    //             tombstoned: false,
    //             nodes: HashMap::from([
    //                 ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
    //                 ("fn_map".to_string(), Node::Compute(fn_map)),
    //                 ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
    //                 ("fn_convert".to_string(), Node::Compute(fn_convert)),
    //             ]),
    //             edges: HashMap::from([
    //                 ("fn_gen".to_string(), vec!["fn_map".to_string()]),
    //                 ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
    //                 ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
    //             ]),
    //             description: "description graph_R".to_string(),
    //             code: ComputeGraphCode {
    //                 path: "cg_path".to_string(),
    //                 size: 23,
    //                 sha256_hash: "hash123".to_string(),
    //             },
    //             version: GraphVersion::from("1"),
    //             created_at: 5,
    //             start_fn: Node::Compute(fn_gen),
    //             runtime_information: RuntimeInformation {
    //                 major_version: 3,
    //                 minor_version: 10,
    //                 sdk_version: "1.2.3".to_string(),
    //             },
    //             replaying: false,
    //         }
    //     };
    //     let cg_request = CreateOrUpdateComputeGraphRequest {
    //         namespace: graph.namespace.clone(),
    //         compute_graph: graph.clone(),
    //         upgrade_tasks_to_current_version: false,
    //     };
    //     indexify_state
    //         .write(StateMachineUpdateRequest {
    //             payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
    //             processed_state_changes: vec![],
    //         })
    //         .await?;
    //     let invocation_payload = InvocationPayloadBuilder::default()
    //         .namespace(TEST_NAMESPACE.to_string())
    //         .compute_graph_name(graph.name.clone())
    //         .payload(DataPayload {
    //             path: "test".to_string(),
    //             size: 23,
    //             sha256_hash: "hash1232".to_string(),
    //         })
    //         .encoding("application/octet-stream".to_string())
    //         .build()?;

    //     let make_finalize_request =
    //         |compute_fn_name: &str, task: &Task, num_outputs: usize| -> IngestTaskOutputsRequest {
    //             // let invocation_payload_clone = invocation_payload.clone();
    //             let node_outputs = (0..num_outputs)
    //                 .map(|_| {
    //                     mock_node_fn_output(
    //                         invocation_payload.id.as_str(),
    //                         invocation_payload.compute_graph_name.as_str(),
    //                         compute_fn_name,
    //                         None,
    //                     )
    //                 })
    //                 .collect();
    //             IngestTaskOutputsRequest {
    //                 namespace: invocation_payload.namespace.clone(),
    //                 compute_graph: invocation_payload.compute_graph_name.clone(),
    //                 compute_fn: compute_fn_name.to_string(),
    //                 invocation_id: invocation_payload.id.clone(),
    //                 task: task.clone(),
    //                 node_outputs,
    //                 task_outcome: TaskOutcome::Success,
    //                 executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    //                 diagnostics: None,
    //             }
    //         };

    //     let check_pending_tasks = |expected_num, expected_fn_name| ->
    // Result<Vec<Task>> {         let tasks = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )
    //             .unwrap()
    //             .0;

    //         let pending_tasks: Vec<Task> = tasks
    //             .into_iter()
    //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    //             .collect();

    //         assert_eq!(
    //             pending_tasks.len(),
    //             expected_num,
    //             "pending tasks: {:?}",
    //             pending_tasks
    //         );
    //         pending_tasks.iter().for_each(|t| {
    //             assert_eq!(t.compute_fn_name, expected_fn_name);
    //         });

    //         Ok(pending_tasks)
    //     };

    //     {
    //         let compute_graph = indexify_state
    //             .reader()
    //             .get_compute_graph(&graph.namespace, &graph.name)?
    //             .unwrap();
    //         let ctx = GraphInvocationCtxBuilder::default()
    //             .build(compute_graph)
    //             .unwrap();
    //         let request = InvokeComputeGraphRequest {
    //             namespace: graph.namespace.clone(),
    //             compute_graph_name: graph.name.clone(),
    //             invocation_payload: invocation_payload.clone(),
    //             ctx,
    //         };
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::InvokeComputeGraph(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let tasks: Vec<Task> = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )?
    //             .0;
    //         assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
    //         let task = &tasks.first().unwrap();
    //         assert_eq!(task.compute_fn_name, "fn_gen");

    //         let request = make_finalize_request("fn_gen", task, 3);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_map_tasks = check_pending_tasks(3, "fn_map")?;

    //         let task = pending_map_tasks[0].clone();
    //         let request = make_finalize_request(&task.compute_fn_name, &task, 1);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let tasks = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )
    //             .unwrap()
    //             .0;

    //         let pending_tasks: Vec<Task> = tasks
    //             .into_iter()
    //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    //             .collect();

    //         assert_eq!(pending_tasks.len(), 3, "pending tasks: {:?}",
    // pending_tasks);

    //         let reduce_task = pending_tasks
    //             .iter()
    //             .find(|t| t.compute_fn_name == "fn_reduce")
    //             .unwrap();

    //         // Completing all fn_map tasks
    //         let request = IngestTaskOutputsRequest {
    //             namespace: invocation_payload.namespace.clone(),
    //             compute_graph: invocation_payload.compute_graph_name.clone(),
    //             compute_fn: "fn_reduce".to_string(),
    //             invocation_id: invocation_payload.id.clone(),
    //             task: reduce_task.clone(),
    //             node_outputs: vec![],
    //             task_outcome: TaskOutcome::Failure, // Failure!
    //             executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    //             diagnostics: None,
    //         };
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_tasks = check_pending_tasks(2, "fn_map")?;

    //         // Completing all fn_map tasks
    //         for task in pending_tasks {
    //             let request = make_finalize_request(&task.compute_fn_name, &task,
    // 1);             indexify_state
    //                 .write(StateMachineUpdateRequest {
    //                     payload: RequestPayload::IngestTaskOutputs(request),
    //                     processed_state_changes: vec![],
    //                 })
    //                 .await?;

    //             // FIXME: Batching all tasks will currently break the reducing.
    //             test_srv.process_all_state_changes().await?;
    //         }
    //     }

    //     {
    //         let state_changes = indexify_state
    //             .reader()
    //             .unprocessed_state_changes(&None, &None)?;
    //         assert_eq!(state_changes.changes.len(), 0);

    //         let graph_ctx = indexify_state
    //             .reader()
    //             .invocation_ctx(&graph.namespace, &graph.name,
    // &invocation_payload.id)?             .unwrap();
    //         assert_eq!(graph_ctx.outstanding_tasks, 0);
    //         assert!(graph_ctx.completed);
    //     }

    //     Ok(())
    // }

    // #[tokio::test]
    // async fn test_reducer_graph_reducer_parent_errors() -> Result<()> {
    //     let test_srv = testing::TestService::new().await?;
    //     let Service { indexify_state, .. } = test_srv.service.clone();

    //     let graph = {
    //         let fn_gen = test_compute_fn("fn_gen", "image_hash".to_string());
    //         let fn_map = test_compute_fn("fn_map", "image_hash".to_string());
    //         let fn_reduce = reducer_fn("fn_reduce");
    //         let fn_convert = test_compute_fn("fn_convert",
    // "image_hash".to_string());         ComputeGraph {
    //             namespace: TEST_NAMESPACE.to_string(),
    //             name: "graph_R".to_string(),
    //             tags: HashMap::new(),
    //             tombstoned: false,
    //             nodes: HashMap::from([
    //                 ("fn_gen".to_string(), Node::Compute(fn_gen.clone())),
    //                 ("fn_map".to_string(), Node::Compute(fn_map)),
    //                 ("fn_reduce".to_string(), Node::Compute(fn_reduce)),
    //                 ("fn_convert".to_string(), Node::Compute(fn_convert)),
    //             ]),
    //             edges: HashMap::from([
    //                 ("fn_gen".to_string(), vec!["fn_map".to_string()]),
    //                 ("fn_map".to_string(), vec!["fn_reduce".to_string()]),
    //                 ("fn_reduce".to_string(), vec!["fn_convert".to_string()]),
    //             ]),
    //             description: "description graph_R".to_string(),
    //             code: ComputeGraphCode {
    //                 path: "cg_path".to_string(),
    //                 size: 23,
    //                 sha256_hash: "hash123".to_string(),
    //             },
    //             version: GraphVersion::from("1"),
    //             created_at: 5,
    //             start_fn: Node::Compute(fn_gen),
    //             runtime_information: RuntimeInformation {
    //                 major_version: 3,
    //                 minor_version: 10,
    //                 sdk_version: "1.2.3".to_string(),
    //             },
    //             replaying: false,
    //         }
    //     };
    //     let cg_request = CreateOrUpdateComputeGraphRequest {
    //         namespace: graph.namespace.clone(),
    //         compute_graph: graph.clone(),
    //         upgrade_tasks_to_current_version: false,
    //     };
    //     indexify_state
    //         .write(StateMachineUpdateRequest {
    //             payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
    //             processed_state_changes: vec![],
    //         })
    //         .await?;
    //     let invocation_payload = InvocationPayloadBuilder::default()
    //         .namespace(TEST_NAMESPACE.to_string())
    //         .compute_graph_name(graph.name.clone())
    //         .payload(DataPayload {
    //             path: "test".to_string(),
    //             size: 23,
    //             sha256_hash: "hash1232".to_string(),
    //         })
    //         .encoding("application/octet-stream".to_string())
    //         .build()?;

    //     let make_finalize_request =
    //         |compute_fn_name: &str, task: &Task, num_outputs: usize| -> IngestTaskOutputsRequest {
    //             let node_outputs = (0..num_outputs)
    //                 .map(|_| {
    //                     mock_node_fn_output(
    //                         invocation_payload.id.as_str(),
    //                         invocation_payload.compute_graph_name.as_str(),
    //                         compute_fn_name,
    //                         None,
    //                     )
    //                 })
    //                 .collect();
    //             IngestTaskOutputsRequest {
    //                 namespace: invocation_payload.namespace.clone(),
    //                 compute_graph: invocation_payload.compute_graph_name.clone(),
    //                 compute_fn: compute_fn_name.to_string(),
    //                 invocation_id: invocation_payload.id.clone(),
    //                 task: task.clone(),
    //                 node_outputs,
    //                 task_outcome: TaskOutcome::Success,
    //                 executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    //                 diagnostics: None,
    //             }
    //         };

    //     let check_pending_tasks = |expected_num, expected_fn_name| ->
    // Result<Vec<Task>> {         let tasks = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )
    //             .unwrap()
    //             .0;

    //         let pending_tasks: Vec<Task> = tasks
    //             .into_iter()
    //             .filter(|t| t.outcome == TaskOutcome::Unknown)
    //             .collect();

    //         assert_eq!(
    //             pending_tasks.len(),
    //             expected_num,
    //             "pending tasks: {:?}",
    //             pending_tasks
    //         );
    //         pending_tasks.iter().for_each(|t| {
    //             assert_eq!(t.compute_fn_name, expected_fn_name);
    //         });

    //         Ok(pending_tasks)
    //     };

    //     {
    //         let compute_graph = indexify_state
    //             .reader()
    //             .get_compute_graph(&graph.namespace, &graph.name)?
    //             .unwrap();
    //         let ctx = GraphInvocationCtxBuilder::default()
    //             .build(compute_graph)
    //             .unwrap();
    //         let request = InvokeComputeGraphRequest {
    //             namespace: graph.namespace.clone(),
    //             compute_graph_name: graph.name.clone(),
    //             invocation_payload: invocation_payload.clone(),
    //             ctx,
    //         };
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::InvokeComputeGraph(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let tasks: Vec<Task> = indexify_state
    //             .reader()
    //             .list_tasks_by_compute_graph(
    //                 &graph.namespace,
    //                 &graph.name,
    //                 &invocation_payload.id,
    //                 None,
    //                 None,
    //             )?
    //             .0;
    //         assert_eq!(tasks.len(), 1, "tasks: {:?}", tasks);
    //         let task = &tasks.first().unwrap();
    //         assert_eq!(task.compute_fn_name, "fn_gen");

    //         let request = make_finalize_request("fn_gen", task, 3);
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     {
    //         let pending_tasks = check_pending_tasks(3, "fn_map")?;

    //         let request =
    //             make_finalize_request(&pending_tasks[0].compute_fn_name,
    // &pending_tasks[0], 1);         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         // FAIL second fn_map task
    //         let request = IngestTaskOutputsRequest {
    //             namespace: invocation_payload.namespace.clone(),
    //             compute_graph: invocation_payload.compute_graph_name.clone(),
    //             compute_fn: "fn_map".to_string(),
    //             invocation_id: invocation_payload.id.clone(),
    //             task: pending_tasks[1].clone(),
    //             node_outputs: vec![],
    //             task_outcome: TaskOutcome::Failure, // Failure!
    //             executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
    //             diagnostics: None,
    //         };
    //         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         let request =
    //             make_finalize_request(&pending_tasks[2].compute_fn_name,
    // &pending_tasks[2], 1);         indexify_state
    //             .write(StateMachineUpdateRequest {
    //                 payload: RequestPayload::IngestTaskOutputs(request),
    //                 processed_state_changes: vec![],
    //             })
    //             .await?;

    //         test_srv.process_all_state_changes().await?;
    //     }

    //     // Expect no more tasks and a completed graph
    //     {
    //         let state_changes = indexify_state
    //             .reader()
    //             .unprocessed_state_changes(&None, &None)?;
    //         assert_eq!(state_changes.changes.len(), 0);

    //         let graph_ctx = indexify_state
    //             .reader()
    //             .invocation_ctx(&graph.namespace, &graph.name,
    // &invocation_payload.id)?             .unwrap();
    //         assert_eq!(graph_ctx.outstanding_tasks, 0);
    //         assert!(graph_ctx.completed);
    //     }

    //     Ok(())
    // }

    #[tokio::test]
    async fn test_create_read_and_delete_compute_graph() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let _ = test_state_store::with_simple_graph(&indexify_state).await;

        let (compute_graphs, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_compute_graphs(TEST_NAMESPACE, None, None)
            .unwrap();

        // Check if the compute graph was created
        assert!(compute_graphs.iter().any(|cg| cg.name == "graph_A"));

        // Delete the compute graph
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneComputeGraph(DeleteComputeGraphRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "graph_A".to_string(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Read the compute graph again
        let (compute_graphs, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_compute_graphs(TEST_NAMESPACE, None, None)
            .unwrap();

        // Check if the compute graph was deleted
        assert!(!compute_graphs.iter().any(|cg| cg.name == "graph_A"));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_stream() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: mock_executor(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        test_srv.process_all_state_changes().await?;

        let res = indexify_state
            .in_memory_state
            .read()
            .await
            .active_tasks_for_executor(mock_executor_id().get(), 10);
        assert_eq!(res.len(), 1);

        let mut stream = task_stream(indexify_state.clone(), mock_executor_id().clone(), 10);

        let res = stream.next().await.unwrap()?;
        assert_eq!(res.len(), 1);

        let mut task = res.first().unwrap().clone();
        task.status = TaskStatus::Completed;
        task.outcome = TaskOutcome::Success;

        let join_handler = tokio::spawn(async move {
            let res = stream.next().await.unwrap()?;
            Ok::<_, anyhow::Error>(res)
        });

        indexify_state
            .write(StateMachineUpdateRequest {
                processed_state_changes: vec![],
                payload: RequestPayload::IngestTaskOutputs(IngestTaskOutputsRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    compute_graph: "graph_A".to_string(),
                    compute_fn: "fn_a".to_string(),
                    invocation_id: invocation_id.clone(),
                    task,
                    node_outputs: vec![mock_node_fn_output(
                        invocation_id.as_str(),
                        "graph_A",
                        "fn_a",
                        None,
                    )],
                    executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                    diagnostics: None,
                }),
            })
            .await?;

        // Process the task
        test_srv.process_all_state_changes().await?;

        let res = join_handler.await??;
        assert_eq!(res.len(), 2, "res: {:#?}", res);

        Ok(())
    }
}
