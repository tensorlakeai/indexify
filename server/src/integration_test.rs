#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, vec};

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{test_executor_metadata, TEST_EXECUTOR_ID, TEST_NAMESPACE},
        Task,
        TaskFailureReason,
        TaskOutcome,
    };
    use rocksdb::{IteratorMode, TransactionDB};
    use state_store::{
        requests::{DeleteComputeGraphRequest, RequestPayload, StateMachineUpdateRequest},
        state_machine::IndexifyObjectsColumns,
        test_state_store,
    };
    use strum::IntoEnumIterator;

    use crate::{
        service::Service,
        testing::{self, allocation_key_from_proto, FinalizeTaskArgs, TaskStateAssertions},
    };

    fn assert_cf_counts(db: Arc<TransactionDB>, mut asserts: HashMap<String, usize>) -> Result<()> {
        if !asserts.contains_key(IndexifyObjectsColumns::StateMachineMetadata.as_ref()) {
            asserts.insert(
                IndexifyObjectsColumns::StateMachineMetadata
                    .as_ref()
                    .to_string(),
                1,
            );
        }
        for col in IndexifyObjectsColumns::iter() {
            let cf = col.cf_db(&db);
            let count = db.iterator_cf(cf, IteratorMode::Start).count();
            let all = db
                .full_iterator_cf(cf, IteratorMode::Start)
                .filter_map(|result| result.ok())
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        String::from_utf8_lossy(&k),
                        String::from_utf8_lossy(&v)
                    )
                })
                .collect::<Vec<_>>();
            let expected_count = *asserts.get(col.as_ref()).unwrap_or(&0);
            assert_eq!(count, expected_count, "cf={} vals={:?}", col.as_ref(), all,);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_can_process_initial_graph_state_changes() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Invoke a simple graph
        test_state_store::with_simple_graph(&indexify_state).await;

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

        // Do the processing
        test_srv.process_all_state_changes().await?;

        // Should have 0 unprocessed state changes
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?;
        assert_eq!(
            unprocessed_state_changes.changes.len(),
            0,
            "{:#?}",
            unprocessed_state_changes
        );

        // And now, we should have an unallocated task
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_add_executor_allocates_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Invoke a simple graph
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // We should have an unallocated task
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        // Add an executor...
        test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // And now the task should be allocated
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_graph_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.task_allocations.len(), 1,);
            let task_allocation = desired_state.task_allocations.first().unwrap();
            executor
                .finalize_task(
                    &task_allocation,
                    FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                        .task_outcome(TaskOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;

            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 3,
                    allocated: 2,
                    unallocated: 0,
                    completed_success: 1,
                })
                .await?;
        }

        // finalize the remaining tasks
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.task_allocations.len(), 2,);

            for task_allocation in desired_state.task_allocations {
                executor
                    .finalize_task(
                        &task_allocation,
                        FinalizeTaskArgs::new(allocation_key_from_proto(&task_allocation))
                            .task_outcome(TaskOutcome::Success),
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

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.task_allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.task_allocations
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
    async fn test_graph_deletion_with_allocations() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register an executor
        test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;

        test_srv.process_all_state_changes().await?;

        // delete the graph...
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

        // verify that everything was deleted
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 0,
                allocated: 0,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        // This makes sure we never leak any data on deletion!
        assert_cf_counts(
            indexify_state.db.clone(),
            HashMap::from([
                (IndexifyObjectsColumns::GcUrls.as_ref().to_string(), 1), // input
            ]),
        )?;

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_deletion_after_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.task_allocations.len(),
                1,
                "Executor tasks: {:#?}",
                desired_state.task_allocations
            );
            let task_allocation = desired_state.task_allocations.first().unwrap();
            executor
                .finalize_task(
                    task_allocation,
                    FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                        .task_outcome(TaskOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;

            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 3,
                    allocated: 2,
                    unallocated: 0,
                    completed_success: 1,
                })
                .await?;
        }

        // finalize the remaining tasks
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.task_allocations.len(),
                2,
                "fn_b and fn_c tasks: {:#?}",
                desired_state.task_allocations
            );

            for task_allocation in desired_state.task_allocations {
                executor
                    .finalize_task(
                        &task_allocation,
                        FinalizeTaskArgs::new(allocation_key_from_proto(&task_allocation))
                            .task_outcome(TaskOutcome::Success)
                            .diagnostics(true, true),
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

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.task_allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.task_allocations
            );

            let invocation = indexify_state
                .reader()
                .invocation_ctx(TEST_NAMESPACE, "graph_A", &invocation_id)?
                .unwrap();

            assert!(invocation.completed);
        }

        // Delete graph and expect everything to be deleted
        {
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
            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 0,
                    allocated: 0,
                    unallocated: 0,
                    completed_success: 0,
                })
                .await?;

            // This makes sure we never leak any data on deletion!
            assert_cf_counts(
                indexify_state.db.clone(),
                HashMap::from([
                    (IndexifyObjectsColumns::GcUrls.as_ref().to_string(), 8), /* 1x input, 3x
                                                                               * output, 4x
                                                                               * diagnostics */
                ]),
            )?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_failure() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id = test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task with failure
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.task_allocations.len(),
                1,
                "{:#?}",
                desired_state.task_allocations
            );
            let task_allocation = desired_state.task_allocations.first().unwrap();

            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 1,
                    allocated: 1,
                    unallocated: 0,
                    completed_success: 0,
                })
                .await?;

            executor
                .finalize_task(
                    task_allocation,
                    FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                        .task_outcome(TaskOutcome::Failure(TaskFailureReason::FunctionError)),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 1,
                    allocated: 0,
                    unallocated: 0,
                    completed_success: 0,
                })
                .await?;

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.task_allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.task_allocations
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
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor1, task assigned to it
        let executor1 = {
            let executor1 = test_srv
                .create_executor(test_executor_metadata("executor_1".into()))
                .await?;
            test_srv.process_all_state_changes().await?;

            let desired_state = executor1.desired_state().await;
            assert_eq!(
                desired_state.task_allocations.len(),
                1,
                "Executor tasks: {:#?}",
                desired_state.task_allocations
            );

            executor1
        };

        // register executor2, no tasks assigned to it
        let executor2 = {
            let executor2 = test_srv
                .create_executor(test_executor_metadata("executor_2".into()))
                .await?;
            test_srv.process_all_state_changes().await?;

            let desired_state = executor2.desired_state().await;
            assert!(
                desired_state.task_allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.task_allocations
            );

            executor2
        };

        // verify tasks are reassigned to executor2 when executor1 deregisters
        {
            executor1.deregister().await?;

            test_srv.process_all_state_changes().await?;

            // verify that the tasks are still allocated
            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 1,
                    allocated: 1,
                    unallocated: 0,
                    completed_success: 0,
                })
                .await?;

            // verify that the tasks are reassigned to executor2
            let desired_state = executor2.desired_state().await;
            assert_eq!(
                desired_state.task_allocations.len(),
                1,
                "Executor tasks: {:#?}",
                desired_state.task_allocations
            );
        }

        // verify tasks are unallocated when executor2 deregisters
        {
            executor2.deregister().await?;

            test_srv.process_all_state_changes().await?;

            // verify that the tasks become unallocated
            test_srv
                .assert_task_states(TaskStateAssertions {
                    total: 1,
                    allocated: 0,
                    unallocated: 1,
                    completed_success: 0,
                })
                .await?;
        }

        Ok(())
    }

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
}
