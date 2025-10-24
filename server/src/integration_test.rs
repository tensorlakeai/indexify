#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use anyhow::Result;
    use strum::IntoEnumIterator;

    use crate::{
        assert_function_run_counts,
        data_model::{
            ApplicationState,
            FunctionRunFailureReason,
            FunctionRunOutcome,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_data_payload,
                mock_executor_metadata,
                mock_updates,
            },
        },
        executors::EXECUTOR_TIMEOUT,
        service::Service,
        state_store::{
            driver::{IterOptions, Reader, rocksdb::RocksDBDriver},
            requests::{
                CreateOrUpdateApplicationRequest,
                DeleteApplicationRequest,
                RequestPayload,
                StateMachineUpdateRequest,
            },
            state_machine::IndexifyObjectsColumns,
            test_state_store::{self, invoke_application},
        },
        testing::{self, FinalizeFunctionRunArgs, allocation_key_from_proto},
    };

    const TEST_FN_MAX_RETRIES: u32 = 3;

    fn assert_cf_counts(db: Arc<RocksDBDriver>, mut asserts: HashMap<String, usize>) -> Result<()> {
        if !asserts.contains_key(IndexifyObjectsColumns::StateMachineMetadata.as_ref()) {
            asserts.insert(
                IndexifyObjectsColumns::StateMachineMetadata
                    .as_ref()
                    .to_string(),
                1,
            );
        }
        for col in IndexifyObjectsColumns::iter() {
            let count_options = IterOptions::default();
            let count = db.iter(col.as_ref(), count_options).count();

            let scan_options = IterOptions::default().scan_fully();
            let all = db
                .iter(col.as_ref(), scan_options)
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
        test_state_store::with_simple_application(&indexify_state).await;

        // Should have 1 unprocessed state - one task created event
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?;
        assert_eq!(
            1,
            unprocessed_state_changes.changes.len(),
            "{unprocessed_state_changes:?}"
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
            "{unprocessed_state_changes:#?}"
        );

        // And now, we should have an unallocated task
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_executor_allocates_tasks() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Invoke a simple graph
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // We should have an unallocated task
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // Add an executor...
        test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // And now the task should be allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_app_request_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Invoke the app
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.allocations.len(), 1,);
            let allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(allocation),
                        Some(mock_updates()),
                        None,
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;

            assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);
        }

        // finalize tasks for fn_b and fn_c
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.allocations.len(), 2,);

            for allocation in desired_state.allocations {
                executor
                    .finalize_allocation(
                        &allocation,
                        FinalizeFunctionRunArgs::new(
                            allocation_key_from_proto(&allocation),
                            None,
                            Some(mock_data_payload()),
                        )
                        .function_run_outcome(FunctionRunOutcome::Success),
                    )
                    .await?;
            }

            test_srv.process_all_state_changes().await?;
        }
        // finalize task for fn_d
        {
            let desired_state = executor.desired_state().await;
            let task_allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    task_allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(task_allocation),
                        None,
                        Some(mock_data_payload()),
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            let function_runs = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .unwrap()
                .unwrap()
                .function_runs
                .values()
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(function_runs.len(), 4, "{function_runs:#?}");
            let successful_tasks = function_runs
                .into_iter()
                .filter(|t| t.outcome == Some(FunctionRunOutcome::Success))
                .collect::<Vec<_>>();
            assert_eq!(successful_tasks.len(), 4, "{successful_tasks:#?}");

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all allocations to be finalized: {:#?}",
                desired_state.allocations
            );

            let request_ctx = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)?
                .unwrap();

            assert!(request_ctx.outcome.is_some());
        }

        {
            let (allocation_usage, cursor) =
                indexify_state.reader().allocation_usage(None).unwrap();

            assert_eq!(allocation_usage.len(), 4, "{allocation_usage:#?}");
            assert!(cursor.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_deletion_with_allocations() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register an executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;

        test_srv.process_all_state_changes().await?;

        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.allocations.len(), 1,);
            let task_allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    task_allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(task_allocation),
                        None,
                        Some(mock_data_payload()),
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // delete the graph...
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "graph_A".to_string(),
                }),
            })
            .await?;

        test_srv.process_all_state_changes().await?;

        // verify that everything was deleted
        assert_function_run_counts!(test_srv, total: 0, allocated: 0, pending: 0, completed_success: 0);

        // This makes sure we never leak any data on deletion!
        assert_cf_counts(
            indexify_state.db.clone(),
            HashMap::from([
                (IndexifyObjectsColumns::GcUrls.as_ref().to_string(), 3), // input
                (
                    IndexifyObjectsColumns::AllocationUsage.as_ref().to_string(),
                    1,
                ), // one per allocation
            ]),
        )?;

        Ok(())
    }

    #[tokio::test]
    async fn test_application_deletion_after_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the app
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                1,
                "Executor tasks: {:#?}",
                desired_state.allocations
            );
            let task_allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    task_allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(task_allocation),
                        Some(mock_updates()),
                        None,
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;

            test_srv.process_all_state_changes().await?;

            assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);
        }

        // finalize the remaining allocs
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                2,
                "fn_b and fn_c allocations: {:#?}",
                desired_state.allocations
            );

            for allocation in desired_state.allocations {
                executor
                    .finalize_allocation(
                        &allocation,
                        FinalizeFunctionRunArgs::new(
                            allocation_key_from_proto(&allocation),
                            None,
                            Some(mock_data_payload()),
                        )
                        .function_run_outcome(FunctionRunOutcome::Success),
                    )
                    .await?;
            }

            test_srv.process_all_state_changes().await?;
        }
        {
            let desired_state = executor.desired_state().await;
            let task_allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    task_allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(task_allocation),
                        None,
                        Some(mock_data_payload()),
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            let function_runs = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .unwrap()
                .unwrap()
                .function_runs
                .values()
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(function_runs.len(), 4, "{function_runs:#?}");
            let successful_tasks = function_runs
                .into_iter()
                .filter(|t| t.outcome == Some(FunctionRunOutcome::Success))
                .collect::<Vec<_>>();
            assert_eq!(successful_tasks.len(), 4, "{successful_tasks:#?}");

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.allocations
            );

            let request_ctx = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)?
                .unwrap();

            assert!(request_ctx.outcome.is_some());
        }

        // Delete graph and expect everything to be deleted
        {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        name: "graph_A".to_string(),
                    }),
                })
                .await?;

            test_srv.process_all_state_changes().await?;
            assert_function_run_counts!(test_srv, total: 0, allocated: 0, pending: 0, completed_success: 0);

            // This makes sure we never leak any data on deletion!
            assert_cf_counts(
                indexify_state.db.clone(),
                HashMap::from([
                    (IndexifyObjectsColumns::GcUrls.as_ref().to_string(), 7), /* 1x input, 3x
                                                                               * output */
                    (
                        IndexifyObjectsColumns::AllocationUsage.as_ref().to_string(),
                        4,
                    ), // one per allocation
                ]),
            )?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_app_failure_on_request_error() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the application
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task with failure
        {
            // first, verify the executor state and task states
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                1,
                "{:#?}",
                desired_state.allocations
            );

            assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

            let allocation = desired_state.allocations.first().unwrap();

            // NB RequestError is a user request for a permanent failure.
            executor
                .finalize_allocation(
                    allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(allocation),
                        None,
                        None,
                        //"fn_b".to_string(),
                    )
                    .function_run_outcome(FunctionRunOutcome::Failure(
                        FunctionRunFailureReason::RequestError,
                    )),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // check for completion
        {
            assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.allocations
            );

            let request_ctx = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)?
                .unwrap();

            assert!(request_ctx.outcome.is_some());
        }

        Ok(())
    }

    async fn test_function_run_retry_attempt_used(
        reason: FunctionRunFailureReason,
        max_retries: u32,
    ) -> Result<()> {
        assert!(reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Invoke the app
        let request_id =
            test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // track the attempt number
        let mut attempt_number: u32 = 0;

        // validate the initial task retry attempt number
        {
            let tasks = test_srv.get_all_function_runs().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // loop over retries
        while attempt_number <= max_retries {
            // finalize the starting node task with our retryable failure (using an attempt)
            {
                let desired_state = executor.desired_state().await;
                let task_allocation = desired_state.allocations.first().unwrap();

                executor
                    .finalize_allocation(
                        task_allocation,
                        FinalizeFunctionRunArgs::new(
                            allocation_key_from_proto(task_allocation),
                            None,
                            None,
                        )
                        .function_run_outcome(FunctionRunOutcome::Failure(reason)),
                    )
                    .await?;

                test_srv.process_all_state_changes().await?;
            }

            // validate the task retry attempt number was incremented
            // if it was less than the retry max
            if attempt_number < max_retries {
                let function_runs = test_srv.get_all_function_runs().await?;
                assert_eq!(1, function_runs.len());
                println!("function_runs: {:?}", function_runs);
                assert_eq!(
                    attempt_number + 1,
                    function_runs.first().unwrap().attempt_number
                );
            }

            attempt_number += 1;
        }

        // check for completion
        {
            assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

            let desired_state = executor.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all allocations to be finalized: {:#?}",
                desired_state.allocations
            );

            let request_ctx = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)?
                .unwrap();

            assert!(request_ctx.outcome.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_task_retry_attempt_used_on_internal_error() -> Result<()> {
        test_function_run_retry_attempt_used(
            FunctionRunFailureReason::InternalError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_task_retry_attempt_used_on_internal_error_no_retries() -> Result<()> {
        test_function_run_retry_attempt_used(FunctionRunFailureReason::InternalError, 0).await
    }

    #[tokio::test]
    async fn test_task_retry_attempt_used_on_function_error() -> Result<()> {
        test_function_run_retry_attempt_used(
            FunctionRunFailureReason::FunctionError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_task_retry_attempt_used_on_function_error_no_retries() -> Result<()> {
        test_function_run_retry_attempt_used(FunctionRunFailureReason::FunctionError, 0).await
    }

    #[tokio::test]
    async fn test_task_retry_attempt_used_on_function_timeout() -> Result<()> {
        test_function_run_retry_attempt_used(
            FunctionRunFailureReason::FunctionTimeout,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_function_run_retry_attempt_used_on_function_timeout_no_retries() -> Result<()> {
        test_function_run_retry_attempt_used(FunctionRunFailureReason::FunctionTimeout, 0).await
    }

    async fn test_function_run_retry_attempt_not_used(
        reason: FunctionRunFailureReason,
        max_retries: u32,
    ) -> Result<()> {
        assert!(!reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the application
        test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // make sure the task is allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // track the attempt number
        let attempt_number: u32 = 0;

        // validate the initial task retry attempt number
        {
            let tasks = test_srv.get_all_function_runs().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // finalize the starting node task with our retryable failure (not using an
        // attempt)
        {
            let desired_state = executor.desired_state().await;
            let task_allocation = desired_state.allocations.first().unwrap();

            executor
                .finalize_allocation(
                    task_allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(task_allocation),
                        None,
                        None,
                    )
                    .function_run_outcome(FunctionRunOutcome::Failure(reason)),
                )
                .await?;

            test_srv.process_all_state_changes().await?;
        }

        // validate the task retry attempt number was not changed
        {
            let tasks = test_srv.get_all_function_runs().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // make sure the task is still allocated iff the reason is retriable.
        if reason.is_retriable() {
            assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);
        } else {
            assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_function_run_retry_attempt_not_used_on_function_executor_terminated() -> Result<()>
    {
        test_function_run_retry_attempt_not_used(
            FunctionRunFailureReason::FunctionExecutorTerminated,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_function_run_retry_attempt_not_used_on_function_executor_terminated_no_retries()
    -> Result<()> {
        test_function_run_retry_attempt_not_used(
            FunctionRunFailureReason::FunctionExecutorTerminated,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_executor_task_reassignment() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor1, task assigned to it
        let executor1 = {
            let executor1 = test_srv
                .create_executor(mock_executor_metadata("executor_1".into()))
                .await?;
            test_srv.process_all_state_changes().await?;

            let desired_state = executor1.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                1,
                "Executor tasks: {:#?}",
                desired_state.allocations
            );

            executor1
        };

        // register executor2, no tasks assigned to it
        let mut executor2 = {
            let executor2 = test_srv
                .create_executor(mock_executor_metadata("executor_2".into()))
                .await?;
            test_srv.process_all_state_changes().await?;

            let desired_state = executor2.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all tasks to be finalized: {:#?}",
                desired_state.allocations
            );

            executor2
        };

        // simulate network partition by pausing time and advancing it
        {
            // pause time to control clock advancement
            tokio::time::pause();

            // advance time to almost the EXECUTOR_TIMEOUT
            tokio::time::advance(EXECUTOR_TIMEOUT - std::time::Duration::from_secs(1)).await;

            // heartbeat executor2 to ensure it's still considered alive
            executor2
                .heartbeat(executor2.executor_metadata.clone())
                .await?;
            test_srv.process_all_state_changes().await?;

            // verify that both executors are still alive and tasks are still on executor1
            assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);
            let desired_state = executor1.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                1,
                "Executor1 tasks: {:#?}",
                desired_state.allocations
            );
            let desired_state = executor2.desired_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "Executor2 tasks: {:#?}",
                desired_state.allocations
            );

            // advance time past the EXECUTOR_TIMEOUT to trigger executor1 timeout
            tokio::time::advance(std::time::Duration::from_secs(2)).await;

            // process lapsed executors to trigger reassignment
            test_srv
                .service
                .executor_manager
                .process_lapsed_executors()
                .await?;
            test_srv.process_all_state_changes().await?;

            // verify that the tasks are still allocated but moved to executor2
            assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

            // verify that the tasks are reassigned to executor2
            let desired_state = executor2.desired_state().await;
            assert_eq!(
                desired_state.allocations.len(),
                1,
                "Executor2 tasks: {:#?}",
                desired_state.allocations
            );

            // resume time
            tokio::time::resume();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_read_and_delete_application() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();
        let _ = test_state_store::with_simple_application(&indexify_state).await;

        let (applications, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_applications(TEST_NAMESPACE, None, None)
            .unwrap();

        // Check if the application was created
        assert!(applications.iter().any(|cg| cg.name == "graph_A"));

        // Delete the application graph
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "graph_A".to_string(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Read the application graph again
        let (applications, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_applications(TEST_NAMESPACE, None, None)
            .unwrap();

        // Check if the application was deleted
        assert!(!applications.iter().any(|cg| cg.name == "graph_A"));

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_app() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // create the application
        let mut app = test_state_store::create_or_update_application(&indexify_state, 0).await;
        assert_eq!(ApplicationState::Active, app.state);

        app.state = ApplicationState::Disabled {
            reason: "disabled in test".to_string(),
        };

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: app.namespace.clone(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: true,
                    },
                )),
            })
            .await?;

        let result = invoke_application(&indexify_state, &app).await;
        let err = result.unwrap_err();
        assert_eq!(
            "Application is not enabled: disabled in test",
            err.to_string()
        );

        Ok(())
    }
}
