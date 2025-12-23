#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use anyhow::Result;
    use strum::IntoEnumIterator;

    use crate::{
        assert_function_run_counts,
        data_model::{
            ApplicationState,
            FunctionRunFailureReason,
            FunctionRunOutcome,
            FunctionRunStatus,
            RequestFailureReason,
            RequestOutcome,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_data_payload,
                mock_executor_metadata,
                mock_updates,
            },
        },
        executors::EXECUTOR_TIMEOUT,
        state_store::{
            driver::{self, IterOptions, Reader, rocksdb::RocksDBDriver},
            requests::{
                CreateOrUpdateApplicationRequest,
                DeleteApplicationRequest,
                DeleteRequestRequest,
                RequestPayload,
                StateMachineUpdateRequest,
            },
            state_machine::IndexifyObjectsColumns,
            test_state_store::{self, invoke_application, invoke_application_with_request_id},
        },
        testing::{self, FinalizeFunctionRunArgs, allocation_key_from_proto},
    };

    const TEST_FN_MAX_RETRIES: u32 = 3;

    async fn assert_cf_counts(
        db: Arc<RocksDBDriver>,
        mut asserts: HashMap<String, usize>,
    ) -> Result<()> {
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
            let count = db.iter(col.as_ref(), count_options).await.count();

            let scan_options = IterOptions::default().scan_fully();
            let all = db
                .iter(col.as_ref(), scan_options)
                .await
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
        let indexify_state = test_srv.service.indexify_state.clone();

        // Invoke a simple graph
        test_state_store::with_simple_application(&indexify_state).await;

        // Should have 1 unprocessed state - one task created event
        let unprocessed_state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)
            .await?;
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
            .unprocessed_state_changes(&None, &None)
            .await?;
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
        let indexify_state = test_srv.service.indexify_state.clone();

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
        let indexify_state = test_srv.service.indexify_state.clone();

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
                .await?
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
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
                .unwrap();

            assert!(request_ctx.outcome.is_some());
        }

        {
            let (allocation_usage, cursor) = indexify_state
                .reader()
                .allocation_usage(None)
                .await
                .unwrap();

            assert_eq!(allocation_usage.len(), 4, "{allocation_usage:#?}");
            assert!(cursor.is_none());
        }

        // Tombstone the request
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneRequest(DeleteRequestRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    application: "graph_A".to_string(),
                    request_id: request_id.clone(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // verify the request was deleted
        let request_ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?;
        assert!(request_ctx.is_none());

        let all_function_runs = test_srv.get_all_function_runs().await?;
        assert_eq!(0, all_function_runs.len());
        let all_allocations = indexify_state
            .reader()
            .get_allocations_by_request_id(TEST_NAMESPACE, "graph_A", &request_id)
            .await
            .unwrap();
        assert_eq!(0, all_allocations.len());

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_deletion_with_allocations() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

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

        // Drain request state change events before checking column counts
        test_srv.drain_request_state_change_events().await?;

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
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_application_deletion_after_completion() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

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
                .await?
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
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
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

            // Drain request state change events before checking column counts
            test_srv.drain_request_state_change_events().await?;

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
            )
            .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_app_failure_on_request_error() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

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
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
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
        let indexify_state = test_srv.service.indexify_state.clone();

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
                        .function_run_outcome(FunctionRunOutcome::Failure(reason.clone())),
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
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
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
        let indexify_state = test_srv.service.indexify_state.clone();

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
                    .function_run_outcome(FunctionRunOutcome::Failure(reason.clone())),
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
        let indexify_state = test_srv.service.indexify_state.clone();

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
        let indexify_state = test_srv.service.indexify_state.clone();
        let _ = test_state_store::with_simple_application(&indexify_state).await;

        let (applications, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_applications(TEST_NAMESPACE, None, None)
            .await?;

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
            .await?;

        // Check if the application was deleted
        assert!(!applications.iter().any(|cg| cg.name == "graph_A"));

        Ok(())
    }

    #[tokio::test]
    async fn test_disabling_app() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // create the application
        let mut app =
            test_state_store::create_or_update_application(&indexify_state, "graph_A", 0).await;
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

    #[tokio::test]
    async fn test_tombstone_application() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // create the application without invoking
        test_state_store::create_or_update_application(&indexify_state, "app_1", 0).await;

        // verify the application was created
        let (applications, _) = test_srv
            .service
            .indexify_state
            .reader()
            .list_applications(TEST_NAMESPACE, None, None)
            .await?;
        assert_eq!(1, applications.len());
        assert_eq!("app_1", applications[0].name);

        // tombstone the application
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "app_1".to_string(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // verify the application was tombstoned (deleted)
        let application = test_srv
            .service
            .indexify_state
            .reader()
            .get_application(TEST_NAMESPACE, "app_1")
            .await?;
        assert!(application.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_tombstone_request() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // invoke the application to create a request
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // verify the request was created
        let request_ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?;
        assert!(request_ctx.is_some());

        // We should have an unallocated task
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // tombstone the request
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneRequest(DeleteRequestRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    application: "graph_A".to_string(),
                    request_id: request_id.clone(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // verify the request was deleted
        let request_ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?;
        assert!(request_ctx.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_upgrade_application_and_requests() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        let new_app = test_state_store::mock_application("graph_A", "2");

        let request = CreateOrUpdateApplicationRequest {
            namespace: TEST_NAMESPACE.to_string(),
            application: new_app,
            upgrade_requests_to_current_version: true,
        };
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(request)),
            })
            .await
            .unwrap();
        test_srv.process_all_state_changes().await?;

        let request_ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        let versions = request_ctx
            .function_runs
            .values()
            .map(|r| r.version.clone())
            .collect::<HashSet<_>>();
        assert_eq!(1, versions.len());
        assert_eq!("2", versions.iter().next().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_request_failure_reason_out_of_memory() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Invoke the app
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // finalize the starting node task with OutOfMemory failure
        let desired_state = executor.desired_state().await;
        assert_eq!(desired_state.allocations.len(), 1);
        let allocation = desired_state.allocations.first().unwrap();
        executor
            .finalize_allocation(
                allocation,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(allocation),
                    Some(mock_updates()),
                    None,
                )
                .function_run_outcome(FunctionRunOutcome::Failure(
                    FunctionRunFailureReason::OutOfMemory,
                )),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // check that the request outcome is Failure(OutOfMemory)
        let request_ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();

        assert_eq!(
            request_ctx.outcome,
            Some(RequestOutcome::Failure(RequestFailureReason::OutOfMemory))
        );

        // check that function_runs have the same failure reason
        let function_runs = request_ctx
            .function_runs
            .values()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(function_runs.len(), 1);
        let function_run = &function_runs[0];
        assert_eq!(
            function_run.outcome,
            Some(FunctionRunOutcome::Failure(
                FunctionRunFailureReason::OutOfMemory
            ))
        );

        // check that allocations have the same failure reason
        let allocations = indexify_state
            .reader()
            .get_allocations_by_request_id(TEST_NAMESPACE, "graph_A", &request_id)
            .await?;
        assert_eq!(allocations.len(), 1);
        let allocation = &allocations[0];
        assert_eq!(
            allocation.outcome,
            FunctionRunOutcome::Failure(FunctionRunFailureReason::OutOfMemory)
        );

        Ok(())
    }

    /// Test that request state change events are persisted to RocksDB
    /// and contain the expected event types as the request progresses
    #[tokio::test]
    async fn test_request_state_change_events_persistence() -> Result<()> {
        use crate::state_store::request_events::RequestStateChangeEvent;

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no request state change events should exist
        let (events, _) = indexify_state
            .reader()
            .request_state_change_events(None)
            .await?;
        assert!(events.is_empty(), "Expected no events initially");

        // Invoke the application - this should generate RequestStarted event
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Check that RequestStarted event was persisted
        let (events, _) = indexify_state
            .reader()
            .request_state_change_events(None)
            .await?;
        assert!(
            !events.is_empty(),
            "Expected RequestStarted event to be persisted"
        );

        let has_request_started = events.iter().any(|e| {
            matches!(e.event, RequestStateChangeEvent::RequestStarted(_)) &&
                e.event.request_id() == request_id
        });
        assert!(
            has_request_started,
            "Expected RequestStarted event for request_id {}",
            request_id
        );

        // Register executor and process - this should generate FunctionRunAssigned
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        let (events, _) = indexify_state
            .reader()
            .request_state_change_events(None)
            .await?;

        let has_function_run_assigned = events.iter().any(|e| {
            matches!(e.event, RequestStateChangeEvent::FunctionRunAssigned(_)) &&
                e.event.request_id() == request_id
        });
        assert!(
            has_function_run_assigned,
            "Expected FunctionRunAssigned event for request_id {}",
            request_id
        );

        let has_function_run_created = events.iter().any(|e| {
            matches!(e.event, RequestStateChangeEvent::FunctionRunCreated(_)) &&
                e.event.request_id() == request_id
        });
        assert!(
            has_function_run_created,
            "Expected FunctionRunCreated event for request_id {}",
            request_id
        );

        // Finalize the first allocation
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.allocations.len(), 1);
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
        }

        // Check that FunctionRunCompleted event was persisted
        let (events, _) = indexify_state
            .reader()
            .request_state_change_events(None)
            .await?;

        let has_function_run_completed = events.iter().any(|e| {
            matches!(e.event, RequestStateChangeEvent::FunctionRunCompleted(_)) &&
                e.event.request_id() == request_id
        });
        assert!(
            has_function_run_completed,
            "Expected FunctionRunCompleted event for request_id {}",
            request_id
        );

        // Finalize remaining allocations (fn_b and fn_c)
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(desired_state.allocations.len(), 2);

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

        // Finalize fn_d
        {
            let desired_state = executor.desired_state().await;
            let allocation = desired_state.allocations.first().unwrap();
            executor
                .finalize_allocation(
                    allocation,
                    FinalizeFunctionRunArgs::new(
                        allocation_key_from_proto(allocation),
                        None,
                        Some(mock_data_payload()),
                    )
                    .function_run_outcome(FunctionRunOutcome::Success),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        // Check that RequestFinished event was persisted
        let (events, _) = indexify_state
            .reader()
            .request_state_change_events(None)
            .await?;

        let has_request_finished = events.iter().any(|e| {
            matches!(e.event, RequestStateChangeEvent::RequestFinished(_)) &&
                e.event.request_id() == request_id
        });
        assert!(
            has_request_finished,
            "Expected RequestFinished event for request_id {}",
            request_id
        );

        // Verify all expected event types are present
        let event_types: HashSet<String> = events
            .iter()
            .filter(|e| e.event.request_id() == request_id)
            .map(|e| e.event.message().to_string())
            .collect();

        assert!(
            event_types.contains("Request Started"),
            "Missing 'Request Started' event type"
        );
        assert!(
            event_types.contains("Function Run Created"),
            "Missing 'Function Run Created' event type"
        );
        assert!(
            event_types.contains("Function Run Assigned"),
            "Missing 'Function Run Assigned' event type"
        );
        assert!(
            event_types.contains("Function Run Completed"),
            "Missing 'Function Run Completed' event type"
        );
        assert!(
            event_types.contains("Request Finished"),
            "Missing 'Request Finished' event type"
        );

        // Verify events are ordered by ID (monotonically increasing)
        let request_events: Vec<_> = events
            .iter()
            .filter(|e| e.event.request_id() == request_id)
            .collect();

        for i in 1..request_events.len() {
            assert!(
                request_events[i].id.value() > request_events[i - 1].id.value(),
                "Events should be monotonically ordered by ID"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_executor_deregister_retries_function_run() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Set up a simple application and invoke it
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register an executor - this should allocate the function run
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify the function run is allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Get the allocation details before deregistering
        let desired_state = executor.desired_state().await;
        assert_eq!(desired_state.allocations.len(), 1);
        let allocation = desired_state.allocations.first().unwrap();
        let allocation_key = allocation_key_from_proto(allocation);

        // Get the function run status - should be Running
        let request_ctx_before = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        let function_run_before = request_ctx_before.function_runs.values().next().unwrap();
        assert!(
            matches!(function_run_before.status, FunctionRunStatus::Running(_)),
            "Expected Running status, got {:?}",
            function_run_before.status
        );

        // Now deregister the executor while the allocation is running
        // This simulates the executor going away (e.g., spot instance terminated)
        executor.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // The allocation should be marked as terminal (failed)
        let allocation_after = indexify_state
            .reader()
            .get_allocation(&allocation_key)
            .await?
            .unwrap();
        assert!(
            allocation_after.is_terminal(),
            "Allocation should be terminal after executor deregistration"
        );
        assert!(
            matches!(
                allocation_after.outcome,
                FunctionRunOutcome::Failure(FunctionRunFailureReason::ExecutorRemoved)
            ),
            "Expected ExecutorRemoved failure, got {:?}",
            allocation_after.outcome
        );

        // BUG: The function run should be Pending (ready for retry), but it's stuck in
        // Running
        let request_ctx_after = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        let function_run_after = request_ctx_after.function_runs.values().next().unwrap();

        // This is what we WANT (correct behavior) - function run should be retried
        assert!(
            matches!(function_run_after.status, FunctionRunStatus::Pending),
            "BUG: Function run should be Pending for retry after executor deregistration, \
             but got {:?}. This means the function run is stuck and the request will never complete.",
            function_run_after.status
        );

        // Verify the request can eventually complete with a new executor
        let executor2 = test_srv
            .create_executor(mock_executor_metadata("executor_2".into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // The pending function run should be reallocated to the new executor
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Complete the function run
        let desired_state = executor2.desired_state().await;
        assert_eq!(
            desired_state.allocations.len(),
            1,
            "New executor should have the retried allocation"
        );
        let new_allocation = desired_state.allocations.first().unwrap();
        executor2
            .finalize_allocation(
                new_allocation,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(new_allocation),
                    Some(mock_updates()),
                    None,
                )
                .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // The request should now be progressing
        assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_server_restart_executor_cleanup_retries_function_run() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Set up a simple application and invoke it
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register an executor - this should allocate the function run
        let executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify the function run is allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Get the allocation details
        let desired_state = executor.desired_state().await;
        assert_eq!(desired_state.allocations.len(), 1);
        let allocation = desired_state.allocations.first().unwrap();
        let allocation_key = allocation_key_from_proto(allocation);

        // Get the function run status - should be Running
        let request_ctx_before = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        let function_run_before = request_ctx_before.function_runs.values().next().unwrap();
        assert!(
            matches!(function_run_before.status, FunctionRunStatus::Running(_)),
            "Expected Running status, got {:?}",
            function_run_before.status
        );

        // SIMULATE SERVER RESTART:
        // Clear executor state but keep allocations (as if loaded from DB)
        // NOTE: We must write to the original in_memory_state, not a clone!
        {
            indexify_state
                .in_memory_state
                .write()
                .await
                .simulate_server_restart_clear_executor_state();
        }

        // Now deregister the executor - this will take the FALLBACK path
        // because executor_server_metadata is None (cleared above)
        executor.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // The allocation should be marked as terminal (failed)
        let allocation_after = indexify_state
            .reader()
            .get_allocation(&allocation_key)
            .await?
            .unwrap();
        assert!(
            allocation_after.is_terminal(),
            "Allocation should be terminal after executor deregistration"
        );
        assert!(
            matches!(
                allocation_after.outcome,
                FunctionRunOutcome::Failure(FunctionRunFailureReason::ExecutorRemoved)
            ),
            "Expected ExecutorRemoved failure, got {:?}",
            allocation_after.outcome
        );

        // CRITICAL: The function run should be Pending (ready for retry)
        // Before the fix, this was stuck in Running due to &mut function_run.clone()
        // bug
        let request_ctx_after = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        let function_run_after = request_ctx_after.function_runs.values().next().unwrap();

        assert!(
            matches!(function_run_after.status, FunctionRunStatus::Pending),
            "Function run should be Pending for retry after server restart + executor cleanup, \
             but got {:?}. This is the bug that was fixed.",
            function_run_after.status
        );

        // Verify the request can eventually complete with a new executor
        let _executor2 = test_srv
            .create_executor(mock_executor_metadata("executor_2".into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // The pending function run should be reallocated to the new executor
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_idempotent_request_creation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        let app =
            test_state_store::create_or_update_application(&indexify_state, "test_app", 0).await;

        // First request with a specific request_id should succeed
        let request_id = "my-unique-request-id-123";
        let result = invoke_application_with_request_id(&indexify_state, &app, request_id).await;
        assert!(result.is_ok(), "First request should succeed");
        assert_eq!(result.unwrap(), request_id);

        // Second request with the same request_id should fail with
        // RequestAlreadyExistsError
        let result = invoke_application_with_request_id(&indexify_state, &app, request_id).await;
        assert!(result.is_err(), "Duplicate request should fail");

        let err = result.unwrap_err();
        let driver_error = err.downcast_ref::<driver::Error>().unwrap();
        assert!(
            driver_error.is_request_already_exists(),
            "Error should be driver::Error::RequestAlreadyExists, got: {}",
            driver_error
        );

        // Request with a different request_id should succeed
        let different_request_id = "another-unique-id-456";
        let result =
            invoke_application_with_request_id(&indexify_state, &app, different_request_id).await;
        assert!(
            result.is_ok(),
            "Request with different ID should succeed: {:?}",
            result.err()
        );
        Ok(())
    }
}
