#[cfg(test)]
mod tests {

    use anyhow::Result;

    use crate::{
        assert_executor_state,
        assert_function_run_counts,
        data_model::{
            ContainerState,
            FunctionAllowlist,
            FunctionExecutorTerminationReason,
            FunctionRunFailureReason,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_executor_metadata,
                mock_updates,
            },
        },
        state_store::test_state_store,
        testing::{self, TestExecutor},
    };

    const TEST_FN_MAX_RETRIES: u32 = 3;

    #[tokio::test]
    async fn test_dev_mode_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // Register executor in dev mode - task should be allocated
        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Finalize task - the new tasks should also be allocated
        let commands = executor.recv_commands().await;
        assert_eq!(1, commands.run_allocations.len());
        let task_allocation = &commands.run_allocations[0];
        executor
            .report_command_responses(vec![TestExecutor::make_allocation_completed(
                task_allocation,
                Some(mock_updates()),
                None,
                Some(1000),
            )])
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_allowlist_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // Register executor with non-dev mode and specific allowlist
        let mut executor_meta = mock_executor_metadata(TEST_EXECUTOR_ID.into());
        executor_meta.function_allowlist = Some(vec![FunctionAllowlist {
            namespace: Some(TEST_NAMESPACE.to_string()),
            application: Some("graph_A".to_string()),
            function: Some("fn_a".to_string()),
        }]);

        let mut executor = test_srv.create_executor(executor_meta).await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Finalize task - new tasks should be allocated for b and c functions
        let commands = executor.recv_commands().await;
        assert_eq!(1, commands.run_allocations.len());
        let task_allocation = &commands.run_allocations[0];
        executor
            .report_command_responses(vec![TestExecutor::make_allocation_completed(
                task_allocation,
                Some(mock_updates()),
                None,
                Some(1000),
            )])
            .await?;
        test_srv.process_all_state_changes().await?;

        // Tasks for fn_b and fn_c should be created but unallocated since they're not
        // in allowlist
        assert_function_run_counts!(test_srv, total: 3, allocated: 0, pending: 2, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 0); // Still has fn_a executor, no tasks allocated

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_executor_allocation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a task
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register first executor with no allowlist
        let mut executor1_meta = mock_executor_metadata("executor_1".into());
        executor1_meta.function_allowlist = None;

        let executor1 = test_srv.create_executor(executor1_meta).await?;
        test_srv.process_all_state_changes().await?;

        // Function run should be allocated to the first executor
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Register second executor
        let executor2 = test_srv
            .create_executor(mock_executor_metadata("executor_2".into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_executor_state!(executor1, num_func_executors: 1, num_allocated_tasks: 1);

        assert_executor_state!(executor2, num_func_executors: 0, num_allocated_tasks: 0);

        // When the first executor is deregistered, task should be allocated to the
        // second executor
        executor1.deregister().await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor2, num_func_executors: 1, num_allocated_tasks: 1);

        // Deregister second executor
        executor2.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // Task should be unallocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_function_executor_add_remove() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a task
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register executor in dev mode
        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify initial state - fn_a task should be allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Complete the task to create fn_b and fn_c tasks
        {
            let commands = executor.recv_commands().await;
            assert_eq!(1, commands.run_allocations.len());
            let task_allocation = &commands.run_allocations[0];
            executor
                .report_command_responses(vec![TestExecutor::make_allocation_completed(
                    task_allocation,
                    Some(mock_updates()),
                    None,
                    Some(1000),
                )])
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 3, num_allocated_tasks: 2); // Should have fn_a, fn_b, fn_c and fn_b, fn_c tasks

        executor.mark_function_executors_as_running().await?;

        // Remove fn_a from function executors
        {
            let mut fes: Vec<crate::data_model::Container> = executor
                .get_executor_server_state()
                .await?
                .containers
                .into_values()
                .collect();
            for fe in fes.iter_mut() {
                if fe.function_name == "fn_a" {
                    fe.state = ContainerState::Terminated {
                        reason: FunctionExecutorTerminationReason::FunctionCancelled,
                    };
                }
            }
            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        // Should still have fn_b and fn_c tasks allocated
        assert_function_run_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 0);

        // The FE for fn_a should be removed
        let executor_server_state = executor.get_executor_server_state().await?;
        assert!(
            executor_server_state
                .containers
                .iter()
                .all(|(_id, fe)| { fe.function_name != "fn_a" })
        );

        Ok(())
    }

    /// Test that the v2 protocol path (AllocationFailed via
    /// report_command_responses) correctly applies the retry policy: each
    /// failure counts against retries, and after exhausting max_retries the
    /// function run completes with failure.
    async fn test_function_executor_retry_attempt_used(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason = FunctionRunFailureReason::from(&reason);
        assert!(task_failure_reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // invoke the app
        let request_id =
            test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // make sure the task is allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // track the attempt number
        let mut attempt_number: u32 = 0;

        // validate the initial task retry attempt number
        {
            let function_runs = test_srv.get_all_function_runs().await?;
            assert_eq!(1, function_runs.len());
            assert_eq!(
                attempt_number,
                function_runs.first().unwrap().attempt_number
            );
        }

        // loop over retries — fail allocations via the v2 protocol path
        while attempt_number <= max_retries {
            // Receive the RunAllocation command from the command stream,
            // just like a production dataplane would.
            let commands = executor.recv_commands().await;
            assert_eq!(
                1,
                commands.run_allocations.len(),
                "expected exactly one RunAllocation command"
            );
            let allocation = &commands.run_allocations[0];

            // Respond with AllocationFailed via report_command_responses.
            // This is the v2 protocol path production dataplanes use.
            executor
                .report_command_responses(vec![TestExecutor::make_allocation_failed(
                    allocation,
                    task_failure_reason.clone(),
                    None,
                    None,
                )])
                .await?;
            test_srv.process_all_state_changes().await?;

            // validate the task retry attempt number was incremented
            // if it was less than the retry max
            if attempt_number < max_retries {
                let function_runs = test_srv.get_all_function_runs().await?;
                assert_eq!(1, function_runs.len());
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

            let desired_state = executor.srv_executor_state().await;
            assert!(
                desired_state.allocations.is_empty(),
                "expected all allocs to be finalized: {:#?}",
                desired_state.allocations
            );

            let request = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
                .unwrap();

            assert!(request.outcome.is_some());
        }

        Ok(())
    }

    // StartupFailedInternalError maps to ContainerStartupInternalError, which
    // is retriable but does NOT count against retry attempts (free retry).
    // The function run is always re-queued without incrementing attempt_number.
    #[tokio::test]
    async fn test_fe_free_retry_on_startup_failed_internal_error() -> Result<()> {
        test_container_termination_free_retry(
            FunctionExecutorTerminationReason::StartupFailedInternalError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_free_retry_on_startup_failed_internal_error_no_retries() -> Result<()> {
        test_container_termination_free_retry(
            FunctionExecutorTerminationReason::StartupFailedInternalError,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_startup_failed_function_error() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::StartupFailedFunctionError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_startup_failed_function_error_no_retries() -> Result<()>
    {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::StartupFailedFunctionError,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_startup_failed_function_timeout() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::StartupFailedFunctionTimeout,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_startup_failed_function_timeout_no_retries() -> Result<()>
    {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::StartupFailedFunctionTimeout,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_unhealthy() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::Unhealthy,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_unhealthy_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(FunctionExecutorTerminationReason::Unhealthy, 0)
            .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_internal_error() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::InternalError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_internal_error_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::InternalError,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_function_error() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_function_error_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionError,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_function_timeout() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionTimeout,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_function_timeout_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionTimeout,
            0,
        )
        .await
    }

    /// Test container termination for reasons that give free retries
    /// (retriable but don't count against retry attempts). The function run
    /// is always re-queued without incrementing attempt_number, regardless
    /// of max_retries.
    async fn test_container_termination_free_retry(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason = FunctionRunFailureReason::from(&reason);
        assert!(task_failure_reason.is_retriable());
        assert!(!task_failure_reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        {
            let tasks = test_srv.get_all_function_runs().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(0, tasks.first().unwrap().attempt_number);
        }

        // Terminate container — the actual termination reason is applied.
        // Since the reason doesn't count against retries, this is a free retry:
        // the function run is re-queued without incrementing attempt_number.
        executor
            .set_function_executor_states(ContainerState::Terminated { reason })
            .await?;

        // Free retry: attempt stays at 0, function run still allocated
        let tasks = test_srv.get_all_function_runs().await?;
        assert_eq!(1, tasks.len());
        assert_eq!(0, tasks.first().unwrap().attempt_number);
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        Ok(())
    }

    /// Test the reconciliation fallback path for non-retriable container
    /// termination reasons. When the server discovers a terminated container
    /// via heartbeat (set_function_executor_states), the allocation should
    /// fail immediately without retry.
    async fn test_container_termination_no_retry(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason = FunctionRunFailureReason::from(&reason);
        assert!(!task_failure_reason.is_retriable());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        executor
            .set_function_executor_states(ContainerState::Terminated { reason })
            .await?;

        let tasks = test_srv.get_all_function_runs().await?;
        assert_eq!(1, tasks.len());
        assert_eq!(0, tasks.first().unwrap().attempt_number);
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

        Ok(())
    }

    // Reconciliation path: FunctionCancelled is non-retriable, fails immediately.
    #[tokio::test]
    async fn test_reconciliation_no_retry_on_function_cancelled() -> Result<()> {
        test_container_termination_no_retry(
            FunctionExecutorTerminationReason::FunctionCancelled,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_reconciliation_no_retry_on_function_cancelled_no_retries() -> Result<()> {
        test_container_termination_no_retry(FunctionExecutorTerminationReason::FunctionCancelled, 0)
            .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_unknown() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::Unknown,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_unknown_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(FunctionExecutorTerminationReason::Unknown, 0)
            .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_desired_state_removed() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::DesiredStateRemoved,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_desired_state_removed_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::DesiredStateRemoved,
            0,
        )
        .await
    }

    /// Test that the v2 protocol path (AllocationFailed via
    /// report_command_responses) correctly handles free-retry failure
    /// reasons: the function run is re-queued without incrementing
    /// attempt_number, regardless of max_retries.
    async fn test_allocation_failed_free_retry(
        reason: FunctionRunFailureReason,
        max_retries: u32,
    ) -> Result<()> {
        assert!(reason.is_retriable());
        assert!(!reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Receive the allocation from the command stream.
        let commands = executor.recv_commands().await;
        assert_eq!(1, commands.run_allocations.len());
        let allocation = &commands.run_allocations[0];

        // Fail the allocation with a free-retry reason via the v2 protocol path.
        executor
            .report_command_responses(vec![TestExecutor::make_allocation_failed(
                allocation,
                reason.clone(),
                None,
                None,
            )])
            .await?;
        test_srv.process_all_state_changes().await?;

        // Free retry: attempt stays at 0, function run still allocated (re-queued).
        let function_runs = test_srv.get_all_function_runs().await?;
        assert_eq!(1, function_runs.len());
        assert_eq!(0, function_runs.first().unwrap().attempt_number);
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        Ok(())
    }

    /// Test the reconciliation fallback path for reasons that count against
    /// retry attempts. When the server discovers a terminated container via
    /// heartbeat (set_function_executor_states), the allocation should be
    /// retried with attempt_number incremented, and after exhausting
    /// max_retries the function run should complete with failure.
    async fn test_container_termination_retry_attempt_used(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason = FunctionRunFailureReason::from(&reason);
        assert!(task_failure_reason.should_count_against_function_run_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        let request_id =
            test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        let mut attempt_number: u32 = 0;

        {
            let function_runs = test_srv.get_all_function_runs().await?;
            assert_eq!(1, function_runs.len());
            assert_eq!(
                attempt_number,
                function_runs.first().unwrap().attempt_number
            );
        }

        // Loop over retries — terminate containers via the reconciliation path.
        while attempt_number <= max_retries {
            // Consume any commands (RunAllocation) from the stream so they
            // don't pile up.
            let _commands = executor.recv_commands().await;

            // Terminate the container via heartbeat reconciliation.
            executor
                .set_function_executor_states(ContainerState::Terminated {
                    reason: reason.clone(),
                })
                .await?;

            if attempt_number < max_retries {
                let function_runs = test_srv.get_all_function_runs().await?;
                assert_eq!(1, function_runs.len());
                assert_eq!(
                    attempt_number + 1,
                    function_runs.first().unwrap().attempt_number
                );
            }

            attempt_number += 1;
        }

        // Check for completion — retries exhausted, function run failed.
        {
            assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

            let request = indexify_state
                .reader()
                .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
                .await?
                .unwrap();

            assert!(request.outcome.is_some());
        }

        Ok(())
    }

    /// Test that the v2 protocol path (AllocationFailed via
    /// report_command_responses) correctly handles non-retriable failure
    /// reasons: the function run is completed with failure immediately,
    /// regardless of max_retries. No retry is attempted.
    async fn test_allocation_failed_no_retry(
        reason: FunctionRunFailureReason,
        max_retries: u32,
    ) -> Result<()> {
        assert!(!reason.is_retriable());

        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        let request_id =
            test_state_store::with_simple_retry_application(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Receive the allocation from the command stream.
        let commands = executor.recv_commands().await;
        assert_eq!(1, commands.run_allocations.len());
        let allocation = &commands.run_allocations[0];

        // Fail the allocation via the v2 protocol path with a non-retriable
        // reason. The server must NOT retry — the function run should complete
        // with failure immediately.
        executor
            .report_command_responses(vec![TestExecutor::make_allocation_failed(
                allocation,
                reason.clone(),
                None,
                None,
            )])
            .await?;
        test_srv.process_all_state_changes().await?;

        // The function run should be done (not pending, not allocated).
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

        // attempt_number should still be 0 — no retry was attempted.
        let function_runs = test_srv.get_all_function_runs().await?;
        assert_eq!(1, function_runs.len());
        assert_eq!(0, function_runs.first().unwrap().attempt_number);

        // The request should be complete.
        let request = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)
            .await?
            .unwrap();
        assert!(request.outcome.is_some());

        Ok(())
    }

    // FunctionCancelled maps to FunctionRunCancelled, which is NOT retriable.
    // The function run is completed with failure immediately.
    #[tokio::test]
    async fn test_fe_no_retry_on_function_cancelled() -> Result<()> {
        test_allocation_failed_no_retry(
            FunctionRunFailureReason::FunctionRunCancelled,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_no_retry_on_function_cancelled_no_retries() -> Result<()> {
        test_allocation_failed_no_retry(FunctionRunFailureReason::FunctionRunCancelled, 0).await
    }

    // V2 path: ContainerStartupInternalError is a free-retry reason.
    #[tokio::test]
    async fn test_v2_free_retry_on_container_startup_internal_error() -> Result<()> {
        test_allocation_failed_free_retry(
            FunctionRunFailureReason::ContainerStartupInternalError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_v2_free_retry_on_container_startup_internal_error_no_retries() -> Result<()> {
        test_allocation_failed_free_retry(
            FunctionRunFailureReason::ContainerStartupInternalError,
            0,
        )
        .await
    }

    // V2 path: ExecutorRemoved is a free-retry reason.
    #[tokio::test]
    async fn test_v2_free_retry_on_executor_removed() -> Result<()> {
        test_allocation_failed_free_retry(
            FunctionRunFailureReason::ExecutorRemoved,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_v2_free_retry_on_executor_removed_no_retries() -> Result<()> {
        test_allocation_failed_free_retry(FunctionRunFailureReason::ExecutorRemoved, 0).await
    }

    // Reconciliation path: FunctionError counts against retries.
    #[tokio::test]
    async fn test_reconciliation_retry_attempt_used_on_function_error() -> Result<()> {
        test_container_termination_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_reconciliation_retry_attempt_used_on_function_error_no_retries() -> Result<()> {
        test_container_termination_retry_attempt_used(
            FunctionExecutorTerminationReason::FunctionError,
            0,
        )
        .await
    }

    // Reconciliation path: ExecutorRemoved is a free-retry reason.
    #[tokio::test]
    async fn test_reconciliation_free_retry_on_executor_removed() -> Result<()> {
        test_container_termination_free_retry(
            FunctionExecutorTerminationReason::ExecutorRemoved,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_reconciliation_free_retry_on_executor_removed_no_retries() -> Result<()> {
        test_container_termination_free_retry(FunctionExecutorTerminationReason::ExecutorRemoved, 0)
            .await
    }
}
