#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        assert_executor_state,
        assert_task_counts,
        data_model::{
            test_objects::tests::{test_executor_metadata, TEST_EXECUTOR_ID, TEST_NAMESPACE},
            FunctionAllowlist,
            FunctionExecutorState,
            FunctionExecutorTerminationReason,
            GraphVersion,
            TaskOutcome,
        },
        service::Service,
        state_store::test_state_store,
        testing::{self, allocation_key_from_proto, FinalizeTaskArgs},
    };

    const TEST_FN_MAX_RETRIES: u32 = 3;

    #[tokio::test]
    async fn test_dev_mode_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // Register executor in dev mode - task should be allocated
        let executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Finalize task - the new tasks should also be allocated
        let desired_state = executor.desired_state().await;
        let task_allocation = desired_state.task_allocations.first().unwrap();
        executor
            .finalize_task(
                task_allocation,
                FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                    .task_outcome(TaskOutcome::Success),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_allowlist_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        // Register executor with non-dev mode and specific allowlist
        let mut executor_meta = test_executor_metadata(TEST_EXECUTOR_ID.into());
        executor_meta.function_allowlist = Some(vec![FunctionAllowlist {
            namespace: Some(TEST_NAMESPACE.to_string()),
            compute_graph_name: Some("graph_A".to_string()),
            compute_fn_name: Some("fn_a".to_string()),
            version: Some(GraphVersion("1".to_string())),
        }]);

        let executor = test_srv.create_executor(executor_meta).await?;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Finalize task - new tasks should be allocated for b and c functions
        let desired_state = executor.desired_state().await;
        let task_allocation = desired_state.task_allocations.first().unwrap();
        executor
            .finalize_task(
                task_allocation,
                FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                    .task_outcome(TaskOutcome::Success),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // Tasks for fn_b and fn_c should be created but unallocated since they're not
        // in allowlist
        assert_task_counts!(test_srv, total: 3, allocated: 0, pending: 2, completed_success: 1);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 0); // Still has fn_a executor, no tasks allocated

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_executor_allocation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register first executor with no allowlist
        let mut executor1_meta = test_executor_metadata("executor_1".into());
        executor1_meta.function_allowlist = None;

        let executor1 = test_srv.create_executor(executor1_meta).await?;
        test_srv.process_all_state_changes().await?;

        // Task should be allocated to the first executor
        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Register second executor
        let executor2 = test_srv
            .create_executor(test_executor_metadata("executor_2".into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        assert_executor_state!(executor1, num_func_executors: 1, num_allocated_tasks: 1);

        assert_executor_state!(executor2, num_func_executors: 0, num_allocated_tasks: 0);

        // When the first executor is deregistered, task should be allocated to the
        // second executor
        executor1.deregister().await?;
        test_srv.process_all_state_changes().await?;

        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor2, num_func_executors: 1, num_allocated_tasks: 1);

        // Deregister second executor
        executor2.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // Task should be unallocated
        assert_task_counts!(test_srv, total: 1, allocated: 0, pending: 1, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_function_executor_add_remove() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register executor in dev mode
        let mut executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify initial state - fn_a task should be allocated
        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        assert_executor_state!(executor, num_func_executors: 1, num_allocated_tasks: 1);

        // Complete the task to create fn_b and fn_c tasks
        {
            let desired_state = executor.desired_state().await;
            let task_allocation = desired_state.task_allocations.first().unwrap();
            executor
                .finalize_task(
                    task_allocation,
                    FinalizeTaskArgs::new(allocation_key_from_proto(task_allocation))
                        .task_outcome(TaskOutcome::Success),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        assert_task_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);

        assert_executor_state!(executor, num_func_executors: 3, num_allocated_tasks: 2); // Should have fn_a, fn_b, fn_c and fn_b, fn_c tasks

        executor.mark_function_executors_as_running().await?;

        // Remove fn_a from function executors
        {
            let mut fes: Vec<crate::data_model::FunctionExecutor> = executor
                .get_executor_server_state()
                .await?
                .function_executors
                .into_values()
                .collect();
            for fe in fes.iter_mut() {
                if fe.compute_fn_name == "fn_a" {
                    fe.state = FunctionExecutorState::Terminated {
                        reason: FunctionExecutorTerminationReason::FunctionCancelled,
                        failed_alloc_ids: Vec::new(),
                    };
                }
            }
            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        // Should still have fn_b and fn_c tasks allocated
        assert_task_counts!(test_srv, total: 3, allocated: 2, pending: 0, completed_success: 1);

        // The FE for fn_a should be removed
        let executor_server_state = executor.get_executor_server_state().await?;
        assert!(executor_server_state
            .function_executors
            .iter()
            .all(|(_id, fe)| {
                fe.compute_fn_name != "fn_a"
            }));

        Ok(())
    }

    async fn test_function_executor_retry_attempt_used(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason: crate::data_model::TaskFailureReason = reason.into();
        assert!(task_failure_reason.should_count_against_task_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        let invocation_id =
            test_state_store::with_simple_retry_graph(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let mut executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // track the attempt number
        let mut attempt_number: u32 = 0;

        // validate the initial task retry attempt number
        {
            let tasks = test_srv.get_all_tasks().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // loop over retries
        while attempt_number <= max_retries {
            // update the function executors
            let allocs = executor
                .desired_state()
                .await
                .task_allocations
                .iter()
                .filter_map(|ta| ta.allocation_id.as_ref())
                .cloned()
                .collect();
            executor
                .set_function_executor_states(FunctionExecutorState::Terminated {
                    reason,
                    failed_alloc_ids: allocs,
                })
                .await?;

            // validate the task retry attempt number was incremented
            // if it was less than the retry max
            if attempt_number < max_retries {
                let tasks = test_srv.get_all_tasks().await?;
                assert_eq!(1, tasks.len());
                assert_eq!(attempt_number + 1, tasks.first().unwrap().attempt_number);
            }

            attempt_number += 1;
        }

        // check for completion
        {
            assert_task_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 0);

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
    async fn test_fe_retry_attempt_used_on_startup_failed_internal_error() -> Result<()> {
        test_function_executor_retry_attempt_used(
            FunctionExecutorTerminationReason::StartupFailedInternalError,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_used_on_startup_failed_internal_error_no_retries() -> Result<()>
    {
        test_function_executor_retry_attempt_used(
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

    async fn test_function_executor_retry_attempt_not_used(
        reason: FunctionExecutorTerminationReason,
        max_retries: u32,
    ) -> Result<()> {
        let task_failure_reason: crate::data_model::TaskFailureReason = reason.into();
        assert!(!task_failure_reason.should_count_against_task_retry_attempts());

        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // invoke the graph
        test_state_store::with_simple_retry_graph(&indexify_state, max_retries).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let mut executor = test_srv
            .create_executor(test_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // make sure the task is allocated
        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // track the attempt number
        let attempt_number: u32 = 0;

        // validate the initial task retry attempt number
        {
            let tasks = test_srv.get_all_tasks().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // update the function executors with our retryable termination reason (not
        // using an attempt)
        let allocs = executor
            .desired_state()
            .await
            .task_allocations
            .iter()
            .filter_map(|ta| ta.allocation_id.as_ref())
            .cloned()
            .collect();
        executor
            .set_function_executor_states(FunctionExecutorState::Terminated {
                reason,
                failed_alloc_ids: allocs,
            })
            .await?;

        // validate the task retry attempt number was not changed
        {
            let tasks = test_srv.get_all_tasks().await?;
            assert_eq!(1, tasks.len());
            assert_eq!(attempt_number, tasks.first().unwrap().attempt_number);
        }

        // make sure the task is still allocated
        assert_task_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_unknown() -> Result<()> {
        test_function_executor_retry_attempt_not_used(
            FunctionExecutorTerminationReason::Unknown,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_unknown_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_not_used(FunctionExecutorTerminationReason::Unknown, 0)
            .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_desired_state_removed() -> Result<()> {
        test_function_executor_retry_attempt_not_used(
            FunctionExecutorTerminationReason::DesiredStateRemoved,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_desired_state_removed_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_not_used(
            FunctionExecutorTerminationReason::DesiredStateRemoved,
            0,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_function_cancelled() -> Result<()> {
        test_function_executor_retry_attempt_not_used(
            FunctionExecutorTerminationReason::FunctionCancelled,
            TEST_FN_MAX_RETRIES,
        )
        .await
    }

    #[tokio::test]
    async fn test_fe_retry_attempt_not_used_on_function_cancelled_no_retries() -> Result<()> {
        test_function_executor_retry_attempt_not_used(
            FunctionExecutorTerminationReason::FunctionCancelled,
            0,
        )
        .await
    }
}
