#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{mock_dev_executor, mock_executor_id, TEST_NAMESPACE},
        ExecutorId,
        FunctionExecutorStatus,
        FunctionURI,
        GraphVersion,
        TaskOutcome,
    };
    use state_store::test_state_store;

    use crate::{
        service::Service,
        testing::{self, ExecutorStateAssertions, FinalizeTaskArgs, TaskStateAssertions},
    };

    #[tokio::test]
    async fn test_dev_mode_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        // Register executor in dev mode - task should be allocated
        let executor = test_srv
            .create_executor(mock_dev_executor(mock_executor_id()))
            .await?;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        // Finalize task - the new tasks should also be allocated
        let executor_tasks = executor.get_tasks().await?;
        executor
            .finalize_task(
                executor_tasks.first().unwrap(),
                FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
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

        Ok(())
    }

    #[tokio::test]
    async fn test_allowlist_executor_behavior() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task first (will be unallocated)
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        // Register executor with non-dev mode and specific allowlist
        let mut executor_meta = mock_dev_executor(mock_executor_id());
        executor_meta.development_mode = false;
        executor_meta.function_allowlist = Some(vec![FunctionURI {
            namespace: TEST_NAMESPACE.to_string(),
            compute_graph_name: "graph_A".to_string(),
            compute_fn_name: "fn_a".to_string(),
            version: Some(GraphVersion("1".to_string())),
        }]);

        let executor = test_srv.create_executor(executor_meta).await?;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        // Finalize task - new tasks should be allocated for b and c functions
        let executor_tasks = executor.get_tasks().await?;
        executor
            .finalize_task(
                executor_tasks.first().unwrap(),
                FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // Tasks for fn_b and fn_c should be created but unallocated since they're not
        // in allowlist
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 0,
                unallocated: 2,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,  // Still has fn_a executor
                num_allocated_tasks: 0, // No tasks allocated
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_executor_allocation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register first executor in non-dev mode with no allowlist
        let mut executor1_meta = mock_dev_executor(ExecutorId::new("executor_1".to_string()));
        executor1_meta.development_mode = false;
        executor1_meta.function_allowlist = None;

        let executor1 = test_srv.create_executor(executor1_meta).await?;
        test_srv.process_all_state_changes().await?;

        // Task should remain unallocated
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        // Register second executor in dev mode
        let executor2 = test_srv
            .create_executor(mock_dev_executor(ExecutorId::new("executor_2".to_string())))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Task should now be allocated to the dev mode executor
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor1
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 0,
                num_allocated_tasks: 0,
            })
            .await?;

        executor2
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        // When the dev mode executor is deregistered, task should be unallocated again
        executor2.deregister().await?;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 0,
                unallocated: 1,
                completed_success: 0,
            })
            .await?;

        // Register third executor with correct allowlist
        let mut executor3_meta = mock_dev_executor(ExecutorId::new("executor_3".to_string()));
        executor3_meta.development_mode = false;
        executor3_meta.function_allowlist = Some(vec![FunctionURI {
            namespace: TEST_NAMESPACE.to_string(),
            compute_graph_name: "graph_A".to_string(),
            compute_fn_name: "fn_a".to_string(),
            version: None,
        }]);

        let executor3 = test_srv.create_executor(executor3_meta).await?;
        test_srv.process_all_state_changes().await?;

        // Task should now be allocated to executor with allowlist
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor3
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

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
        let executor = test_srv
            .create_executor(mock_dev_executor(mock_executor_id()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify initial state - fn_a task should be allocated
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        // Complete the task to create fn_b and fn_c tasks
        {
            let executor_tasks = executor.get_tasks().await?;
            executor
                .finalize_task(
                    executor_tasks.first().unwrap(),
                    FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 2,
                unallocated: 0,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 3,  // Should have fn_a, fn_b, fn_c
                num_allocated_tasks: 2, // fn_b and fn_c tasks
            })
            .await?;

        executor.mark_function_executors_as_idle().await?;

        // Remove fn_a from function executors
        {
            let fes = executor
                .get_executor_server_state()
                .await?
                .function_executors
                .into_values()
                // Remove fn_a from the list
                .filter(|fe| fe.compute_fn_name != "fn_a")
                .collect();

            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        // Should still have fn_b and fn_c tasks allocated
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 2,
                unallocated: 0,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 2,  // fn_b and fn_c
                num_allocated_tasks: 2, // fn_b and fn_c tasks
            })
            .await?;

        executor.mark_function_executors_as_idle().await?;

        // Save the current function executor IDs
        let prev_fe_ids = executor
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|fe| fe.id)
            .collect::<HashSet<_>>();

        // Remove all function executors
        {
            executor.update_function_executors(vec![]).await?;
            test_srv.process_all_state_changes().await?;
        }

        // Verify deleted function executors are replaced
        let new_fe_ids = executor
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|fe| fe.id)
            .collect::<HashSet<_>>();

        // None of the old IDs should be present in the new IDs
        let surviving_fes = prev_fe_ids.intersection(&new_fe_ids).collect::<Vec<_>>();
        assert!(
            surviving_fes.is_empty(),
            "Removed function executors should have been replaced: {:?}",
            surviving_fes
        );

        // All tasks should be unallocated
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 2,
                unallocated: 0,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 2,
                num_allocated_tasks: 2,
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_unhealthy_function_executors() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register executor
        let executor = test_srv
            .create_executor(mock_dev_executor(mock_executor_id()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Complete first task to create fn_b and fn_c tasks
        {
            let executor_tasks = executor.get_tasks().await?;
            executor
                .finalize_task(
                    executor_tasks.first().unwrap(),
                    FinalizeTaskArgs::new().task_outcome(TaskOutcome::Success),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        // Verify state before marking executors unhealthy
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 2,
                unallocated: 0,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 3,
                num_allocated_tasks: 2,
            })
            .await?;

        executor.mark_function_executors_as_idle().await?;

        // Save the current function executor IDs
        let prev_fe_ids = executor
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|fe| fe.id)
            .collect::<HashSet<_>>();

        // Mark all function executors as unhealthy
        {
            let fes = executor
                .get_executor_server_state()
                .await?
                .function_executors
                .into_values()
                .map(|mut fe| {
                    fe.status = FunctionExecutorStatus::Unhealthy;
                    fe
                })
                .collect();

            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        // Verify unhealthy function executors are replaced
        let new_fe_ids = executor
            .get_executor_server_state()
            .await?
            .function_executors
            .into_values()
            .map(|fe| fe.id)
            .collect::<HashSet<_>>();

        // None of the old IDs should be present in the new IDs
        let surviving_fes = prev_fe_ids.intersection(&new_fe_ids).collect::<Vec<_>>();
        assert!(
            surviving_fes.is_empty(),
            "Unhealthy function executors should have been replaced: {:?}",
            surviving_fes
        );

        // Tasks should still be allocated to the new function executors
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 3,
                allocated: 2,
                unallocated: 0,
                completed_success: 1,
            })
            .await?;

        executor
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 2,  /* Should have been replaced, fn_a is not replaced since
                                         * not needed */
                num_allocated_tasks: 2, // Tasks should be reallocated to new function executors
            })
            .await?;

        Ok(())
    }
}
