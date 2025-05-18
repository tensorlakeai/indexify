#[cfg(test)]
mod tests {
    use anyhow::Result;
    use data_model::{
        test_objects::tests::{mock_executor, mock_executor_id, TEST_NAMESPACE},
        ExecutorId,
        FunctionAllowlist,
        FunctionExecutorState,
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
            .create_executor(mock_executor(mock_executor_id()))
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
        let desired_state = executor.desired_state().await;
        let task_allocation = desired_state.task_allocations.first().unwrap();
        executor
            .finalize_task(
                task_allocation,
                FinalizeTaskArgs::new(task_allocation.allocation_id().to_string())
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
        let mut executor_meta = mock_executor(mock_executor_id());
        executor_meta.function_allowlist = Some(vec![FunctionAllowlist {
            namespace: Some(TEST_NAMESPACE.to_string()),
            compute_graph_name: Some("graph_A".to_string()),
            compute_fn_name: Some("fn_a".to_string()),
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
        let desired_state = executor.desired_state().await;
        let task_allocation = desired_state.task_allocations.first().unwrap();
        executor
            .finalize_task(
                task_allocation,
                FinalizeTaskArgs::new(task_allocation.allocation_id().to_string())
                    .task_outcome(TaskOutcome::Success),
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

        // Register first executor with no allowlist
        let mut executor1_meta = mock_executor(ExecutorId::new("executor_1".to_string()));
        executor1_meta.function_allowlist = None;

        let executor1 = test_srv.create_executor(executor1_meta).await?;
        test_srv.process_all_state_changes().await?;

        // Task should be allocated to the first executor
        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        // Register second executor
        let executor2 = test_srv
            .create_executor(mock_executor(ExecutorId::new("executor_2".to_string())))
            .await?;
        test_srv.process_all_state_changes().await?;

        executor1
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        executor2
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 0,
                num_allocated_tasks: 0,
            })
            .await?;

        // When the first executor is deregistered, task should be allocated to the
        // second executor
        executor1.deregister().await?;
        test_srv.process_all_state_changes().await?;

        test_srv
            .assert_task_states(TaskStateAssertions {
                total: 1,
                allocated: 1,
                unallocated: 0,
                completed_success: 0,
            })
            .await?;

        executor2
            .assert_state(ExecutorStateAssertions {
                num_func_executors: 1,
                num_allocated_tasks: 1,
            })
            .await?;

        // Deregister second executor
        executor2.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // Task should be unallocated
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
    async fn test_function_executor_add_remove() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // Create a task
        test_state_store::with_simple_graph(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Register executor in dev mode
        let executor = test_srv
            .create_executor(mock_executor(mock_executor_id()))
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
            let desired_state = executor.desired_state().await;
            let task_allocation = desired_state.task_allocations.first().unwrap();
            executor
                .finalize_task(
                    task_allocation,
                    FinalizeTaskArgs::new(task_allocation.allocation_id().to_string())
                        .task_outcome(TaskOutcome::Success),
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

        executor.mark_function_executors_as_running().await?;

        // Remove fn_a from function executors
        {
            let mut fes: Vec<data_model::FunctionExecutor> = executor
                .get_executor_server_state()
                .await?
                .function_executors
                .into_values()
                .collect();
            for fe in fes.iter_mut() {
                if fe.compute_fn_name == "fn_a" {
                    fe.state = FunctionExecutorState::Terminated;
                }
            }
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

        // The FE for fn_a should be removed
        let executor_server_state = executor.get_executor_server_state().await?;
        assert!(executor_server_state
            .function_executors
            .iter()
            .all(|(_id, fe)| {
                if fe.compute_fn_name == "fn_a" {
                    false
                } else {
                    true
                }
            }));

        Ok(())
    }
}
