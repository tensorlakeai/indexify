#[cfg(test)]
mod tests {
    use anyhow::Result;
    use data_model::{
        test_objects::tests::{mock_dev_executor, mock_executor_id, TEST_NAMESPACE},
        FunctionExecutorId,
        FunctionURI,
        GraphVersion,
        TaskOutcome,
    };
    use state_store::test_state_store;

    use crate::{
        service::Service,
        testing::{self, FinalizeTaskArgs},
    };

    #[tokio::test]
    async fn test_reconciliation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service { indexify_state, .. } = test_srv.service.clone();

        // register executor
        let mut executor = {
            let executor = test_srv
                .create_executor(mock_dev_executor(mock_executor_id()))
                .await?;
            test_srv.process_all_state_changes().await?;
            executor
        };

        test_srv.assert_task_states(0, 0, 0, 0).await?;
        executor.assert_state(0, 0).await?;

        // create a task
        {
            test_state_store::with_simple_graph(&indexify_state).await;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 1, 0, 0).await?;
        executor.assert_state(1, 1).await?;

        // Toggle dev mode to false
        {
            executor.update_config(Some(false), None).await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 0, 1, 0).await?;
        executor.assert_state(0, 0).await?;

        // Add Allowlist
        {
            executor
                .update_config(
                    Some(false),
                    Some(Some(vec![FunctionURI {
                        namespace: TEST_NAMESPACE.to_string(),
                        compute_graph_name: "graph_A".to_string(),
                        compute_fn_name: "fn_a".to_string(),
                        version: None,
                    }])),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 1, 0, 0).await?;
        executor.assert_state(1, 1).await?;

        // Change Allowlist to different version
        {
            executor
                .update_config(
                    Some(false),
                    Some(Some(vec![FunctionURI {
                        namespace: TEST_NAMESPACE.to_string(),
                        compute_graph_name: "graph_A".to_string(),
                        compute_fn_name: "fn_a".to_string(),
                        version: Some(GraphVersion("2".to_string())),
                    }])),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 0, 1, 0).await?;
        executor.assert_state(0, 0).await?;

        // Change Allowlist to current version
        {
            executor
                .update_config(
                    Some(false),
                    Some(Some(vec![FunctionURI {
                        namespace: TEST_NAMESPACE.to_string(),
                        compute_graph_name: "graph_A".to_string(),
                        compute_fn_name: "fn_a".to_string(),
                        version: Some(GraphVersion("1".to_string())),
                    }])),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 1, 0, 0).await?;
        executor.assert_state(1, 1).await?;

        // Change Allowlist to any version and support fn_b
        {
            executor
                .update_config(
                    Some(false),
                    Some(Some(vec![
                        FunctionURI {
                            namespace: TEST_NAMESPACE.to_string(),
                            compute_graph_name: "graph_A".to_string(),
                            compute_fn_name: "fn_a".to_string(),
                            version: None,
                        },
                        FunctionURI {
                            namespace: TEST_NAMESPACE.to_string(),
                            compute_graph_name: "graph_A".to_string(),
                            compute_fn_name: "fn_b".to_string(),
                            version: None,
                        },
                        FunctionURI {
                            namespace: TEST_NAMESPACE.to_string(),
                            compute_graph_name: "graph_A".to_string(),
                            compute_fn_name: "fn_c".to_string(),
                            version: None,
                        },
                    ])),
                )
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(1, 1, 0, 0).await?;
        executor.assert_state(1, 1).await?;

        // Finalize Task (create 2 more)
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

        test_srv.assert_task_states(3, 2, 0, 1).await?;
        executor.assert_state(3, 2).await?;

        // Remove fn_a from FunctionExecutors
        {
            let fes = executor
                .get_function_executors()
                .await?
                .into_iter()
                // Remove fn_a from the list
                .filter(|fe| fe.compute_fn_name != "fn_a")
                .collect();
            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(3, 2, 0, 1).await?;
        executor.assert_state(2, 2).await?;

        // Support non versioned function executors
        {
            let fes = executor
                .get_function_executors()
                .await?
                .into_iter()
                .map(|mut fe| {
                    // change ID to not versioned
                    fe.id = FunctionExecutorId::new(format!("{}/{}", "not_versioned", fe.id.get()));
                    fe
                })
                .collect();
            executor.update_function_executors(fes).await?;
            test_srv.process_all_state_changes().await?;
        }

        test_srv.assert_task_states(3, 2, 0, 1).await?;
        executor.assert_state(2, 2).await?;

        Ok(())
    }
}
