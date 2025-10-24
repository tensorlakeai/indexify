#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        assert_function_run_counts,
        data_model::{
            FunctionRunOutcome,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_data_payload,
                mock_executor_metadata,
                mock_updates,
            },
        },
        service::Service,
        state_store::test_state_store,
        testing::{self, FinalizeFunctionRunArgs, allocation_key_from_proto},
    };

    #[tokio::test]
    async fn test_app_request_completion_blocking_calls() -> Result<()> {
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
}
