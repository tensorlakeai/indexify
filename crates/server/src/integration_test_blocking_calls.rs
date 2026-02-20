#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        data_model::{
            FunctionRunOutcome,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_data_payload,
                mock_executor_metadata,
            },
        },
        state_store::test_state_store,
        testing::{self, TestExecutor},
    };

    #[tokio::test]
    async fn test_app_request_completion_blocking_calls() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Invoke the app
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // register executor
        let mut executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Add fn_b and fn_c for execution as a blocking function call
        // get the function call id from the command stream
        let cmds = executor.recv_commands().await;
        let allocation_fn_a = cmds
            .run_allocations
            .into_iter()
            .find(|a| a.function.clone().unwrap().function_name() == "fn_a")
            .unwrap();
        let function_call_id = allocation_fn_a.function_call_id.clone().unwrap();
        let _fn_b_call_id = executor
            .invoke_blocking_function_call(
                "fn_b",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id.into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        let cmds = executor.recv_commands().await;
        let allocation_fn_b = cmds
            .run_allocations
            .into_iter()
            .find(|a| a.function.clone().unwrap().function_name() == "fn_b")
            .unwrap();

        let function_call_id = allocation_fn_b.function_call_id.clone().unwrap();
        let _fn_c_call_id = executor
            .invoke_blocking_function_call(
                "fn_c",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id.into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        let cmds = executor.recv_commands().await;
        let allocation_fn_c = cmds
            .run_allocations
            .into_iter()
            .find(|a| a.function.clone().unwrap().function_name() == "fn_c")
            .unwrap();

        // Add fn_c for execution as a blocking function call
        executor
            .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                &allocation_fn_c,
                None,
                Some(mock_data_payload()),
                Some(1000),
            )])
            .await?;

        test_srv.process_all_state_changes().await?;
        executor
            .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                &allocation_fn_b,
                None,
                Some(mock_data_payload()),
                Some(1000),
            )])
            .await?;

        test_srv.process_all_state_changes().await?;
        executor
            .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                &allocation_fn_a,
                None,
                Some(mock_data_payload()),
                Some(1000),
            )])
            .await?;

        test_srv.process_all_state_changes().await?;

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
            assert_eq!(function_runs.len(), 3, "{function_runs:#?}");
            let successful_tasks = function_runs
                .into_iter()
                .filter(|t| t.outcome == Some(FunctionRunOutcome::Success))
                .collect::<Vec<_>>();
            assert_eq!(successful_tasks.len(), 3, "{successful_tasks:#?}");

            let desired_state = executor.srv_executor_state().await;
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
            let (allocation_usage, cursor) = indexify_state.reader().allocation_usage(None).await?;

            assert_eq!(allocation_usage.len(), 3, "{allocation_usage:#?}");
            assert!(cursor.is_none());
        }

        Ok(())
    }
}
