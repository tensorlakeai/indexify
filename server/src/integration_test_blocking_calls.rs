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

        // Add fn_b and fn_c for execution as a blocking function call
        // get the function call id from the desired state
        let desired_state = executor.desired_state().await;
        let allocation_fn_a = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_a")
            .next()
            .unwrap();
        let function_call_id = allocation_fn_a.function_call_id.clone().unwrap();
        executor
            .invoke_blocking_function_call(
                "fn_b",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id.into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        let desired_state = executor.desired_state().await;
        let allocation_fn_b = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_b")
            .next()
            .unwrap();

        let function_call_id = allocation_fn_b.function_call_id.clone().unwrap();
        executor
            .invoke_blocking_function_call(
                "fn_c",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id.into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        let desired_state = executor.desired_state().await;
        let allocation_fn_c = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_c")
            .next()
            .unwrap();

        // Add fn_c for execution as a blocking function call
        executor
            .finalize_allocation(
                &allocation_fn_c,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(&allocation_fn_c),
                    None,
                    Some(mock_data_payload()),
                )
                .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await?;

        test_srv.process_all_state_changes().await?;
        executor
            .finalize_allocation(
                &allocation_fn_b,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(&allocation_fn_b),
                    None,
                    Some(mock_data_payload()),
                )
                .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await?;

        test_srv.process_all_state_changes().await?;
        executor
            .finalize_allocation(
                &allocation_fn_a,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(&allocation_fn_a),
                    None,
                    Some(mock_data_payload()),
                )
                .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await?;

        test_srv.process_all_state_changes().await?;

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
            assert_eq!(function_runs.len(), 3, "{function_runs:#?}");
            let successful_tasks = function_runs
                .into_iter()
                .filter(|t| t.outcome == Some(FunctionRunOutcome::Success))
                .collect::<Vec<_>>();
            assert_eq!(successful_tasks.len(), 3, "{successful_tasks:#?}");

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

            assert_eq!(allocation_usage.len(), 3, "{allocation_usage:#?}");
            assert!(cursor.is_none());
        }

        Ok(())
    }
}
