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
        state_store::{executor_watches::ExecutorWatch, test_state_store},
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

        let desired_state = executor.desired_state().await;
        let allocation_fn_b = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_b")
            .next()
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
            let (allocation_usage, cursor) = indexify_state.reader().allocation_usage(None).await?;

            assert_eq!(allocation_usage.len(), 3, "{allocation_usage:#?}");
            assert!(cursor.is_none());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_executor_watches_blocking_function_calls() -> Result<()> {
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

        // Get the function call id from the desired state for fn_a
        let desired_state = executor.desired_state().await;
        let allocation_fn_a = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_a")
            .next()
            .unwrap();
        let function_call_id_fn_a = allocation_fn_a.function_call_id.clone().unwrap();

        // Invoke fn_b as a blocking function call from fn_a
        let function_call_id_fn_b = executor
            .invoke_blocking_function_call(
                "fn_b",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id_fn_a.clone().into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // Get allocation for fn_b
        let desired_state = executor.desired_state().await;
        let allocation_fn_b = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_b")
            .next()
            .unwrap();

        // Invoke fn_c as a blocking function call from fn_b
        let function_call_id_fn_c = executor
            .invoke_blocking_function_call(
                "fn_c",
                TEST_NAMESPACE,
                "graph_A",
                &request_id,
                function_call_id_fn_b.clone().into(),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // Get allocation for fn_c
        let desired_state = executor.desired_state().await;
        let allocation_fn_c = desired_state
            .allocations
            .clone()
            .into_iter()
            .filter(|a| a.function.clone().unwrap().function_name() == "fn_c")
            .next()
            .unwrap();

        // Complete fn_c and fn_b first (before adding watches)
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

        // Now add watches for fn_b and fn_c AFTER they have completed
        // This tests that the server returns results for already-completed function
        // calls
        use std::collections::HashSet;
        let mut watches = HashSet::new();
        watches.insert(ExecutorWatch {
            namespace: TEST_NAMESPACE.into(),
            application: "graph_A".into(),
            request_id: request_id.clone(),
            function_call_id: function_call_id_fn_b.0.clone(),
        });
        watches.insert(ExecutorWatch {
            namespace: TEST_NAMESPACE.into(),
            application: "graph_A".into(),
            request_id: request_id.clone(),
            function_call_id: function_call_id_fn_c.0.clone(),
        });

        // Send watches via UpsertExecutor state machine request (as executors do in
        // production)
        executor.update_watches(watches.clone()).await?;

        // Check that both fn_b and fn_c results are now available in desired state
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.function_call_results.len(),
                2,
                "Expected 2 function call results in desired state: {:#?}",
                desired_state.function_call_results
            );

            let fn_b_result = desired_state
                .function_call_results
                .iter()
                .find(|r| r.function_call_id.as_ref().unwrap() == &function_call_id_fn_b.0);
            assert!(
                fn_b_result.is_some(),
                "Expected fn_b result to be in desired state"
            );
            assert_eq!(
                fn_b_result.unwrap().outcome_code,
                Some(1), // ALLOCATION_OUTCOME_CODE_SUCCESS
                "Expected fn_b to have succeeded"
            );

            let fn_c_result = desired_state
                .function_call_results
                .iter()
                .find(|r| r.function_call_id.as_ref().unwrap() == &function_call_id_fn_c.0);
            assert!(
                fn_c_result.is_some(),
                "Expected fn_c result to be in desired state"
            );
            assert_eq!(
                fn_c_result.unwrap().outcome_code,
                Some(1), // ALLOCATION_OUTCOME_CODE_SUCCESS
                "Expected fn_c to have succeeded"
            );
        }

        // Remove watch for fn_c to verify server stops returning it
        let mut updated_watch_set = HashSet::new();
        updated_watch_set.insert(ExecutorWatch {
            namespace: TEST_NAMESPACE.into(),
            application: "graph_A".into(),
            request_id: request_id.clone(),
            function_call_id: function_call_id_fn_b.0.clone(),
        });

        executor.update_watches(updated_watch_set).await?;

        // Check that only fn_b result is now in desired state
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.function_call_results.len(),
                1,
                "Expected 1 function call result in desired state after removing fn_c watch: {:#?}",
                desired_state.function_call_results
            );

            let fn_b_result = desired_state
                .function_call_results
                .iter()
                .find(|r| r.function_call_id.as_ref().unwrap() == &function_call_id_fn_b.0);
            assert!(
                fn_b_result.is_some(),
                "Expected fn_b result to still be in desired state"
            );
        }

        // Remove all watches
        executor.update_watches(HashSet::new()).await?;

        // Check that no function call results are in desired state
        {
            let desired_state = executor.desired_state().await;
            assert_eq!(
                desired_state.function_call_results.len(),
                0,
                "Expected 0 function call results in desired state after removing all watches: {:#?}",
                desired_state.function_call_results
            );
        }
        Ok(())
    }
}
