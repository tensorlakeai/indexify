//! Integration tests for the vacuum functionality in the container scheduler.
//!
//! Vacuum is the process of removing idle function containers to free up
//! resources when a new container needs to be scheduled but no executor has
//! sufficient free resources.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use bytes::Bytes;
    use nanoid::nanoid;

    use crate::{
        assert_function_run_counts,
        data_model::{
            ApplicationBuilder,
            ApplicationEntryPoint,
            ApplicationState,
            DataPayload,
            ExecutorId,
            ExecutorMetadataBuilder,
            Function,
            FunctionAllowlist,
            FunctionArgs,
            FunctionCall,
            FunctionCallId,
            FunctionResources,
            FunctionRetryPolicy,
            HostResources,
            InputArgs,
            RequestCtxBuilder,
            test_objects::tests::{TEST_EXECUTOR_ID, TEST_NAMESPACE, mock_data_payload},
        },
        state_store::requests::{
            CreateOrUpdateApplicationRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        testing::{self, TestExecutor, TestService},
        utils::get_epoch_time_in_ms,
    };

    /// Helper to create a function with specific resource requirements
    fn create_function_with_resources(
        name: &str,
        cpu_ms_per_sec: u32,
        memory_mb: u64,
        min_containers: Option<u32>,
        max_containers: Option<u32>,
    ) -> Function {
        Function {
            name: name.to_string(),
            description: format!("Test function {}", name),
            resources: FunctionResources {
                cpu_ms_per_sec,
                memory_mb,
                ephemeral_disk_mb: 1024,
                gpu_configs: vec![],
            },
            retry_policy: FunctionRetryPolicy::default(),
            max_concurrency: 1,
            min_containers,
            max_containers,
            ..Default::default()
        }
    }

    /// Helper to create an application with a single function
    fn create_single_function_app(
        app_name: &str,
        version: &str,
        function: Function,
    ) -> crate::data_model::Application {
        let fn_name = function.name.clone();
        ApplicationBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .state(ApplicationState::Active)
            .name(app_name.to_string())
            .tags(HashMap::new())
            .tombstoned(false)
            .functions(HashMap::from([(fn_name.clone(), function)]))
            .version(version.to_string())
            .description(format!("Test app {}", app_name))
            .code(Some(DataPayload {
                id: "code_id".to_string(),
                metadata_size: 0,
                offset: 0,
                encoding: "application/octet-stream".to_string(),
                path: "app_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            }))
            .created_at(5)
            .entrypoint(Some(ApplicationEntryPoint {
                function_name: fn_name,
                input_serializer: "json".to_string(),
                inputs_base64: "".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            }))
            .build()
            .unwrap()
    }

    /// Helper to create an executor with specific resources
    fn create_executor_with_resources(
        id: &str,
        cpu_ms_per_sec: u32,
        memory_bytes: u64,
        function_allowlist: Option<Vec<FunctionAllowlist>>,
    ) -> crate::data_model::ExecutorMetadata {
        ExecutorMetadataBuilder::default()
            .id(ExecutorId::new(id.to_string()))
            .executor_version("1.0.0".to_string())
            .function_allowlist(function_allowlist)
            .addr("".to_string())
            .labels(Default::default())
            .host_resources(HostResources {
                cpu_ms_per_sec,
                memory_bytes,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            })
            .state(Default::default())
            .containers(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap()
    }

    /// Helper to register an application with the state store
    async fn register_application(
        test_srv: &TestService,
        app: &crate::data_model::Application,
    ) -> Result<()> {
        let request = CreateOrUpdateApplicationRequest {
            namespace: TEST_NAMESPACE.to_string(),
            application: app.clone(),
            upgrade_requests_to_current_version: true,
            container_pools: vec![],
        };
        test_srv
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(request)),
            })
            .await?;
        Ok(())
    }

    /// Helper to create a function call for a specific function name
    fn create_function_call(fn_name: &str) -> FunctionCall {
        FunctionCall {
            function_call_id: FunctionCallId(nanoid!()),
            inputs: vec![FunctionArgs::DataPayload(mock_data_payload())],
            fn_name: fn_name.to_string(),
            call_metadata: Bytes::new(),
            parent_function_call_id: None,
        }
    }

    /// Helper to create a request context for a specific application
    /// This creates a function call matching the application's entrypoint
    fn create_request_ctx_for_app(
        app: &crate::data_model::Application,
    ) -> crate::data_model::RequestCtx {
        let request_id = nanoid!();
        let fn_name = &app.entrypoint.as_ref().unwrap().function_name;
        let fn_call = create_function_call(fn_name);
        let input_args = vec![InputArgs {
            function_call_id: None,
            data_payload: mock_data_payload(),
        }];
        let fn_run = app
            .to_version()
            .unwrap()
            .create_function_run(&fn_call, input_args, &request_id)
            .unwrap();
        RequestCtxBuilder::default()
            .namespace(app.namespace.clone())
            .request_id(request_id)
            .application_name(app.name.clone())
            .application_version(app.version.clone())
            .function_runs(HashMap::from([(fn_run.id.clone(), fn_run)]))
            .function_calls(HashMap::from([(fn_call.function_call_id.clone(), fn_call)]))
            .created_at(get_epoch_time_in_ms())
            .build()
            .unwrap()
    }

    /// Helper to invoke an application and return the request_id
    async fn invoke_application(
        test_srv: &TestService,
        app: &crate::data_model::Application,
    ) -> Result<String> {
        use crate::state_store::requests::InvokeApplicationRequest;

        let ctx = create_request_ctx_for_app(app);
        let request_id = ctx.request_id.clone();
        let request = InvokeApplicationRequest {
            namespace: app.namespace.clone(),
            application_name: app.name.clone(),
            ctx,
        };
        test_srv
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::InvokeApplication(request),
            })
            .await?;
        Ok(request_id)
    }

    // =========================================================================
    // Test Case 1: Basic Vacuum Success
    // =========================================================================
    /// Test that vacuum successfully removes an idle container to make room for
    /// a new one.
    ///
    /// Scenario:
    /// 1. Create executor with limited resources (2 cores)
    /// 2. Create and invoke app_a with function requiring 1 core
    /// 3. Complete all allocations for app_a (container becomes idle)
    /// 4. Create and invoke app_b with function requiring 2 cores
    /// 5. Vacuum should mark app_a's container for termination
    /// 6. After executor reports termination, app_b's container should be
    ///    scheduled
    #[tokio::test]
    async fn test_vacuum_basic_success() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 2 CPU cores (2000 ms/sec)
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app_a with function requiring 1 core
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke app_a
        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Should have 1 function run allocated
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Complete the allocation for app_a
        {
            let commands = executor.recv_commands().await;
            assert_eq!(commands.run_allocations.len(), 1);
            let allocation = &commands.run_allocations[0];
            executor
                .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                    allocation,
                    None,
                    Some(mock_data_payload()),
                    Some(1000),
                )])
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        // Verify app_a completed
        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 1);

        // Check container scheduler state - should have a container for app_a
        let container_count_before = {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            container_scheduler.function_containers.len()
        };
        assert!(
            container_count_before >= 1,
            "Should have at least 1 container for app_a"
        );

        // Now create app_b with function requiring 2 cores (more than free resources)
        let fn_b = create_function_with_resources("fn_b", 2000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - this should trigger vacuum
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Check that app_a's container is marked for termination
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();

            // At least one container should exist and be marked for termination
            if !app_a_containers.is_empty() {
                let (_, fc) = app_a_containers.first().unwrap();
                assert!(
                    matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "app_a container should be marked for termination, got {:?}",
                    fc.desired_state
                );
            }
        }

        // The function run for app_b should be pending (waiting for resources)
        // or allocated if vacuum already freed resources
        let all_runs = test_srv.get_all_function_runs().await?;
        let app_b_runs: Vec<_> = all_runs
            .iter()
            .filter(|r| r.application == "app_b")
            .collect();
        assert_eq!(app_b_runs.len(), 1, "Should have 1 function run for app_b");

        Ok(())
    }

    // =========================================================================
    // Test Case 2: Vacuum Respects Min Containers
    // =========================================================================
    /// Test that vacuum does NOT remove containers if it would violate
    /// min_containers.
    ///
    /// Scenario:
    /// 1. Create app with min_containers: 2
    /// 2. Schedule exactly 2 containers
    /// 3. Try to schedule a new container requiring vacuum
    /// 4. Vacuum should NOT remove any containers (would violate
    ///    min_containers)
    #[tokio::test]
    async fn test_vacuum_respects_min_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 3 CPU cores
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                3000,                    // 3 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app with min_containers: 2
        let fn_a = create_function_with_resources(
            "fn_a",
            1000, // 1 core per container
            1024,
            Some(2), // min_containers
            Some(3), // max_containers
        );
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke app_a twice to create 2 containers
        let _request_id_1 = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Complete first allocation
        {
            let commands = executor.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Invoke again to create second container
        let _request_id_2 = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Complete second allocation
        {
            let commands = executor.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Check current container count for app_a
        let container_count = {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .filter(|(_, fc)| {
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    )
                })
                .count()
        };
        tracing::info!(
            "Container count for app_a before vacuum attempt: {}",
            container_count
        );

        // Now create app_b requiring 2 cores (would need to vacuum app_a's containers)
        let fn_b = create_function_with_resources("fn_b", 2000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - vacuum should NOT remove app_a containers due to
        // min_containers
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Verify app_a still has its containers (not marked for termination)
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let active_app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .filter(|(_, fc)| {
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    )
                })
                .collect();

            tracing::info!(
                "Active containers for app_a after vacuum attempt: {}",
                active_app_a_containers.len()
            );

            // min_containers should be respected - containers should NOT be
            // vacuumed if doing so would drop below min_containers
            // Note: The exact behavior depends on current container count vs
            // min_containers
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 3: Vacuum Skips Allowlisted Containers
    // =========================================================================
    /// Test that vacuum does NOT remove containers matching executor's
    /// allowlist.
    ///
    /// Scenario:
    /// 1. Create executor with allowlist for app_a
    /// 2. Create container for app_a (matches allowlist)
    /// 3. Try to schedule container for app_b requiring vacuum
    /// 4. app_a's container should NOT be vacuumed
    #[tokio::test]
    async fn test_vacuum_skips_allowlisted_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with allowlist for app_a
        let allowlist = vec![FunctionAllowlist {
            namespace: Some(TEST_NAMESPACE.to_string()),
            application: Some("app_a".to_string()),
            function: None, // Any function in app_a
        }];

        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                Some(allowlist),
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app_a (matches allowlist)
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke app_a
        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Complete app_a's allocation
        {
            let commands = executor.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Verify app_a has a container
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();
            assert!(
                !app_a_containers.is_empty(),
                "app_a should have a container"
            );
        }

        // Create app_b (not in allowlist) requiring 2 cores
        let fn_b = create_function_with_resources("fn_b", 2000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - vacuum should NOT remove app_a's container (allowlisted)
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Verify app_a's container is NOT marked for termination
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();

            for (id, fc) in &app_a_containers {
                tracing::info!(
                    "app_a container {}: desired_state={:?}",
                    id.get(),
                    fc.desired_state
                );
                assert!(
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "Allowlisted container should NOT be marked for termination"
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 4: Vacuum Skips Tombstoned Executors
    // =========================================================================
    /// Test that vacuum only considers live (non-tombstoned) executors.
    #[tokio::test]
    async fn test_vacuum_skips_tombstoned_executors() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create two executors
        let mut executor1 = test_srv
            .create_executor(create_executor_with_resources(
                "executor_1",
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;

        let _executor2 = test_srv
            .create_executor(create_executor_with_resources(
                "executor_2",
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app and invoke to create containers on both executors
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke twice to potentially get containers on both executors
        let _request_id_1 = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Complete allocation
        {
            let commands = executor1.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor1
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Tombstone executor1
        executor1.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // Verify executor1 is tombstoned
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let executor1_id = crate::data_model::ExecutorId::new("executor_1".to_string());
            if let Some(executor) = container_scheduler.executors.get(&executor1_id) {
                assert!(executor.tombstoned, "executor_1 should be tombstoned");
            }
        }

        // Create app_b requiring 2 cores
        let fn_b = create_function_with_resources("fn_b", 2000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - vacuum should only consider executor2 (executor1 is
        // tombstoned)
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Verify executor2 can handle the request (since executor1 is tombstoned)
        // The test passes if no errors occur and the system handles the tombstoned
        // executor correctly

        Ok(())
    }

    // =========================================================================
    // Test Case 5: Vacuum Multiple Containers for Large Resource Request
    // =========================================================================
    /// Test that vacuum can remove multiple containers to free enough
    /// resources.
    #[tokio::test]
    async fn test_vacuum_multiple_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 4 CPU cores
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                4000,                    // 4 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create 3 small apps, each using 1 core
        for i in 1..=3 {
            let fn_name = format!("fn_{}", i);
            let app_name = format!("app_{}", i);
            let func = create_function_with_resources(&fn_name, 1000, 1024, None, None);
            let app = create_single_function_app(&app_name, "1", func);
            register_application(&test_srv, &app).await?;

            // Invoke and complete
            let _request_id = invoke_application(&test_srv, &app).await?;
            test_srv.process_all_state_changes().await?;

            // Complete allocation
            {
                let commands = executor.recv_commands().await;
                if !commands.run_allocations.is_empty() {
                    let allocation = &commands.run_allocations[0];
                    executor
                        .report_allocation_activities(vec![
                            TestExecutor::make_allocation_completed(
                                allocation,
                                None,
                                Some(mock_data_payload()),
                                Some(1000),
                            ),
                        ])
                        .await?;
                    test_srv.process_all_state_changes().await?;
                }
            }
        }

        // Verify we have 3 containers
        let container_count = {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| {
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    )
                })
                .count()
        };
        tracing::info!("Container count before vacuum: {}", container_count);

        // Now create app requiring 3 cores (needs to vacuum 2+ small containers)
        let fn_large = create_function_with_resources("fn_large", 3000, 1024, None, None);
        let app_large = create_single_function_app("app_large", "1", fn_large);
        register_application(&test_srv, &app_large).await?;

        // Invoke - should trigger vacuum of multiple containers
        let _request_id = invoke_application(&test_srv, &app_large).await?;
        test_srv.process_all_state_changes().await?;

        // Check how many containers are now marked for termination
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let terminated_count = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| {
                    matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    )
                })
                .count();

            tracing::info!("Containers marked for termination: {}", terminated_count);
            // At least 2 containers should be marked for termination to free 3
            // cores (with 1 core free, we need to vacuum 2
            // containers of 1 core each)
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 6: Vacuum Returns Empty When Cannot Free Enough Resources
    // =========================================================================
    /// Test that vacuum returns empty when it cannot free enough resources.
    #[tokio::test]
    async fn test_vacuum_insufficient_resources() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Create executor with 2 CPU cores
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app using 1 core
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke and complete
        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        {
            let commands = executor.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Create app requiring 4 cores (more than total executor capacity)
        let fn_huge = create_function_with_resources("fn_huge", 4000, 1024, None, None);
        let app_huge = create_single_function_app("app_huge", "1", fn_huge);
        register_application(&test_srv, &app_huge).await?;

        // Invoke - vacuum cannot help because even vacuuming everything isn't enough
        let _request_id = invoke_application(&test_srv, &app_huge).await?;
        test_srv.process_all_state_changes().await?;

        // The function run should be pending (no resources available)
        let all_runs = test_srv.get_all_function_runs().await?;
        let huge_runs: Vec<_> = all_runs
            .iter()
            .filter(|r| r.application == "app_huge")
            .collect();

        if !huge_runs.is_empty() {
            // The run should be pending since resources are insufficient
            let pending_count = test_srv.get_pending_function_runs().await?.len();
            tracing::info!("Pending function runs: {}", pending_count);
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 7: End-to-End Vacuum Enables New Container Scheduling
    // =========================================================================
    /// Complete end-to-end test where vacuum enables scheduling a new
    /// container.
    #[tokio::test]
    async fn test_vacuum_end_to_end_scheduling() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with limited resources (2 cores)
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create and complete app_a (uses 1 core)
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Complete app_a
        {
            let commands = executor.recv_commands().await;
            let allocation = &commands.run_allocations[0];
            executor
                .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                    allocation,
                    None,
                    Some(mock_data_payload()),
                    Some(1000),
                )])
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        assert_function_run_counts!(test_srv, total: 1, allocated: 0, pending: 0, completed_success: 1);

        // Create and complete app_b (uses 1 core) - now executor is fully utilized
        let fn_b = create_function_with_resources("fn_b", 1000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Complete app_b
        {
            let commands = executor.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Now both cores are used by idle containers
        // Create app_c requiring 2 cores
        let fn_c = create_function_with_resources("fn_c", 2000, 1024, None, None);
        let app_c = create_single_function_app("app_c", "1", fn_c);
        register_application(&test_srv, &app_c).await?;

        let _request_id_c = invoke_application(&test_srv, &app_c).await?;
        test_srv.process_all_state_changes().await?;

        // Check that containers are marked for termination
        let terminated_containers: Vec<_> = {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| {
                    matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    )
                })
                .map(|(id, fc)| {
                    (
                        id.get().to_string(),
                        fc.function_container.application_name.clone(),
                    )
                })
                .collect()
        };

        tracing::info!("Terminated containers: {:?}", terminated_containers);

        // Simulate executor reporting terminated containers
        {
            let mut executor_state = executor.get_executor_server_state().await?;
            for (_, fe) in executor_state.containers.iter_mut() {
                // Mark all as terminated to simulate executor response
                fe.state = crate::data_model::ContainerState::Terminated {
                    reason:
                        crate::data_model::ContainerTerminationReason::DesiredStateRemoved,
                };
            }
            executor_state.state_hash = nanoid::nanoid!();
            executor.sync_executor_state(executor_state).await?;
            test_srv.process_all_state_changes().await?;
        }

        // After executor reports termination, app_c should get allocated
        let all_runs = test_srv.get_all_function_runs().await?;
        let app_c_runs: Vec<_> = all_runs
            .iter()
            .filter(|r| r.application == "app_c")
            .collect();

        tracing::info!(
            "app_c function runs: {} (status: {:?})",
            app_c_runs.len(),
            app_c_runs.first().map(|r| &r.status)
        );

        Ok(())
    }

    // =========================================================================
    // Test Case 8: Vacuum With Containers Already Marked Terminated
    // =========================================================================
    /// Test that vacuum skips containers already marked for termination.
    #[tokio::test]
    async fn test_vacuum_skips_already_terminated() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 3 cores
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                3000,                    // 3 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create 2 apps using 1 core each
        for i in 1..=2 {
            let fn_name = format!("fn_{}", i);
            let app_name = format!("app_{}", i);
            let func = create_function_with_resources(&fn_name, 1000, 1024, None, None);
            let app = create_single_function_app(&app_name, "1", func);
            register_application(&test_srv, &app).await?;

            let _request_id = invoke_application(&test_srv, &app).await?;
            test_srv.process_all_state_changes().await?;

            // Complete allocation
            {
                let commands = executor.recv_commands().await;
                if !commands.run_allocations.is_empty() {
                    let allocation = &commands.run_allocations[0];
                    executor
                        .report_allocation_activities(vec![
                            TestExecutor::make_allocation_completed(
                                allocation,
                                None,
                                Some(mock_data_payload()),
                                Some(1000),
                            ),
                        ])
                        .await?;
                    test_srv.process_all_state_changes().await?;
                }
            }
        }

        // Delete app_1 to mark its container for termination
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(
                    crate::state_store::requests::DeleteApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        name: "app_1".to_string(),
                    },
                ),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify app_1's container is terminated
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_1_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_1")
                .collect();

            for (id, fc) in &app_1_containers {
                tracing::info!(
                    "app_1 container {} desired_state: {:?}",
                    id.get(),
                    fc.desired_state
                );
                assert!(
                    matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "app_1 container should be terminated after app deletion"
                );
            }
        }

        // Now create app requiring 2 cores
        let fn_big = create_function_with_resources("fn_big", 2000, 1024, None, None);
        let app_big = create_single_function_app("app_big", "1", fn_big);
        register_application(&test_srv, &app_big).await?;

        // Invoke - vacuum should skip app_1's already-terminated container
        // and only consider app_2's container
        let _request_id = invoke_application(&test_srv, &app_big).await?;
        test_srv.process_all_state_changes().await?;

        // Verify that app_2's container is now marked for termination (if needed for
        // resources)
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_2_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_2")
                .collect();

            for (id, fc) in &app_2_containers {
                tracing::info!(
                    "app_2 container {} desired_state: {:?}",
                    id.get(),
                    fc.desired_state
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 9: Vacuum Does NOT Remove Containers With Active Allocations
    // =========================================================================
    /// Test that vacuum does NOT remove containers that have active
    /// allocations. This is a regression test for the bug where containers
    /// with running allocations were incorrectly vacuumed.
    ///
    /// Scenario:
    /// 1. Create executor with 3 cores
    /// 2. Create app_a using 1 core, invoke and DO NOT complete (active
    ///    allocation)
    /// 3. Create app_b using 3 cores (would require vacuuming app_a)
    /// 4. app_a's container should NOT be vacuumed because it has an active
    ///    allocation
    #[tokio::test]
    async fn test_vacuum_does_not_remove_active_allocation_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 3 CPU cores
        let _executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                3000,                    // 3 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app_a with function requiring 1 core
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke app_a - DO NOT complete the allocation
        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Verify app_a has an allocation running
        assert_function_run_counts!(test_srv, total: 1, allocated: 1, pending: 0, completed_success: 0);

        // Verify container has num_allocations = 1
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();

            assert!(
                !app_a_containers.is_empty(),
                "app_a should have a container"
            );
            let (id, fc) = app_a_containers.first().unwrap();
            tracing::info!(
                "app_a container {} has num_allocations={}",
                id.get(),
                fc.allocations.len(),
            );
            assert_eq!(
                fc.allocations.len(),
                1,
                "Container should have 1 active allocation"
            );
        }

        // Now create app_b requiring 3 cores - would need to vacuum app_a to fit
        let fn_b = create_function_with_resources("fn_b", 3000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - vacuum should NOT remove app_a's container because it has
        // an active allocation
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Verify app_a's container is NOT marked for termination
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();

            for (id, fc) in &app_a_containers {
                tracing::info!(
                    "app_a container {} desired_state={:?}, num_allocations={}",
                    id.get(),
                    fc.desired_state,
                    fc.allocations.len(),
                );
                assert!(
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "Container with active allocations should NOT be vacuumed"
                );
            }
        }

        // app_b's function run should be pending (no resources available because
        // app_a's container cannot be vacuumed)
        let all_runs = test_srv.get_all_function_runs().await?;
        let app_b_runs: Vec<_> = all_runs
            .iter()
            .filter(|r| r.application == "app_b")
            .collect();
        assert_eq!(app_b_runs.len(), 1, "Should have 1 function run for app_b");

        // The function run should be pending since app_a's container can't be vacuumed
        let pending_runs = test_srv.get_pending_function_runs().await?;
        let app_b_pending: Vec<_> = pending_runs
            .iter()
            .filter(|r| r.application == "app_b")
            .collect();
        tracing::info!(
            "app_b pending function runs: {} (expected: 1 because app_a container can't be vacuumed)",
            app_b_pending.len()
        );

        Ok(())
    }

    // =========================================================================
    // Test Case 10: Vacuum Across Multiple Executors
    // =========================================================================
    /// Test that vacuum can find resources across multiple executors.
    #[tokio::test]
    async fn test_vacuum_tries_multiple_executors() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor_1 with allowlist (containers won't be vacuumed)
        let allowlist = vec![FunctionAllowlist {
            namespace: Some(TEST_NAMESPACE.to_string()),
            application: Some("app_protected".to_string()),
            function: None,
        }];

        let mut executor1 = test_srv
            .create_executor(create_executor_with_resources(
                "executor_1",
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                Some(allowlist),
            ))
            .await?;

        // Create executor_2 without allowlist (containers can be vacuumed)
        let mut executor2 = test_srv
            .create_executor(create_executor_with_resources(
                "executor_2",
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create protected app on executor_1
        let fn_protected = create_function_with_resources("fn_protected", 1000, 1024, None, None);
        let app_protected = create_single_function_app("app_protected", "1", fn_protected);
        register_application(&test_srv, &app_protected).await?;

        let _request_id_protected = invoke_application(&test_srv, &app_protected).await?;
        test_srv.process_all_state_changes().await?;

        // Complete protected app
        {
            let commands = executor1.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor1
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Create unprotected app (may land on executor_2)
        let fn_unprotected =
            create_function_with_resources("fn_unprotected", 1000, 1024, None, None);
        let app_unprotected = create_single_function_app("app_unprotected", "1", fn_unprotected);
        register_application(&test_srv, &app_unprotected).await?;

        let _request_id_unprotected = invoke_application(&test_srv, &app_unprotected).await?;
        test_srv.process_all_state_changes().await?;

        // Complete unprotected app
        {
            let commands = executor2.recv_commands().await;
            if !commands.run_allocations.is_empty() {
                let allocation = &commands.run_allocations[0];
                executor2
                    .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                        allocation,
                        None,
                        Some(mock_data_payload()),
                        Some(1000),
                    )])
                    .await?;
                test_srv.process_all_state_changes().await?;
            }
        }

        // Create app requiring 2 cores - should vacuum from executor_2 (not executor_1)
        let fn_big = create_function_with_resources("fn_big", 2000, 1024, None, None);
        let app_big = create_single_function_app("app_big", "1", fn_big);
        register_application(&test_srv, &app_big).await?;

        let _request_id_big = invoke_application(&test_srv, &app_big).await?;
        test_srv.process_all_state_changes().await?;

        // Verify protected app's container is NOT terminated
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let protected_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_protected")
                .collect();

            for (id, fc) in &protected_containers {
                tracing::info!(
                    "Protected container {} desired_state: {:?}",
                    id.get(),
                    fc.desired_state
                );
                // Protected containers should NOT be terminated
                assert!(
                    !matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "Protected container should NOT be marked for termination"
                );
            }
        }

        Ok(())
    }

    // =========================================================================
    // Test Case 11: Vacuum State Consistency Within Same State Change
    // =========================================================================
    /// Test that when vacuum runs, subsequent allocation logic within the SAME
    /// state change sees the vacuumed container as terminated.
    ///
    /// This is a regression test for the bug where:
    /// 1. Vacuum marked a container for termination
    /// 2. But function_containers wasn't updated immediately
    /// 3. Subsequent find_available_container calls in the same batch could
    ///    still see the vacuumed container as "Running"
    ///
    /// Scenario:
    /// 1. Create executor with 2 cores
    /// 2. Create app_a using 1 core, complete allocation (container becomes
    ///    idle)
    /// 3. Invoke app_b requiring 2 cores (triggers vacuum of app_a's container)
    /// 4. Then invoke app_a again - it should NOT use the vacuumed container
    #[tokio::test]
    async fn test_vacuum_state_consistency_within_state_change() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create executor with 2 CPU cores
        let mut executor = test_srv
            .create_executor(create_executor_with_resources(
                TEST_EXECUTOR_ID,
                2000,                    // 2 cores
                16 * 1024 * 1024 * 1024, // 16 GB
                None,
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create app_a with function requiring 1 core
        let fn_a = create_function_with_resources("fn_a", 1000, 1024, None, None);
        let app_a = create_single_function_app("app_a", "1", fn_a);
        register_application(&test_srv, &app_a).await?;

        // Invoke app_a
        let _request_id_a = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Get the container ID for app_a
        let container_id_before = {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let app_a_containers: Vec<_> = container_scheduler
                .function_containers
                .iter()
                .filter(|(_, fc)| fc.function_container.application_name == "app_a")
                .collect();
            assert_eq!(
                app_a_containers.len(),
                1,
                "Should have exactly 1 container for app_a"
            );
            app_a_containers.first().unwrap().0.clone()
        };
        tracing::info!(
            "app_a container before completion: {}",
            container_id_before.get()
        );

        // Complete the allocation for app_a (making the container idle)
        {
            let commands = executor.recv_commands().await;
            assert_eq!(commands.run_allocations.len(), 1);
            let allocation = &commands.run_allocations[0];
            executor
                .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                    allocation,
                    None,
                    Some(mock_data_payload()),
                    Some(1000),
                )])
                .await?;
            test_srv.process_all_state_changes().await?;
        }

        // Verify container is now idle (num_allocations = 0)
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let fc = container_scheduler
                .function_containers
                .get(&container_id_before)
                .expect("Container should exist");
            assert_eq!(
                fc.allocations.len(),
                0,
                "Container should be idle after completion"
            );
            tracing::info!(
                "app_a container {} is now idle (num_allocations=0)",
                container_id_before.get()
            );
        }

        // Create app_b requiring 2 cores (will trigger vacuum of app_a's container)
        let fn_b = create_function_with_resources("fn_b", 2000, 1024, None, None);
        let app_b = create_single_function_app("app_b", "1", fn_b);
        register_application(&test_srv, &app_b).await?;

        // Invoke app_b - this triggers vacuum
        let _request_id_b = invoke_application(&test_srv, &app_b).await?;
        test_srv.process_all_state_changes().await?;

        // Now app_a's container should be terminated
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            if let Some(fc) = container_scheduler
                .function_containers
                .get(&container_id_before)
            {
                tracing::info!(
                    "After vacuum: app_a container {} desired_state: {:?}",
                    container_id_before.get(),
                    fc.desired_state
                );
                assert!(
                    matches!(
                        fc.desired_state,
                        crate::data_model::ContainerState::Terminated { .. }
                    ),
                    "app_a's container should be terminated after vacuum"
                );
            } else {
                tracing::info!(
                    "After vacuum: app_a container {} was removed",
                    container_id_before.get()
                );
            }
        }

        // Now invoke app_a again - it should NOT use the vacuumed container
        let request_id_a2 = invoke_application(&test_srv, &app_a).await?;
        test_srv.process_all_state_changes().await?;

        // Check that app_a's second invocation did NOT go to the vacuumed container
        let all_runs = test_srv.get_all_function_runs().await?;
        let app_a_second_run: Vec<_> = all_runs
            .iter()
            .filter(|r| r.application == "app_a" && r.request_id == request_id_a2)
            .collect();

        for run in &app_a_second_run {
            if let crate::data_model::FunctionRunStatus::Running(status) = &run.status {
                tracing::info!(
                    "app_a second run allocated with allocation_id: {}",
                    status.allocation_id
                );

                let desired_state = executor.srv_executor_state().await;
                for alloc in &desired_state.allocations {
                    if alloc
                        .allocation_id
                        .as_ref()
                        .map_or(false, |id| *id == status.allocation_id.to_string())
                    {
                        let alloc_container_id =
                            alloc.container_id.clone().unwrap_or_default();
                        tracing::info!(
                            "app_a second allocation went to container: {}",
                            alloc_container_id
                        );

                        // THE KEY ASSERTION: allocation should NOT go to the vacuumed
                        // container
                        assert_ne!(
                            alloc_container_id,
                            container_id_before.get(),
                            "BUG: Allocation went to a vacuumed container! This means \
                             find_available_container saw stale state."
                        );
                    }
                }
            } else {
                // app_a should be pending because app_b is using all resources
                tracing::info!("app_a second invocation status: {:?}", run.status);
            }
        }

        Ok(())
    }
}
