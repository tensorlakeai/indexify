#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::{
        data_model::{
            ContainerResources,
            SandboxBuilder,
            SandboxId,
            SandboxPendingReason,
            SandboxStatus,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_executor_metadata,
                mock_sandbox_executor_metadata,
            },
        },
        http_objects_v1::ApplicationMetadata,
        metrics,
        routes::routes_state::RouteState,
        routes_internal::list_executors,
        state_store::{
            requests::{
                CreateSandboxRequest,
                RequestPayload,
                SchedulerUpdatePayload,
                StateMachineUpdateRequest,
            },
            test_state_store::with_simple_application,
        },
        testing::{TestExecutor, TestService},
        utils::get_epoch_time_in_ns,
    };

    async fn create_test_route_state() -> RouteState {
        let test_service = TestService::new()
            .await
            .expect("Failed to create test service");
        RouteState {
            indexify_state: test_service.service.indexify_state.clone(),
            blob_storage: test_service.service.blob_storage_registry.clone(),
            executor_manager: test_service.service.executor_manager.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
            config: test_service.service.config.clone(),
            shutdown_rx: test_service.service.shutdown_rx.clone(),
        }
    }

    fn create_test_application_metadata(
        namespace: &str,
        app_name: &str,
        version: &str,
    ) -> ApplicationMetadata {
        let json_payload = json!({
            "namespace": namespace,
            "application": app_name,
            "version": version,
            "upgrade_requests": false,
            "manifest": {
                "name": app_name,
                "namespace": namespace,
                "description": format!("Test application {}", app_name),
                "tombstoned": false,
                "version": version,
                "tags": {
                    "test": "true"
                },
                "functions": {
                    "test_fn": {
                        "name": "test_fn",
                        "description": "Test function",
                        "secret_names": [],
                        "timeout_sec": 300,
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 100
                        },
                        "retry_policy": {
                            "max_retries": 3,
                            "initial_delay_sec": 1.0,
                            "max_delay_sec": 60.0,
                            "delay_multiplier": 2.0
                        },
                        "placement_constraints": {
                            "filter_expressions": []
                        },
                        "max_concurrency": 1
                    }
                },
                "created_at": 0,
                "entrypoint": {
                    "function_name": "test_fn",
                    "input_serializer": "json",
                    "output_serializer": "json",
                    "output_type_hints_base64": ""
                }
            },
            "code_digest": {
                "url": "s3://test-bucket/code.zip",
                "size_bytes": 1024,
                "sha256_hash": "abc123def456"
            }
        });

        serde_json::from_value(json_payload).expect("Failed to create ApplicationMetadata")
    }

    #[tokio::test]
    async fn test_create_or_update_application_with_metadata_create_new_app() {
        let route_state = create_test_route_state().await;

        let namespace = "test-namespace";
        let app_name = "test-app";
        let version = "1.0.0";

        let metadata = create_test_application_metadata(namespace, app_name, version);

        // Call the handler function directly
        let result = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata),
        )
        .await;

        assert!(
            result.is_ok(),
            "Expected Ok when creating new application, got: {:?}",
            result
        );

        // Verify the application was created
        let created_app = route_state
            .indexify_state
            .reader()
            .get_application(namespace, app_name)
            .await
            .unwrap();
        assert!(
            created_app.is_some(),
            "Application should exist after creation"
        );
        assert_eq!(created_app.unwrap().version, version);
    }

    #[tokio::test]
    async fn test_create_or_update_application_with_metadata_update_existing_app() {
        let route_state = create_test_route_state().await;

        let namespace = "test-namespace";
        let app_name = "test-app";
        let version_1 = "1.0.0";
        let version_2 = "2.0.0";

        // Create initial application
        let metadata_v1 = create_test_application_metadata(namespace, app_name, version_1);
        let result_1 = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata_v1),
        )
        .await;

        assert!(
            result_1.is_ok(),
            "Expected Ok when creating initial application, got: {:?}",
            result_1
        );

        // Verify initial version
        let app_v1 = route_state
            .indexify_state
            .reader()
            .get_application(namespace, app_name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(app_v1.version, version_1);

        // Update with new version
        let metadata_v2 = create_test_application_metadata(namespace, app_name, version_2);
        let result_2 = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata_v2),
        )
        .await;

        assert!(
            result_2.is_ok(),
            "Expected Ok when updating application with new version, got: {:?}",
            result_2
        );

        // Verify the application was updated
        let updated_app = route_state
            .indexify_state
            .reader()
            .get_application(namespace, app_name)
            .await
            .unwrap();
        assert!(
            updated_app.is_some(),
            "Application should exist after update"
        );
        assert_eq!(updated_app.unwrap().version, version_2);
    }

    #[tokio::test]
    async fn test_create_or_update_application_with_all_metadata_fields() {
        let route_state = create_test_route_state().await;

        let namespace = "test-namespace";
        let app_name = "test-app-full";
        let version = "1.0.0";

        // Create application with comprehensive metadata
        let json_payload = json!({
            "namespace": namespace,
            "application": app_name,
            "version": version,
            "upgrade_requests": true,
            "manifest": {
                "name": app_name,
                "namespace": namespace,
                "description": "Comprehensive test application",
                "tombstoned": false,
                "version": version,
                "tags": {
                    "environment": "test",
                    "team": "platform",
                    "version_tag": "v1.0.0"
                },
                "functions": {
                    "extract_text": {
                        "name": "extract_text",
                        "description": "Extracts text from PDF files",
                        "secret_names": [],
                        "timeout_sec": 600,
                        "resources": {
                            "cpus": 2.0,
                            "memory_mb": 1024,
                            "ephemeral_disk_mb": 500
                        },
                        "retry_policy": {
                            "max_retries": 2,
                            "initial_delay_sec": 2.0,
                            "max_delay_sec": 120.0,
                            "delay_multiplier": 2.0
                        },
                        "placement_constraints": {
                            "filter_expressions": []
                        },
                        "max_concurrency": 2
                    },
                    "classify_document": {
                        "name": "classify_document",
                        "description": "Classifies documents based on content",
                        "secret_names": [],
                        "timeout_sec": 300,
                        "resources": {
                            "cpus": 1.0,
                            "memory_mb": 512,
                            "ephemeral_disk_mb": 100
                        },
                        "retry_policy": {
                            "max_retries": 3,
                            "initial_delay_sec": 1.0,
                            "max_delay_sec": 60.0,
                            "delay_multiplier": 2.0
                        },
                        "placement_constraints": {
                            "filter_expressions": []
                        },
                        "max_concurrency": 1
                    }
                },
                "created_at": 0,
                "entrypoint": {
                    "function_name": "extract_text",
                    "input_serializer": "json",
                    "output_serializer": "json",
                    "output_type_hints_base64": "base64encodedtypehints"
                }
            },
            "code_digest": {
                "url": "s3://indexify-bucket/applications/test-app-full.tar.gz",
                "size_bytes": 2048,
                "sha256_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            }
        });

        let metadata: ApplicationMetadata =
            serde_json::from_value(json_payload).expect("Failed to create ApplicationMetadata");

        let result = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata),
        )
        .await;

        assert!(
            result.is_ok(),
            "Expected Ok when creating application with all metadata fields, got: {:?}",
            result
        );

        // Verify all metadata was persisted correctly
        let app = route_state
            .indexify_state
            .reader()
            .get_application(namespace, app_name)
            .await
            .unwrap()
            .expect("Application should exist");

        assert_eq!(app.name, app_name);
        assert_eq!(app.namespace, namespace);
        assert_eq!(app.version, version);
        assert_eq!(
            app.description, "Comprehensive test application",
            "Description should match"
        );
        assert_eq!(app.functions.len(), 2, "Should have 2 functions");
        assert!(
            app.functions.contains_key("extract_text"),
            "Should have extract_text function"
        );
        assert!(
            app.functions.contains_key("classify_document"),
            "Should have classify_document function"
        );
        assert_eq!(app.tags.len(), 3, "Should have 3 tags");
        assert_eq!(
            app.tags.get("environment").map(|s| s.as_str()),
            Some("test"),
            "environment tag should match"
        );
    }

    #[tokio::test]
    async fn test_executor_ready_for_teardown_idle_executor() {
        let test_srv = TestService::new().await.unwrap();

        // Create an idle executor (no allocations, no sandboxes)
        let executor_id = format!("{}-idle", TEST_EXECUTOR_ID);
        let executor = mock_executor_metadata(executor_id.clone().into());
        test_srv.create_executor(executor).await.unwrap();

        // Process state changes to ensure executor is registered
        test_srv.process_all_state_changes().await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: test_srv.service.indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Call list_executors
        let result = list_executors(axum::extract::State(route_state))
            .await
            .unwrap();

        // Find our executor
        let executor_metadata = result
            .0
            .iter()
            .find(|e| e.id == executor_id)
            .expect("Executor should be in the list");

        // Idle executor should be ready for teardown
        assert!(
            executor_metadata.ready_for_teardown,
            "Idle executor should be ready for teardown"
        );
    }

    #[tokio::test]
    async fn test_executor_ready_for_teardown_with_running_allocation() {
        let test_srv = TestService::new().await.unwrap();
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application and executor
        with_simple_application(&indexify_state).await;
        let executor_id = TEST_EXECUTOR_ID.to_string();
        let executor = mock_executor_metadata(executor_id.clone().into());
        test_srv.create_executor(executor).await.unwrap();

        // Process state changes to get an allocation
        test_srv.process_all_state_changes().await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Call list_executors
        let result = crate::routes_internal::list_executors(axum::extract::State(route_state))
            .await
            .unwrap();

        // Find our executor
        let executor_metadata = result
            .0
            .iter()
            .find(|e| e.id == executor_id)
            .expect("Executor should be in the list");

        // Executor with running allocation should NOT be ready for teardown
        assert!(
            !executor_metadata.ready_for_teardown,
            "Executor with running allocation should NOT be ready for teardown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_executor_ready_for_teardown_with_terminal_allocations() {
        let test_srv = TestService::new().await.unwrap();
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application and executor
        with_simple_application(&indexify_state).await;
        let executor_id = TEST_EXECUTOR_ID.to_string();
        let executor = mock_executor_metadata(executor_id.clone().into());
        let mut test_executor = test_srv.create_executor(executor).await.unwrap();

        // Process state changes to get an allocation
        test_srv.process_all_state_changes().await.unwrap();

        // Mark the function executor as running
        test_executor
            .mark_function_executors_as_running()
            .await
            .unwrap();

        // Complete the allocation
        let commands = test_executor.recv_commands().await;
        assert_eq!(
            commands.run_allocations.len(),
            1,
            "Should have one allocation"
        );

        let allocation = &commands.run_allocations[0];
        test_executor
            .report_allocation_activities(vec![TestExecutor::make_allocation_completed(
                allocation,
                None,
                None,
                Some(1000),
            )])
            .await
            .unwrap();

        test_srv.process_all_state_changes().await.unwrap();

        // --- Phase 1: executor has idle container → NOT ready for teardown ---

        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        let result = crate::routes_internal::list_executors(axum::extract::State(route_state))
            .await
            .unwrap();

        let executor_metadata = result
            .0
            .iter()
            .find(|e| e.id == executor_id)
            .expect("Executor should be in the list");

        // Executor still has a non-terminated function container (the one that
        // ran the allocation), so it should NOT be ready for teardown yet.
        assert!(
            !executor_metadata.ready_for_teardown,
            "Executor with non-terminated containers should NOT be ready for teardown"
        );

        // --- Phase 2: advance time, run vacuum → container terminated → ready ---

        // Advance time past the vacuum idle threshold
        let max_idle_age = std::time::Duration::from_secs(300);
        tokio::time::advance(std::time::Duration::from_secs(600)).await;

        // Run periodic vacuum on the container scheduler
        let scheduler_update = {
            let container_scheduler = indexify_state.container_scheduler.read().await.clone();
            let mut guard = container_scheduler.write().await;
            guard.periodic_vacuum(max_idle_age).unwrap()
        };

        assert!(
            !scheduler_update.containers.is_empty(),
            "vacuum should have terminated the idle container"
        );

        // Persist the vacuum update (mirrors handle_cluster_vacuum)
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                    update: Box::new(scheduler_update),
                    processed_state_changes: vec![],
                }),
            })
            .await
            .unwrap();

        // Re-query: executor should now be ready for teardown
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
        };

        let result = crate::routes_internal::list_executors(axum::extract::State(route_state))
            .await
            .unwrap();

        let executor_metadata = result
            .0
            .iter()
            .find(|e| e.id == executor_id)
            .expect("Executor should be in the list after vacuum");

        assert!(
            executor_metadata.ready_for_teardown,
            "Executor should be ready for teardown after vacuum terminates its containers"
        );
    }

    #[tokio::test]
    async fn test_executor_ready_for_teardown_with_sandbox() {
        let test_srv = TestService::new().await.unwrap();
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a sandbox-capable executor
        let executor_id = format!("{}-sandbox", TEST_EXECUTOR_ID);
        let executor = mock_sandbox_executor_metadata(executor_id.clone().into());
        test_srv.create_executor(executor).await.unwrap();

        // Create a sandbox
        let sandbox_id = SandboxId::default();
        let sandbox = SandboxBuilder::default()
            .id(sandbox_id.clone())
            .namespace(TEST_NAMESPACE.to_string())
            .image("test-image:latest".to_string())
            .status(SandboxStatus::Pending {
                reason: SandboxPendingReason::Scheduling,
            })
            .creation_time_ns(get_epoch_time_in_ns())
            .resources(ContainerResources {
                cpu_ms_per_sec: 100,
                memory_mb: 256,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .secret_names(vec![])
            .timeout_secs(600)
            .build()
            .unwrap();

        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateSandbox(CreateSandboxRequest {
                sandbox: sandbox.clone(),
            }),
        };

        indexify_state.write(request).await.unwrap();

        // Process state changes to allocate sandbox to executor
        test_srv.process_all_state_changes().await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Call list_executors
        let result = crate::routes_internal::list_executors(axum::extract::State(route_state))
            .await
            .unwrap();

        // Find our executor
        let executor_metadata = result
            .0
            .iter()
            .find(|e| e.id == executor_id)
            .expect("Executor should be in the list");

        // Executor with sandbox should NOT be ready for teardown
        assert!(
            !executor_metadata.ready_for_teardown,
            "Executor with sandbox should NOT be ready for teardown"
        );
    }

    #[tokio::test]
    async fn test_cordon_specific_executors() {
        let test_srv = TestService::new().await.unwrap();

        // Create multiple executors
        let executor1_id = format!("{}-1", TEST_EXECUTOR_ID);
        let executor2_id = format!("{}-2", TEST_EXECUTOR_ID);
        let executor3_id = format!("{}-3", TEST_EXECUTOR_ID);

        let executor1 = mock_executor_metadata(executor1_id.clone().into());
        let executor2 = mock_executor_metadata(executor2_id.clone().into());
        let executor3 = mock_executor_metadata(executor3_id.clone().into());

        test_srv.create_executor(executor1).await.unwrap();
        test_srv.create_executor(executor2).await.unwrap();
        test_srv.create_executor(executor3).await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: test_srv.service.indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Cordon executor1 and executor2 with short timeout
        // (test executors don't actively heartbeat, so acknowledgement will timeout)
        let request = crate::http_objects::CordonExecutorsRequest {
            executor_ids: Some(vec![executor1_id.clone(), executor2_id.clone()]),
            timeout_seconds: 0, // Don't wait for acknowledgement in tests
        };

        let result = crate::routes_internal::cordon_executors(
            axum::extract::State(route_state.clone()),
            axum::Json(request),
        )
        .await;

        assert!(
            result.is_ok(),
            "Cordon request should succeed: {:?}",
            result
        );

        // Note: The response may be empty if executors don't acknowledge in time,
        // but the state should still be updated. We verify the state below.

        // Verify executor states in container scheduler
        let container_sched = test_srv
            .service
            .indexify_state
            .container_scheduler
            .read()
            .await;

        let exec1_state = container_sched
            .executors
            .get(&executor1_id.into())
            .unwrap()
            .state
            .clone();
        let exec2_state = container_sched
            .executors
            .get(&executor2_id.into())
            .unwrap()
            .state
            .clone();
        let exec3_state = container_sched
            .executors
            .get(&executor3_id.into())
            .unwrap()
            .state
            .clone();

        assert_eq!(
            exec1_state,
            crate::data_model::ExecutorState::SchedulingDisabled,
            "Executor1 should be in SchedulingDisabled state"
        );
        assert_eq!(
            exec2_state,
            crate::data_model::ExecutorState::SchedulingDisabled,
            "Executor2 should be in SchedulingDisabled state"
        );
        assert_ne!(
            exec3_state,
            crate::data_model::ExecutorState::SchedulingDisabled,
            "Executor3 should NOT be in SchedulingDisabled state"
        );
    }

    #[tokio::test]
    async fn test_cordon_all_executors() {
        let test_srv = TestService::new().await.unwrap();

        // Create multiple executors
        let executor1_id = format!("{}-1", TEST_EXECUTOR_ID);
        let executor2_id = format!("{}-2", TEST_EXECUTOR_ID);

        let executor1 = mock_executor_metadata(executor1_id.clone().into());
        let executor2 = mock_executor_metadata(executor2_id.clone().into());

        test_srv.create_executor(executor1).await.unwrap();
        test_srv.create_executor(executor2).await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: test_srv.service.indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Cordon all executors (empty list) with short timeout
        let request = crate::http_objects::CordonExecutorsRequest {
            executor_ids: None,
            timeout_seconds: 0, // Don't wait for acknowledgement in tests
        };

        let result = crate::routes_internal::cordon_executors(
            axum::extract::State(route_state.clone()),
            axum::Json(request),
        )
        .await;

        assert!(
            result.is_ok(),
            "Cordon all request should succeed: {:?}",
            result
        );

        // Verify all executors are in SchedulingDisabled state
        let container_sched = test_srv
            .service
            .indexify_state
            .container_scheduler
            .read()
            .await;

        for executor_id in [executor1_id, executor2_id] {
            let executor_state = container_sched
                .executors
                .get(&executor_id.into())
                .unwrap()
                .state
                .clone();
            assert_eq!(
                executor_state,
                crate::data_model::ExecutorState::SchedulingDisabled,
                "All executors should be in SchedulingDisabled state"
            );
        }
    }

    #[tokio::test]
    async fn test_cordon_already_cordoned_executor() {
        let test_srv = TestService::new().await.unwrap();

        // Create an executor
        let executor_id = format!("{}-1", TEST_EXECUTOR_ID);
        let executor = mock_executor_metadata(executor_id.clone().into());
        test_srv.create_executor(executor).await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: test_srv.service.indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Cordon the executor first time
        let request = crate::http_objects::CordonExecutorsRequest {
            executor_ids: Some(vec![executor_id.clone()]),
            timeout_seconds: 1,
        };

        let result = crate::routes_internal::cordon_executors(
            axum::extract::State(route_state.clone()),
            axum::Json(request.clone()),
        )
        .await;

        assert!(result.is_ok(), "First cordon should succeed");

        // Cordon the same executor again
        let result = crate::routes_internal::cordon_executors(
            axum::extract::State(route_state.clone()),
            axum::Json(request),
        )
        .await;

        assert!(result.is_ok(), "Second cordon should succeed");
        let response = result.unwrap().0;

        // Executor should be in cordoned list (already cordoned executors are included)
        assert_eq!(
            response.cordoned.len(),
            1,
            "Should have 1 executor in cordoned list"
        );
        assert!(
            response.cordoned.contains(&executor_id),
            "Executor should be in cordoned list"
        );
    }

    #[tokio::test]
    async fn test_cordon_nonexistent_executor() {
        let test_srv = TestService::new().await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: test_srv.service.indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: Arc::new(metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
            shutdown_rx: test_srv.service.shutdown_rx.clone(),
        };

        // Try to cordon a non-existent executor
        let request = crate::http_objects::CordonExecutorsRequest {
            executor_ids: Some(vec!["nonexistent-executor".to_string()]),
            timeout_seconds: 1,
        };

        let result = crate::routes_internal::cordon_executors(
            axum::extract::State(route_state),
            axum::Json(request),
        )
        .await;

        assert!(
            result.is_ok(),
            "Cordon should succeed even with nonexistent executor"
        );
        let response = result.unwrap().0;

        // Should return empty cordoned list (nonexistent executors are silently
        // skipped)
        assert_eq!(
            response.cordoned.len(),
            0,
            "Should have 0 cordoned executors"
        );
    }
}
