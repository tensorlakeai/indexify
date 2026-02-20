#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::{
        data_model::{
            ContainerResources,
            FunctionRunOutcome,
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
            requests::{CreateSandboxRequest, RequestPayload, StateMachineUpdateRequest},
            test_state_store::with_simple_application,
        },
        testing::{FinalizeFunctionRunArgs, TestService, allocation_key_from_proto},
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

    #[tokio::test]
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
        let desired_state = test_executor.desired_state().await;
        assert_eq!(
            desired_state.allocations.len(),
            1,
            "Should have one allocation"
        );

        let allocation = &desired_state.allocations[0];
        test_executor
            .finalize_allocation(
                allocation,
                FinalizeFunctionRunArgs::new(allocation_key_from_proto(allocation), None, None)
                    .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await
            .unwrap();

        test_srv.process_all_state_changes().await.unwrap();

        // Create route state
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
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

        // Executor with only terminal allocations should be ready for teardown
        assert!(
            executor_metadata.ready_for_teardown,
            "Executor with only terminal allocations should be ready for teardown"
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
    async fn test_executor_ready_for_teardown_with_pool_container() {
        let test_srv = TestService::new().await.unwrap();
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an executor
        let executor_id = format!("{}-pool", TEST_EXECUTOR_ID);
        let executor = mock_executor_metadata(executor_id.clone().into());
        test_srv.create_executor(executor).await.unwrap();

        // Process state changes to register the executor
        test_srv.process_all_state_changes().await.unwrap();

        // Simulate a warm sandbox pool container by inserting a Sandbox-type
        // container with a pool_id (no active allocation, no sandbox)
        {
            use crate::data_model::{
                ContainerBuilder,
                ContainerPoolId,
                ContainerServerMetadataBuilder,
                ContainerState,
                ContainerType,
            };

            let container_id =
                crate::data_model::ContainerId::new("warm-pool-container-1".to_string());

            let container = ContainerBuilder::default()
                .id(container_id.clone())
                .namespace(TEST_NAMESPACE.to_string())
                .application_name(String::new())
                .function_name("pool:test-pool".to_string())
                .version(String::new())
                .state(ContainerState::Running)
                .resources(ContainerResources {
                    cpu_ms_per_sec: 100,
                    memory_mb: 256,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                })
                .max_concurrency(1u32)
                .pool_id(Some(ContainerPoolId::new("test-pool")))
                .build()
                .unwrap();

            let container_meta = ContainerServerMetadataBuilder::default()
                .executor_id(executor_id.clone().into())
                .function_container(container)
                .desired_state(ContainerState::Running)
                .container_type(ContainerType::Sandbox)
                .build()
                .unwrap();

            let mut container_sched = indexify_state.container_scheduler.write().await;
            if let Some(executor_state) = container_sched
                .executor_states
                .get_mut(&executor_id.clone().into())
            {
                executor_state
                    .function_container_ids
                    .insert(container_id.clone());
            }
            container_sched
                .function_containers
                .insert(container_id, Box::new(container_meta));
        }

        // Create route state
        let route_state = RouteState {
            indexify_state: indexify_state.clone(),
            blob_storage: test_srv.service.blob_storage_registry.clone(),
            executor_manager: test_srv.service.executor_manager.clone(),
            metrics: std::sync::Arc::new(crate::metrics::api_io_stats::Metrics::new()),
            config: test_srv.service.config.clone(),
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

        // Executor with a warm pool container should NOT be ready for teardown
        assert!(
            !executor_metadata.ready_for_teardown,
            "Executor with warm pool container should NOT be ready for teardown"
        );
    }
}
