#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::response::IntoResponse;
    use serde_json::json;

    use crate::{
        http_objects_v1::ApplicationMetadata, metrics, routes::routes_state::RouteState,
        testing::TestService,
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

    // Integration tests for progress_stream endpoint

    #[tokio::test]
    async fn test_progress_stream_basic() {
        let route_state = create_test_route_state().await;
        let namespace = "test-namespace";
        let app_name = "test-app-progress";
        let version = "1.0.0";

        // Create application
        let metadata = create_test_application_metadata(namespace, app_name, version);
        let result = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata),
        )
        .await;
        assert!(result.is_ok(), "Should create application");

        // Invoke the application
        let invoke_result = crate::routes::invoke::invoke_application_with_object_v1(
            axum::extract::Path((namespace.to_string(), app_name.to_string())),
            axum::extract::State(route_state.clone()),
            axum::http::HeaderMap::new(),
            axum::body::Body::from(r#"{"test": "data"}"#),
        )
        .await;

        assert!(invoke_result.is_ok(), "Should invoke application");

        // Get the request_id from the response
        // The response is already IntoResponse, we need to convert it
        use axum::response::Response;
        let response: Response = invoke_result.unwrap().into_response();
        let (_parts, body) = response.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let request_id = response_json["request_id"].as_str().unwrap();

        // Test progress stream
        let progress_result = crate::routes::invoke::progress_stream(
            axum::extract::Path((
                namespace.to_string(),
                app_name.to_string(),
                request_id.to_string(),
            )),
            axum::extract::State(route_state),
        )
        .await;

        assert!(progress_result.is_ok(), "Should get progress stream");
    }

    #[tokio::test]
    async fn test_progress_stream_already_finished() {
        let route_state = create_test_route_state().await;
        let namespace = "test-namespace";
        let app_name = "test-app-finished";
        let version = "1.0.0";

        // Create application
        let metadata = create_test_application_metadata(namespace, app_name, version);
        let result = crate::routes_internal::create_or_update_application_with_metadata(
            axum::extract::Path(namespace.to_string()),
            axum::extract::State(route_state.clone()),
            axum::Json(metadata),
        )
        .await;
        assert!(result.is_ok(), "Should create application");

        // Invoke the application
        let invoke_result = crate::routes::invoke::invoke_application_with_object_v1(
            axum::extract::Path((namespace.to_string(), app_name.to_string())),
            axum::extract::State(route_state.clone()),
            axum::http::HeaderMap::new(),
            axum::body::Body::from(r#"{"test": "data"}"#),
        )
        .await;

        assert!(invoke_result.is_ok(), "Should invoke application");

        // Get the request_id
        let response = invoke_result.unwrap().into_response();
        let (_parts, body) = response.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let request_id = response_json["request_id"].as_str().unwrap();

        // Manually set the request to finished state
        let mut ctx = route_state
            .indexify_state
            .reader()
            .request_ctx(namespace, app_name, request_id)
            .await
            .unwrap()
            .unwrap();

        // Update context with outcome
        ctx.outcome = Some(crate::data_model::RequestOutcome::Success);

        // Write updated context back using SchedulerUpdateRequest
        let mut scheduler_update = crate::state_store::requests::SchedulerUpdateRequest::default();
        scheduler_update.add_request_state(&ctx);

        route_state
            .indexify_state
            .write(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate((
                    Box::new(scheduler_update),
                    vec![],
                )),
            })
            .await
            .unwrap();

        // Test progress stream - should return finished event immediately
        let progress_result = crate::routes::invoke::progress_stream(
            axum::extract::Path((
                namespace.to_string(),
                app_name.to_string(),
                request_id.to_string(),
            )),
            axum::extract::State(route_state),
        )
        .await;

        assert!(progress_result.is_ok(), "Should get progress stream");
    }
}
