#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use bytes::Bytes;
    use serde_json::json;
    use tokio::sync::broadcast;

    use crate::{
        data_model::{
            self, DataPayload, FunctionCallId, RequestCtx, RequestCtxBuilder, RequestFailureReason,
            RequestOutcome,
            test_objects::tests::{TEST_NAMESPACE, mock_app_with_retries, mock_data_payload},
        },
        metrics,
        routes::{invoke::create_request_progress_stream, routes_state::RouteState},
        state_store::{
            request_events::{RequestFinishedEvent, RequestStateChangeEvent},
            requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
        },
        testing::TestService,
        utils::get_epoch_time_in_ms,
    };

    // Test constants
    const TEST_APP_NAME: &str = "test_app";
    const TEST_APP_VERSION: &str = "1.0.0";
    const DEFAULT_CHANNEL_CAPACITY: usize = 100;
    const LAG_CHANNEL_CAPACITY: usize = 1;

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

    async fn create_request_ctx_with_outcome(
        namespace: &str,
        application_name: &str,
        application_version: &str,
        request_id: &str,
        outcome: Option<RequestOutcome>,
    ) -> RequestCtx {
        create_request_ctx_with_outcome_and_output(
            namespace,
            application_name,
            application_version,
            request_id,
            outcome,
            None,
        )
        .await
    }

    async fn create_request_ctx_with_outcome_and_output(
        namespace: &str,
        application_name: &str,
        application_version: &str,
        request_id: &str,
        outcome: Option<RequestOutcome>,
        output_payload: Option<DataPayload>,
    ) -> RequestCtx {
        let app = mock_app_with_retries(application_name, application_version, 0);
        let fn_call = app
            .to_version()
            .unwrap()
            .functions
            .get("fn_a")
            .unwrap()
            .create_function_call(
                FunctionCallId::from(request_id),
                vec![mock_data_payload()],
                Bytes::new(),
                None,
            );
        let mut fn_run = app
            .to_version()
            .unwrap()
            .create_function_run(
                &fn_call,
                vec![data_model::InputArgs {
                    function_call_id: None,
                    data_payload: mock_data_payload(),
                }],
                request_id,
            )
            .unwrap();

        // Set output if provided
        if let Some(output) = output_payload {
            fn_run.output = Some(output);
        }

        RequestCtxBuilder::default()
            .namespace(namespace.to_string())
            .application_name(application_name.to_string())
            .application_version(application_version.to_string())
            .request_id(request_id.to_string())
            .outcome(outcome)
            .function_runs(HashMap::from([(fn_run.id.clone(), fn_run)]))
            .function_calls(HashMap::from([(fn_call.function_call_id.clone(), fn_call)]))
            .created_at(get_epoch_time_in_ms())
            .build()
            .unwrap()
    }

    async fn create_json_output_payload(
        state: &RouteState,
        namespace: &str,
        json_data: serde_json::Value,
    ) -> DataPayload {
        let json_bytes = serde_json::to_vec(&json_data).unwrap();
        let payload_key = format!("{}/output", nanoid::nanoid!());

        let blob_store = state.blob_storage.get_blob_store(namespace);
        let stream = futures::stream::iter(vec![Ok(Bytes::from(json_bytes.clone()))]);
        let put_result = blob_store
            .put(&payload_key, Box::pin(stream))
            .await
            .unwrap();

        DataPayload {
            id: nanoid::nanoid!(),
            metadata_size: 0,
            path: put_result.url,
            size: json_bytes.len() as u64,
            sha256_hash: put_result.sha256_hash,
            offset: 0,
            encoding: "application/json".to_string(),
        }
    }

    async fn write_request_ctx_to_state(state: &RouteState, ctx: RequestCtx) -> anyhow::Result<()> {
        // First, ensure the application exists
        let app = mock_app_with_retries(&ctx.application_name, &ctx.application_version, 0);
        state
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    crate::state_store::requests::CreateOrUpdateApplicationRequest {
                        namespace: ctx.namespace.clone(),
                        application: app,
                        upgrade_requests_to_current_version: false,
                    },
                )),
            })
            .await?;

        // Then write the request context
        state
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::InvokeApplication(InvokeApplicationRequest {
                    namespace: ctx.namespace.clone(),
                    application_name: ctx.application_name.clone(),
                    ctx,
                }),
            })
            .await
    }

    /// Helper to collect all events from a stream into a Vec of debug strings
    /// Note: This requires the stream to be pinned, so we use a macro-like
    /// pattern
    async fn collect_stream_events_pinned(
        stream: impl futures::Stream<Item = Result<axum::response::sse::Event, axum::Error>>,
    ) -> Vec<String> {
        use futures::StreamExt;
        futures::pin_mut!(stream);
        let mut events = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(event) => {
                    let event_str = format!("{:?}", event);
                    events.push(event_str);
                }
                Err(_) => break,
            }
        }
        events
    }

    /// Helper to update an existing request context's outcome in state
    async fn update_request_outcome(
        state: &RouteState,
        namespace: &str,
        app_name: &str,
        request_id: &str,
        outcome: RequestOutcome,
    ) -> anyhow::Result<()> {
        let mut ctx = state
            .indexify_state
            .reader()
            .request_ctx(namespace, app_name, request_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Request not found"))?;
        ctx.outcome = Some(outcome);

        let mut scheduler_update = crate::state_store::requests::SchedulerUpdateRequest::default();
        scheduler_update.add_request_state(&ctx);

        state
            .indexify_state
            .write(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate((
                    Box::new(scheduler_update),
                    vec![],
                )),
            })
            .await?;
        Ok(())
    }

    /// Helper to create a RequestStarted event
    fn create_request_started_event(
        namespace: &str,
        app_name: &str,
        app_version: &str,
        request_id: &str,
    ) -> RequestStateChangeEvent {
        RequestStateChangeEvent::RequestStarted(
            crate::state_store::request_events::RequestStartedEvent {
                namespace: namespace.to_string(),
                application_name: app_name.to_string(),
                application_version: app_version.to_string(),
                request_id: request_id.to_string(),
                created_at: chrono::Utc::now(),
            },
        )
    }

    /// Helper to create a RequestFinished event
    fn create_request_finished_event(
        namespace: &str,
        app_name: &str,
        app_version: &str,
        request_id: &str,
        outcome: RequestOutcome,
    ) -> RequestStateChangeEvent {
        RequestStateChangeEvent::RequestFinished(RequestFinishedEvent {
            namespace: namespace.to_string(),
            application_name: app_name.to_string(),
            application_version: app_version.to_string(),
            request_id: request_id.to_string(),
            outcome,
            created_at: chrono::Utc::now(),
            output: None,
        })
    }

    /// Helper to create a FunctionRunCreated event
    fn create_function_run_created_event(
        namespace: &str,
        app_name: &str,
        app_version: &str,
        request_id: &str,
        function_name: &str,
        function_run_id: &str,
    ) -> RequestStateChangeEvent {
        RequestStateChangeEvent::FunctionRunCreated(
            crate::state_store::request_events::FunctionRunCreated {
                namespace: namespace.to_string(),
                application_name: app_name.to_string(),
                application_version: app_version.to_string(),
                request_id: request_id.to_string(),
                function_name: function_name.to_string(),
                function_run_id: function_run_id.to_string(),
                created_at: chrono::Utc::now(),
            },
        )
    }

    /// Helper to set up a test with a request context and return the state and
    /// context
    async fn setup_test_with_request_ctx(
        namespace: &str,
        app_name: &str,
        app_version: &str,
        request_id: &str,
        outcome: Option<RequestOutcome>,
    ) -> (RouteState, RequestCtx) {
        let state = create_test_route_state().await;
        let ctx =
            create_request_ctx_with_outcome(namespace, app_name, app_version, request_id, outcome)
                .await;
        write_request_ctx_to_state(&state, ctx.clone())
            .await
            .unwrap();
        (state, ctx)
    }

    /// Helper to create a stream for testing
    /// Returns the sender and the stream
    async fn create_test_stream(
        state: RouteState,
        namespace: &str,
        app_name: &str,
        request_id: &str,
        channel_capacity: usize,
    ) -> (
        broadcast::Sender<RequestStateChangeEvent>,
        impl futures::Stream<Item = Result<axum::response::sse::Event, axum::Error>>,
    ) {
        let (tx, rx) = broadcast::channel(channel_capacity);
        let stream = create_request_progress_stream(
            rx,
            state,
            namespace.to_string(),
            app_name.to_string(),
            request_id.to_string(),
        )
        .await;
        (tx, stream)
    }

    /// Helper to assert that events contain a finished event
    fn assert_contains_finished_event(events: &[String]) {
        assert!(
            events
                .iter()
                .any(|e| e.contains("request_finished") || e.contains("RequestFinished")),
            "Events should contain a finished event: {:?}",
            events
        );
    }

    #[tokio::test]
    async fn test_initial_check_finished_success() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-123";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
        )
        .await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield one finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_initial_check_finished_failure() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-456";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Failure(RequestFailureReason::FunctionError)),
        )
        .await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield one finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_initial_check_finished_unknown() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-789";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Unknown),
        )
        .await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield one finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_initial_check_not_found() {
        let namespace = TEST_NAMESPACE;
        let request_id = "nonexistent-req";
        let state = create_test_route_state().await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(
            events.len(),
            0,
            "Should return empty stream when request not found"
        );
    }

    #[tokio::test]
    async fn test_initial_check_no_outcome() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-no-outcome";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        assert!(events.len() >= 1, "Should receive the finished event");
    }

    #[tokio::test]
    async fn test_receive_request_started_event() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-started";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let started_event =
            create_request_started_event(namespace, TEST_APP_NAME, TEST_APP_VERSION, request_id);
        tx.send(started_event).unwrap();
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(events.len(), 1, "Should receive the RequestStarted event");
    }

    #[tokio::test]
    async fn test_receive_function_run_created_event() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-fn-created";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let fn_created_event = create_function_run_created_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            "fn_a",
            "fn_run_1",
        );
        tx.send(fn_created_event).unwrap();
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(
            events.len(),
            1,
            "Should receive the FunctionRunCreated event"
        );
    }

    #[tokio::test]
    async fn test_receive_multiple_events() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-multiple";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let started_event =
            create_request_started_event(namespace, TEST_APP_NAME, TEST_APP_VERSION, request_id);
        tx.send(started_event).unwrap();

        let fn_created_event = create_function_run_created_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            "fn_a",
            "fn_run_1",
        );
        tx.send(fn_created_event).unwrap();
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(events.len(), 2, "Should receive both events");
    }

    #[tokio::test]
    async fn test_channel_closed() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-closed";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(events.len(), 0, "Should handle closed channel gracefully");
    }

    #[tokio::test]
    async fn test_receive_finished_event_with_finished_result() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-finished-1";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(events.len(), 1, "Should yield finished event");
    }

    #[tokio::test]
    async fn test_receive_finished_event_with_no_outcome() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-finished-2";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        // Send a RequestFinished event
        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        // Should yield the original event since check_for_finished returns NoOutcome
        assert_eq!(events.len(), 1, "Should yield original finished event");
    }

    #[tokio::test]
    async fn test_receive_finished_event_with_not_found() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-finished-3";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(
            events.len(),
            1,
            "Should yield finished event from check_for_finished"
        );
    }

    #[tokio::test]
    async fn test_success_outcome_no_output() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-success-no-output";

        let (state, mut ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
        )
        .await;
        if let Some(fn_run) = ctx.function_runs.get_mut(&FunctionCallId::from(request_id)) {
            fn_run.output = None;
        }
        let mut scheduler_update = crate::state_store::requests::SchedulerUpdateRequest::default();
        scheduler_update.add_request_state(&ctx);
        state
            .indexify_state
            .write(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate((
                    Box::new(scheduler_update),
                    vec![],
                )),
            })
            .await
            .unwrap();

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_stream_progress_then_finished() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-progress-then-finished";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let started_event =
            create_request_started_event(namespace, TEST_APP_NAME, TEST_APP_VERSION, request_id);
        tx.send(started_event).unwrap();

        let fn_created_event = create_function_run_created_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            "fn_a",
            "fn_run_1",
        );
        tx.send(fn_created_event).unwrap();

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        assert!(
            events.len() >= 2,
            "Should receive progress events and finished event"
        );
    }

    #[tokio::test]
    async fn test_stream_starts_finished() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-already-finished";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
        )
        .await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        drop(_tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(events.len(), 1, "Should return finished event immediately");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_success_outcome_with_json_output() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-success-json";
        let state = create_test_route_state().await;

        let json_data = json!({"result": "success", "value": 42});
        let output_payload = create_json_output_payload(&state, namespace, json_data.clone()).await;

        let ctx = create_request_ctx_with_outcome_and_output(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
            Some(output_payload),
        )
        .await;
        write_request_ctx_to_state(&state, ctx).await.unwrap();

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_success_outcome_with_non_json_output() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-success-non-json";
        let state = create_test_route_state().await;

        let payload_key = format!("{}/output", nanoid::nanoid!());
        let blob_store = state.blob_storage.get_blob_store(namespace);
        let data: Vec<u8> = b"binary data".to_vec();
        let data_len = data.len();
        let stream = futures::stream::iter(vec![Ok(Bytes::from(data))]);
        let put_result = blob_store
            .put(&payload_key, Box::pin(stream))
            .await
            .unwrap();

        let output_payload = DataPayload {
            id: nanoid::nanoid!(),
            metadata_size: 0,
            path: put_result.url,
            size: data_len as u64,
            sha256_hash: put_result.sha256_hash,
            offset: 0,
            encoding: "application/octet-stream".to_string(),
        };

        let ctx = create_request_ctx_with_outcome_and_output(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
            Some(output_payload),
        )
        .await;
        write_request_ctx_to_state(&state, ctx).await.unwrap();

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_success_outcome_with_large_output() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-success-large";
        let state = create_test_route_state().await;

        let large_data: Vec<u8> = vec![0; 2 * 1024 * 1024];
        let payload_key = format!("{}/output", nanoid::nanoid!());
        let blob_store = state.blob_storage.get_blob_store(namespace);
        let stream = futures::stream::iter(vec![Ok(Bytes::from(large_data.clone()))]);
        let put_result = blob_store
            .put(&payload_key, Box::pin(stream))
            .await
            .unwrap();

        let output_payload = DataPayload {
            id: nanoid::nanoid!(),
            metadata_size: 0,
            path: put_result.url,
            size: large_data.len() as u64,
            sha256_hash: put_result.sha256_hash,
            offset: 0,
            encoding: "application/json".to_string(),
        };

        let ctx = create_request_ctx_with_outcome_and_output(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
            Some(output_payload),
        )
        .await;
        write_request_ctx_to_state(&state, ctx).await.unwrap();

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;
        let events = collect_stream_events_pinned(stream).await;

        assert_eq!(events.len(), 1, "Should yield finished event");
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_lag_recovery_finished() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-lag-finished";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state.clone(),
            namespace,
            TEST_APP_NAME,
            request_id,
            LAG_CHANNEL_CAPACITY,
        )
        .await;

        for i in 0..5 {
            let event = create_request_started_event(
                namespace,
                TEST_APP_NAME,
                TEST_APP_VERSION,
                &format!("{}-{}", request_id, i),
            );
            let _ = tx.send(event);
        }

        update_request_outcome(
            &state,
            namespace,
            TEST_APP_NAME,
            request_id,
            RequestOutcome::Success,
        )
        .await
        .unwrap();

        let events = collect_stream_events_pinned(stream).await;
        assert!(events.len() >= 1, "Should receive finished event after lag");
    }

    #[tokio::test]
    async fn test_finished_event_fallback_to_original() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-fallback";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None,
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        // Send a RequestFinished event
        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        // Should yield the original event since check_for_finished returns NoOutcome
        assert_eq!(events.len(), 1, "Should yield original finished event");
    }

    #[tokio::test]
    async fn test_always_send_final_message_when_finished() {
        let namespace = TEST_NAMESPACE;
        let request_id = "req-always-final-1";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            Some(RequestOutcome::Success),
        )
        .await;

        let (_tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(
            events.len(),
            1,
            "Should send final message when request is finished"
        );
        assert_contains_finished_event(&events);

        let request_id2 = "req-always-final-2";
        let (state2, _ctx2) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id2,
            None,
        )
        .await;

        let (tx2, stream2) = create_test_stream(
            state2,
            namespace,
            TEST_APP_NAME,
            request_id2,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id2,
            RequestOutcome::Success,
        );
        tx2.send(finished_event).unwrap();

        let events2 = collect_stream_events_pinned(stream2).await;
        assert_eq!(
            events2.len(),
            1,
            "Should send final message when RequestFinished event received"
        );
        assert_contains_finished_event(&events2);
    }

    #[tokio::test]
    async fn test_lag_recovery_no_outcome_continues_stream() {
        // Tests that when lag occurs but request is still in progress (NoOutcome),
        // the stream continues listening and can receive the finished event later
        let namespace = TEST_NAMESPACE;
        let request_id = "req-lag-no-outcome";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None, // No outcome yet - request still in progress
        )
        .await;

        let (tx, stream) = create_test_stream(
            state.clone(),
            namespace,
            TEST_APP_NAME,
            request_id,
            LAG_CHANNEL_CAPACITY, // Small capacity to trigger lag
        )
        .await;

        // Send multiple events to trigger lag
        for i in 0..3 {
            let event = create_request_started_event(
                namespace,
                TEST_APP_NAME,
                TEST_APP_VERSION,
                &format!("{}-{}", request_id, i),
            );
            let _ = tx.send(event);
        }

        // After lag recovery (request still in progress), send the actual finished event
        // This simulates the request completing after the lag was handled
        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Success,
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        // Should have received the finished event after recovering from lag
        assert!(
            events.len() >= 1,
            "Should receive finished event after lag recovery with NoOutcome"
        );
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_lag_with_not_found_stops_stream() {
        // Tests that when lag occurs and request is not found, the stream stops
        let namespace = TEST_NAMESPACE;
        let request_id = "req-lag-not-found";

        // Create state but DON'T create the request - simulates request not found
        let state = create_test_route_state().await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        // The initial check will return NotFound and stop the stream
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        assert_eq!(
            events.len(),
            0,
            "Should return empty stream when request not found"
        );
    }

    #[tokio::test]
    async fn test_lag_then_finished_event_received() {
        // Tests the full flow: lag occurs, request still in progress, then finished event arrives
        let namespace = TEST_NAMESPACE;
        let request_id = "req-lag-then-finish";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None, // No outcome yet
        )
        .await;

        let (tx, stream) = create_test_stream(
            state.clone(),
            namespace,
            TEST_APP_NAME,
            request_id,
            LAG_CHANNEL_CAPACITY,
        )
        .await;

        // Trigger lag with multiple events
        for i in 0..5 {
            let event = create_function_run_created_event(
                namespace,
                TEST_APP_NAME,
                TEST_APP_VERSION,
                request_id,
                "fn_a",
                &format!("fn_run_{}", i),
            );
            let _ = tx.send(event);
        }

        // Now update the state to have an outcome and send finished event
        update_request_outcome(
            &state,
            namespace,
            TEST_APP_NAME,
            request_id,
            RequestOutcome::Failure(RequestFailureReason::FunctionError),
        )
        .await
        .unwrap();

        let finished_event = create_request_finished_event(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            RequestOutcome::Failure(RequestFailureReason::FunctionError),
        );
        tx.send(finished_event).unwrap();

        let events = collect_stream_events_pinned(stream).await;
        // Should receive at least the finished event
        assert!(
            events.len() >= 1,
            "Should receive finished event after lag and state update"
        );
    }

    #[tokio::test]
    async fn test_channel_closed_checks_for_finished() {
        // Tests that when the channel closes, we check if the request finished
        // and send the final event if so
        let namespace = TEST_NAMESPACE;
        let request_id = "req-closed-finished";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None, // No outcome yet
        )
        .await;

        let (tx, stream) = create_test_stream(
            state.clone(),
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        // Update the request to finished state before closing channel
        update_request_outcome(
            &state,
            namespace,
            TEST_APP_NAME,
            request_id,
            RequestOutcome::Success,
        )
        .await
        .unwrap();

        // Close the channel without sending any events
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        // Should receive the finished event because we check state on channel close
        assert_eq!(
            events.len(),
            1,
            "Should receive finished event when channel closes after request completes"
        );
        assert_contains_finished_event(&events);
    }

    #[tokio::test]
    async fn test_channel_closed_no_outcome() {
        // Tests that when the channel closes and request has no outcome,
        // we don't send anything
        let namespace = TEST_NAMESPACE;
        let request_id = "req-closed-no-outcome";

        let (state, _ctx) = setup_test_with_request_ctx(
            namespace,
            TEST_APP_NAME,
            TEST_APP_VERSION,
            request_id,
            None, // No outcome
        )
        .await;

        let (tx, stream) = create_test_stream(
            state,
            namespace,
            TEST_APP_NAME,
            request_id,
            DEFAULT_CHANNEL_CAPACITY,
        )
        .await;

        // Close the channel without updating outcome
        drop(tx);

        let events = collect_stream_events_pinned(stream).await;
        // Should not receive any events since request has no outcome
        assert_eq!(
            events.len(),
            0,
            "Should not receive events when channel closes and request has no outcome"
        );
    }
}
