#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use mock_instant::global::MockClock;

    use crate::{
        data_model::{
            Allocation,
            FunctionRunOutcome,
            Namespace,
            test_objects::tests::{self as test_objects, TEST_EXECUTOR_ID, TEST_NAMESPACE},
        },
        state_store::{
            requests::{
                CreateOrUpdateApplicationRequest,
                DeleteApplicationRequest,
                NamespaceRequest,
                RequestPayload,
                StateMachineUpdateRequest,
            },
            scanner::CursorDirection,
            state_machine::IndexifyObjectsColumns,
            test_state_store,
        },
        testing::{FinalizeFunctionRunArgs, TestService, allocation_key_from_proto},
    };

    #[tokio::test]
    async fn test_get_gc_urls() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no GC URLs
        let (urls, cursor) = indexify_state.reader().get_gc_urls(None)?;
        assert_eq!(urls.len(), 0);
        assert!(cursor.is_none());

        // Create an application
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        // Delete the application to generate GC URLs
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "graph_A".to_string(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Now there should be GC URLs
        let (urls, cursor) = indexify_state.reader().get_gc_urls(None)?;
        assert!(urls.len() > 0);
        assert!(cursor.is_none()); // Since limit is None, all should be returned

        // Test with limit
        let limit = Some(urls.len() / 2);
        let (limited_urls, cursor) = indexify_state.reader().get_gc_urls(limit)?;
        if urls.len() > 1 {
            assert_eq!(limited_urls.len(), limit.unwrap());
            assert!(cursor.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_all_unprocessed_state_changes() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no unprocessed state changes
        let changes = indexify_state.reader().all_unprocessed_state_changes()?;
        assert_eq!(changes.len(), 0);

        // Create an application to generate state changes
        test_state_store::with_simple_application(&indexify_state).await;

        // Should have state changes now
        let changes = indexify_state.reader().all_unprocessed_state_changes()?;
        assert!(changes.len() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_unprocessed_state_changes() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application to generate state changes
        test_state_store::with_simple_application(&indexify_state).await;

        // Get unprocessed state changes
        let unprocessed = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?;
        assert!(unprocessed.changes.len() > 0);

        // Process them
        test_srv.process_all_state_changes().await?;

        // Should be no more unprocessed
        let unprocessed = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)?;
        assert_eq!(unprocessed.changes.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_allocation_usage() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no allocation usage
        let (usage, cursor) = indexify_state.reader().allocation_usage(None)?;
        assert_eq!(usage.len(), 0);
        assert!(cursor.is_none());

        // Create application, executor, and complete tasks to generate allocation usage
        let _request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        let executor = test_srv
            .create_executor(test_objects::mock_executor_metadata(
                TEST_EXECUTOR_ID.into(),
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        let desired_state = executor.desired_state().await;
        assert_eq!(desired_state.allocations.len(), 1);
        let allocation = desired_state.allocations.first().unwrap();
        executor
            .finalize_allocation(
                allocation,
                FinalizeFunctionRunArgs::new(
                    allocation_key_from_proto(allocation),
                    Some(test_objects::mock_updates()),
                    None,
                )
                .function_run_outcome(FunctionRunOutcome::Success),
            )
            .await?;
        test_srv.process_all_state_changes().await?;

        // Should have allocation usage now
        let (usage, cursor) = indexify_state.reader().allocation_usage(None)?;
        assert!(usage.len() > 0);
        assert!(cursor.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_namespace() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no namespace
        let ns = indexify_state.reader().get_namespace("nonexistent")?;
        assert!(ns.is_none());

        // Create a namespace
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "test_ns".to_string(),
                    blob_storage_bucket: None,
                    blob_storage_region: None,
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Should be able to get it
        let ns = indexify_state.reader().get_namespace("test_ns")?;
        assert!(ns.is_some());
        assert_eq!(ns.unwrap().name, "test_ns");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_namespaces() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, might have some default namespaces
        let namespaces = indexify_state.reader().get_all_namespaces()?;
        let initial_count = namespaces.len();

        // Create a namespace
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "test_ns".to_string(),
                    blob_storage_bucket: None,
                    blob_storage_region: None,
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Should have one more
        let namespaces = indexify_state.reader().get_all_namespaces()?;
        assert_eq!(namespaces.len(), initial_count + 1);
        assert!(namespaces.iter().any(|ns| ns.name == "test_ns"));

        Ok(())
    }

    #[tokio::test]
    async fn test_list_applications() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no applications in TEST_NAMESPACE
        let (apps, cursor) =
            indexify_state
                .reader()
                .list_applications(TEST_NAMESPACE, None, None)?;
        assert_eq!(apps.len(), 0);
        assert!(cursor.is_none());

        // Create an applications
        test_state_store::create_or_update_application(&indexify_state, "app_1", 0).await;
        test_state_store::create_or_update_application(&indexify_state, "app_2", 0).await;
        test_state_store::create_or_update_application(&indexify_state, "app_3", 0).await;
        test_state_store::create_or_update_application(&indexify_state, "app_4", 0).await;

        // Should list all applications
        let (apps, cursor) =
            indexify_state
                .reader()
                .list_applications(TEST_NAMESPACE, None, None)?;
        assert_eq!(4, apps.len());
        assert!(cursor.is_none());
        assert_eq!("app_1", apps[0].name);
        assert_eq!("app_2", apps[1].name);
        assert_eq!("app_3", apps[2].name);
        assert_eq!("app_4", apps[3].name);

        // Should list only the first two applications
        let (apps, cursor) =
            indexify_state
                .reader()
                .list_applications(TEST_NAMESPACE, None, Some(2))?;
        assert_eq!(2, apps.len());
        assert!(cursor.is_some());
        assert_eq!("app_1", apps[0].name);
        assert_eq!("app_2", apps[1].name);

        // Should list only the next two applications
        let (apps, cursor) =
            indexify_state
                .reader()
                .list_applications(TEST_NAMESPACE, cursor.as_deref(), None)?;
        assert_eq!(2, apps.len());
        assert!(cursor.is_none());
        assert_eq!("app_3", apps[0].name);
        assert_eq!("app_4", apps[1].name);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_application() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no application
        let app = indexify_state
            .reader()
            .get_application(TEST_NAMESPACE, "graph_A")?;
        assert!(app.is_none());

        // Create an application
        test_state_store::with_simple_application(&indexify_state).await;

        // Should be able to get it
        let app = indexify_state
            .reader()
            .get_application(TEST_NAMESPACE, "graph_A")?;
        assert!(app.is_some());
        assert_eq!(app.unwrap().name, "graph_A");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_application_version() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application
        let mut app = test_objects::mock_application();
        app.name = "test_app".to_string();
        app.version = "v1.0".to_string();

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: true,
                    },
                )),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Should be able to get the version
        let app_version =
            indexify_state
                .reader()
                .get_application_version(TEST_NAMESPACE, "test_app", "v1.0")?;
        assert!(app_version.is_some());
        assert_eq!(app_version.unwrap().version, "v1.0");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_allocation() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create application and executor to generate allocations
        test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        test_srv
            .create_executor(test_objects::mock_executor_metadata(
                TEST_EXECUTOR_ID.into(),
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Get all allocations (we need to find one)
        let (allocations, _) = indexify_state
            .reader()
            .get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                Some(1),
            )?;
        assert!(allocations.len() > 0);

        let allocation_key = allocations[0].key();
        let allocation = indexify_state.reader().get_allocation(&allocation_key)?;
        assert!(allocation.is_some());
        assert_eq!(allocation.unwrap().id, allocations[0].id);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_allocations_by_request_id() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create application and executor to generate allocations
        let request_id = test_state_store::with_simple_application(&indexify_state).await;
        test_srv.process_all_state_changes().await?;

        test_srv
            .create_executor(test_objects::mock_executor_metadata(
                TEST_EXECUTOR_ID.into(),
            ))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Should have allocations for the request
        let allocations = indexify_state.reader().get_allocations_by_request_id(
            TEST_NAMESPACE,
            "graph_A",
            &request_id,
        )?;
        assert!(allocations.len() > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_request_ctx() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially, no request context
        let ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", "nonexistent")?;
        assert!(ctx.is_none());

        // Create an application (which creates a request)
        let request_id = test_state_store::with_simple_application(&indexify_state).await;

        // Should have request context
        let ctx = indexify_state
            .reader()
            .request_ctx(TEST_NAMESPACE, "graph_A", &request_id)?;
        assert!(ctx.is_some());
        assert_eq!(ctx.unwrap().request_id, request_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_requests() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application to generate requests
        let app = test_state_store::create_or_update_application(&indexify_state, "app_1", 0).await;
        test_srv.process_all_state_changes().await?;
        let request_id = test_state_store::invoke_application(&indexify_state, &app).await?;
        test_srv.process_all_state_changes().await?;

        MockClock::advance_system_time(Duration::from_secs(1));

        // List requests
        let (requests, prev_cursor, next_cursor) =
            indexify_state
                .reader()
                .list_requests(TEST_NAMESPACE, &app.name, None, 10, None)?;
        assert_eq!(1, requests.len());
        assert_eq!(request_id, requests[0].request_id);
        assert!(prev_cursor.is_none()); // First page
        assert!(next_cursor.is_none()); // All fit in one page

        Ok(())
    }

    #[tokio::test]
    async fn test_list_requests_with_direction() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create an application
        let app = test_state_store::create_or_update_application(&indexify_state, "app_1", 0).await;
        test_srv.process_all_state_changes().await?;

        // Invoke the application multiple times to create multiple requests
        let request_id1 = test_state_store::invoke_application(&indexify_state, &app).await?;
        test_srv.process_all_state_changes().await?;
        MockClock::advance_system_time(Duration::from_secs(1));

        let request_id2 = test_state_store::invoke_application(&indexify_state, &app).await?;
        test_srv.process_all_state_changes().await?;
        MockClock::advance_system_time(Duration::from_secs(1));

        let request_id3 = test_state_store::invoke_application(&indexify_state, &app).await?;
        test_srv.process_all_state_changes().await?;
        MockClock::advance_system_time(Duration::from_secs(1));

        let request_id4 = test_state_store::invoke_application(&indexify_state, &app).await?;
        test_srv.process_all_state_changes().await?;
        MockClock::advance_system_time(Duration::from_secs(1));

        // List requests with forward direction
        let (requests, ..) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            None,
            10,
            Some(CursorDirection::Forward),
        )?;
        assert_eq!(4, requests.len());
        assert_eq!(request_id4, requests[0].request_id);
        assert_eq!(request_id3, requests[1].request_id);
        assert_eq!(request_id2, requests[2].request_id);
        assert_eq!(request_id1, requests[3].request_id);

        // List requests with backward direction
        let (requests, ..) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            None,
            10,
            Some(CursorDirection::Backward),
        )?;
        // The ordering is the same as the forward direction.
        // See https://github.com/tensorlakeai/indexify/blob/75e392fb2c944631d9f99783ab39d185cd2ac740/server/src/state_store/scanner.rs#L344-L347
        assert_eq!(4, requests.len());
        assert_eq!(request_id4, requests[0].request_id);
        assert_eq!(request_id3, requests[1].request_id);
        assert_eq!(request_id2, requests[2].request_id);
        assert_eq!(request_id1, requests[3].request_id);

        // List requests with a cursor
        let (requests, prev_cursor, next_cursor) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            None,
            2,
            Some(CursorDirection::Forward),
        )?;
        assert_eq!(2, requests.len());
        assert_eq!(request_id4, requests[0].request_id);
        assert_eq!(request_id3, requests[1].request_id);
        assert_eq!(None, prev_cursor);
        assert!(next_cursor.is_some());

        let (requests, prev_cursor, next_cursor) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            next_cursor.as_deref(),
            2,
            Some(CursorDirection::Forward),
        )?;
        assert_eq!(2, requests.len());
        assert_eq!(request_id2, requests[0].request_id);
        assert_eq!(request_id1, requests[1].request_id);
        assert!(prev_cursor.is_some());
        assert_eq!(None, next_cursor);

        let (requests, prev_cursor, next_cursor) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            None,
            2,
            Some(CursorDirection::Backward),
        )?;
        assert_eq!(2, requests.len());
        assert_eq!(request_id2, requests[0].request_id);
        assert_eq!(request_id1, requests[1].request_id);
        assert!(prev_cursor.is_some());
        assert_eq!(None, next_cursor);

        let (requests, prev_cursor, next_cursor) = indexify_state.reader().list_requests(
            TEST_NAMESPACE,
            &app.name,
            prev_cursor.as_deref(),
            2,
            Some(CursorDirection::Backward),
        )?;
        assert_eq!(2, requests.len());
        assert_eq!(request_id4, requests[0].request_id);
        assert_eq!(request_id3, requests[1].request_id);
        assert_eq!(None, prev_cursor);
        assert!(next_cursor.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_from_cf() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a namespace
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "test_ns".to_string(),
                    blob_storage_bucket: None,
                    blob_storage_region: None,
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Test get_from_cf
        let ns: Option<Namespace> = indexify_state
            .reader()
            .get_from_cf(&IndexifyObjectsColumns::Namespaces, "test_ns")?;
        assert!(ns.is_some());
        assert_eq!(ns.unwrap().name, "test_ns");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_rows_from_cf() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create a namespace
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "test_ns".to_string(),
                    blob_storage_bucket: None,
                    blob_storage_region: None,
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // Test get_all_rows_from_cf
        let rows: Vec<(String, Namespace)> = indexify_state
            .reader()
            .get_all_rows_from_cf(IndexifyObjectsColumns::Namespaces)?;
        assert!(rows.len() > 0);
        assert!(rows.iter().any(|(_, ns)| ns.name == "test_ns"));

        Ok(())
    }
}
