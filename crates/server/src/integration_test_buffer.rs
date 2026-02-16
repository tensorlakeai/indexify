//! Integration tests for the container buffer (warm pool) feature.
//!
//! These tests verify end-to-end functionality of:
//! - Function buffer_containers: Keeping idle containers for functions
//! - Container pools: Pre-warming containers for sandboxes
//!
//! The buffer_reconciler runs during state change processing. When a pool is
//! created via CreateContainerPoolRequest, it generates a CreateContainerPool
//! state change that triggers buffer reconciliation when processed.

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        data_model::{
            ContainerPool,
            ContainerPoolBuilder,
            ContainerPoolId,
            ContainerPoolKey,
            ContainerResources,
            SandboxStatus,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_app_with_buffer,
                mock_container_pool,
                mock_executor_metadata,
                mock_sandbox_executor_metadata,
                mock_sandbox_with_pool,
            },
        },
        state_store::requests::{
            CreateContainerPoolRequest,
            CreateOrUpdateApplicationRequest,
            CreateSandboxRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        testing::{self, TestService},
    };

    /// Helper to register a container pool using the proper state machine flow.
    ///
    /// This creates the pool via CreateContainerPoolRequest, which:
    /// 1. Persists the pool to storage
    /// 2. Updates container_scheduler.function_pools or sandbox_pools
    /// 3. Generates a CreateContainerPool state change event
    ///
    /// After calling this, you must call `process_all_state_changes()` to
    /// trigger buffer reconciliation which will create warm containers.
    async fn register_container_pool(test_srv: &TestService, pool: ContainerPool) -> Result<()> {
        test_srv
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateContainerPool(CreateContainerPoolRequest { pool }),
            })
            .await?;
        Ok(())
    }

    /// Helper to get the count of containers for a function from the container
    /// scheduler
    async fn get_function_container_counts(
        test_srv: &TestService,
        namespace: &str,
        app_name: &str,
        fn_name: &str,
        version: &str,
    ) -> (u32, u32) {
        let function_uri = crate::data_model::FunctionURI {
            namespace: namespace.to_string(),
            application: app_name.to_string(),
            function: fn_name.to_string(),
            version: version.to_string(),
        };
        let container_scheduler = test_srv
            .service
            .indexify_state
            .container_scheduler
            .read()
            .await;
        container_scheduler.count_active_idle_containers(&function_uri)
    }

    /// Helper to get the count of containers for a container pool
    async fn get_pool_container_counts(
        test_srv: &TestService,
        pool_key: &ContainerPoolKey,
    ) -> (u32, u32) {
        let container_scheduler = test_srv
            .service
            .indexify_state
            .container_scheduler
            .read()
            .await;
        container_scheduler.count_pool_containers(pool_key)
    }

    /// Helper to create an application with buffer_containers configured
    async fn create_app_with_buffer(
        test_srv: &TestService,
        app_name: &str,
        buffer: u32,
    ) -> Result<()> {
        let app = mock_app_with_buffer(app_name, "1", buffer);
        let version = app.version.clone();
        // Create container pools for all functions (as API layer would)
        let container_pools: Vec<ContainerPool> = app
            .functions
            .values()
            .map(|function| {
                ContainerPool::from_function(TEST_NAMESPACE, app_name, &version, function)
            })
            .collect();
        let request = CreateOrUpdateApplicationRequest {
            namespace: TEST_NAMESPACE.to_string(),
            application: app,
            upgrade_requests_to_current_version: true,
            container_pools,
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

    // ==================== FUNCTION BUFFER TESTS ====================

    #[tokio::test]
    async fn test_function_buffer_creates_idle_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Create app with function having buffer_containers=3
        create_app_with_buffer(&test_srv, "buffer_app", 3).await?;
        test_srv.process_all_state_changes().await?;

        // Register regular executor (not sandbox executor) to provide resources for
        // functions Note: sandbox executors (version 0.2.0) only handle
        // sandboxes, not functions
        let _executor = test_srv
            .create_executor(mock_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Get container counts for fn_a (which has buffer=3)
        let (active, idle) =
            get_function_container_counts(&test_srv, TEST_NAMESPACE, "buffer_app", "fn_a", "1")
                .await;

        // Should have 3 idle containers (buffer) and 0 active (no work allocated)
        assert_eq!(active, 0, "Expected 0 active containers, got {}", active);
        assert_eq!(idle, 3, "Expected 3 idle containers (buffer), got {}", idle);

        Ok(())
    }

    #[tokio::test]
    async fn test_function_buffer_no_executor_no_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Create app with function having buffer_containers=3
        create_app_with_buffer(&test_srv, "buffer_app", 3).await?;
        test_srv.process_all_state_changes().await?;

        // DON'T register executor - no resources available
        // Note: buffer reconciler won't run without state changes, and no executor
        // means no state changes from executor heartbeat

        // Get container counts
        let (active, idle) =
            get_function_container_counts(&test_srv, TEST_NAMESPACE, "buffer_app", "fn_a", "1")
                .await;

        // Should have 0 containers - no executor to schedule on
        assert_eq!(active, 0, "Expected 0 active containers without executor");
        assert_eq!(idle, 0, "Expected 0 idle containers without executor");

        Ok(())
    }

    // ==================== SANDBOX POOL TESTS ====================

    #[tokio::test]
    async fn test_sandbox_pool_prewarms_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Register sandbox executor first (realistic scenario)
        let _executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register container pool AFTER executor exists
        let pool = mock_container_pool(TEST_NAMESPACE, "test_pool");
        let pool_key = ContainerPoolKey::from(&pool);
        register_container_pool(&test_srv, pool).await?;

        // Process state changes to trigger buffer reconciliation
        // The CreateContainerPool state change triggers reconciliation automatically
        test_srv.process_all_state_changes().await?;

        // Get pool container counts
        let (claimed, warm) = get_pool_container_counts(&test_srv, &pool_key).await;

        // Should have 3 warm containers (buffer) and 0 claimed
        assert_eq!(claimed, 0, "Expected 0 claimed containers, got {}", claimed);
        assert_eq!(warm, 3, "Expected 3 warm containers (buffer), got {}", warm);

        Ok(())
    }

    #[tokio::test]
    async fn test_pool_respects_max_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Register sandbox executor first
        let _executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register pool AFTER executor (realistic scenario)
        let pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("max_test_pool"))
            .namespace(TEST_NAMESPACE.to_string())
            .image("python:3.11".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(0))
            .max_containers(Some(5))
            .buffer_containers(Some(10)) // buffer wants more than max allows
            .build()
            .unwrap();
        let pool_key = ContainerPoolKey::from(&pool);
        register_container_pool(&test_srv, pool).await?;

        // Process state changes to trigger buffer reconciliation
        test_srv.process_all_state_changes().await?;

        // Get pool container counts
        let (claimed, warm) = get_pool_container_counts(&test_srv, &pool_key).await;
        let total = claimed + warm;

        // Should be capped at max=5
        assert!(
            total <= 5,
            "Total containers {} should not exceed max_containers=5",
            total
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pool_min_takes_priority() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Register sandbox executor first
        let _executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register pool AFTER executor (realistic scenario)
        let pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("min_test_pool"))
            .namespace(TEST_NAMESPACE.to_string())
            .image("python:3.11".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(5)) // min is higher than buffer
            .max_containers(Some(10))
            .buffer_containers(Some(2))
            .build()
            .unwrap();
        let pool_key = ContainerPoolKey::from(&pool);
        register_container_pool(&test_srv, pool).await?;

        // Process state changes to trigger buffer reconciliation
        test_srv.process_all_state_changes().await?;

        // Get pool container counts
        let (claimed, warm) = get_pool_container_counts(&test_srv, &pool_key).await;
        let total = claimed + warm;

        // Should have at least min=5 containers
        assert!(
            total >= 5,
            "Total containers {} should be at least min_containers=5",
            total
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_claims_from_pool() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register sandbox executor first
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register pool AFTER executor (realistic scenario)
        let pool = mock_container_pool(TEST_NAMESPACE, "claim_test_pool");
        let pool_key = ContainerPoolKey::from(&pool);
        register_container_pool(&test_srv, pool).await?;

        // Process state changes to trigger buffer reconciliation
        test_srv.process_all_state_changes().await?;

        // Mark pool containers as Running via heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify warm containers exist
        let (_claimed_before, warm_before) = get_pool_container_counts(&test_srv, &pool_key).await;
        assert!(warm_before > 0, "Should have warm containers in pool");

        // Create a sandbox that references the pool
        let sandbox = mock_sandbox_with_pool(TEST_NAMESPACE, "claim_test_pool");
        let sandbox_id = sandbox.id.clone();

        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateSandbox(CreateSandboxRequest {
                sandbox: sandbox.clone(),
            }),
        };
        indexify_state.write(request).await?;
        test_srv.process_all_state_changes().await?;

        // After claim, sandbox is Pending/WaitingForContainer. Heartbeat promotes it.
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox was promoted to Running
        let sandbox = indexify_state
            .reader()
            .get_sandbox(TEST_NAMESPACE, sandbox_id.get())
            .await?
            .expect("Sandbox should exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Running,
            "Sandbox should be Running after container reports Running"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pool_no_executor_no_containers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Register pool
        let pool = mock_container_pool(TEST_NAMESPACE, "no_exec_pool");
        let pool_key = ContainerPoolKey::from(&pool);
        register_container_pool(&test_srv, pool).await?;

        // DON'T register executor - no resources available
        // Note: buffer_reconciler won't run without state changes from executor
        // heartbeat

        // Get pool container counts
        let (claimed, warm) = get_pool_container_counts(&test_srv, &pool_key).await;

        // Should have 0 containers - no executor to schedule on
        assert_eq!(claimed, 0, "Expected 0 claimed containers without executor");
        assert_eq!(warm, 0, "Expected 0 warm containers without executor");

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_pools_fair_allocation() -> Result<()> {
        let test_srv = testing::TestService::new().await?;

        // Register sandbox executor first
        let _executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register both pools AFTER executor (realistic scenario)
        let pool1 = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("pool1"))
            .namespace(TEST_NAMESPACE.to_string())
            .image("python:3.11".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(0))
            .max_containers(Some(10))
            .buffer_containers(Some(3))
            .build()
            .unwrap();
        let pool1_key = ContainerPoolKey::from(&pool1);

        let pool2 = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("pool2"))
            .namespace(TEST_NAMESPACE.to_string())
            .image("python:3.11".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(0))
            .max_containers(Some(10))
            .buffer_containers(Some(3))
            .build()
            .unwrap();
        let pool2_key = ContainerPoolKey::from(&pool2);

        register_container_pool(&test_srv, pool1).await?;
        register_container_pool(&test_srv, pool2).await?;

        // Process state changes to trigger buffer reconciliation
        test_srv.process_all_state_changes().await?;

        // Get counts for both pools
        let (_, warm1) = get_pool_container_counts(&test_srv, &pool1_key).await;
        let (_, warm2) = get_pool_container_counts(&test_srv, &pool2_key).await;

        // Both pools should have containers (round-robin fairness)
        // Note: exact count depends on available resources, but both should get some
        tracing::info!("Pool1 warm: {}, Pool2 warm: {}", warm1, warm2);

        // If enough resources, both should have 3 each
        // If limited, both should have at least 1 (fair distribution)
        if warm1 + warm2 >= 2 {
            assert!(warm1 > 0, "Pool1 should have at least 1 warm container");
            assert!(warm2 > 0, "Pool2 should have at least 1 warm container");
        }

        Ok(())
    }

    /// Tests that when a sandbox claims a container from a pool, the container
    /// has the correct sandbox_id set. This is the key invariant for the bug
    /// fix where pool container termination needs to find the associated
    /// sandbox.
    #[tokio::test]
    async fn test_pool_container_has_sandbox_id_after_claim() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register sandbox executor first
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register pool
        let pool = mock_container_pool(TEST_NAMESPACE, "sandbox_id_test_pool");
        let pool_key = ContainerPoolKey::from(&pool);
        let pool_id = pool.id.clone();
        register_container_pool(&test_srv, pool).await?;
        test_srv.process_all_state_changes().await?;

        // Mark pool containers as Running via heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify warm containers exist and they have pool_id but no sandbox_id
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let warm_container = container_scheduler
                .function_containers
                .iter()
                .find(|(_, meta)| {
                    meta.function_container.pool_id.as_ref() == Some(&pool_id) &&
                        meta.function_container.sandbox_id.is_none()
                })
                .map(|(id, meta)| (id.clone(), meta.function_container.clone()));

            assert!(
                warm_container.is_some(),
                "Should have a warm container with pool_id set but no sandbox_id"
            );
            let (_, container) = warm_container.unwrap();
            assert_eq!(
                container.pool_id,
                Some(pool_id.clone()),
                "Warm container should have pool_id set"
            );
            assert!(
                container.sandbox_id.is_none(),
                "Warm container should not have sandbox_id before claim"
            );
        }

        // Create a sandbox that references the pool
        let sandbox = mock_sandbox_with_pool(TEST_NAMESPACE, "sandbox_id_test_pool");
        let sandbox_id = sandbox.id.clone();

        let request = StateMachineUpdateRequest {
            payload: RequestPayload::CreateSandbox(CreateSandboxRequest {
                sandbox: sandbox.clone(),
            }),
        };
        indexify_state.write(request).await?;
        test_srv.process_all_state_changes().await?;

        // After claim, sandbox is Pending/WaitingForContainer. Heartbeat promotes it.
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is running
        let sandbox = indexify_state
            .reader()
            .get_sandbox(TEST_NAMESPACE, sandbox_id.get())
            .await?
            .expect("Sandbox should exist");
        assert_eq!(
            sandbox.status,
            SandboxStatus::Running,
            "Sandbox should be Running"
        );

        // Verify the container now has sandbox_id set (the key invariant!)
        {
            let container_scheduler = indexify_state.container_scheduler.read().await;
            let claimed_container = container_scheduler
                .function_containers
                .iter()
                .find(|(_, meta)| meta.function_container.sandbox_id.as_ref() == Some(&sandbox_id))
                .map(|(id, meta)| (id.clone(), meta.function_container.clone()));

            assert!(
                claimed_container.is_some(),
                "Should find a container with sandbox_id set after claim"
            );
            let (_, container) = claimed_container.unwrap();
            assert_eq!(
                container.sandbox_id.as_ref(),
                Some(&sandbox_id),
                "Claimed container should have sandbox_id set"
            );
            assert_eq!(
                container.pool_id,
                Some(pool_id.clone()),
                "Claimed container should still have pool_id set"
            );
        }

        // Also verify pool counts changed correctly
        let (claimed, _warm) = get_pool_container_counts(&test_srv, &pool_key).await;
        assert!(claimed >= 1, "Should have at least 1 claimed container");

        Ok(())
    }
}
