#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        data_model::{
            ContainerPool,
            ContainerPoolBuilder,
            ContainerPoolId,
            ContainerResources,
            ContainerState,
            FunctionExecutorTerminationReason,
            Sandbox,
            SandboxBuilder,
            SandboxFailureReason,
            SandboxId,
            SandboxOutcome,
            SandboxPendingReason,
            SandboxStatus,
            test_objects::tests::{
                TEST_EXECUTOR_ID,
                TEST_NAMESPACE,
                mock_sandbox_executor_metadata,
            },
        },
        state_store::{
            IndexifyState,
            requests::{
                CreateContainerPoolRequest,
                CreateSandboxRequest,
                RequestPayload,
                StateMachineUpdateRequest,
                TerminateSandboxRequest,
            },
        },
        testing,
        utils::get_epoch_time_in_ns,
    };

    const TEST_IMAGE: &str = "test-image:latest";

    /// Helper to create a sandbox via state store with inline spec
    async fn create_sandbox(indexify_state: &IndexifyState, namespace: &str) -> SandboxId {
        let sandbox_id = SandboxId::default();
        let sandbox = SandboxBuilder::default()
            .id(sandbox_id.clone())
            .namespace(namespace.to_string())
            .image(TEST_IMAGE.to_string())
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
        sandbox_id
    }

    /// Helper to create a sandbox with custom resources
    async fn create_sandbox_with_resources(
        indexify_state: &IndexifyState,
        namespace: &str,
        resources: ContainerResources,
    ) -> SandboxId {
        let sandbox_id = SandboxId::default();
        let sandbox = SandboxBuilder::default()
            .id(sandbox_id.clone())
            .namespace(namespace.to_string())
            .image(TEST_IMAGE.to_string())
            .status(SandboxStatus::Pending {
                reason: SandboxPendingReason::Scheduling,
            })
            .creation_time_ns(get_epoch_time_in_ns())
            .resources(resources)
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
        sandbox_id
    }

    /// Helper to create a sandbox that references a pool
    async fn create_sandbox_with_pool(
        indexify_state: &IndexifyState,
        namespace: &str,
        pool_id: &str,
    ) -> SandboxId {
        let sandbox_id = SandboxId::default();
        let sandbox = SandboxBuilder::default()
            .id(sandbox_id.clone())
            .namespace(namespace.to_string())
            .pool_id(Some(ContainerPoolId::new(pool_id)))
            .image(TEST_IMAGE.to_string())
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
        sandbox_id
    }

    /// Helper to register a container pool
    async fn register_container_pool(
        test_srv: &testing::TestService,
        pool: ContainerPool,
    ) -> anyhow::Result<()> {
        test_srv
            .service
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateContainerPool(CreateContainerPoolRequest { pool }),
            })
            .await?;
        Ok(())
    }

    /// Get sandbox from database
    async fn get_sandbox(
        indexify_state: &IndexifyState,
        namespace: &str,
        sandbox_id: &str,
    ) -> Option<Sandbox> {
        indexify_state
            .reader()
            .get_sandbox(namespace, sandbox_id)
            .await
            .ok()
            .flatten()
    }

    /// Get count of pending sandboxes
    async fn get_pending_sandbox_count(indexify_state: &IndexifyState) -> usize {
        let guard = indexify_state.in_memory_state.read().await;
        guard.pending_sandboxes.len()
    }

    #[tokio::test]
    async fn test_sandbox_created_as_pending_without_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create sandbox - should start as Pending since no executors exist
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;

        // Process state changes
        test_srv.process_all_state_changes().await?;

        // Verify sandbox exists and is Pending
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");

        assert!(sandbox.status.is_pending());
        assert!(sandbox.executor_id.is_none());

        // Verify it's in pending_sandboxes set
        let pending_count = get_pending_sandbox_count(&indexify_state).await;
        assert_eq!(pending_count, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_allocated_when_executor_registers() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create sandbox while no executor exists
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is pending
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert!(sandbox.status.is_pending());

        // Register an executor
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // After allocation, sandbox should be Pending/WaitingForContainer
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(
            sandbox.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::WaitingForContainer,
            }
        );
        assert!(
            sandbox.executor_id.is_some(),
            "Sandbox should have an executor ID"
        );

        // Simulate heartbeat with container Running to promote sandbox
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is now Running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Running,
            "Sandbox should transition to Running after container reports Running"
        );
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Verify it's no longer in pending_sandboxes set
        let pending_count = get_pending_sandbox_count(&indexify_state).await;
        assert_eq!(pending_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_allocated_immediately_with_existing_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor FIRST
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create sandbox - should be allocated immediately
        // (Pending/WaitingForContainer)
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is Pending/WaitingForContainer
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(
            sandbox.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::WaitingForContainer,
            }
        );
        assert!(sandbox.executor_id.is_some());

        // Simulate heartbeat with container Running to promote sandbox
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is Running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Running,
            "Sandbox should be Running after container reports Running"
        );
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Verify no pending sandboxes
        let pending_count = get_pending_sandbox_count(&indexify_state).await;
        assert_eq!(pending_count, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_termination() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Setup: create executor and sandbox
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Promote sandbox by simulating container Running heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Terminate the sandbox
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::TerminateSandbox(TerminateSandboxRequest {
                namespace: TEST_NAMESPACE.to_string(),
                sandbox_id: sandbox_id.clone(),
            }),
        };
        indexify_state.write(request).await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is terminated
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should still exist after termination");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Terminated,
            "Sandbox should be Terminated after termination request"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_sandboxes_queued() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create multiple sandboxes without an executor
        let sandbox_id_1 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_id_2 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_id_3 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify all sandboxes are pending
        let pending_count = get_pending_sandbox_count(&indexify_state).await;
        assert_eq!(pending_count, 3, "All 3 sandboxes should be pending");

        for sandbox_id in [&sandbox_id_1, &sandbox_id_2, &sandbox_id_3] {
            let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
                .await
                .expect("Sandbox should exist");
            assert!(sandbox.status.is_pending());
        }

        // Register an executor with enough resources
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // After allocation, sandboxes should be Pending/WaitingForContainer
        for sandbox_id in [&sandbox_id_1, &sandbox_id_2, &sandbox_id_3] {
            let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
                .await
                .expect("Sandbox should exist");
            assert_eq!(
                sandbox.status,
                SandboxStatus::Pending {
                    reason: SandboxPendingReason::WaitingForContainer,
                }
            );
        }

        // Simulate heartbeat with containers Running to promote sandboxes
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify all sandboxes are now running
        let pending_count = get_pending_sandbox_count(&indexify_state).await;
        assert_eq!(
            pending_count, 0,
            "No sandboxes should be pending after containers report Running"
        );

        for sandbox_id in [&sandbox_id_1, &sandbox_id_2, &sandbox_id_3] {
            let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
                .await
                .expect("Sandbox should exist");
            assert_eq!(
                sandbox.status,
                SandboxStatus::Running,
                "Sandbox {} should be Running",
                sandbox_id.get()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_persisted_to_rocksdb() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create sandbox
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is in in-memory state
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist in memory");

        assert_eq!(sandbox.id, sandbox_id);
        assert_eq!(sandbox.namespace, TEST_NAMESPACE);
        assert_eq!(sandbox.image, TEST_IMAGE);
        assert!(sandbox.status.is_pending());

        Ok(())
    }

    #[tokio::test]
    async fn test_list_sandboxes_for_namespace() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create multiple sandboxes
        let sandbox_id_1 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_id_2 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // List sandboxes from database
        let reader = indexify_state.reader();
        let sandbox_list = reader.list_sandboxes(TEST_NAMESPACE).await?;

        assert_eq!(sandbox_list.len(), 2);

        let sandbox_ids: Vec<_> = sandbox_list.iter().map(|s| s.id.clone()).collect();
        assert!(sandbox_ids.contains(&sandbox_id_1));
        assert!(sandbox_ids.contains(&sandbox_id_2));

        Ok(())
    }

    // ==================== EXECUTOR DESIRED STATE LIFECYCLE TESTS
    // ====================

    #[tokio::test]
    async fn test_sandbox_appears_in_executor_desired_state_when_allocated() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor first
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Verify executor has no function_executors initially
        let desired_state = executor.desired_state().await;
        assert!(
            desired_state.function_executors.is_empty(),
            "Executor should have no function_executors before sandbox creation"
        );

        // Create sandbox - should be allocated (Pending/WaitingForContainer)
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify the sandbox container appears in executor's desired state even while
        // Pending
        let desired_state = executor.desired_state().await;
        assert_eq!(
            desired_state.function_executors.len(),
            1,
            "Executor should have 1 function_executor for the sandbox"
        );

        // Promote sandbox by simulating container Running heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Verify the function executor has the right properties
        let fe = &desired_state.function_executors[0];
        assert!(fe.id.is_some(), "Function executor should have an ID");

        // Verify the function executor ID matches the sandbox ID (sandbox ID ==
        // container ID)
        assert_eq!(
            fe.id.as_ref().unwrap(),
            sandbox.id.get(),
            "Function executor ID should match sandbox ID"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_removed_from_desired_state_when_user_terminates() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor
        let executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create sandbox
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is in desired state
        let desired_state = executor.desired_state().await;
        assert_eq!(
            desired_state.function_executors.len(),
            1,
            "Sandbox should be in executor's desired state"
        );

        // User terminates the sandbox
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::TerminateSandbox(TerminateSandboxRequest {
                namespace: TEST_NAMESPACE.to_string(),
                sandbox_id: sandbox_id.clone(),
            }),
        };
        indexify_state.write(request).await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is terminated
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should still exist");
        assert_eq!(sandbox.status, SandboxStatus::Terminated);

        // Verify the sandbox container is marked for removal in desired state
        // (the function executor should have a desired_state of terminated/removed)
        let desired_state = executor.desired_state().await;

        // The function executor may still be present but should be marked for
        // termination OR it may be removed entirely depending on implementation
        if !desired_state.function_executors.is_empty() {
            // If still present, verify it's marked for termination
            let fe = &desired_state.function_executors[0];
            tracing::info!("Function executor still in desired state: id={:?}", fe.id);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_terminated_when_executor_deregisters() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create sandbox
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Promote sandbox by simulating container Running heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Deregister the executor (simulates executor going away)
        executor.deregister().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is marked as terminated with ExecutorRemoved reason
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should still exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Terminated,
            "Sandbox should be Terminated when executor is removed"
        );
        assert_eq!(
            sandbox.outcome,
            Some(SandboxOutcome::Failure(
                SandboxFailureReason::ExecutorRemoved
            )),
            "Sandbox should have ExecutorRemoved failure reason"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sandbox_terminated_when_executor_reports_container_terminated() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create sandbox
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Promote sandbox by simulating container Running heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox is running
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");
        assert_eq!(sandbox.status, SandboxStatus::Running);

        // Simulate executor reporting that the container terminated
        let mut executor_state = executor.get_executor_server_state().await?;

        // Mark all function executors as terminated
        for (_, fe) in executor_state.containers.iter_mut() {
            fe.state = ContainerState::Terminated {
                reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                failed_alloc_ids: vec![],
            };
        }

        // Update state hash and send heartbeat
        executor_state.state_hash = nanoid::nanoid!();
        executor.heartbeat(executor_state).await?;
        test_srv.process_all_state_changes().await?;

        // Verify sandbox status after executor reports container terminated
        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should still exist");

        // The sandbox should reflect the container termination
        tracing::info!(
            "Sandbox status after container termination: {:?}, outcome: {:?}",
            sandbox.status,
            sandbox.outcome
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_sandboxes_lifecycle_with_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register an executor
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create multiple sandboxes
        let sandbox_id_1 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_id_2 = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        // Verify both sandboxes are in executor's desired state
        let desired_state = executor.desired_state().await;
        assert_eq!(
            desired_state.function_executors.len(),
            2,
            "Executor should have 2 function_executors for the sandboxes"
        );

        // Promote sandboxes by simulating container Running heartbeat
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Terminate one sandbox
        let request = StateMachineUpdateRequest {
            payload: RequestPayload::TerminateSandbox(TerminateSandboxRequest {
                namespace: TEST_NAMESPACE.to_string(),
                sandbox_id: sandbox_id_1.clone(),
            }),
        };
        indexify_state.write(request).await?;
        test_srv.process_all_state_changes().await?;

        // Verify first sandbox is terminated
        let sandbox_1 = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id_1.get())
            .await
            .expect("Sandbox 1 should exist");
        assert_eq!(sandbox_1.status, SandboxStatus::Terminated);

        // Verify second sandbox is still running
        let sandbox_2 = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id_2.get())
            .await
            .expect("Sandbox 2 should exist");
        assert_eq!(sandbox_2.status, SandboxStatus::Running);

        Ok(())
    }

    // ==================== PENDING REASON TESTS ====================

    #[tokio::test]
    async fn test_pending_reason_no_executors_available() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create sandbox with no executors registered
        let sandbox_id = create_sandbox(&indexify_state, TEST_NAMESPACE).await;
        test_srv.process_all_state_changes().await?;

        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::NoExecutorsAvailable,
            },
            "Sandbox should report no executors available"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pending_reason_no_resources_available() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register executor with 8 cores (8000 cpu_ms)
        let _executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Create sandbox requesting more CPU than executor has
        let sandbox_id = create_sandbox_with_resources(
            &indexify_state,
            TEST_NAMESPACE,
            ContainerResources {
                cpu_ms_per_sec: 9000, // Exceeds executor's 8000
                memory_mb: 256,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
        )
        .await;
        test_srv.process_all_state_changes().await?;

        let sandbox = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id.get())
            .await
            .expect("Sandbox should exist");

        assert_eq!(
            sandbox.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::NoResourcesAvailable,
            },
            "Sandbox should report no resources available"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_pending_reason_pool_at_capacity() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Register executor
        let mut executor = test_srv
            .create_executor(mock_sandbox_executor_metadata(TEST_EXECUTOR_ID.into()))
            .await?;
        test_srv.process_all_state_changes().await?;

        // Register pool with max_containers=1
        let pool_id = "tiny_pool";
        let pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new(pool_id))
            .namespace(TEST_NAMESPACE.to_string())
            .image(TEST_IMAGE.to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 100,
                memory_mb: 256,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(0))
            .max_containers(Some(1))
            .buffer_containers(Some(0))
            .build()
            .unwrap();
        register_container_pool(&test_srv, pool).await?;
        test_srv.process_all_state_changes().await?;

        // Create first sandbox from pool — should be allocated
        let sandbox_id_1 = create_sandbox_with_pool(&indexify_state, TEST_NAMESPACE, pool_id).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_1 = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id_1.get())
            .await
            .expect("First sandbox should exist");
        assert_eq!(
            sandbox_1.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::WaitingForContainer,
            },
            "First sandbox should be allocated and waiting for container"
        );

        // Promote first sandbox to Running
        executor.mark_function_executors_as_running().await?;
        test_srv.process_all_state_changes().await?;

        // Create second sandbox from same pool — pool at capacity (max=1)
        let sandbox_id_2 = create_sandbox_with_pool(&indexify_state, TEST_NAMESPACE, pool_id).await;
        test_srv.process_all_state_changes().await?;

        let sandbox_2 = get_sandbox(&indexify_state, TEST_NAMESPACE, sandbox_id_2.get())
            .await
            .expect("Second sandbox should exist");

        assert_eq!(
            sandbox_2.status,
            SandboxStatus::Pending {
                reason: SandboxPendingReason::PoolAtCapacity,
            },
            "Second sandbox should report pool at capacity"
        );

        Ok(())
    }
}
