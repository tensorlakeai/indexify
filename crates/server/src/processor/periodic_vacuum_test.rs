#[cfg(test)]
mod tests {
    use crate::{
        data_model::{
            AllocationId,
            ContainerBuilder,
            ContainerId,
            ContainerPoolBuilder,
            ContainerPoolId,
            ContainerPoolKey,
            ContainerPoolType,
            ContainerResources,
            ContainerServerMetadata,
            ContainerState,
            ContainerType,
            ExecutorId,
            ExecutorServerMetadata,
            SandboxId,
            test_objects::tests::{TEST_NAMESPACE, mock_executor_metadata},
        },
        processor::container_scheduler::ContainerScheduler,
        state_store::requests::{RequestPayload, SchedulerUpdatePayload, SchedulerUpdateRequest},
    };

    /// Build a minimal ContainerScheduler with one executor pre-registered.
    fn setup_scheduler() -> (ContainerScheduler, ExecutorId) {
        let executor_id = ExecutorId::new("exec-1".to_string());
        let executor_meta = mock_executor_metadata(executor_id.clone());

        let executor_state = ExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_container_ids: imbl::HashSet::new(),
            free_resources: executor_meta.host_resources.clone(),
            resource_claims: imbl::HashMap::new(),
        };

        let mut scheduler = ContainerScheduler {
            executors: imbl::HashMap::new(),
            containers_by_function_uri: imbl::HashMap::new(),
            function_containers: imbl::HashMap::new(),
            executor_states: imbl::HashMap::new(),
            containers_by_pool: imbl::HashMap::new(),
            warm_containers_by_pool: imbl::HashMap::new(),
            function_pools: imbl::HashMap::new(),
            sandbox_pools: imbl::HashMap::new(),
            dirty_pools: imbl::HashSet::new(),
            blocked_pools: imbl::HashSet::new(),
            executors_by_free_memory: imbl::OrdSet::new(),
            executor_classes: imbl::HashMap::new(),
            executors_by_class: imbl::HashMap::new(),
            blocked_work: Default::default(),
            idle_containers: imbl::OrdSet::new(),
            reaped_containers: Default::default(),
            last_placement: None,
        };

        scheduler
            .executors
            .insert(executor_id.clone(), Box::new(executor_meta));
        scheduler.set_executor_state(executor_id.clone(), Box::new(executor_state));

        (scheduler, executor_id)
    }

    /// Build a SchedulerUpdateRequest that registers a container, mirroring
    /// what `register_container()` produces in production. The caller then
    /// applies it via `scheduler.update()`.
    fn build_container_update(
        scheduler: &ContainerScheduler,
        executor_id: &ExecutorId,
        container_meta: ContainerServerMetadata,
    ) -> SchedulerUpdateRequest {
        // Clone the executor state and add the container (mirrors
        // register_container's in-place mutation).
        let mut exec_state = (**scheduler.executor_states.get(executor_id).unwrap()).clone();
        exec_state
            .add_container(&container_meta.function_container)
            .unwrap();

        let mut update = SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id.clone(), Box::new(exec_state));
        update.containers.insert(
            container_meta.function_container.id.clone(),
            Box::new(container_meta),
        );
        update
    }

    /// Apply a SchedulerUpdateRequest through the production update() path.
    fn apply_update(scheduler: &mut ContainerScheduler, update: SchedulerUpdateRequest) {
        let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update));
        scheduler.update(&payload).unwrap();
    }

    /// Helper: register a Function-type container through the production
    /// update() path and return its ID.
    fn add_function_container(
        scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
        pool_id: &str,
    ) -> ContainerId {
        let container = ContainerBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .application_name("app".to_string())
            .function_name("fn1".to_string())
            .version("1".to_string())
            .state(ContainerState::Running)
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1u32)
            .container_type(ContainerType::Function)
            .pool_id(Some(ContainerPoolId::new(pool_id)))
            .build()
            .unwrap();

        let cid = container.id.clone();
        let meta = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container: container,
            desired_state: ContainerState::Running,
            container_type: ContainerType::Function,
            allocations: imbl::HashSet::new(),
            idle_since: Some(tokio::time::Instant::now()),
        };

        let update = build_container_update(scheduler, executor_id, meta);
        apply_update(scheduler, update);
        cid
    }

    /// Helper: register a warm Sandbox-type pool container (unclaimed,
    /// pool-backed) through the production update() path.
    fn add_warm_sandbox_container(
        scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
        pool_id: &str,
    ) -> ContainerId {
        let container = ContainerBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .application_name("sandbox_app".to_string())
            .function_name("sandbox_fn".to_string())
            .version("1".to_string())
            .state(ContainerState::Running)
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1u32)
            .container_type(ContainerType::Sandbox)
            // sandbox_id = None  → warm (unclaimed)
            // pool_id = Some(..) → belongs to a pool
            .pool_id(Some(ContainerPoolId::new(pool_id)))
            .build()
            .unwrap();

        let cid = container.id.clone();
        let meta = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container: container,
            desired_state: ContainerState::Running,
            container_type: ContainerType::Sandbox,
            allocations: imbl::HashSet::new(),
            idle_since: Some(tokio::time::Instant::now()),
        };

        let update = build_container_update(scheduler, executor_id, meta);
        apply_update(scheduler, update);
        cid
    }

    /// Helper: register a claimed Sandbox container (sandbox_id set) through
    /// the production update() path.
    fn add_claimed_sandbox_container(
        scheduler: &mut ContainerScheduler,
        executor_id: &ExecutorId,
        pool_id: &str,
    ) -> ContainerId {
        let container = ContainerBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .application_name("sandbox_app".to_string())
            .function_name("sandbox_fn".to_string())
            .version("1".to_string())
            .state(ContainerState::Running)
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1u32)
            .container_type(ContainerType::Sandbox)
            // sandbox_id = Some(..) → claimed
            .sandbox_id(Some(SandboxId::new("sb-1".to_string())))
            .pool_id(Some(ContainerPoolId::new(pool_id)))
            .build()
            .unwrap();

        let cid = container.id.clone();
        let meta = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container: container,
            desired_state: ContainerState::Running,
            container_type: ContainerType::Sandbox,
            allocations: imbl::HashSet::new(),
            idle_since: Some(tokio::time::Instant::now()),
        };

        let update = build_container_update(scheduler, executor_id, meta);
        apply_update(scheduler, update);
        cid
    }

    /// Register a pool in the scheduler so min_containers is respected.
    fn register_pool(
        scheduler: &mut ContainerScheduler,
        pool_id: &str,
        pool_type: ContainerPoolType,
        min_containers: u32,
    ) {
        let pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new(pool_id))
            .namespace(TEST_NAMESPACE.to_string())
            .pool_type(pool_type.clone())
            .image("python:3.11".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .min_containers(Some(min_containers))
            .max_containers(Some(10))
            .buffer_containers(Some(0))
            .build()
            .unwrap();

        let key = ContainerPoolKey::new(TEST_NAMESPACE, &ContainerPoolId::new(pool_id));
        match pool_type {
            ContainerPoolType::Function => {
                scheduler.function_pools.insert(key, Box::new(pool));
            }
            ContainerPoolType::Sandbox => {
                scheduler.sandbox_pools.insert(key, Box::new(pool));
            }
        }
    }

    /// Simulate adding an allocation to a container (mirrors
    /// FunctionRunProcessor::allocate_function_run).
    fn add_allocation(
        scheduler: &mut ContainerScheduler,
        container_id: &ContainerId,
    ) -> AllocationId {
        let alloc_id = AllocationId::default();
        let fc = scheduler.function_containers.get(container_id).unwrap();
        let mut updated = *fc.clone();
        updated.allocations.insert(alloc_id.clone());
        updated.idle_since = None; // now busy

        let mut update = SchedulerUpdateRequest::default();
        update
            .containers
            .insert(container_id.clone(), Box::new(updated));
        apply_update(scheduler, update);
        alloc_id
    }

    /// Simulate finishing an allocation (mirrors
    /// FunctionRunCreator::handle_allocation_ingestion).
    fn finish_allocation(
        scheduler: &mut ContainerScheduler,
        container_id: &ContainerId,
        alloc_id: &AllocationId,
    ) {
        let fc = scheduler.function_containers.get(container_id).unwrap();
        let mut updated = *fc.clone();
        updated.allocations.remove(alloc_id);
        if updated.allocations.is_empty() {
            updated.idle_since = Some(tokio::time::Instant::now());
        }

        let mut update = SchedulerUpdateRequest::default();
        update
            .containers
            .insert(container_id.clone(), Box::new(updated));
        apply_update(scheduler, update);
    }

    // ===================================================================
    // Test: busy container (with allocation) is NOT reaped
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn busy_container_not_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");
        let _alloc_id = add_allocation(&mut scheduler, &cid);

        // Container has an active allocation → idle_since is None
        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert!(
            update.containers.is_empty(),
            "busy container (with allocation) must not be reaped"
        );
    }

    // ===================================================================
    // Test: idle container IS reaped (no age threshold in eager reaping)
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn idle_container_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert_eq!(update.containers.len(), 1, "idle container must be reaped");
        assert!(update.containers.contains_key(&cid));
    }

    // ===================================================================
    // Test: reaping optimistically frees executor resources
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn reaping_frees_executor_resources() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let initial_free = scheduler
            .executor_states
            .get(&executor_id)
            .unwrap()
            .free_resources
            .memory_bytes;

        let _cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        let after_add = scheduler
            .executor_states
            .get(&executor_id)
            .unwrap()
            .free_resources
            .memory_bytes;
        assert!(
            after_add < initial_free,
            "adding container should consume resources"
        );

        let _update = scheduler.reap_idle_containers(std::time::Duration::ZERO);

        let after_reap = scheduler
            .executor_states
            .get(&executor_id)
            .unwrap()
            .free_resources
            .memory_bytes;
        assert_eq!(
            after_reap, initial_free,
            "reaping must free resources back to initial level"
        );
    }

    // ===================================================================
    // Test: full lifecycle — busy → idle → reaped
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn full_lifecycle_busy_then_idle_then_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        // Step 1: Create container (starts idle)
        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        // Step 2: Add allocation → busy
        let alloc_id = add_allocation(&mut scheduler, &cid);
        assert!(
            scheduler
                .reap_idle_containers(std::time::Duration::ZERO)
                .containers
                .is_empty(),
            "step 2: busy container must not be reaped"
        );

        // Step 3: Finish allocation → idle again
        finish_allocation(&mut scheduler, &cid, &alloc_id);
        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert_eq!(
            update.containers.len(),
            1,
            "step 3: idle container must be reaped"
        );
        assert!(update.containers.contains_key(&cid));
    }

    // ===================================================================
    // Test: warm sandbox pool containers are NEVER reaped
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn warm_sandbox_pool_containers_never_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(
            &mut scheduler,
            "sandbox_pool",
            ContainerPoolType::Sandbox,
            0,
        );

        let _warm_cid = add_warm_sandbox_container(&mut scheduler, &executor_id, "sandbox_pool");

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert!(
            update.containers.is_empty(),
            "warm sandbox pool container must never be reaped"
        );
    }

    // ===================================================================
    // Test: claimed sandbox containers are NOT reaped
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn claimed_sandbox_containers_not_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(
            &mut scheduler,
            "sandbox_pool",
            ContainerPoolType::Sandbox,
            0,
        );

        let _claimed_cid =
            add_claimed_sandbox_container(&mut scheduler, &executor_id, "sandbox_pool");

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert!(
            update.containers.is_empty(),
            "claimed sandbox container must not be reaped"
        );
    }

    // ===================================================================
    // Test: mixed scenario — function, warm sandbox, claimed sandbox
    // Only idle Function containers are reaped.
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn mixed_containers_only_eligible_ones_reaped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "fn_pool", ContainerPoolType::Function, 0);
        register_pool(&mut scheduler, "sb_pool", ContainerPoolType::Sandbox, 0);

        // 1. Idle function container (should be reaped)
        let fn_cid = add_function_container(&mut scheduler, &executor_id, "fn_pool");

        // 2. Busy function container (should NOT be reaped)
        let busy_cid = add_function_container(&mut scheduler, &executor_id, "fn_pool");
        let _alloc = add_allocation(&mut scheduler, &busy_cid);

        // 3. Warm sandbox pool container (should NEVER be reaped)
        let warm_cid = add_warm_sandbox_container(&mut scheduler, &executor_id, "sb_pool");

        // 4. Claimed sandbox container (also NOT reaped)
        let claimed_cid = add_claimed_sandbox_container(&mut scheduler, &executor_id, "sb_pool");

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);

        assert!(
            update.containers.contains_key(&fn_cid),
            "idle function container must be reaped"
        );
        assert!(
            !update.containers.contains_key(&busy_cid),
            "busy function container must NOT be reaped"
        );
        assert!(
            !update.containers.contains_key(&warm_cid),
            "warm sandbox pool container must NOT be reaped"
        );
        assert!(
            !update.containers.contains_key(&claimed_cid),
            "claimed sandbox container must NOT be reaped"
        );
    }

    // ===================================================================
    // Test: min_containers is respected
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn min_containers_respected() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 2);

        // Add 3 idle containers to the pool
        let cid1 = add_function_container(&mut scheduler, &executor_id, "pool1");
        let cid2 = add_function_container(&mut scheduler, &executor_id, "pool1");
        let cid3 = add_function_container(&mut scheduler, &executor_id, "pool1");

        // Reap → only 1 should be reaped (3 - min 2 = 1 evictable)
        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert_eq!(
            update.containers.len(),
            1,
            "only 1 container should be reaped (min_containers=2, total=3)"
        );

        let reaped_id = update.containers.keys().next().unwrap();
        assert!(
            *reaped_id == cid1 || *reaped_id == cid2 || *reaped_id == cid3,
            "reaped container must be one of the pool's containers"
        );
    }

    // ===================================================================
    // Test: already-terminated containers are skipped
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn terminated_containers_skipped() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        // Mark as terminated via the production update() path
        let fc = scheduler.function_containers.get(&cid).unwrap();
        let mut terminated = *fc.clone();
        terminated.desired_state = ContainerState::Terminated {
            reason: crate::data_model::ContainerTerminationReason::DesiredStateRemoved,
        };
        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(cid.clone(), Box::new(terminated));
        apply_update(&mut scheduler, update);

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert!(
            update.containers.is_empty(),
            "already-terminated container must not appear in reap candidates"
        );
    }

    // ===================================================================
    // Test: reaped containers are tracked for restoration
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn reaped_containers_tracked() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let _cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        let update = scheduler.reap_idle_containers(std::time::Duration::ZERO);
        assert_eq!(update.containers.len(), 1);

        // reaped_containers should have an entry
        assert!(
            !scheduler.reaped_containers.is_empty(),
            "reaped_containers must be populated for anti-churn restoration"
        );
    }
}
