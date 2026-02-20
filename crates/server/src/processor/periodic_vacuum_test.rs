#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tokio::time::Duration;

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
            function_container_ids: HashSet::new(),
            free_resources: executor_meta.host_resources.clone(),
            resource_claims: HashMap::new(),
        };

        let mut scheduler = ContainerScheduler {
            clock: 0,
            executors: imbl::HashMap::new(),
            containers_by_function_uri: imbl::HashMap::new(),
            function_containers: imbl::HashMap::new(),
            executor_states: imbl::HashMap::new(),
            containers_by_pool: imbl::HashMap::new(),
            warm_containers_by_pool: imbl::HashMap::new(),
            function_pools: imbl::HashMap::new(),
            sandbox_pools: imbl::HashMap::new(),
            dirty_pools: std::collections::HashSet::new(),
            blocked_pools: std::collections::HashSet::new(),
        };

        scheduler
            .executors
            .insert(executor_id.clone(), Box::new(executor_meta));
        scheduler
            .executor_states
            .insert(executor_id.clone(), Box::new(executor_state));

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
            allocations: HashSet::new(),
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
            allocations: HashSet::new(),
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
            allocations: HashSet::new(),
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
    // Test: busy container (with allocation) is NOT vacuumed
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn busy_container_not_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");
        let _alloc_id = add_allocation(&mut scheduler, &cid);

        // Container has an active allocation → idle_since is None
        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(0));
        assert!(
            candidates.is_empty(),
            "busy container (with allocation) must not be vacuumed"
        );
    }

    // ===================================================================
    // Test: container that just became idle is NOT vacuumed (below age)
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn recently_idle_container_not_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");
        let alloc_id = add_allocation(&mut scheduler, &cid);
        finish_allocation(&mut scheduler, &cid, &alloc_id);

        // Container just became idle — too young for 300s threshold
        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(300));
        assert!(
            candidates.is_empty(),
            "container that just became idle must not be vacuumed (below max_idle_age)"
        );
    }

    // ===================================================================
    // Test: container idle longer than threshold IS vacuumed
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn idle_container_vacuumed_after_threshold() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);

        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");
        let alloc_id = add_allocation(&mut scheduler, &cid);
        finish_allocation(&mut scheduler, &cid, &alloc_id);

        // Advance time past the threshold
        tokio::time::advance(Duration::from_secs(600)).await;

        // Idle for 600s > 300s threshold → vacuumed
        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(300));
        assert_eq!(
            candidates.len(),
            1,
            "idle container past threshold must be vacuumed"
        );
        assert_eq!(candidates[0].0, cid);
    }

    // ===================================================================
    // Test: full lifecycle — busy → idle-young → idle-old
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn full_lifecycle_busy_then_idle_then_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "pool1", ContainerPoolType::Function, 0);
        let max_idle = Duration::from_secs(300);

        // Step 1: Create container (starts idle)
        let cid = add_function_container(&mut scheduler, &executor_id, "pool1");

        // Step 2: Add allocation → busy
        let alloc_id = add_allocation(&mut scheduler, &cid);
        assert!(
            scheduler.periodic_vacuum_candidates(max_idle).is_empty(),
            "step 2: busy container must not be vacuumed"
        );

        // Step 3: Finish allocation → idle again, but just now
        finish_allocation(&mut scheduler, &cid, &alloc_id);
        assert!(
            scheduler.periodic_vacuum_candidates(max_idle).is_empty(),
            "step 3: recently-idle container must not be vacuumed"
        );

        // Step 4: Time passes beyond threshold
        tokio::time::advance(Duration::from_secs(600)).await;
        let candidates = scheduler.periodic_vacuum_candidates(max_idle);
        assert_eq!(
            candidates.len(),
            1,
            "step 4: idle container past threshold must be vacuumed"
        );
        assert_eq!(candidates[0].0, cid);
    }

    // ===================================================================
    // Test: warm sandbox pool containers are NEVER vacuumed
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn warm_sandbox_pool_containers_never_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(
            &mut scheduler,
            "sandbox_pool",
            ContainerPoolType::Sandbox,
            0,
        );

        let warm_cid = add_warm_sandbox_container(&mut scheduler, &executor_id, "sandbox_pool");
        tokio::time::advance(Duration::from_secs(9999)).await;

        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(0));
        assert!(
            !candidates.iter().any(|(id, _)| id == &warm_cid),
            "warm sandbox pool container must never be vacuumed"
        );
    }

    // ===================================================================
    // Test: claimed sandbox containers are NOT vacuumed
    // (can_be_removed only allows Function type — sandboxes are managed
    //  by the sandbox lifecycle, not the periodic vacuum)
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn claimed_sandbox_containers_not_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(
            &mut scheduler,
            "sandbox_pool",
            ContainerPoolType::Sandbox,
            0,
        );

        let claimed_cid =
            add_claimed_sandbox_container(&mut scheduler, &executor_id, "sandbox_pool");
        tokio::time::advance(Duration::from_secs(9999)).await;

        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(0));
        assert!(
            !candidates.iter().any(|(id, _)| id == &claimed_cid),
            "claimed sandbox container must not be vacuumed (sandbox lifecycle manages these)"
        );
    }

    // ===================================================================
    // Test: mixed scenario — function, warm sandbox, claimed sandbox
    // Only idle Function containers past the threshold are vacuumed.
    // All sandbox types (warm or claimed) are protected.
    // ===================================================================
    #[tokio::test(start_paused = true)]
    async fn mixed_containers_only_eligible_ones_vacuumed() {
        let (mut scheduler, executor_id) = setup_scheduler();
        register_pool(&mut scheduler, "fn_pool", ContainerPoolType::Function, 0);
        register_pool(&mut scheduler, "sb_pool", ContainerPoolType::Sandbox, 0);

        // 1. Idle function container (should be vacuumed)
        let fn_cid = add_function_container(&mut scheduler, &executor_id, "fn_pool");

        // 2. Busy function container (should NOT be vacuumed)
        let busy_cid = add_function_container(&mut scheduler, &executor_id, "fn_pool");
        let _alloc = add_allocation(&mut scheduler, &busy_cid);

        // 3. Warm sandbox pool container (should NEVER be vacuumed)
        let warm_cid = add_warm_sandbox_container(&mut scheduler, &executor_id, "sb_pool");

        // 4. Claimed sandbox container (also NOT vacuumed — sandbox lifecycle)
        let claimed_cid = add_claimed_sandbox_container(&mut scheduler, &executor_id, "sb_pool");

        // Advance time past threshold for all idle containers
        tokio::time::advance(Duration::from_secs(9999)).await;

        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(300));
        let vacuumed_ids: HashSet<ContainerId> =
            candidates.iter().map(|(id, _)| id.clone()).collect();

        assert!(
            vacuumed_ids.contains(&fn_cid),
            "idle function container must be vacuumed"
        );
        assert!(
            !vacuumed_ids.contains(&busy_cid),
            "busy function container must NOT be vacuumed"
        );
        assert!(
            !vacuumed_ids.contains(&warm_cid),
            "warm sandbox pool container must NOT be vacuumed"
        );
        assert!(
            !vacuumed_ids.contains(&claimed_cid),
            "claimed sandbox container must NOT be vacuumed (sandbox lifecycle)"
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

        // Advance time past threshold
        tokio::time::advance(Duration::from_secs(9999)).await;

        // Vacuum → only 1 should be vacuumed (3 - min 2 = 1 evictable)
        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(0));
        assert_eq!(
            candidates.len(),
            1,
            "only 1 container should be vacuumed (min_containers=2, total=3)"
        );

        let vacuumed_id = &candidates[0].0;
        assert!(
            *vacuumed_id == cid1 || *vacuumed_id == cid2 || *vacuumed_id == cid3,
            "vacuumed container must be one of the pool's containers"
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

        // Advance time past threshold
        tokio::time::advance(Duration::from_secs(9999)).await;

        // Mark as terminated via the production update() path
        let fc = scheduler.function_containers.get(&cid).unwrap();
        let mut terminated = *fc.clone();
        terminated.desired_state = ContainerState::Terminated {
            reason: crate::data_model::ContainerTerminationReason::DesiredStateRemoved,
        };
        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(cid.clone(), Box::new(terminated));
        apply_update(&mut scheduler, update);

        let candidates = scheduler.periodic_vacuum_candidates(Duration::from_secs(0));
        assert!(
            candidates.is_empty(),
            "already-terminated container must not appear in vacuum candidates"
        );
    }
}
