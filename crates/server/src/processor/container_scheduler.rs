use std::sync::Arc;

use anyhow::Result;
use opentelemetry::metrics::ObservableGauge;
use rand::seq::IndexedRandom;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::{
    config::WorkloadPlacementConstraints,
    data_model::{
        Application,
        ApplicationState,
        Container,
        ContainerBuilder,
        ContainerId,
        ContainerResources,
        ContainerServerMetadata,
        ContainerState,
        ContainerType,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        Function,
        FunctionExecutorTerminationReason,
        FunctionResources,
        FunctionURI,
        Sandbox,
        filter,
    },
    state_store::{
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

/// Gauges for monitoring the container scheduler state.
/// Must be kept alive for callbacks to fire.
#[allow(dead_code)]
pub struct ContainerSchedulerGauges {
    pub total_executors: ObservableGauge<u64>,
}

impl ContainerSchedulerGauges {
    pub fn new(container_scheduler: Arc<RwLock<ContainerScheduler>>) -> Self {
        let meter = opentelemetry::global::meter("container_scheduler");
        let scheduler_clone = container_scheduler.clone();
        let total_executors = meter
            .u64_observable_gauge("indexify.total_executors")
            .with_description("Total number of executors")
            .with_callback(move |observer| {
                if let Ok(scheduler) = scheduler_clone.try_read() {
                    observer.observe(scheduler.executors.len() as u64, &[]);
                }
            })
            .build();
        Self { total_executors }
    }
}

#[derive(Debug, Clone)]
pub enum Error {
    ConstraintUnsatisfiable {
        reason: String,
        version: String,
        function_name: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ConstraintUnsatisfiable { reason, .. } => reason.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn version(&self) -> &str {
        match self {
            Error::ConstraintUnsatisfiable { version, .. } => version,
        }
    }

    pub fn function_name(&self) -> &str {
        match self {
            Error::ConstraintUnsatisfiable { function_name, .. } => function_name,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DesiredContainers {
    pub min_count: Option<u32>,
    pub max_count: Option<u32>,
}

#[derive(Debug)]
pub struct ContainerScheduler {
    pub clock: u64,
    pub desired_containers: imbl::HashMap<FunctionURI, DesiredContainers>,
    // ExecutorId -> ExecutorMetadata
    // This is the metadata that executor is sending us, not the **Desired** state
    // from the perspective of the state store.
    pub executors: imbl::HashMap<ExecutorId, Box<ExecutorMetadata>>,
    pub containers_by_function_uri: imbl::HashMap<FunctionURI, imbl::HashSet<ContainerId>>,
    pub function_containers: imbl::HashMap<ContainerId, Box<ContainerServerMetadata>>,
    // ExecutorId -> (FE ID -> List of Function Executors)
    pub executor_states: imbl::HashMap<ExecutorId, Box<ExecutorServerMetadata>>,
    // Placement constraint expressions for workload matching
    pub workload_placement_constraints: WorkloadPlacementConstraints,
}

impl ContainerScheduler {
    pub fn new(
        in_memory_state: &InMemoryState,
        workload_placement_constraints: WorkloadPlacementConstraints,
    ) -> Self {
        let mut desired_containers = imbl::HashMap::new();
        for application in in_memory_state.applications.values() {
            for function in application.functions.values() {
                let fn_uri = FunctionURI {
                    namespace: application.namespace.clone(),
                    application: application.name.clone(),
                    function: function.name.clone(),
                    version: application.version.clone(),
                };
                let dc = DesiredContainers {
                    max_count: function.max_containers,
                    min_count: function.min_containers,
                };
                desired_containers.insert(fn_uri, dc);
            }
        }
        Self {
            desired_containers,
            executors: imbl::HashMap::new(),
            containers_by_function_uri: imbl::HashMap::new(),
            function_containers: imbl::HashMap::new(),
            executor_states: imbl::HashMap::new(),
            clock: in_memory_state.clock,
            workload_placement_constraints,
        }
    }

    pub fn update(&mut self, state_machine_update_request: &RequestPayload) -> Result<()> {
        match state_machine_update_request {
            RequestPayload::UpsertExecutor(request) => {
                self.upsert_executor(&request.executor);
            }
            RequestPayload::CreateOrUpdateApplication(request) => {
                self.update_application(&request.application);
            }
            RequestPayload::DeleteApplicationRequest((request, _)) => {
                self.delete_application(&request.namespace, &request.name);
            }
            RequestPayload::SchedulerUpdate((request, _)) => {
                self.update_scheduler_update(request);
            }
            RequestPayload::DeregisterExecutor(request) => {
                self.deregister_executor(&request.executor_id);
            }
            _ => return Ok(()),
        }
        Ok(())
    }

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        Arc::new(tokio::sync::RwLock::new(ContainerScheduler {
            clock: self.clock,
            desired_containers: self.desired_containers.clone(),
            executors: self.executors.clone(),
            containers_by_function_uri: self.containers_by_function_uri.clone(),
            function_containers: self.function_containers.clone(),
            executor_states: self.executor_states.clone(),
            workload_placement_constraints: self.workload_placement_constraints.clone(),
        }))
    }

    fn upsert_executor(&mut self, executor_metadata: &ExecutorMetadata) {
        // Only update executor metadata here.
        // num_allocations updates are handled in update_scheduler_update
        // when processing updated_allocations from the processors.
        self.executors.insert(
            executor_metadata.id.clone(),
            executor_metadata.clone().into(),
        );
    }

    fn deregister_executor(&mut self, executor_id: &ExecutorId) {
        if let Some(executor) = self.executors.get_mut(executor_id) {
            executor.tombstoned = true;
        }
    }

    fn update_application(&mut self, application: &Application) {
        // TODO: When applications are updated, we need to fire a new
        // state change to the scheduler so that it asks the container
        // scheduler to rebalance the clusters if max and min containers
        // have changed.
        for function in application.functions.values() {
            let fn_uri = FunctionURI {
                namespace: application.namespace.clone(),
                application: application.name.clone(),
                function: function.name.clone(),
                version: application.version.clone(),
            };
            let dc = DesiredContainers {
                max_count: function.max_containers,
                min_count: function.min_containers,
            };
            self.desired_containers.insert(fn_uri, dc);
        }
    }

    fn delete_application(&mut self, namespace: &str, name: &str) {
        // Remove from desired_containers
        let mut desired_containers_to_remove = Vec::new();
        for (fn_uri, _) in self.desired_containers.iter_mut() {
            if fn_uri.namespace == namespace && fn_uri.application == name {
                desired_containers_to_remove.push(fn_uri.clone());
            }
        }
        for fn_uri in desired_containers_to_remove {
            let _ = self.desired_containers.remove(&fn_uri);
        }

        // Mark existing containers' desired_state as Terminated
        // The actual removal from indices happens when executor reports them as
        // terminated
        for (_, fc) in self.function_containers.iter_mut() {
            if fc.function_container.namespace == namespace &&
                fc.function_container.application_name == name
            {
                fc.desired_state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                    failed_alloc_ids: vec![],
                };
            }
        }
    }

    fn update_scheduler_update(&mut self, scheduler_update: &SchedulerUpdateRequest) {
        for (executor_id, new_executor_server_metadata) in &scheduler_update.updated_executor_states
        {
            // Get the old state to find removed containers
            if let Some(old_state) = self.executor_states.get(executor_id) {
                // Find containers that were removed (in old but not in new)
                for container_id in &old_state.function_container_ids {
                    if !new_executor_server_metadata
                        .function_container_ids
                        .contains(container_id)
                    {
                        // This container was removed - clean up from indices
                        if let Some(fc) = self.function_containers.remove(container_id) {
                            let fn_uri = FunctionURI::from(&fc.function_container);
                            if let Some(container_ids) =
                                self.containers_by_function_uri.get_mut(&fn_uri)
                            {
                                container_ids.retain(|id| id != container_id);
                                if container_ids.is_empty() {
                                    self.containers_by_function_uri.remove(&fn_uri);
                                }
                            }
                        }
                    }
                }
            }

            self.executor_states
                .insert(executor_id.clone(), new_executor_server_metadata.clone());
        }
        for (container_id, new_function_container) in &scheduler_update.containers {
            let fn_uri = FunctionURI::from(&new_function_container.function_container);

            self.function_containers
                .insert(container_id.clone(), new_function_container.clone());

            // Also update the containers_by_function_uri index
            self.containers_by_function_uri
                .entry(fn_uri)
                .or_default()
                .insert(container_id.clone());
        }

        for removed_executor_id in &scheduler_update.remove_executors {
            let _ = self.executor_states.remove(removed_executor_id);
            let _ = self.executors.remove(removed_executor_id);
        }
    }

    pub fn create_container_for_function(
        &mut self,
        namespace: &str,
        application: &str,
        version: &str,
        function: &Function,
        application_state: &ApplicationState,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Check if the application is disabled
        if let ApplicationState::Disabled { reason } = application_state {
            return Err(Error::ConstraintUnsatisfiable {
                version: version.to_string(),
                function_name: function.name.clone(),
                reason: reason.clone(),
            }
            .into());
        }

        let container_resources = ContainerResources {
            cpu_ms_per_sec: function.resources.cpu_ms_per_sec,
            memory_mb: function.resources.memory_mb,
            ephemeral_disk_mb: function.resources.ephemeral_disk_mb,
            gpu: function.resources.gpu_configs.first().cloned(),
        };

        let function_container = ContainerBuilder::default()
            .namespace(namespace.to_string())
            .application_name(application.to_string())
            .function_name(function.name.clone())
            .version(version.to_string())
            .state(ContainerState::Pending)
            .resources(container_resources)
            .max_concurrency(function.max_concurrency)
            .container_type(ContainerType::Function)
            .secret_names(function.secret_names.clone().unwrap_or_default())
            .timeout_secs(function.timeout.0 as u64 / 1000) // Convert ms to secs
            .build()?;

        self.create_container(
            namespace,
            application,
            Some(function),
            &function.resources,
            function_container,
            ContainerType::Function,
            Some(&function.placement_constraints),
        )
    }

    pub fn create_container_for_sandbox(
        &mut self,
        sandbox: &Sandbox,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let resources = FunctionResources {
            cpu_ms_per_sec: sandbox.resources.cpu_ms_per_sec,
            memory_mb: sandbox.resources.memory_mb,
            ephemeral_disk_mb: sandbox.resources.ephemeral_disk_mb,
            gpu_configs: sandbox
                .resources
                .gpu
                .clone()
                .map(|g| vec![g])
                .unwrap_or_default(),
        };

        // Use sandbox ID as the container ID (1:1 relationship)
        let container_id = ContainerId::from(&sandbox.id);
        let function_container = ContainerBuilder::default()
            .id(container_id)
            .namespace(sandbox.namespace.clone())
            .application_name(sandbox.application.clone())
            .function_name(sandbox.id.get().to_string())
            .version(sandbox.application_version.clone())
            .state(ContainerState::Pending)
            .resources(sandbox.resources.clone())
            .max_concurrency(1u32)
            .container_type(ContainerType::Sandbox)
            .image(Some(sandbox.image.clone()))
            .secret_names(sandbox.secret_names.clone())
            .timeout_secs(sandbox.timeout_secs)
            .entrypoint(sandbox.entrypoint.clone().unwrap_or_default())
            .network_policy(sandbox.network_policy.clone())
            .build()?;

        self.create_container(
            &sandbox.namespace,
            &sandbox.application,
            None, // No function for sandboxes
            &resources,
            function_container,
            ContainerType::Sandbox,
            Some(&sandbox.placement_constraints),
        )
    }

    fn create_container(
        &mut self,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        resources: &FunctionResources,
        function_container: Container,
        container_type: ContainerType,
        placement_constraints: Option<&filter::LabelsFilter>,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let mut candidates = self.candidate_hosts(
            namespace,
            application,
            function,
            resources,
            placement_constraints,
        );
        let mut update = SchedulerUpdateRequest::default();

        // If no candidates, try vacuuming to free up resources
        if candidates.is_empty() {
            let function_executors_to_remove = self.vacuum_function_container_candidates(resources);
            for fe in function_executors_to_remove {
                let mut update_fe = fe.clone();
                update_fe.desired_state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                    failed_alloc_ids: Vec::new(),
                };
                let Some(executor_server_state) = self.executor_states.get_mut(&fe.executor_id)
                else {
                    continue;
                };
                executor_server_state.remove_container(&fe.function_container)?;
                update.updated_executor_states.insert(
                    executor_server_state.executor_id.clone(),
                    executor_server_state.clone(),
                );
                update.containers.insert(
                    update_fe.function_container.id.clone(),
                    Box::new(update_fe.clone()),
                );
            }
        }

        // Apply vacuum updates
        self.update(&RequestPayload::SchedulerUpdate((
            Box::new(update.clone()),
            vec![],
        )))?;

        // Try again after vacuum
        candidates = self.candidate_hosts(
            namespace,
            application,
            function,
            resources,
            placement_constraints,
        );
        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            // No host available, return vacuum update (container stays pending)
            return Ok(Some(update));
        };

        let executor_id = candidate.executor_id.clone();
        if !self.executor_states.contains_key(&executor_id) {
            return Ok(Some(update));
        }

        // Consume resources
        let _ = candidate
            .free_resources
            .consume_function_resources(resources)?;

        // Register the container
        let container_update =
            self.register_container(executor_id, function_container, container_type)?;
        update.extend(container_update);

        Ok(Some(update))
    }

    pub fn candidate_hosts(
        &self,
        namespace: &str,
        application: &str,
        function: Option<&Function>,
        resources: &FunctionResources,
        placement_constraints: Option<&filter::LabelsFilter>,
    ) -> Vec<ExecutorServerMetadata> {
        let mut candidates = Vec::new();

        for (_, executor_state) in &self.executor_states {
            let Some(executor) = self.executors.get(&executor_state.executor_id) else {
                error!(
                    executor_id = executor_state.executor_id.get(),
                    "executor not found for candidate executors but was found in executor_states"
                );
                continue;
            };

            if executor.tombstoned {
                continue;
            }

            // Check allowlist based on whether this is a function or sandbox
            if let Some(func) = function {
                // Function: check full allowlist including function name
                if !executor.is_function_allowed(namespace, application, func) {
                    continue;
                }
                if let Some(placement_constraints) = placement_constraints {
                    if !placement_constraints.matches_with_additional_constraints(
                        &executor.labels,
                        &self.workload_placement_constraints.application,
                    ) {
                        continue;
                    }
                }
            } else {
                // Sandbox: check allowlist for namespace/app only
                if !executor.is_app_allowed(namespace, application) {
                    continue;
                }
                // Check placement constraints for sandboxes
                if let Some(placement_constraints) = placement_constraints {
                    if !placement_constraints.matches_with_additional_constraints(
                        &executor.labels,
                        &self.workload_placement_constraints.sandbox,
                    ) {
                        continue;
                    }
                }
            }

            // Check resources
            if executor_state
                .free_resources
                .can_handle_function_resources(resources)
                .is_ok()
            {
                candidates.push(*executor_state.clone());
            }
        }

        candidates
    }

    fn register_container(
        &mut self,
        executor_id: ExecutorId,
        function_container: Container,
        container_type: ContainerType,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(executor_server_metadata) = self.executor_states.get_mut(&executor_id) else {
            return Err(anyhow::anyhow!(
                "Executor state not found for {}",
                executor_id.get()
            ));
        };

        info!(
            executor_id = executor_id.get(),
            container_id = function_container.id.get(),
            container_type = ?container_type,
            "registering container"
        );

        executor_server_metadata.add_container(&function_container)?;

        let fe_server_metadata = ContainerServerMetadata {
            executor_id: executor_id.clone(),
            function_container,
            desired_state: ContainerState::Running,
            container_type,
            allocations: std::collections::HashSet::new(),
        };

        let mut update = SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id, executor_server_metadata.clone());
        update.containers.insert(
            fe_server_metadata.function_container.id.clone(),
            Box::new(fe_server_metadata),
        );

        Ok(update)
    }

    #[tracing::instrument(skip_all)]
    pub fn vacuum_function_container_candidates(
        &self,
        fe_resource: &FunctionResources,
    ) -> Vec<ContainerServerMetadata> {
        // For each executor in the system
        for (executor_id, executor) in &self.executors {
            if executor.tombstoned {
                continue;
            }
            let Some(executor_state) = self.executor_states.get(executor_id) else {
                continue;
            };

            // Get function executors for this executor from our in-memory state
            let function_executors = executor_state.function_container_ids.clone();

            // Start with the current free resources on this executor
            let mut available_resources = executor_state.free_resources.clone();

            let mut function_executors_to_remove = Vec::new();
            for fe_metadata in function_executors.iter() {
                // Skip if the FE is already marked for termination
                let Some(fe_server_metadata) = self.function_containers.get(fe_metadata) else {
                    continue;
                };
                if matches!(
                    fe_server_metadata.desired_state,
                    ContainerState::Terminated { .. }
                ) {
                    continue;
                }

                if !self.executors.contains_key(executor_id) {
                    function_executors_to_remove.push(*fe_server_metadata.clone());
                    continue;
                };

                if self.fe_can_be_removed(fe_server_metadata) {
                    let mut simulated_resources = available_resources.clone();
                    if simulated_resources
                        .free(&fe_server_metadata.function_container.resources)
                        .is_err()
                    {
                        continue;
                    }

                    function_executors_to_remove.push(*fe_server_metadata.clone());
                    available_resources = simulated_resources;

                    if available_resources
                        .can_handle_function_resources(fe_resource)
                        .is_ok()
                    {
                        return function_executors_to_remove;
                    }
                }
            }
        }

        Vec::new()
    }

    fn fe_can_be_removed(&self, fe_meta: &ContainerServerMetadata) -> bool {
        // Check if this container matches the executor's allowlist
        if let Some(executor) = self.executors.get(&fe_meta.executor_id) &&
            let Some(allowlist) = &executor.function_allowlist
        {
            for allowlist_entry in allowlist {
                if allowlist_entry.matches_function_executor(&fe_meta.function_container) {
                    return false;
                }
            }
        }

        // Check if container has active allocations or is a sandbox
        if !fe_meta.can_be_removed() {
            return false;
        }

        let function_uri = FunctionURI::from(&fe_meta.function_container);

        // Check if function is in desired_containers
        let Some(desired_containers) = self.desired_containers.get(&function_uri) else {
            return false;
        };

        // Check min/max container constraints
        let num_containers = self
            .containers_by_function_uri
            .get(&function_uri)
            .map_or(0, |containers| containers.len() as u32);
        let min_count = desired_containers.min_count.unwrap_or(0);
        let max_count = desired_containers.max_count.unwrap_or(u32::MAX);

        min_count < num_containers && num_containers <= max_count
    }

    /// Terminate a container by its ID
    pub fn terminate_container(
        &mut self,
        container_id: &ContainerId,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        let Some(fc) = self.function_containers.get_mut(container_id) else {
            return Ok(None); // Container not found, nothing to terminate
        };

        // Mark for termination
        fc.desired_state = ContainerState::Terminated {
            reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
            failed_alloc_ids: vec![],
        };

        let mut update = SchedulerUpdateRequest::default();
        update.containers.insert(container_id.clone(), fc.clone());

        Ok(Some(update))
    }
}
