use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use opentelemetry::KeyValue;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::{
    data_model::{
        Allocation,
        AllocationId,
        Application,
        ApplicationVersion,
        ContainerId,
        ContainerResources,
        ContainerServerMetadata,
        ContainerState,
        DataPayload,
        ExecutorId,
        Function,
        FunctionCall,
        FunctionCallId,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        Namespace,
        NamespaceBuilder,
        NetworkPolicy,
        PersistedRequestCtx,
        RequestCtx,
        RequestCtxKey,
        Sandbox,
        SandboxKey,
        SandboxStatus,
    },
    state_store::{
        ExecutorCatalog,
        executor_watches::ExecutorWatch,
        in_memory_metrics::InMemoryStoreMetrics,
        requests::RequestPayload,
        scanner::StateReader,
        state_machine::IndexifyObjectsColumns,
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ms},
};

/// A hashable resource profile for bucketing pending work by resource
/// requirements and placement constraints
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceProfile {
    pub cpu_ms_per_sec: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub gpu_count: u32,
    pub gpu_model: Option<String>,
    /// Placement constraint expressions (e.g., "gpu_type==nvidia-a100")
    pub placement_constraints: Vec<String>,
}

impl ResourceProfile {
    pub fn from_function(function: &Function) -> Self {
        let (gpu_count, gpu_model) = function
            .resources
            .gpu_configs
            .first()
            .map(|g| (g.count, Some(g.model.clone())))
            .unwrap_or((0, None));
        // Convert placement constraints to sorted strings for consistent hashing
        let mut placement_constraints: Vec<String> = function
            .placement_constraints
            .0
            .iter()
            .map(|expr| expr.to_string())
            .collect();
        placement_constraints.sort();
        Self {
            cpu_ms_per_sec: function.resources.cpu_ms_per_sec,
            memory_mb: function.resources.memory_mb,
            disk_mb: function.resources.ephemeral_disk_mb,
            gpu_count,
            gpu_model,
            placement_constraints,
        }
    }

    pub fn from_container_resources(resources: &ContainerResources) -> Self {
        let (gpu_count, gpu_model) = resources
            .gpu
            .as_ref()
            .map(|g| (g.count, Some(g.model.clone())))
            .unwrap_or((0, None));
        Self {
            cpu_ms_per_sec: resources.cpu_ms_per_sec,
            memory_mb: resources.memory_mb,
            disk_mb: resources.ephemeral_disk_mb,
            gpu_count,
            gpu_model,
            placement_constraints: vec![], // Sandboxes don't have placement constraints
        }
    }
}

/// Histogram of resource profiles with counts
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceProfileHistogram {
    pub profiles: HashMap<ResourceProfile, u64>,
}

impl ResourceProfileHistogram {
    pub fn increment(&mut self, profile: ResourceProfile) {
        *self.profiles.entry(profile).or_insert(0) += 1;
    }

    pub fn increment_by(&mut self, profile: ResourceProfile, count: u64) {
        if count > 0 {
            *self.profiles.entry(profile).or_insert(0) += count;
        }
    }

    pub fn decrement(&mut self, profile: &ResourceProfile) {
        if let Some(count) = self.profiles.get_mut(profile) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.profiles.remove(profile);
            }
        } else {
            error!(
                ?profile,
                "attempted to decrement non-existent resource profile - possible state drift"
            );
        }
    }
}

/// Tracks pending resource requirements for capacity reporting
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PendingResources {
    pub function_runs: ResourceProfileHistogram,
    pub sandboxes: ResourceProfileHistogram,
    /// Pool deficits: gap between target and current container counts.
    /// Target = min(max, max(min, active + buffer)).
    /// Computed by buffer reconciler after container creation attempts.
    pub pool_deficits: ResourceProfileHistogram,
}

pub struct DesiredStateFunctionExecutor {
    pub function_executor: Box<ContainerServerMetadata>,
    pub resources: ContainerResources,
    pub secret_names: Vec<String>,
    pub initialization_timeout_ms: u32,
    pub code_payload: Option<DataPayload>,
    pub image: Option<String>,
    pub allocation_timeout_ms: u32,
    pub sandbox_timeout_secs: u64,
    pub entrypoint: Vec<String>,
    pub network_policy: Option<NetworkPolicy>,
}

pub struct FunctionCallOutcome {
    pub namespace: String,
    pub request_id: String,
    pub function_call_id: FunctionCallId,
    pub outcome: FunctionRunOutcome,
    pub failure_reason: Option<FunctionRunFailureReason>,
    pub return_value: Option<DataPayload>,
    pub request_error: Option<DataPayload>,
}

pub struct DesiredExecutorState {
    #[allow(clippy::vec_box)]
    pub function_executors: Vec<Box<DesiredStateFunctionExecutor>>,
    #[allow(clippy::box_collection)]
    pub function_run_allocations: std::collections::HashMap<ContainerId, Vec<Allocation>>,
    pub function_call_outcomes: Vec<FunctionCallOutcome>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FunctionRunKey(String);

impl From<&ExecutorWatch> for FunctionRunKey {
    fn from(executor_watch: &ExecutorWatch) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            executor_watch.namespace,
            executor_watch.application,
            executor_watch.request_id,
            executor_watch.function_call_id
        ))
    }
}

impl From<&FunctionRun> for FunctionRunKey {
    fn from(function_run: &FunctionRun) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<Box<FunctionRun>> for FunctionRunKey {
    fn from(function_run: Box<FunctionRun>) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<&Box<FunctionRun>> for FunctionRunKey {
    fn from(function_run: &Box<FunctionRun>) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<&Allocation> for FunctionRunKey {
    fn from(allocation: &Allocation) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            allocation.namespace,
            allocation.application,
            allocation.request_id,
            allocation.function_call_id
        ))
    }
}

impl From<&Box<Allocation>> for FunctionRunKey {
    fn from(allocation: &Box<Allocation>) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            allocation.namespace,
            allocation.application,
            allocation.request_id,
            allocation.function_call_id
        ))
    }
}

impl Display for FunctionRunKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct InMemoryState {
    // clock is the value of the state_id this in-memory state is at.
    pub clock: u64,

    pub namespaces: imbl::HashMap<String, Box<Namespace>>,

    // Namespace|CG Name -> ComputeGraph
    pub applications: imbl::HashMap<String, Box<Application>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub application_versions: imbl::OrdMap<String, Box<ApplicationVersion>>,

    // ExecutorId -> (FE ID -> Map of AllocationId -> Allocation)
    pub allocations_by_executor:
        imbl::HashMap<ExecutorId, HashMap<ContainerId, HashMap<AllocationId, Box<Allocation>>>>,

    // TaskKey -> Task
    pub unallocated_function_runs: imbl::OrdSet<FunctionRunKey>,

    // Function Run Key -> Function Run
    pub function_runs: imbl::OrdMap<FunctionRunKey, Box<FunctionRun>>,

    // Request Context
    pub request_ctx: imbl::OrdMap<RequestCtxKey, Box<RequestCtx>>,

    // Configured executor label sets
    pub executor_catalog: ExecutorCatalog,

    // Sandboxes - SandboxKey -> Sandbox
    pub sandboxes: imbl::OrdMap<SandboxKey, Box<Sandbox>>,

    // Reverse index: ContainerId -> SandboxKey
    pub sandbox_by_container: imbl::HashMap<ContainerId, SandboxKey>,

    // Reverse index: ExecutorId -> Set<SandboxKey>
    pub sandboxes_by_executor: imbl::HashMap<ExecutorId, imbl::HashSet<SandboxKey>>,

    // Pending sandboxes waiting for executor allocation
    pub pending_sandboxes: imbl::OrdSet<SandboxKey>,

    // Tracks resource profile histogram for pending function runs and sandboxes
    pub pending_resources: PendingResources,

    // Histogram metrics for task latency measurements for direct recording
    metrics: InMemoryStoreMetrics,
}

impl InMemoryState {
    pub async fn new(
        clock: u64,
        reader: StateReader,
        executor_catalog: ExecutorCatalog,
    ) -> Result<Self> {
        info!(clock, "initializing in-memory state from state store",);
        let start_time = Instant::now();

        let metrics = InMemoryStoreMetrics::new();

        // Execute all independent database queries in parallel.
        let (
            all_ns,
            all_apps,
            all_cg_versions,
            allocations_result,
            all_persisted_request_ctx,
            all_function_runs,
            all_function_calls,
            all_sandboxes,
        ) = tokio::join!(
            reader.get_all_namespaces(),
            reader.get_all_rows_from_cf::<Application>(IndexifyObjectsColumns::Applications),
            reader.get_all_rows_from_cf::<ApplicationVersion>(
                IndexifyObjectsColumns::ApplicationVersions
            ),
            reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            ),
            reader.get_all_rows_from_cf::<PersistedRequestCtx>(IndexifyObjectsColumns::RequestCtx),
            reader.get_all_rows_from_cf::<FunctionRun>(IndexifyObjectsColumns::FunctionRuns),
            reader.get_all_rows_from_cf::<FunctionCall>(IndexifyObjectsColumns::FunctionCalls),
            reader.get_all_rows_from_cf::<Sandbox>(IndexifyObjectsColumns::Sandboxes),
        );

        // Unwrap all results
        let all_ns = all_ns?;
        let all_apps: Vec<(String, Application)> = all_apps?;
        let all_cg_versions: Vec<(String, ApplicationVersion)> = all_cg_versions?;
        let (allocations, _) = allocations_result?;
        let all_persisted_request_ctx: Vec<(String, PersistedRequestCtx)> =
            all_persisted_request_ctx?;
        let all_function_runs: Vec<(String, FunctionRun)> = all_function_runs?;
        let all_function_calls: Vec<(String, FunctionCall)> = all_function_calls?;
        let all_sandboxes: Vec<(String, Sandbox)> = all_sandboxes?;

        debug!(
            duration = get_elapsed_time(start_time.elapsed().as_millis(), TimeUnit::Milliseconds),
            "loaded in-memory initialization data from database",
        );

        let mut namespaces = imbl::HashMap::new();
        for ns in all_ns {
            namespaces.insert(ns.name.clone(), Box::new(ns));
        }

        let mut applications = imbl::HashMap::new();
        for (_, app) in all_apps {
            applications.insert(app.key(), Box::new(app));
        }

        let mut application_versions = imbl::OrdMap::new();
        for (id, cg) in all_cg_versions {
            application_versions.insert(id, Box::new(cg));
        }

        let allocations_grouped: Vec<_> = allocations
            .par_iter()
            .filter(|allocation| !allocation.is_terminal())
            .map(|allocation| {
                (
                    allocation.target.executor_id.clone(),
                    allocation.target.function_executor_id.clone(),
                    allocation.id.clone(),
                    allocation.clone(),
                )
            })
            .collect();

        let mut allocations_by_executor: imbl::HashMap<
            ExecutorId,
            HashMap<ContainerId, HashMap<AllocationId, Box<Allocation>>>,
        > = imbl::HashMap::new();
        for (executor_id, fe_id, alloc_id, allocation) in allocations_grouped {
            allocations_by_executor
                .entry(executor_id)
                .or_default()
                .entry(fe_id)
                .or_default()
                .insert(alloc_id, Box::new(allocation));
        }

        // Group function_runs by request key (namespace|app|request_id)
        let mut fr_by_request: HashMap<String, HashMap<FunctionCallId, FunctionRun>> =
            HashMap::new();
        for (_key, fr) in all_function_runs {
            let request_key = format!("{}|{}|{}", fr.namespace, fr.application, fr.request_id);
            fr_by_request
                .entry(request_key)
                .or_default()
                .insert(fr.id.clone(), fr);
        }

        // Group function_calls by request key
        let mut fc_by_request: HashMap<String, HashMap<FunctionCallId, FunctionCall>> =
            HashMap::new();
        for (key, fc) in all_function_calls {
            // Key format: namespace|application|request_id|function_call_id
            // Extract the request key (first 3 parts)
            let request_key = key.rsplitn(2, '|').last().unwrap_or(&key).to_string();
            fc_by_request
                .entry(request_key)
                .or_default()
                .insert(fc.function_call_id.clone(), fc);
        }

        // Reconstruct full RequestCtx from PersistedRequestCtx + maps
        let request_ctx_filtered: Vec<_> = all_persisted_request_ctx
            .into_iter()
            .filter(|(_id, ctx)| ctx.outcome.is_none())
            .map(|(_id, persisted)| {
                let key = persisted.key();
                let frs = fr_by_request.remove(&key).unwrap_or_default();
                let fcs = fc_by_request.remove(&key).unwrap_or_default();
                let full_ctx = RequestCtx::from_persisted(persisted, frs, fcs);
                (full_ctx.key().into(), Box::new(full_ctx))
            })
            .collect();

        let mut request_ctx = imbl::OrdMap::new();
        for (key, ctx) in request_ctx_filtered {
            request_ctx.insert(key, ctx);
        }

        let mut function_runs = imbl::OrdMap::new();
        let mut unallocated_function_runs = imbl::OrdSet::new();
        let mut pending_resources = PendingResources::default();

        for ctx in request_ctx.values() {
            for function_run in ctx.function_runs.values() {
                if function_run.status == FunctionRunStatus::Pending {
                    unallocated_function_runs.insert(function_run.into());
                    if let Some(function) =
                        Self::get_function_static(&application_versions, function_run)
                    {
                        pending_resources
                            .function_runs
                            .increment(ResourceProfile::from_function(&function));
                    }
                }
                function_runs.insert(function_run.into(), Box::new(function_run.clone()));
            }
        }

        let sandboxes_filtered: Vec<_> = all_sandboxes
            .par_iter()
            .filter(|(_key, sandbox)| sandbox.status != SandboxStatus::Terminated)
            .map(|(_key, sandbox)| {
                let sandbox_key = SandboxKey::from(sandbox);
                let is_pending = sandbox.status.is_pending();
                (sandbox_key, Box::new(sandbox.clone()), is_pending)
            })
            .collect();

        let mut sandboxes = imbl::OrdMap::new();
        let mut sandbox_by_container = imbl::HashMap::new();
        let mut sandboxes_by_executor = imbl::HashMap::new();
        let mut pending_sandboxes = imbl::OrdSet::new();
        for (sandbox_key, sandbox, is_pending) in sandboxes_filtered {
            if is_pending {
                pending_sandboxes.insert(sandbox_key.clone());
                pending_resources
                    .sandboxes
                    .increment(ResourceProfile::from_container_resources(
                        &sandbox.resources,
                    ));
            }
            // Build reverse index for containers serving sandboxes
            if let Some(container_id) = &sandbox.container_id {
                sandbox_by_container.insert(container_id.clone(), sandbox_key.clone());
            }
            // Build reverse index for sandboxes by executor
            if let Some(executor_id) = &sandbox.executor_id {
                sandboxes_by_executor
                    .entry(executor_id.clone())
                    .or_insert_with(imbl::HashSet::new)
                    .insert(sandbox_key.clone());
            }
            sandboxes.insert(sandbox_key, sandbox);
        }

        let in_memory_state = Self {
            clock,
            namespaces,
            applications,
            application_versions,
            function_runs,
            unallocated_function_runs,
            request_ctx,
            allocations_by_executor,
            executor_catalog,
            sandboxes,
            sandbox_by_container,
            sandboxes_by_executor,
            pending_sandboxes,
            pending_resources,
            metrics,
        };

        info!(
            clock,
            duration = get_elapsed_time(start_time.elapsed().as_millis(), TimeUnit::Milliseconds),
            "completed in-memory state initialization from state store",
        );
        Ok(in_memory_state)
    }

    pub fn update_state(
        &mut self,
        new_clock: u64,
        state_machine_update_request: &RequestPayload,
        _ctx: &str,
    ) -> Result<HashSet<ExecutorId>> {
        // keep track of what clock we are at for this update state
        self.clock = new_clock;

        // Collect all executors that are being changed to notify them.
        let mut changed_executors = HashSet::new();

        match state_machine_update_request {
            RequestPayload::InvokeApplication(req) => {
                self.request_ctx
                    .insert(req.ctx.key().into(), Box::new(req.ctx.clone()));
                for function_run in req.ctx.function_runs.values() {
                    self.function_runs
                        .insert(function_run.into(), Box::new(function_run.clone()));
                    self.unallocated_function_runs.insert(function_run.into());
                    if let Some(function) = self.get_function(function_run) {
                        self.pending_resources
                            .function_runs
                            .increment(ResourceProfile::from_function(&function));
                    }
                }
            }
            RequestPayload::CreateNameSpace(req) => {
                // If the namespace already exists, get its created_at time
                let created_at = if let Some(existing_ns) = self.namespaces.get(&req.name) {
                    existing_ns.created_at
                } else {
                    get_epoch_time_in_ms()
                };
                self.namespaces.insert(
                    req.name.clone(),
                    Box::new(
                        NamespaceBuilder::default()
                            .name(req.name.clone())
                            .created_at(created_at)
                            .blob_storage_bucket(req.blob_storage_bucket.clone())
                            .blob_storage_region(req.blob_storage_region.clone())
                            .build()?,
                    ),
                );
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                self.applications
                    .insert(req.application.key(), Box::new(req.application.clone()));
                let req_version = req.application.to_version()?;
                let version = req_version.version.clone();

                self.application_versions
                    .insert(req_version.key(), Box::new(req_version));

                // FIXME - we should set this in the API and not here, so that these things are
                // not set in the state store
                if req.upgrade_requests_to_current_version {
                    // Update request ctxs and function runs
                    {
                        let mut request_ctx_to_update = vec![];
                        let request_ctx_key_prefix = RequestCtx::key_prefix_for_application(
                            &req.namespace,
                            &req.application.name,
                        );
                        self.request_ctx
                            .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                                request_ctx_key_prefix.clone().into()..,
                            )
                            .take_while(|(k, _v)| k.0.starts_with(&request_ctx_key_prefix))
                            .for_each(|(_k, v)| {
                                let mut ctx = v.clone();
                                ctx.application_version = version.clone();
                                request_ctx_to_update.push(ctx);
                            });

                        for ctx in request_ctx_to_update.iter_mut() {
                            for (_function_call_id, function_run) in
                                ctx.function_runs.clone().iter_mut()
                            {
                                if function_run.version != version {
                                    function_run.version = version.clone();
                                    ctx.function_runs
                                        .insert(function_run.id.clone(), function_run.clone());
                                }
                                let _ = self
                                    .function_runs
                                    .entry(FunctionRunKey(function_run.key()))
                                    .and_modify(|existing_function_run| {
                                        **existing_function_run = function_run.clone();
                                    });
                            }
                        }

                        for ctx in request_ctx_to_update {
                            self.request_ctx.insert(ctx.key().into(), ctx);
                        }
                    }
                }
            }
            RequestPayload::DeleteRequestRequest((req, _)) => {
                self.delete_request(&req.namespace, &req.application, &req.request_id);
            }
            RequestPayload::DeleteApplicationRequest((req, _)) => {
                // Remove app
                let key = Application::key_from(&req.namespace, &req.name);
                self.applications.remove(&key);

                {
                    let request_key_prefix =
                        RequestCtx::key_prefix_for_application(&req.namespace, &req.name);
                    let requests_to_remove = self
                        .request_ctx
                        .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                            request_key_prefix.clone().into()..,
                        )
                        .take_while(|(k, _v)| k.0.starts_with(&request_key_prefix))
                        .map(|(_k, v)| v.request_id.clone())
                        .collect::<Vec<String>>();
                    for k in requests_to_remove {
                        self.delete_request(&req.namespace, &req.name, &k);
                    }
                }

                // Remove app versions (after request contexts are cleaned up)
                {
                    let version_key_prefix =
                        ApplicationVersion::key_prefix_from(&req.namespace, &req.name);
                    let keys_to_remove = self
                        .application_versions
                        .range(version_key_prefix.clone()..)
                        .take_while(|(k, _v)| k.starts_with(&version_key_prefix))
                        .map(|(k, _v)| k.clone())
                        .collect::<Vec<String>>();
                    for k in keys_to_remove {
                        self.application_versions.remove(&k);
                    }
                }
            }
            RequestPayload::SchedulerUpdate(payload) => {
                let req = &payload.update;
                // Note: Allocations are removed from allocations_by_executor in two places:
                // 1. UpsertExecutor handler - when allocation output is ingested
                // 2. remove_function_executors handling below - when FE is terminated
                // So we don't need to remove updated_allocations here.

                // Track executors with updated allocations for notification
                for allocation in &req.updated_allocations {
                    changed_executors.insert(allocation.target.executor_id.clone());
                }

                for (ctx_key, function_call_ids) in &req.updated_function_runs {
                    for function_call_id in function_call_ids {
                        let Some(ctx) = req.updated_request_states.get(ctx_key) else {
                            error!(
                                ctx_key = ctx_key,
                                "request ctx not found for updated function runs"
                            );
                            continue;
                        };
                        let Some(function_run) = ctx.function_runs.get(function_call_id) else {
                            error!(
                                ctx_key = ctx_key,
                                fn_call_id = %function_call_id,
                                "function run not found for updated function runs"
                            );
                            continue;
                        };

                        // Check if the function run was previously pending BEFORE modifying state
                        let was_pending = self
                            .unallocated_function_runs
                            .contains(&function_run.into());
                        let is_pending = function_run.status == FunctionRunStatus::Pending;

                        // Update pending resources based on state transitions
                        match (was_pending, is_pending) {
                            (false, true) => {
                                self.unallocated_function_runs.insert(function_run.into());
                                if let Some(function) = self.get_function(function_run) {
                                    self.pending_resources
                                        .function_runs
                                        .increment(ResourceProfile::from_function(&function));
                                }
                            }
                            (true, false) => {
                                self.unallocated_function_runs.remove(&function_run.into());
                                if let Some(function) = self.get_function(function_run) {
                                    self.pending_resources
                                        .function_runs
                                        .decrement(&ResourceProfile::from_function(&function));
                                }
                            }
                            (_, true) => {
                                self.unallocated_function_runs.insert(function_run.into());
                            }
                            (_, false) => {
                                self.unallocated_function_runs.remove(&function_run.into());
                            }
                        }

                        self.function_runs
                            .insert(function_run.into(), Box::new(function_run.clone()));
                    }
                }

                {
                    let start_time = Instant::now();
                    for (key, request_ctx) in &req.updated_request_states {
                        // Remove function runs for request ctx if completed
                        if request_ctx.outcome.is_some() {
                            self.delete_request(
                                &request_ctx.namespace,
                                &request_ctx.application_name,
                                &request_ctx.request_id,
                            );
                        } else {
                            self.request_ctx
                                .insert(key.into(), Box::new(request_ctx.clone()));
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_delete_requests
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }
                {
                    let start_time = Instant::now();
                    for allocation in &req.new_allocations {
                        if let Some(function_run) = self.function_runs.get(&allocation.into()) {
                            self.allocations_by_executor
                                .entry(allocation.target.executor_id.clone())
                                .or_default()
                                .entry(allocation.target.function_executor_id.clone())
                                .or_default()
                                .insert(allocation.id.clone(), Box::new(allocation.clone()));

                            // Record metrics
                            self.metrics.function_run_pending_latency.record(
                                get_elapsed_time(
                                    function_run.creation_time_ns,
                                    TimeUnit::Nanoseconds,
                                ),
                                &[],
                            );

                            // Executor has a new allocation
                            changed_executors.insert(allocation.target.executor_id.clone());
                        } else {
                            error!(
                                namespace = &allocation.namespace,
                                app = &allocation.application,
                                "fn" = &allocation.function,
                                executor_id = allocation.target.executor_id.get(),
                                allocation_id = %allocation.id,
                                request_id = &allocation.request_id,
                                fn_call_id = allocation.function_call_id.to_string(),
                                "function run not found for new allocation"
                            );
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_insert_new_allocations
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }
                {
                    let start_time = Instant::now();
                    for executor_id in &req.remove_executors {
                        self.allocations_by_executor.remove(executor_id);
                        changed_executors.insert(executor_id.clone());
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_remove_executors
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }

                for (sandbox_key, sandbox) in &req.updated_sandboxes {
                    let was_pending = self.pending_sandboxes.contains(sandbox_key);
                    // Get old state before updating (for index cleanup)
                    let (old_container_id, old_executor_id) = self
                        .sandboxes
                        .get(sandbox_key)
                        .map(|s| (s.container_id.clone(), s.executor_id.clone()))
                        .unwrap_or((None, None));

                    match sandbox.status {
                        SandboxStatus::Pending { .. } => {
                            self.sandboxes
                                .insert(sandbox_key.clone(), Box::new(sandbox.clone()));
                            if !was_pending {
                                self.pending_sandboxes.insert(sandbox_key.clone());
                                // Track pending resources
                                self.pending_resources.sandboxes.increment(
                                    ResourceProfile::from_container_resources(&sandbox.resources),
                                );
                            }
                            // Populate reverse indices for Pending sandboxes that
                            // have an allocated container (WaitingForContainer)
                            if let Some(container_id) = &sandbox.container_id {
                                self.sandbox_by_container
                                    .insert(container_id.clone(), sandbox_key.clone());
                            }
                            if let Some(executor_id) = &sandbox.executor_id {
                                self.sandboxes_by_executor
                                    .entry(executor_id.clone())
                                    .or_default()
                                    .insert(sandbox_key.clone());
                            }
                        }
                        SandboxStatus::Running => {
                            self.sandboxes
                                .insert(sandbox_key.clone(), Box::new(sandbox.clone()));

                            if was_pending {
                                // Pending → Running transition: remove from pending and add to
                                // indices
                                self.pending_sandboxes.remove(sandbox_key);
                                self.pending_resources.sandboxes.decrement(
                                    &ResourceProfile::from_container_resources(&sandbox.resources),
                                );

                                // Add to reverse indices
                                if let Some(container_id) = &sandbox.container_id {
                                    self.sandbox_by_container
                                        .insert(container_id.clone(), sandbox_key.clone());
                                }
                                if let Some(executor_id) = &sandbox.executor_id {
                                    self.sandboxes_by_executor
                                        .entry(executor_id.clone())
                                        .or_default()
                                        .insert(sandbox_key.clone());
                                }
                            }
                        }
                        SandboxStatus::Terminated => {
                            if was_pending {
                                self.pending_resources.sandboxes.decrement(
                                    &ResourceProfile::from_container_resources(&sandbox.resources),
                                );
                            }
                            // Remove from reverse index: container_id
                            if let Some(container_id) = old_container_id {
                                self.sandbox_by_container.remove(&container_id);
                            }
                            // Remove from reverse index: executor_id
                            if let Some(executor_id) = old_executor_id &&
                                let Some(sandboxes) =
                                    self.sandboxes_by_executor.get_mut(&executor_id)
                            {
                                sandboxes.remove(sandbox_key);
                                if sandboxes.is_empty() {
                                    self.sandboxes_by_executor.remove(&executor_id);
                                }
                            }
                            self.sandboxes.remove(sandbox_key);
                            self.pending_sandboxes.remove(sandbox_key);
                        }
                    }
                }

                for fc_metadata in req.containers.values() {
                    // Notify executor when:
                    // 1. Container is Pending (new container being created)
                    // 2. Container desired_state is Terminated (container needs to be stopped)
                    // 3. Container has a sandbox_id (claimed by a sandbox)
                    if matches!(
                        fc_metadata.function_container.state,
                        ContainerState::Pending
                    ) || matches!(fc_metadata.desired_state, ContainerState::Terminated { .. }) ||
                        fc_metadata.function_container.sandbox_id.is_some()
                    {
                        changed_executors.insert(fc_metadata.executor_id.clone());
                    }
                }

                // Update pool deficits if provided by buffer reconciler
                if let Some(deficits) = &req.pool_deficits {
                    self.pending_resources.pool_deficits = deficits.clone();
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                for allocation_output in &req.allocation_outputs {
                    // Remove the allocation
                    {
                        let _ = self
                            .allocations_by_executor
                            .entry(allocation_output.executor_id.clone())
                            .and_modify(|fe_allocations| {
                                if let Some(allocations) = fe_allocations.get_mut(
                                    &allocation_output.allocation.target.function_executor_id,
                                ) && let Some(existing_allocation) =
                                    allocations.remove(&allocation_output.allocation.id)
                                {
                                    // Record metrics
                                    self.metrics.allocation_running_latency.record(
                                        get_elapsed_time(
                                            existing_allocation.created_at,
                                            TimeUnit::Milliseconds,
                                        ),
                                        &[KeyValue::new(
                                            "outcome",
                                            allocation_output.allocation.outcome.to_string(),
                                        )],
                                    );
                                }

                                // Remove the function if no allocations left
                                fe_allocations.retain(|_, f| !f.is_empty());
                            });

                        // Executor's allocation is removed
                        changed_executors.insert(allocation_output.executor_id.clone());
                    }

                    // Record metrics
                    self.metrics.allocation_completion_latency.record(
                        get_elapsed_time(
                            allocation_output.allocation.created_at,
                            TimeUnit::Milliseconds,
                        ),
                        &[KeyValue::new(
                            "outcome",
                            allocation_output.allocation.outcome.to_string(),
                        )],
                    );
                }
            }
            RequestPayload::DataplaneResults(req) => {
                let executor_id = &req.event.executor_id;
                for alloc_event in &req.event.allocation_events {
                    let _ = self
                        .allocations_by_executor
                        .entry(executor_id.clone())
                        .and_modify(|fe_allocations| {
                            if let Some(allocations) = fe_allocations
                                .get_mut(&alloc_event.allocation_target.function_executor_id) &&
                                let Some(existing_allocation) =
                                    allocations.remove(&alloc_event.allocation_id)
                            {
                                self.metrics.allocation_running_latency.record(
                                    get_elapsed_time(
                                        existing_allocation.created_at,
                                        TimeUnit::Milliseconds,
                                    ),
                                    &[KeyValue::new(
                                        "outcome",
                                        alloc_event.allocation_outcome.to_string(),
                                    )],
                                );
                            }
                            fe_allocations.retain(|_, f| !f.is_empty());
                        });
                    changed_executors.insert(executor_id.clone());
                }
            }
            RequestPayload::CreateSandbox(req) => {
                let sandbox_key = SandboxKey::from(&req.sandbox);
                self.sandboxes
                    .insert(sandbox_key.clone(), Box::new(req.sandbox.clone()));
                if req.sandbox.status.is_pending() {
                    self.pending_sandboxes.insert(sandbox_key);
                    self.pending_resources.sandboxes.increment(
                        ResourceProfile::from_container_resources(&req.sandbox.resources),
                    );
                }
            }
            // Pool operations handled by ContainerScheduler
            _ => {}
        }

        Ok(changed_executors)
    }

    pub fn delete_function_runs(&mut self, function_runs: Vec<FunctionRun>) {
        for function_run in function_runs.iter() {
            // Check if it was pending before removing
            let was_pending = self
                .unallocated_function_runs
                .contains(&function_run.into());

            self.function_runs.remove(&function_run.into());
            self.unallocated_function_runs.remove(&function_run.into());

            if was_pending && let Some(function) = self.get_function(function_run) {
                self.pending_resources
                    .function_runs
                    .decrement(&ResourceProfile::from_function(&function));
            }
        }

        for (_executor, allocations_by_fe) in self.allocations_by_executor.iter_mut() {
            for (_fe_id, allocations) in allocations_by_fe.iter_mut() {
                allocations.retain(|_, allocation| {
                    !function_runs
                        .iter()
                        .any(|function_run| function_run.id == allocation.function_call_id)
                });
            }
        }
    }

    pub fn delete_request(&mut self, namespace: &str, application: &str, request_id: &str) {
        // Remove request ctx
        self.request_ctx
            .remove(&RequestCtx::key_from(namespace, application, request_id).into());

        // Remove tasks
        let key_prefix = FunctionRun::key_prefix_for_request(namespace, application, request_id);
        let mut function_runs_to_remove = Vec::new();
        self.function_runs
            .range(FunctionRunKey(key_prefix.clone())..)
            .take_while(|(k, _v)| k.0.starts_with(&key_prefix))
            .for_each(|(_k, v)| {
                function_runs_to_remove.push(*v.clone());
            });
        self.delete_function_runs(function_runs_to_remove);
    }

    pub fn unallocated_function_runs(&self) -> Vec<FunctionRun> {
        let unallocated_function_run_keys = self
            .unallocated_function_runs
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut function_runs = Vec::new();
        for function_run_key in unallocated_function_run_keys {
            if let Some(function_run) = self.function_runs.get(&function_run_key) {
                function_runs.push(*function_run.clone());
            } else {
                error!(
                    function_run_key = function_run_key.0,
                    "function run not found for unallocated function run"
                );
            }
        }
        function_runs
    }

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        Arc::new(tokio::sync::RwLock::new(InMemoryState {
            clock: self.clock,
            namespaces: self.namespaces.clone(),
            applications: self.applications.clone(),
            application_versions: self.application_versions.clone(),
            request_ctx: self.request_ctx.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            executor_catalog: self.executor_catalog.clone(),
            metrics: self.metrics.clone(),
            function_runs: self.function_runs.clone(),
            unallocated_function_runs: self.unallocated_function_runs.clone(),
            sandboxes: self.sandboxes.clone(),
            sandbox_by_container: self.sandbox_by_container.clone(),
            sandboxes_by_executor: self.sandboxes_by_executor.clone(),
            pending_sandboxes: self.pending_sandboxes.clone(),
            pending_resources: self.pending_resources.clone(),
        }))
    }

    pub fn get_pending_resources(&self) -> &PendingResources {
        &self.pending_resources
    }

    pub fn get_function(&self, function_run: &FunctionRun) -> Option<Function> {
        Self::get_function_static(&self.application_versions, function_run)
    }

    fn get_function_static(
        application_versions: &imbl::OrdMap<String, Box<ApplicationVersion>>,
        function_run: &FunctionRun,
    ) -> Option<Function> {
        let app_version = application_versions
            .get(&function_run.key_application_version(&function_run.application))?;
        let function = app_version.functions.get(&function_run.name)?;
        Some(function.clone())
    }

    #[allow(clippy::borrowed_box)]
    pub fn application_version<'a>(
        &'a self,
        namespace: &str,
        application_name: &str,
        version: &str,
    ) -> Option<&'a Box<ApplicationVersion>> {
        self.application_versions
            .get(&ApplicationVersion::key_from(
                namespace,
                application_name,
                version,
            ))
            .or_else(|| {
                error!(
                    namespace = namespace,
                    app = application_name,
                    app_version = version,
                    "application version not found",
                );
                None
            })
    }

    #[allow(clippy::borrowed_box)]
    pub fn get_existing_application_version<'a>(
        &'a self,
        function_run: &FunctionRun,
    ) -> Option<&'a Box<ApplicationVersion>> {
        self.application_versions
            .get(&function_run.key_application_version(&function_run.application))
            .or_else(|| {
                error!(
                    fn_call_id = function_run.id.to_string(),
                    request_id = function_run.request_id.to_string(),
                    namespace = function_run.namespace,
                    app = function_run.application,
                    "fn" = function_run.name,
                    app_version = function_run.version,
                    "application version not found",
                );
                None
            })
    }

    /// Simulates a server restart by clearing executor-related in-memory state
    /// while preserving allocations. This creates the scenario where:
    /// - allocations_by_executor has allocations (loaded from DB)
    /// - executors and executor_states are empty (executors haven't
    ///   re-registered)
    #[cfg(test)]
    pub fn simulate_server_restart_clear_executor_state(&mut self) {
        //self.executors.clear();
        //self.executor_states.clear();
        //self.function_executors_by_fn_uri.clear();
        // Note: allocations_by_executor is intentionally NOT cleared
        // as allocations are persisted and loaded from DB on restart
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;
    /// Macro to easily bootstrap an InMemoryState for tests.
    ///
    /// Usage:
    /// ```
    /// let state = in_memory_state_bootstrap! { clock: 42, tasks: my_tasks };
    /// ```
    /// You can specify any subset of fields; the rest will be defaulted.
    #[macro_export]
    macro_rules! in_memory_state_bootstrap {
        ( $($field:ident : $value:expr),* $(,)? ) => {{
            let mut state = super::InMemoryState::default();
            $( state.$field = $value; )*
            state
        }};
    }

    impl Default for InMemoryState {
        fn default() -> Self {
            Self {
                clock: 0,
                namespaces: imbl::HashMap::new(),
                applications: imbl::HashMap::new(),
                application_versions: imbl::OrdMap::new(),
                allocations_by_executor: imbl::HashMap::new(),
                request_ctx: imbl::OrdMap::new(),
                executor_catalog: ExecutorCatalog::default(),
                metrics: InMemoryStoreMetrics::new(),
                function_runs: imbl::OrdMap::new(),
                unallocated_function_runs: imbl::OrdSet::new(),
                sandboxes: imbl::OrdMap::new(),
                sandbox_by_container: imbl::HashMap::new(),
                sandboxes_by_executor: imbl::HashMap::new(),
                pending_sandboxes: imbl::OrdSet::new(),
                pending_resources: PendingResources::default(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::state_store::in_memory_state::{
        FunctionRunKey,
        ResourceProfile,
        ResourceProfileHistogram,
    };

    #[test]
    fn test_function_run_key_ordering() {
        {
            let fn_run_key1 = FunctionRunKey("task1".to_string());
            let fn_run_key2 = FunctionRunKey("task2".to_string());
            let fn_run_key3 = FunctionRunKey("task3".to_string());
            let fn_run_key4 = FunctionRunKey("task4".to_string());
            let fn_run_key5 = FunctionRunKey("task5".to_string());

            assert!(fn_run_key1 < fn_run_key2);
            assert!(fn_run_key2 < fn_run_key3);
            assert!(fn_run_key3 < fn_run_key4);
            assert!(fn_run_key4 < fn_run_key5);
        }
    }

    #[test]
    fn test_histogram_increment_decrement() {
        let mut histogram = ResourceProfileHistogram::default();
        let profile = ResourceProfile {
            cpu_ms_per_sec: 1000,
            memory_mb: 256,
            disk_mb: 1024,
            gpu_count: 0,
            gpu_model: None,
            placement_constraints: vec![],
        };

        // Initially empty
        assert!(histogram.profiles.is_empty());

        // Increment creates entry
        histogram.increment(profile.clone());
        assert_eq!(histogram.profiles.get(&profile), Some(&1));

        // Increment again
        histogram.increment(profile.clone());
        assert_eq!(histogram.profiles.get(&profile), Some(&2));

        // Decrement reduces count
        histogram.decrement(&profile);
        assert_eq!(histogram.profiles.get(&profile), Some(&1));

        // Decrement to zero removes entry
        histogram.decrement(&profile);
        assert!(histogram.profiles.get(&profile).is_none());
        assert!(histogram.profiles.is_empty());
    }

    #[test]
    fn test_histogram_decrement_nonexistent_profile() {
        let mut histogram = ResourceProfileHistogram::default();
        let profile = ResourceProfile {
            cpu_ms_per_sec: 1000,
            memory_mb: 256,
            disk_mb: 1024,
            gpu_count: 0,
            gpu_model: None,
            placement_constraints: vec![],
        };

        // Decrementing non-existent profile should not panic (logs error)
        histogram.decrement(&profile);
        assert!(histogram.profiles.is_empty());
    }

    #[test]
    fn test_histogram_multiple_profiles() {
        let mut histogram = ResourceProfileHistogram::default();
        let small_profile = ResourceProfile {
            cpu_ms_per_sec: 1000,
            memory_mb: 256,
            disk_mb: 1024,
            gpu_count: 0,
            gpu_model: None,
            placement_constraints: vec![],
        };
        let large_profile = ResourceProfile {
            cpu_ms_per_sec: 8000,
            memory_mb: 4096,
            disk_mb: 10240,
            gpu_count: 2,
            gpu_model: Some("nvidia-h100".to_string()),
            placement_constraints: vec!["gpu_type==nvidia-h100".to_string()],
        };

        // Add multiple of each
        histogram.increment(small_profile.clone());
        histogram.increment(small_profile.clone());
        histogram.increment(small_profile.clone());
        histogram.increment(large_profile.clone());

        assert_eq!(histogram.profiles.get(&small_profile), Some(&3));
        assert_eq!(histogram.profiles.get(&large_profile), Some(&1));
        assert_eq!(histogram.profiles.len(), 2);

        // Decrement one small
        histogram.decrement(&small_profile);
        assert_eq!(histogram.profiles.get(&small_profile), Some(&2));

        // Decrement all large
        histogram.decrement(&large_profile);
        assert!(histogram.profiles.get(&large_profile).is_none());
        assert_eq!(histogram.profiles.len(), 1);
    }
}
