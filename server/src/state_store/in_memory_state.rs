use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt::Display,
    sync::Arc,
    time::Instant,
};

use anyhow::{Result, anyhow};
use opentelemetry::KeyValue;
use tracing::{debug, error, info, warn};

use crate::{
    data_model::{
        Allocation,
        AllocationId,
        Application,
        ApplicationState,
        ApplicationVersion,
        DataPayload,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionCallId,
        FunctionExecutorId,
        FunctionExecutorResources,
        FunctionExecutorServerMetadata,
        FunctionExecutorState,
        FunctionResources,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        FunctionURI,
        Namespace,
        NamespaceBuilder,
        RequestCtx,
        RequestCtxKey,
    },
    executor_api::executor_api_pb::DataPayloadEncoding,
    processor::resource_placement::{ExecutorCapacity, PendingRunPoint, ResourcePlacementIndex},
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

#[derive(Debug, Clone)]
pub enum Error {
    ApplicationVersionNotFound {
        version: String,
        function_name: String,
    },
    FunctionNotFound {
        version: String,
        function_name: String,
    },
    ConstraintUnsatisfiable {
        reason: String,
        version: String,
        function_name: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ApplicationVersionNotFound { version, .. } => {
                write!(f, "Application version not found: {version}")
            }
            Error::FunctionNotFound { function_name, .. } => {
                write!(f, "Function not found: {function_name}")
            }
            Error::ConstraintUnsatisfiable { reason, .. } => reason.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn version(&self) -> &str {
        match self {
            Error::ApplicationVersionNotFound { version, .. } => version,
            Error::FunctionNotFound { version, .. } => version,
            Error::ConstraintUnsatisfiable { version, .. } => version,
        }
    }

    pub fn function_name(&self) -> &str {
        match self {
            Error::ApplicationVersionNotFound { function_name, .. } => function_name,
            Error::FunctionNotFound { function_name, .. } => function_name,
            Error::ConstraintUnsatisfiable { function_name, .. } => function_name,
        }
    }
}

pub struct DesiredStateFunctionExecutor {
    pub function_executor: Box<FunctionExecutorServerMetadata>,
    pub resources: FunctionExecutorResources,
    pub secret_names: Vec<String>,
    pub initialization_timeout_ms: u32,
    pub code_payload: DataPayload,
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
    pub function_run_allocations: std::collections::HashMap<FunctionExecutorId, Vec<Allocation>>,
    pub function_call_outcomes: Vec<FunctionCallOutcome>,
    pub clock: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CandidateFunctionExecutor {
    pub executor_id: ExecutorId,
    pub function_executor_id: FunctionExecutorId,
    pub allocation_count: usize,
}

impl PartialOrd for CandidateFunctionExecutor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandidateFunctionExecutor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by allocation count first (ascending - least loaded first)
        // Then by executor_id and function_executor_id for consistent ordering
        self.allocation_count
            .cmp(&other.allocation_count)
            .then_with(|| self.executor_id.cmp(&other.executor_id))
            .then_with(|| self.function_executor_id.cmp(&other.function_executor_id))
    }
}

pub struct CandidateFunctionExecutors {
    pub function_executors: BTreeSet<CandidateFunctionExecutor>,
    pub num_pending_function_executors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FunctionRunKey(pub(crate) String);

impl FunctionRunKey {
    /// Create a FunctionRunKey from components (for testing)
    #[cfg(test)]
    pub fn new(namespace: &str, app: &str, request_id: &str, fn_call_id: &str) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            namespace, app, request_id, fn_call_id
        ))
    }
}

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

    // ExecutorId -> ExecutorMetadata
    // This is the metadata that executor is sending us, not the **Desired** state
    // from the perspective of the state store.
    pub executors: imbl::HashMap<ExecutorId, Box<ExecutorMetadata>>,

    // ExecutorId -> (FE ID -> List of Function Executors)
    pub executor_states: imbl::HashMap<ExecutorId, Box<ExecutorServerMetadata>>,

    pub function_executors_by_fn_uri: imbl::HashMap<
        FunctionURI,
        imbl::HashMap<FunctionExecutorId, Box<FunctionExecutorServerMetadata>>,
    >,

    // ExecutorId -> (FE ID -> Map of AllocationId -> Allocation)
    pub allocations_by_executor: imbl::HashMap<
        ExecutorId,
        HashMap<FunctionExecutorId, HashMap<AllocationId, Box<Allocation>>>,
    >,

    // TaskKey -> Task
    pub unallocated_function_runs: imbl::OrdSet<FunctionRunKey>,

    // Function Run Key -> Function Run
    pub function_runs: imbl::OrdMap<FunctionRunKey, Box<FunctionRun>>,

    // Request Context
    pub request_ctx: imbl::OrdMap<RequestCtxKey, Box<RequestCtx>>,

    // Configured executor label sets
    pub executor_catalog: ExecutorCatalog,

    // Index: Executor Catalog Entry Name -> Set of Function Run Keys
    // Maps executor catalog entry names to function runs that match those catalog entry labels
    pub function_runs_by_catalog_entry: imbl::HashMap<String, imbl::OrdSet<FunctionRunKey>>,

    /// Spatial index for efficient resource-based matching of pending runs to
    /// executors. Uses R-Tree for O(log N + K) queries instead of O(N)
    /// linear scans.
    pub resource_placement_index: ResourcePlacementIndex,

    // Histogram metrics for task latency measurements for direct recording
    metrics: InMemoryStoreMetrics,
}

impl InMemoryState {
    pub async fn new(
        clock: u64,
        reader: StateReader,
        executor_catalog: ExecutorCatalog,
    ) -> Result<Self> {
        info!(
            "initializing in-memory state from state store at clock {}",
            clock
        );

        let metrics = InMemoryStoreMetrics::new();

        // Creating Namespaces
        let mut namespaces = imbl::HashMap::new();
        let mut applications = imbl::HashMap::new();
        {
            let all_ns = reader.get_all_namespaces().await?;
            for ns in &all_ns {
                // Creating Namespaces
                namespaces.insert(ns.name.clone(), Box::new(ns.clone()));

                // Creating Compute Graphs and Versions
                let cgs = reader.list_applications(&ns.name, None, None).await?.0;
                for cg in cgs {
                    applications.insert(cg.key(), Box::new(cg));
                }
            }
        }

        let mut application_versions = imbl::OrdMap::new();
        {
            let all_cg_versions: Vec<(String, ApplicationVersion)> = reader
                .get_all_rows_from_cf(IndexifyObjectsColumns::ApplicationVersions)
                .await?;
            for (id, cg) in all_cg_versions {
                application_versions.insert(id, Box::new(cg));
            }
        }
        // Creating Allocated Tasks By Function by Executor
        let mut allocations_by_executor: imbl::HashMap<
            ExecutorId,
            HashMap<FunctionExecutorId, HashMap<AllocationId, Box<Allocation>>>,
        > = imbl::HashMap::new();
        {
            let (allocations, _) = reader
                .get_rows_from_cf_with_limits::<Allocation>(
                    &[],
                    None,
                    IndexifyObjectsColumns::Allocations,
                    None,
                )
                .await?;
            for allocation in allocations {
                if allocation.is_terminal() {
                    continue;
                }
                allocations_by_executor
                    .entry(allocation.target.executor_id.clone())
                    .or_default()
                    .entry(allocation.target.function_executor_id.clone())
                    .or_default()
                    .insert(allocation.id.clone(), Box::new(allocation));
            }
        }

        let mut request_ctx = imbl::OrdMap::new();
        {
            let all_app_request_ctx: Vec<(String, RequestCtx)> = reader
                .get_all_rows_from_cf(IndexifyObjectsColumns::RequestCtx)
                .await?;
            for (_id, ctx) in all_app_request_ctx {
                // Do not cache completed requests
                if ctx.outcome.is_some() {
                    continue;
                }
                request_ctx.insert(ctx.key().into(), Box::new(ctx));
            }
        }

        let mut function_runs = imbl::OrdMap::new();
        let mut unallocated_function_runs = imbl::OrdSet::new();
        for ctx in request_ctx.values() {
            for function_run in ctx.function_runs.values() {
                if function_run.status == FunctionRunStatus::Pending {
                    unallocated_function_runs.insert(function_run.into());
                }
                function_runs.insert(function_run.into(), Box::new(function_run.clone()));
            }
        }

        // Build the index of function runs by catalog entry
        let function_runs_by_catalog_entry: imbl::HashMap<String, imbl::OrdSet<FunctionRunKey>> =
            imbl::HashMap::new();

        let mut in_memory_state = Self {
            clock,
            namespaces,
            applications,
            application_versions,
            executors: imbl::HashMap::new(),
            function_runs,
            unallocated_function_runs,
            request_ctx,
            allocations_by_executor,
            // function executors by executor are not known at startup
            executor_states: imbl::HashMap::new(),
            function_executors_by_fn_uri: imbl::HashMap::new(),
            executor_catalog,
            function_runs_by_catalog_entry,
            resource_placement_index: ResourcePlacementIndex::new(),
            metrics,
        };

        // Populate the catalog index for all existing function runs
        let function_runs_to_index: Vec<_> =
            in_memory_state.function_runs.values().cloned().collect();
        for function_run in function_runs_to_index {
            in_memory_state.index_function_run_by_catalog(&function_run);
        }

        info!(
            "completed in-memory state initialization from state store at clock {}",
            clock
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
                    // Index the function run by catalog entry
                    self.index_function_run_by_catalog(function_run);
                    // Add to spatial placement index
                    self.add_pending_run_to_index(function_run);
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
                                        *existing_function_run = Box::new(function_run.clone());
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

                // Remove app versions
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

                // Remove request contexts
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
            }
            RequestPayload::SchedulerUpdate((req, _)) => {
                // Note: Allocations are removed from allocations_by_executor in two places:
                // 1. UpsertExecutor handler - when allocation output is ingested
                // 2. remove_function_executors handling below - when FE is terminated
                // So we don't need to remove updated_allocations here.

                // Track executors with updated allocations for notification
                for allocation in &req.updated_allocations {
                    changed_executors.insert(allocation.target.executor_id.clone());
                }

                {
                    let start_time = Instant::now();
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
                            if function_run.status == FunctionRunStatus::Pending {
                                self.unallocated_function_runs.insert(function_run.into());
                                // Add to spatial placement index
                                self.add_pending_run_to_index(function_run);
                            } else {
                                self.unallocated_function_runs.remove(&function_run.into());
                                // Remove from spatial placement index
                                self.resource_placement_index
                                    .remove_pending_run(&function_run.into());
                            }
                            self.function_runs
                                .insert(function_run.into(), Box::new(function_run.clone()));
                            // Index the function run by catalog entry
                            self.index_function_run_by_catalog(function_run);
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_index_function_run_by_catalog
                        .record(start_time.elapsed().as_secs_f64(), &[]);
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
                    for fe_meta in req.new_function_executors.clone() {
                        let Some(executor_state) =
                            self.executor_states.get_mut(&fe_meta.executor_id)
                        else {
                            error!(
                                executor_id = fe_meta.executor_id.get(),
                                "executor not found for new function executor"
                            );
                            continue;
                        };
                        executor_state.function_executors.insert(
                            fe_meta.function_executor.id.clone(),
                            Box::new(fe_meta.clone()),
                        );

                        executor_state.resource_claims.insert(
                            fe_meta.function_executor.id.clone(),
                            fe_meta.function_executor.resources.clone(),
                        );

                        let fn_uri = FunctionURI::from(&fe_meta);
                        self.function_executors_by_fn_uri
                            .entry(fn_uri)
                            .or_default()
                            .insert(
                                fe_meta.function_executor.id.clone(),
                                Box::new(fe_meta.clone()),
                            );

                        // Executor has a new function executor
                        changed_executors.insert(fe_meta.executor_id.clone());
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_insert_function_executors
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }

                {
                    let start_time = Instant::now();
                    for allocation in &req.new_allocations {
                        if let Some(function_run) = self.function_runs.get(&allocation.into()) {
                            self.unallocated_function_runs.remove(&function_run.into());
                            // Remove from spatial placement index
                            self.resource_placement_index
                                .remove_pending_run(&function_run.into());

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
                    for (executor_id, function_executors) in &req.remove_function_executors {
                        if let Some(fe_allocations) =
                            self.allocations_by_executor.get_mut(executor_id)
                        {
                            fe_allocations
                                .retain(|fe_id, _allocations| !function_executors.contains(fe_id));
                        }

                        for function_executor_id in function_executors {
                            let fe = self.executor_states.get_mut(executor_id).and_then(
                                |executor_state| {
                                    executor_state.resource_claims.remove(function_executor_id);
                                    executor_state
                                        .function_executors
                                        .remove(function_executor_id)
                                },
                            );

                            if let Some(fe) = fe {
                                let fn_uri = FunctionURI::from(&fe);
                                self.function_executors_by_fn_uri
                                    .get_mut(&fn_uri)
                                    .and_then(|fe_map| fe_map.remove(&fe.function_executor.id));
                            }
                            changed_executors.insert(executor_id.clone());
                        }

                        // record the time instead of using a timer because we cannot
                        // borrow the metric as immutable and borrow self as mutable inside the
                        // loop.
                        self.metrics
                            .scheduler_update_remove_function_executors
                            .record(start_time.elapsed().as_secs_f64(), &[]);
                    }
                }

                {
                    let start_time = Instant::now();
                    for executor_id in &req.remove_executors {
                        self.executors.remove(executor_id);
                        self.allocations_by_executor.remove(executor_id);
                        self.executor_states.remove(executor_id);
                        // Remove from spatial placement index
                        self.resource_placement_index.remove_executor(executor_id);

                        // Executor is removed
                        changed_executors.insert(executor_id.clone());
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_remove_executors
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }

                {
                    let start_time = Instant::now();
                    for (executor_id, free_resources) in &req.updated_executor_resources {
                        if let Some(executor) = self.executor_states.get_mut(executor_id) {
                            executor.free_resources = free_resources.clone();
                        }
                        // Update executor capacity in spatial placement index
                        self.update_executor_capacity_in_index(executor_id);
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_free_executor_resources
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                self.executors
                    .insert(req.executor.id.clone(), Box::new(req.executor.clone()));
                if self.executor_states.get(&req.executor.id).is_none() {
                    self.executor_states.insert(
                        req.executor.id.clone(),
                        Box::new(ExecutorServerMetadata {
                            executor_id: req.executor.id.clone(),
                            function_executors: HashMap::new(),
                            resource_claims: HashMap::new(),
                            free_resources: req.executor.host_resources.clone(),
                        }),
                    );
                }
                // Update executor capacity in spatial placement index
                self.update_executor_capacity_in_index(&req.executor.id);

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
            RequestPayload::DeregisterExecutor(req) => {
                let executor = self.executors.get_mut(&req.executor_id);
                if let Some(executor) = executor {
                    executor.tombstoned = true;
                }
            }
            _ => {}
        }

        Ok(changed_executors)
    }

    pub fn fe_resource_for_function_run(
        &self,
        function_run: &FunctionRun,
    ) -> Result<FunctionResources> {
        let application = self
            .application_versions
            .get(&ApplicationVersion::key_from(
                &function_run.namespace,
                &function_run.application,
                &function_run.version,
            ))
            .ok_or(anyhow!(
                "application version: {} not found",
                function_run.version
            ))?;
        let function = application
            .functions
            .get(&function_run.name)
            .ok_or(anyhow!("function: {} not found", function_run.name))?;
        Ok(function.resources.clone())
    }

    #[tracing::instrument(skip_all)]
    pub fn candidate_executors(
        &self,
        function_run: &FunctionRun,
    ) -> Result<Vec<ExecutorServerMetadata>> {
        let application = self
            .application_versions
            .get(&ApplicationVersion::key_from(
                &function_run.namespace,
                &function_run.application,
                &function_run.version,
            ))
            .ok_or_else(|| Error::ApplicationVersionNotFound {
                version: function_run.version.clone(),
                function_name: function_run.name.clone(),
            })?;

        // Check to see whether the app state is marked as
        // active; if not, we do not schedule its tasks, even if there
        // are executors that could handle this particular task.
        if let ApplicationState::Disabled { reason } = &application.state {
            return Err(Error::ConstraintUnsatisfiable {
                version: application.version.to_string(),
                function_name: function_run.name.clone(),
                reason: reason.to_owned(),
            }
            .into());
        }

        let function = application
            .functions
            .get(&function_run.name)
            .ok_or_else(|| Error::FunctionNotFound {
                version: function_run.version.clone(),
                function_name: function_run.name.clone(),
            })?;

        let mut candidates = Vec::new();

        for (_, executor_state) in &self.executor_states {
            let Some(executor) = self.executors.get(&executor_state.executor_id) else {
                error!(
                    executor_id = executor_state.executor_id.get(),
                    "executor not found for candidate executors but was found in executor_states"
                );
                continue;
            };
            if executor.tombstoned || !executor.is_function_allowed(function_run) {
                continue;
            }

            // Check if this executor's labels matches the function's
            // placement constraints
            if !function.placement_constraints.matches(&executor.labels) {
                continue;
            }

            // TODO: Match functions to GPU models according to prioritized order in
            // gpu_configs.
            if executor_state
                .free_resources
                .can_handle_function_resources(&function.resources)
                .is_ok()
            {
                candidates.push(*executor_state.clone());
            }
        }

        Ok(candidates)
    }

    pub fn candidate_function_executors(
        &self,
        function_run: &FunctionRun,
        capacity_threshold: u32,
    ) -> Result<CandidateFunctionExecutors> {
        let mut candidates = BTreeSet::new();

        let fn_uri = FunctionURI::from(function_run);
        let function_executors = self.function_executors_by_fn_uri.get(&fn_uri);
        let mut num_pending_function_executors = 0;
        if let Some(function_executors) = function_executors {
            for (_, metadata) in function_executors.iter() {
                if metadata.function_executor.state == FunctionExecutorState::Pending ||
                    metadata.function_executor.state == FunctionExecutorState::Unknown
                {
                    num_pending_function_executors += 1;
                }
                if matches!(
                    metadata.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) || matches!(
                    metadata.function_executor.state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }
                // FIXME - Create a reverse index of fe_id -> # active allocations
                let allocation_count = self
                    .allocations_by_executor
                    .get(&metadata.executor_id)
                    .and_then(|alloc_map| alloc_map.get(&metadata.function_executor.id))
                    .map(|allocs| allocs.len())
                    .unwrap_or(0);
                if (allocation_count as u32) <
                    capacity_threshold * metadata.function_executor.max_concurrency
                {
                    candidates.insert(CandidateFunctionExecutor {
                        executor_id: metadata.executor_id.clone(),
                        function_executor_id: metadata.function_executor.id.clone(),
                        allocation_count,
                    });
                }
            }
        }
        Ok(CandidateFunctionExecutors {
            function_executors: candidates,
            num_pending_function_executors,
        })
    }

    pub fn delete_function_runs(&mut self, function_runs: Vec<FunctionRun>) {
        for function_run in function_runs.iter() {
            self.function_runs.remove(&function_run.into());
            self.unallocated_function_runs.remove(&function_run.into());
            // Remove from catalog entry index
            self.unindex_function_run_from_catalog(function_run);
            // Remove from spatial placement index
            self.resource_placement_index
                .remove_pending_run(&function_run.into());
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

    pub fn get_fe_resources_by_uri(
        &self,
        ns: &str,
        cg: &str,
        fn_name: &str,
        version: &str,
    ) -> Option<FunctionResources> {
        let cg_version = self
            .application_versions
            .get(&ApplicationVersion::key_from(ns, cg, version))
            .cloned()?;
        cg_version
            .functions
            .get(fn_name)
            .map(|node| node.resources.clone())
    }

    pub fn get_fe_max_concurrency_by_uri(
        &self,
        ns: &str,
        cg: &str,
        fn_name: &str,
        version: &str,
    ) -> Option<u32> {
        let cg_version = self
            .application_versions
            .get(&ApplicationVersion::key_from(ns, cg, version))
            .cloned()?;
        cg_version
            .functions
            .get(fn_name)
            .map(|node| node.max_concurrency)
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

    /// Find pending runs that can be placed on an executor using the spatial
    /// index. This is O(log N + K) instead of O(N) - much faster for large
    /// numbers of pending runs.
    pub fn find_placeable_runs_for_executor(
        &self,
        executor_id: &ExecutorId,
        limit: usize,
    ) -> Vec<FunctionRun> {
        let placeable = self
            .resource_placement_index
            .find_runs_for_executor(executor_id, limit);

        placeable
            .into_iter()
            .filter_map(|point| self.function_runs.get(&point.run_key).map(|fr| *fr.clone()))
            .collect()
    }

    /// Find pending runs for a specific function. O(1) lookup + O(K) iteration.
    /// Used to check if there's work for an FE after an allocation completes.
    pub fn find_pending_runs_for_function(
        &self,
        fn_uri: &FunctionURI,
        limit: usize,
    ) -> Vec<FunctionRun> {
        self.resource_placement_index
            .get_runs_for_function(fn_uri)
            .into_iter()
            .take(limit)
            .filter_map(|run_key| self.function_runs.get(&run_key).map(|fr| *fr.clone()))
            .collect()
    }

    /// Add a pending function run to the spatial placement index.
    pub fn add_pending_run_to_index(&mut self, function_run: &FunctionRun) {
        let Some(app_version) = self.get_existing_application_version(function_run) else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                "Skipping placement index: application version not found"
            );
            return;
        };

        let Some(function) = app_version.functions.get(&function_run.name) else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                fn_name = &function_run.name,
                "Skipping placement index: function not found"
            );
            return;
        };

        let point = PendingRunPoint::new(
            function_run.into(),
            FunctionURI::from(function_run),
            &function.resources,
            function.placement_constraints.clone(),
            function_run.creation_time_ns as u64,
        );

        self.resource_placement_index.add_pending_run(point);
    }

    /// Update executor capacity in the spatial placement index.
    pub fn update_executor_capacity_in_index(&mut self, executor_id: &ExecutorId) {
        let Some(executor_state) = self.executor_states.get(executor_id) else {
            return;
        };

        let Some(executor) = self.executors.get(executor_id) else {
            return;
        };

        let capacity = ExecutorCapacity::new(
            executor_state.free_resources.clone(),
            executor.labels.clone(),
        );

        self.resource_placement_index
            .update_executor_capacity(executor_id.clone(), capacity);
    }

    /// Populate the spatial placement index from existing unallocated runs.
    /// Called during initialization or after state is cloned.
    pub fn populate_placement_index(&mut self) {
        // First, add all unallocated function runs
        let unallocated_keys: Vec<_> = self.unallocated_function_runs.iter().cloned().collect();
        for run_key in unallocated_keys {
            if let Some(function_run) = self.function_runs.get(&run_key).cloned() {
                self.add_pending_run_to_index(&function_run);
            }
        }

        // Then, update all executor capacities
        let executor_ids: Vec<_> = self.executor_states.keys().cloned().collect();
        for executor_id in executor_ids {
            self.update_executor_capacity_in_index(&executor_id);
        }

        debug!(
            pending_count = self.resource_placement_index.pending_count(),
            executor_count = self.resource_placement_index.executor_count(),
            "populated resource placement index"
        );
    }

    #[tracing::instrument(skip_all)]
    pub fn vacuum_function_executors_candidates(
        &self,
        fe_resource: &FunctionResources,
    ) -> Result<Vec<FunctionExecutorServerMetadata>> {
        // For each executor in the system
        for (executor_id, executor) in &self.executors {
            if executor.tombstoned {
                continue;
            }

            // Get function executors for this executor from our in-memory state
            let function_executors = self
                .executor_states
                .get(executor_id)
                .cloned()
                .map(|executor_state| {
                    executor_state
                        .function_executors
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            // Start with the current free resources on this executor
            let mut available_resources = self
                .executor_states
                .get(executor_id)
                .map(|executor_state| executor_state.free_resources.clone())
                .unwrap_or_default();

            let mut function_executors_to_remove = Vec::new();
            for fe_metadata in function_executors.iter() {
                // Skip if the FE is already marked for termination
                if matches!(
                    fe_metadata.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }

                let fe = &fe_metadata.function_executor;
                let Some(executor) = self.executors.get(executor_id) else {
                    function_executors_to_remove.push(*fe_metadata.clone());
                    continue;
                };

                let Some(latest_cg_version) = self
                    .applications
                    .get(&Application::key_from(&fe.namespace, &fe.application_name))
                    .map(|cg| cg.version.clone())
                else {
                    function_executors_to_remove.push(*fe_metadata.clone());
                    continue;
                };

                let has_pending_tasks = self.has_pending_tasks(fe_metadata);

                let mut can_be_removed = false;
                if !has_pending_tasks {
                    let mut found_allowlist_match = false;
                    if let Some(allowlist) = executor.function_allowlist.as_ref() {
                        for allowlist_entry in allowlist.iter() {
                            if allowlist_entry.matches_function_executor(fe) &&
                                fe.version == latest_cg_version
                            {
                                found_allowlist_match = true;
                                break;
                            }
                        }
                    }
                    if !found_allowlist_match {
                        debug!(
                            "Candidate for removal: outdated function executor {} from executor {} (version {} < latest {})",
                            fe.id.get(),
                            executor_id.get(),
                            fe.version,
                            latest_cg_version
                        );
                        can_be_removed = true;
                    }
                }

                if can_be_removed {
                    let mut simulated_resources = available_resources.clone();
                    if simulated_resources
                        .free(&fe_metadata.function_executor.resources)
                        .is_err()
                    {
                        continue;
                    }

                    function_executors_to_remove.push(*fe_metadata.clone());
                    available_resources = simulated_resources;

                    if available_resources
                        .can_handle_function_resources(fe_resource)
                        .is_ok()
                    {
                        debug!(
                            "Found sufficient space on executor {} by removing {} function executors",
                            executor_id.get(),
                            function_executors_to_remove.len()
                        );
                        return Ok(function_executors_to_remove);
                    }
                }
            }
            debug!(
                "Could not find sufficient space on executor {} even after vacuuming",
                executor_id.get()
            );
        }

        Ok(Vec::new())
    }

    fn has_pending_tasks(&self, fe_meta: &FunctionExecutorServerMetadata) -> bool {
        let task_prefixes_for_fe = format!(
            "{}|{}|",
            fe_meta.function_executor.namespace, fe_meta.function_executor.application_name
        );
        self.function_runs
            .range(FunctionRunKey(task_prefixes_for_fe.clone())..)
            .take_while(|(k, _v)| k.0.starts_with(&task_prefixes_for_fe))
            .filter(|(_k, v)| {
                v.name == fe_meta.function_executor.function_name &&
                    v.version == fe_meta.function_executor.version
            })
            .any(|(_k, v)| !v.is_terminal())
    }

    pub fn desired_state(
        &self,
        executor_id: &ExecutorId,
        executor_watches: HashSet<ExecutorWatch>,
    ) -> Option<DesiredExecutorState> {
        if let Some(executor) = self.executors.get(executor_id) {
            if executor.tombstoned {
                return None;
            }
        } else {
            return None;
        }
        let mut function_call_outcomes = Vec::new();
        for executor_watch in executor_watches.iter() {
            let Some(function_run) = self.function_runs.get(&executor_watch.into()) else {
                error!(
                    namspace = executor_watch.namespace.clone(),
                    app = executor_watch.application.clone(),
                    request_id = executor_watch.request_id.clone(),
                    function_call_id = executor_watch.function_call_id.clone(),
                    "function run not found for executor watch",
                );
                continue;
            };
            let failure_reason = match function_run.outcome {
                Some(FunctionRunOutcome::Failure(failure_reason)) => Some(failure_reason),
                _ => None,
            };
            function_call_outcomes.push(FunctionCallOutcome {
                namespace: function_run.namespace.clone(),
                request_id: function_run.request_id.clone(),
                function_call_id: function_run.id.clone(),
                outcome: function_run.outcome.unwrap_or(FunctionRunOutcome::Unknown),
                failure_reason,
                return_value: function_run.output.clone(),
                request_error: function_run.request_error.clone(),
            });
        }
        let active_function_executors = self
            .executor_states
            .get(executor_id)
            .cloned()
            .map(|executor_state| executor_state.function_executors.clone())
            .unwrap_or_default()
            .values()
            .filter(|fe_meta| {
                !matches!(
                    fe_meta.desired_state,
                    FunctionExecutorState::Terminated { .. }
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut function_executors = Vec::new();
        let mut task_allocations = std::collections::HashMap::new();
        for fe_meta in active_function_executors.iter() {
            let fe = &fe_meta.function_executor;
            let Some(cg_version) = self
                .application_versions
                .get(&ApplicationVersion::key_from(
                    &fe.namespace,
                    &fe.application_name,
                    &fe.version,
                ))
                .cloned()
            else {
                continue;
            };
            let Some(cg_node) = cg_version.functions.get(&fe.function_name) else {
                continue;
            };
            function_executors.push(Box::new(DesiredStateFunctionExecutor {
                function_executor: fe_meta.clone(),
                resources: fe.resources.clone(),
                secret_names: cg_node.secret_names.clone().unwrap_or_default(),
                initialization_timeout_ms: cg_node.initialization_timeout.0,
                code_payload: DataPayload {
                    id: cg_version.code.id.clone(),
                    metadata_size: 0,
                    path: cg_version.code.path.clone(),
                    size: cg_version.code.size,
                    sha256_hash: cg_version.code.sha256_hash.clone(),
                    offset: 0, // Code always uses its full BLOB
                    encoding: DataPayloadEncoding::BinaryZip.as_str_name().to_string(),
                },
            }));

            let allocations = self
                .allocations_by_executor
                .get(executor_id)
                .and_then(|allocations| allocations.get(&fe_meta.function_executor.id.clone()))
                .map(|allocations| {
                    allocations
                        .values()
                        .map(|allocation| allocation.as_ref().clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            task_allocations.insert(fe_meta.function_executor.id.clone(), allocations);
        }

        Some(DesiredExecutorState {
            function_executors,
            function_run_allocations: task_allocations,
            clock: self.clock,
            function_call_outcomes,
        })
    }

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        let mut cloned = InMemoryState {
            clock: self.clock,
            namespaces: self.namespaces.clone(),
            applications: self.applications.clone(),
            application_versions: self.application_versions.clone(),
            executors: self.executors.clone(),
            request_ctx: self.request_ctx.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            executor_states: self.executor_states.clone(),
            function_executors_by_fn_uri: self.function_executors_by_fn_uri.clone(),
            executor_catalog: self.executor_catalog.clone(),
            function_runs_by_catalog_entry: self.function_runs_by_catalog_entry.clone(),
            // ResourcePlacementIndex is rebuilt from existing data
            resource_placement_index: ResourcePlacementIndex::new(),
            metrics: self.metrics.clone(),
            function_runs: self.function_runs.clone(),
            unallocated_function_runs: self.unallocated_function_runs.clone(),
        };
        // Populate the spatial index from existing unallocated runs and executors
        cloned.populate_placement_index();
        Arc::new(tokio::sync::RwLock::new(cloned))
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

    /// Add a function run to the catalog entry index if it matches any catalog
    /// entry labels and resources
    fn index_function_run_by_catalog(&mut self, function_run: &FunctionRun) {
        // Get the application version to check placement constraints
        let Some(app_version) = self.get_existing_application_version(function_run) else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                "Skipping catalog indexing: application version not found"
            );
            return;
        };

        let Some(function) = app_version.functions.get(&function_run.name).cloned() else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                fn_name = &function_run.name,
                "Skipping catalog indexing: function not found"
            );
            return;
        };

        // Clone catalog entries to avoid borrow issues
        let catalog_entries = self.executor_catalog.entries.clone();

        // Check each catalog entry to see if this function run matches
        for catalog_entry in &catalog_entries {
            // Check if the function's placement constraints match this catalog entry's
            // labels
            if !function
                .placement_constraints
                .matches(&catalog_entry.labels)
            {
                continue;
            }

            // Check if the catalog entry has sufficient resources for this function
            if !function
                .resources
                .can_be_handled_by_catalog_entry(catalog_entry)
            {
                continue;
            }

            // Both labels and resources match - add to index
            self.function_runs_by_catalog_entry
                .entry(catalog_entry.name.clone())
                .or_default()
                .insert(function_run.into());
        }
    }

    /// Remove a function run from the catalog entry index
    fn unindex_function_run_from_catalog(&mut self, function_run: &FunctionRun) {
        let function_run_key: FunctionRunKey = function_run.into();

        // Remove from all catalog entries
        for catalog_entry in &self.executor_catalog.entries {
            let _ = self
                .function_runs_by_catalog_entry
                .entry(catalog_entry.name.clone())
                .and_modify(|run_keys| {
                    run_keys.remove(&function_run_key);
                });
        }
    }

    /// Get all function runs that match a specific catalog entry by name
    #[cfg(test)]
    fn function_runs_for_catalog_entry(&self, catalog_entry_name: &str) -> Vec<FunctionRun> {
        let Some(run_keys) = self.function_runs_by_catalog_entry.get(catalog_entry_name) else {
            return Vec::new();
        };

        let mut results = Vec::new();
        for run_key in run_keys.iter() {
            if let Some(function_run) = self.function_runs.get(run_key) {
                results.push(*function_run.clone());
            }
        }
        results
    }

    /// Get all function runs grouped by catalog entry
    #[cfg(test)]
    fn all_function_runs_by_catalog_entry(&self) -> HashMap<String, Vec<FunctionRun>> {
        let mut result = HashMap::new();

        for (catalog_entry_name, run_keys) in self.function_runs_by_catalog_entry.iter() {
            let mut function_runs = Vec::new();
            for run_key in run_keys.iter() {
                if let Some(function_run) = self.function_runs.get(run_key) {
                    function_runs.push(*function_run.clone());
                }
            }
            if !function_runs.is_empty() {
                result.insert(catalog_entry_name.clone(), function_runs);
            }
        }

        result
    }

    /// Simulates a server restart by clearing executor-related in-memory state
    /// while preserving allocations. This creates the scenario where:
    /// - allocations_by_executor has allocations (loaded from DB)
    /// - executors and executor_states are empty (executors haven't
    ///   re-registered)
    #[cfg(test)]
    pub fn simulate_server_restart_clear_executor_state(&mut self) {
        self.executors.clear();
        self.executor_states.clear();
        self.function_executors_by_fn_uri.clear();
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
                executors: imbl::HashMap::new(),
                executor_states: imbl::HashMap::new(),
                function_executors_by_fn_uri: imbl::HashMap::new(),
                allocations_by_executor: imbl::HashMap::new(),
                request_ctx: imbl::OrdMap::new(),
                executor_catalog: ExecutorCatalog::default(),
                function_runs_by_catalog_entry: imbl::HashMap::new(),
                resource_placement_index: ResourcePlacementIndex::new(),
                metrics: InMemoryStoreMetrics::new(),
                function_runs: imbl::OrdMap::new(),
                unallocated_function_runs: imbl::OrdSet::new(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        config::GpuModel,
        data_model::{
            ComputeOp,
            ExecutorId,
            FunctionCall,
            FunctionCallId,
            FunctionExecutorBuilder,
            FunctionExecutorId,
            FunctionExecutorResources,
            FunctionExecutorServerMetadata,
            FunctionExecutorState,
            FunctionRun,
            FunctionRunBuilder,
            FunctionRunFailureReason,
            FunctionRunOutcome,
            FunctionRunStatus,
        },
        in_memory_state_bootstrap,
        state_store::in_memory_state::FunctionRunKey,
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
    fn test_has_pending_function_runs() {
        // Helper function to create a function run
        fn create_function_run(
            namespace: &str,
            application: &str,
            request_id: &str,
            function: &str,
            outcome: Option<FunctionRunOutcome>,
        ) -> FunctionRun {
            FunctionRunBuilder::default()
                .id(FunctionCallId(format!(
                    "{}-{}-{}-{}",
                    namespace, application, request_id, function
                )))
                .request_id(request_id.to_string())
                .namespace(namespace.to_string())
                .application(application.to_string())
                .name(function.to_string())
                .version("1.0".to_string())
                .compute_op(ComputeOp::FunctionCall(FunctionCall {
                    inputs: vec![],
                    function_call_id: FunctionCallId(format!(
                        "{}-{}-{}-{}",
                        namespace, application, request_id, function
                    )),
                    fn_name: function.to_string(),
                    call_metadata: Bytes::new(),
                    parent_function_call_id: None,
                }))
                .status(FunctionRunStatus::Pending)
                .outcome(outcome)
                .input_args(vec![])
                .attempt_number(0)
                .call_metadata(Bytes::new())
                .build()
                .unwrap()
        }

        // Create function executor metadata for testing
        let executor_id = ExecutorId::new("test-executor".to_string());
        let function_executor = FunctionExecutorBuilder::default()
            .id(FunctionExecutorId::new("test-fe".to_string()))
            .namespace("test-namespace".to_string())
            .application_name("test-graph".to_string())
            .function_name("test-function".to_string())
            .version("1.0".to_string())
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        let fe_metadata = FunctionExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_executor: function_executor.clone(),
            desired_state: FunctionExecutorState::Running,
        };

        // Test case 1: No tasks - should return false
        let mut state = in_memory_state_bootstrap! { clock: 1 };
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 2: Add a terminal function run (Success) - should return false
        let terminal_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-1",
            "test-function",
            Some(FunctionRunOutcome::Success),
        );
        state
            .function_runs
            .insert(FunctionRunKey::from(&terminal_run), Box::new(terminal_run));
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 3: Add a terminal function run (Failure) - should return false
        let terminal_run2 = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-2",
            "test-function",
            Some(FunctionRunOutcome::Failure(
                FunctionRunFailureReason::FunctionError,
            )),
        );
        state.function_runs.insert(
            FunctionRunKey::from(&terminal_run2),
            Box::new(terminal_run2),
        );
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 4: Add a non-terminal function run (None outcome) - should return
        // true
        let pending_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-3",
            "test-function",
            None,
        );
        state
            .function_runs
            .insert(FunctionRunKey::from(&pending_run), Box::new(pending_run));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 5: Add tasks for different namespace/graph - should not affect
        // result
        let different_run = create_function_run(
            "different-namespace",
            "different-graph",
            "inv-4",
            "test-function",
            None,
        );
        state.function_runs.insert(
            FunctionRunKey::from(&different_run),
            Box::new(different_run),
        );
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 6: Add tasks for same namespace/graph but different function -
        // should not affect result
        let different_fn_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-5",
            "different-function",
            None,
        );
        state.function_runs.insert(
            FunctionRunKey::from(&different_fn_run),
            Box::new(different_fn_run),
        );
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 7: Add multiple pending tasks - should still return true
        let pending_run2 = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-6",
            "test-function",
            None,
        );
        state
            .function_runs
            .insert(FunctionRunKey::from(&pending_run2), Box::new(pending_run2));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 8: Change all pending tasks to terminal - should return false
        let keys_to_update: Vec<FunctionRunKey> = state
            .function_runs
            .iter()
            .filter(|(key, function_run)| {
                key.0.starts_with("test-namespace|test-graph|") &&
                    function_run.name == "test-function" &&
                    function_run.outcome.is_none()
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_update {
            if let Some(mut function_run) = state.function_runs.get(&key).cloned() {
                function_run.outcome = Some(FunctionRunOutcome::Success);
                state.function_runs.insert(key, function_run);
            }
        }
        assert!(!state.has_pending_tasks(&fe_metadata));

        let function_executor = FunctionExecutorBuilder::default()
            .id(FunctionExecutorId::new("test-fe-2".to_string()))
            .namespace("test-namespace".to_string())
            .application_name("test-graph".to_string())
            .function_name("different-function".to_string())
            .version("1.0".to_string())
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        // Test case 9: Test with different function executor metadata
        let fe_metadata2 = FunctionExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_executor,
            desired_state: FunctionExecutorState::Running,
        };
        assert!(state.has_pending_tasks(&fe_metadata2));

        // Test case 10: Change the different function task to terminal - should return
        // false
        let keys_to_update2: Vec<FunctionRunKey> = state
            .function_runs
            .iter()
            .filter(|(key, function_run)| {
                key.0.starts_with("test-namespace|test-graph|") &&
                    function_run.name == "different-function" &&
                    function_run.outcome.is_none()
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_update2 {
            if let Some(mut function_run) = state.function_runs.get(&key).cloned() {
                function_run.outcome = Some(FunctionRunOutcome::Success);
                state.function_runs.insert(key, function_run);
            }
        }
        assert!(!state.has_pending_tasks(&fe_metadata2));
    }

    #[test]
    fn test_candidate_function_executor_ordering() {
        use super::CandidateFunctionExecutor;

        // Test that CandidateFunctionExecutor orders correctly by allocation count
        // first, then by executor_id, then by function_executor_id

        let executor_id_1 = ExecutorId::new("executor-1".to_string());
        let executor_id_2 = ExecutorId::new("executor-2".to_string());
        let fe_id_1 = FunctionExecutorId::new("fe-1".to_string());
        let fe_id_2 = FunctionExecutorId::new("fe-2".to_string());

        // Create function executors with different allocation counts
        let fe_1 = FunctionExecutorBuilder::default()
            .id(fe_id_1.clone())
            .namespace("test-ns".to_string())
            .application_name("test-app".to_string())
            .function_name("test-fn".to_string())
            .version("1.0".to_string())
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        let fe_2 = FunctionExecutorBuilder::default()
            .id(fe_id_2.clone())
            .namespace("test-ns".to_string())
            .application_name("test-app".to_string())
            .function_name("test-fn".to_string())
            .version("1.0".to_string())
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        // Test 1: Lower allocation count comes first
        let candidate_1 = CandidateFunctionExecutor {
            executor_id: executor_id_1.clone(),
            function_executor_id: fe_1.id.clone(),
            allocation_count: 5,
        };

        let candidate_2 = CandidateFunctionExecutor {
            executor_id: executor_id_1.clone(),
            function_executor_id: fe_1.id.clone(),
            allocation_count: 10,
        };

        assert!(candidate_1 < candidate_2);
        assert!(candidate_2 > candidate_1);

        // Test 2: Same allocation count, executor_id determines order
        let candidate_3 = CandidateFunctionExecutor {
            executor_id: executor_id_1.clone(),
            function_executor_id: fe_1.id.clone(),
            allocation_count: 5,
        };

        let candidate_4 = CandidateFunctionExecutor {
            executor_id: executor_id_2.clone(),
            function_executor_id: fe_1.id.clone(),
            allocation_count: 5,
        };

        assert!(candidate_3 < candidate_4);

        // Test 3: Same allocation count and executor_id, function_executor_id
        // determines order
        let candidate_5 = CandidateFunctionExecutor {
            executor_id: executor_id_1.clone(),
            function_executor_id: fe_1.id.clone(),
            allocation_count: 5,
        };

        let candidate_6 = CandidateFunctionExecutor {
            executor_id: executor_id_1.clone(),
            function_executor_id: fe_2.id.clone(),
            allocation_count: 5,
        };

        assert!(candidate_5 < candidate_6);

        // Test 4: BTreeSet maintains correct order
        let mut candidates = std::collections::BTreeSet::new();
        candidates.insert(candidate_2.clone()); // allocation_count: 10
        candidates.insert(candidate_1.clone()); // allocation_count: 5
        candidates.insert(candidate_6.clone()); // allocation_count: 5, executor_id_1, fe_id_2

        let first = candidates.first().unwrap();
        assert_eq!(first.allocation_count, 5);
        assert_eq!(first.function_executor_id, fe_id_1);
        assert_eq!(first.executor_id, executor_id_1);

        let last = candidates.last().unwrap();
        assert_eq!(last.allocation_count, 10);
    }

    #[test]
    fn test_function_run_catalog_indexing() {
        use std::collections::HashMap;

        use crate::{
            config::ExecutorCatalogEntry,
            data_model::{
                ApplicationEntryPoint,
                ApplicationState,
                ApplicationVersionBuilder,
                Function,
                FunctionResources,
                GPUResources,
                filter::{Expression, LabelsFilter, Operator},
                test_objects::tests::mock_data_payload,
            },
            in_memory_state_bootstrap,
            state_store::ExecutorCatalog,
        };

        // Create executor catalog entries with different resource capacities and labels
        let catalog_entry_small = ExecutorCatalogEntry {
            name: "small".to_string(),
            cpu_cores: 2, // 2000 ms/sec
            memory_gb: 4, // 4096 MB
            disk_gb: 10,  // 10240 MB
            gpu_model: None,
            labels: HashMap::new(),
        };

        let mut catalog_entry_large_labels = HashMap::new();
        catalog_entry_large_labels.insert("region".to_string(), "us-west".to_string());
        catalog_entry_large_labels.insert("tier".to_string(), "premium".to_string());
        let catalog_entry_large = ExecutorCatalogEntry {
            name: "large".to_string(),
            cpu_cores: 16, // 16000 ms/sec
            memory_gb: 64, // 65536 MB
            disk_gb: 500,  // 512000 MB
            gpu_model: None,
            labels: catalog_entry_large_labels,
        };

        let mut catalog_entry_gpu_labels = HashMap::new();
        catalog_entry_gpu_labels.insert("has_gpu".to_string(), "true".to_string());
        catalog_entry_gpu_labels.insert("region".to_string(), "us-east".to_string());
        let catalog_entry_gpu = ExecutorCatalogEntry {
            name: "gpu-node".to_string(),
            cpu_cores: 8,
            memory_gb: 32,
            disk_gb: 100,
            gpu_model: Some(GpuModel {
                name: "nvidia-a100".to_string(),
                count: 4,
            }),
            labels: catalog_entry_gpu_labels,
        };

        let executor_catalog = ExecutorCatalog {
            entries: vec![
                catalog_entry_small.clone(),
                catalog_entry_large.clone(),
                catalog_entry_gpu.clone(),
            ],
        };

        // Create functions with different resource requirements and placement
        // constraints
        let mut function_light = Function::default();
        function_light.name = "light".to_string();
        function_light.placement_constraints = LabelsFilter::default(); // Matches all labels
        function_light.resources = FunctionResources {
            cpu_ms_per_sec: 1000, // 1 core
            memory_mb: 2048,      // 2 GB
            ephemeral_disk_mb: 5000,
            gpu_configs: vec![],
        };

        let mut function_heavy = Function::default();
        function_heavy.name = "heavy".to_string();
        function_heavy.placement_constraints = LabelsFilter(vec![
            Expression {
                key: "region".to_string(),
                value: "us-west".to_string(),
                operator: Operator::Eq,
            },
            Expression {
                key: "tier".to_string(),
                value: "premium".to_string(),
                operator: Operator::Eq,
            },
        ]);
        function_heavy.resources = FunctionResources {
            cpu_ms_per_sec: 10000, // 10 cores - only fits on large
            memory_mb: 32768,      // 32 GB
            ephemeral_disk_mb: 100000,
            gpu_configs: vec![],
        };

        let mut function_gpu = Function::default();
        function_gpu.name = "gpu_task".to_string();
        function_gpu.placement_constraints = LabelsFilter(vec![Expression {
            key: "region".to_string(),
            value: "us-east".to_string(),
            operator: Operator::Eq,
        }]);
        function_gpu.resources = FunctionResources {
            cpu_ms_per_sec: 2000,
            memory_mb: 8192,
            ephemeral_disk_mb: 10000,
            gpu_configs: vec![GPUResources {
                count: 1,
                model: "nvidia-a100".to_string(),
            }],
        };

        let mut functions = HashMap::new();
        functions.insert("light".to_string(), function_light);
        functions.insert("heavy".to_string(), function_heavy);
        functions.insert("gpu_task".to_string(), function_gpu);

        let app_version = ApplicationVersionBuilder::default()
            .namespace("test-ns".to_string())
            .name("test-app".to_string())
            .created_at(0)
            .version("1.0".to_string())
            .functions(functions)
            .edges(HashMap::new())
            .code(mock_data_payload())
            .entrypoint(ApplicationEntryPoint {
                function_name: "light".to_string(),
                input_serializer: "cloudpickle".to_string(),
                output_serializer: "cloudpickle".to_string(),
                output_type_hints_base64: String::new(),
            })
            .state(ApplicationState::Active)
            .build()
            .unwrap();

        let mut application_versions = imbl::OrdMap::new();
        application_versions.insert(app_version.key(), Box::new(app_version));

        // Create function runs
        let fn_run_light = create_test_function_run("test-ns", "test-app", "req-1", "light", "1.0");
        let fn_run_heavy = create_test_function_run("test-ns", "test-app", "req-2", "heavy", "1.0");
        let fn_run_gpu =
            create_test_function_run("test-ns", "test-app", "req-3", "gpu_task", "1.0");

        let mut function_runs = imbl::OrdMap::new();
        function_runs.insert(
            FunctionRunKey(fn_run_light.key()),
            Box::new(fn_run_light.clone()),
        );
        function_runs.insert(
            FunctionRunKey(fn_run_heavy.key()),
            Box::new(fn_run_heavy.clone()),
        );
        function_runs.insert(
            FunctionRunKey(fn_run_gpu.key()),
            Box::new(fn_run_gpu.clone()),
        );

        let mut state = in_memory_state_bootstrap! {
            clock: 1,
            application_versions: application_versions,
            function_runs: function_runs,
            executor_catalog: executor_catalog,
        };

        // Manually index all function runs
        let function_runs_to_index: Vec<_> = state.function_runs.values().cloned().collect();
        for function_run in function_runs_to_index {
            state.index_function_run_by_catalog(&function_run);
        }

        // Test 1: Light function (no label constraints) should be in small catalog only
        // (large and gpu have labels, but light has no constraints so matches empty
        // labels in small)
        let small_runs = state.function_runs_for_catalog_entry("small");
        assert_eq!(
            small_runs.len(),
            1,
            "Small catalog should have 1 function run (light)"
        );
        assert!(small_runs.iter().any(|fr| fr.id == fn_run_light.id));

        // Test 2: Large catalog should have light + heavy (labels match, resources fit)
        let large_runs = state.function_runs_for_catalog_entry("large");
        assert_eq!(
            large_runs.len(),
            2,
            "Large catalog should have 2 function runs (light + heavy)"
        );
        assert!(large_runs.iter().any(|fr| fr.id == fn_run_light.id));
        assert!(large_runs.iter().any(|fr| fr.id == fn_run_heavy.id));

        // Test 3: Heavy function should ONLY be in large catalog
        // - Labels match (region=us-west, tier=premium)
        // - Resources match (requires 10 cores, 32GB - only large has 16 cores, 64GB)
        assert!(
            !small_runs.iter().any(|fr| fr.id == fn_run_heavy.id),
            "Heavy function should not be in small catalog (insufficient resources)"
        );

        // Test 4: GPU function should ONLY be in gpu-node catalog
        // - Labels match (region=us-east)
        // - Resources match (requires GPU, only gpu-node has nvidia-a100)
        let gpu_runs = state.function_runs_for_catalog_entry("gpu-node");
        assert_eq!(
            gpu_runs.len(),
            2,
            "GPU catalog should have 2 function runs (light + gpu_task)"
        );
        assert!(gpu_runs.iter().any(|fr| fr.id == fn_run_gpu.id));
        assert!(gpu_runs.iter().any(|fr| fr.id == fn_run_light.id));

        // GPU task should not be in other catalogs (they don't have GPUs or don't match
        // labels)
        assert!(
            !small_runs.iter().any(|fr| fr.id == fn_run_gpu.id),
            "GPU function should not be in small catalog (no GPU)"
        );
        assert!(
            !large_runs.iter().any(|fr| fr.id == fn_run_gpu.id),
            "GPU function should not be in large catalog (no GPU)"
        );

        // Test 5: Query non-existent catalog entry
        let non_existent = state.function_runs_for_catalog_entry("non-existent");
        assert_eq!(
            non_existent.len(),
            0,
            "Non-existent catalog should return empty"
        );

        // Test 6: Get all function runs by catalog entry
        let all_by_catalog = state.all_function_runs_by_catalog_entry();
        assert_eq!(
            all_by_catalog.len(),
            3,
            "All three catalogs have at least one function"
        );
        assert_eq!(all_by_catalog.get("small").unwrap().len(), 1); // light
        assert_eq!(all_by_catalog.get("large").unwrap().len(), 2); // light + heavy
        assert_eq!(all_by_catalog.get("gpu-node").unwrap().len(), 2); // light + gpu_task
        assert!(all_by_catalog.contains_key("small"));
        assert!(all_by_catalog.contains_key("large"));
        assert!(all_by_catalog.contains_key("gpu-node"));

        // Test 7: Remove a function run and verify index update
        state.delete_function_runs(vec![fn_run_heavy.clone()]);
        let large_runs_after_delete = state.function_runs_for_catalog_entry("large");
        assert_eq!(
            large_runs_after_delete.len(),
            1,
            "After delete, large catalog should have 1 function run (light)"
        );
        assert!(
            !large_runs_after_delete
                .iter()
                .any(|fr| fr.id == fn_run_heavy.id),
            "Deleted function should not be in index"
        );
        assert!(
            large_runs_after_delete
                .iter()
                .any(|fr| fr.id == fn_run_light.id),
            "Other functions should remain in index"
        );
    }

    // Helper function to create a test function run
    fn create_test_function_run(
        namespace: &str,
        application: &str,
        request_id: &str,
        function_name: &str,
        version: &str,
    ) -> FunctionRun {
        FunctionRunBuilder::default()
            .id(FunctionCallId(format!(
                "{}-{}-{}-{}",
                namespace, application, request_id, function_name
            )))
            .request_id(request_id.to_string())
            .namespace(namespace.to_string())
            .application(application.to_string())
            .name(function_name.to_string())
            .version(version.to_string())
            .compute_op(ComputeOp::FunctionCall(FunctionCall {
                inputs: vec![],
                function_call_id: FunctionCallId(format!(
                    "{}-{}-{}-{}",
                    namespace, application, request_id, function_name
                )),
                fn_name: function_name.to_string(),
                call_metadata: bytes::Bytes::new(),
                parent_function_call_id: None,
            }))
            .status(FunctionRunStatus::Pending)
            .outcome(None)
            .input_args(vec![])
            .attempt_number(0)
            .call_metadata(bytes::Bytes::new())
            .build()
            .unwrap()
    }
}
