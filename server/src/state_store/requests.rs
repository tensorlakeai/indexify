use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicU64},
};

use anyhow::Result;

use crate::{
    data_model::{
        Allocation,
        Application,
        ComputeOp,
        DataPayload,
        ExecutorId,
        ExecutorMetadata,
        FunctionCall,
        FunctionCallId,
        FunctionExecutorId,
        FunctionExecutorServerMetadata,
        FunctionRun,
        FunctionRunOutcome,
        GcUrl,
        HostResources,
        RequestCtx,
        StateChange,
    },
    state_store::{IndexifyState, state_changes},
};

#[derive(Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
}

impl StateMachineUpdateRequest {
    pub fn state_changes(
        &self,
        state_change_id_seq: &AtomicU64,
    ) -> anyhow::Result<Vec<StateChange>> {
        match &self.payload {
            RequestPayload::InvokeApplication(request) => {
                state_changes::invoke_application(state_change_id_seq, request)
            }
            RequestPayload::TombstoneApplication(request) => {
                state_changes::tombstone_application(state_change_id_seq, request)
            }
            RequestPayload::TombstoneRequest(request) => {
                state_changes::tombstone_request(state_change_id_seq, request)
            }
            RequestPayload::DeregisterExecutor(request) => {
                state_changes::tombstone_executor(state_change_id_seq, request)
            }
            RequestPayload::SchedulerUpdate((request, _)) => Ok(request.state_changes.clone()),
            RequestPayload::UpsertExecutor(request) => Ok(request.state_changes.clone()),
            _ => Ok(Vec::new()), // Handle other request types as needed
        }
    }

    pub fn notify_usage_events(&self) -> bool {
        match self.payload {
            RequestPayload::UpsertExecutor(ref req) => req
                .allocation_outputs
                .iter()
                .any(|o| matches!(o.allocation.outcome, FunctionRunOutcome::Success)),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, strum::Display)]
pub enum RequestPayload {
    InvokeApplication(InvokeApplicationRequest),
    CreateNameSpace(NamespaceRequest),
    CreateOrUpdateApplication(Box<CreateOrUpdateApplicationRequest>),
    TombstoneApplication(DeleteApplicationRequest),
    TombstoneRequest(DeleteRequestRequest),
    SchedulerUpdate((Box<SchedulerUpdateRequest>, Vec<StateChange>)),
    UpsertExecutor(UpsertExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    RemoveGcUrls(Vec<GcUrl>),
    DeleteApplicationRequest((DeleteApplicationRequest, Vec<StateChange>)),
    DeleteRequestRequest((DeleteRequestRequest, Vec<StateChange>)),
    ProcessStateChanges(Vec<StateChange>),
}

#[derive(Debug, Clone, Default)]
pub struct SchedulerUpdateRequest {
    pub new_allocations: Vec<Allocation>,
    pub updated_function_runs: HashMap<String, HashSet<FunctionCallId>>,
    pub updated_request_states: HashMap<String, RequestCtx>,
    pub remove_executors: Vec<ExecutorId>,
    pub new_function_executors: Vec<FunctionExecutorServerMetadata>,
    pub remove_function_executors: HashMap<ExecutorId, HashSet<FunctionExecutorId>>,
    pub updated_executor_resources: HashMap<ExecutorId, HostResources>,
    pub state_changes: Vec<StateChange>,
}

impl SchedulerUpdateRequest {
    /// Extends this SchedulerUpdateRequest with contents from another one
    pub fn extend(&mut self, other: SchedulerUpdateRequest) {
        self.new_allocations.extend(other.new_allocations);
        for (ctx_key, function_run_ids) in other.updated_function_runs {
            self.updated_function_runs
                .entry(ctx_key)
                .or_default()
                .extend(function_run_ids);
        }
        self.updated_request_states
            .extend(other.updated_request_states);
        self.state_changes.extend(other.state_changes);

        self.remove_executors.extend(other.remove_executors);
        self.new_function_executors
            .extend(other.new_function_executors);
        self.remove_function_executors
            .extend(other.remove_function_executors);
        self.updated_executor_resources
            .extend(other.updated_executor_resources);
    }

    pub fn add_function_run(&mut self, function_run: FunctionRun, request_ctx: &mut RequestCtx) {
        request_ctx
            .function_runs
            .insert(function_run.id.clone(), function_run.clone());
        self.updated_function_runs
            .entry(request_ctx.key())
            .or_default()
            .insert(function_run.id.clone());
        self.updated_request_states
            .insert(request_ctx.key(), request_ctx.clone());
    }

    pub fn add_request_state(&mut self, request_ctx: &RequestCtx) {
        self.updated_request_states
            .insert(request_ctx.key(), request_ctx.clone());
    }

    pub fn add_function_call(&mut self, function_call: FunctionCall, request_ctx: &mut RequestCtx) {
        request_ctx.function_calls.insert(
            function_call.function_call_id.clone(),
            function_call.clone(),
        );
        self.updated_request_states
            .insert(request_ctx.key(), request_ctx.clone());
    }
}

#[derive(Debug, Clone)]
pub struct RequestUpdates {
    pub request_updates: Vec<ComputeOp>,
    // The function call id which is the root of the call graph of the functions
    // calls
    pub output_function_call_id: FunctionCallId,
}

#[derive(Debug, Clone)]
pub struct AllocationOutput {
    pub request_id: String,
    pub allocation: Allocation,
    pub data_payload: Option<DataPayload>,
    pub executor_id: ExecutorId,
    pub request_exception: Option<DataPayload>,
    pub graph_updates: Option<RequestUpdates>,
}

#[derive(Debug, Clone)]
pub struct InvokeApplicationRequest {
    pub namespace: String,
    pub application_name: String,
    pub ctx: RequestCtx,
}

#[derive(Debug, Clone)]
pub struct NamespaceRequest {
    pub name: String,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateOrUpdateApplicationRequest {
    pub namespace: String,
    pub application: Application,
    pub upgrade_requests_to_current_version: bool,
}

#[derive(Debug, Clone)]
pub struct DeleteApplicationRequest {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DeleteRequestRequest {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
}

/// Request to upsert an executor, including its metadata and diagnostics.
/// **DO NOT** construct this directly, use `UpsertExecutorRequest::build`
/// instead.
#[derive(Debug, Clone)]
pub struct UpsertExecutorRequest {
    pub executor: ExecutorMetadata,
    pub allocation_outputs: Vec<AllocationOutput>,
    pub update_executor_state: bool,
    state_changes: Vec<StateChange>,
}

impl UpsertExecutorRequest {
    /// Builds a new UpsertExecutorRequest.
    /// This function will also generate the state changes
    /// needed to update the executor state in the indexify state.
    ///
    /// It will also check if the allocation outputs can be updated
    /// and generate the necessary state changes
    /// for the allocation outputs.
    pub fn build(
        executor: ExecutorMetadata,
        allocation_outputs: Vec<AllocationOutput>,
        update_executor_state: bool,
        indexify_state: Arc<IndexifyState>,
    ) -> Result<Self> {
        let state_change_id_seq = indexify_state.state_change_id_seq();
        let mut state_changes = Vec::new();

        if update_executor_state {
            let changes = state_changes::upsert_executor(&state_change_id_seq, &executor.id)?;
            state_changes = changes;
        }

        for allocation_output in &allocation_outputs {
            if indexify_state.can_allocation_output_be_updated(allocation_output)? {
                let changes =
                    state_changes::task_outputs_ingested(&state_change_id_seq, allocation_output)?;
                state_changes.extend(changes);
            }
        }

        Ok(Self {
            executor,
            allocation_outputs,
            state_changes,
            update_executor_state,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeregisterExecutorRequest {
    pub executor_id: ExecutorId,
}
