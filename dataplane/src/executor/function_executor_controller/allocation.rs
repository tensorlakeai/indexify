use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;
use tracing::info;

use crate::executor::{
    blob_store::{BlobStore, BlobStoreImpl},
    executor_api::{
        executor_api_pb::{
            execution_plan_update, function_arg::Source, Allocation, AllocationFailureReason,
            AllocationOutcomeCode, DataPayload, ExecutionPlanUpdate, ExecutionPlanUpdates,
            FunctionArg, FunctionCall, FunctionExecutorTerminationReason, FunctionRef,
        },
        ChannelManager, ExecutorStateReporter,
    },
    function_executor::{
        function_executor_service::{
            allocation_result, allocation_update::Update,
            function_executor_client::FunctionExecutorClient, Allocation as FEAllocation,
            AllocationFailureReason as FEAllocationFailureReason, AllocationFunctionCall,
            AllocationOutcomeCode as FEAllocationOutcomeCode, AllocationOutputBlob,
            AllocationOutputBlobRequest, AllocationProgress, AllocationResult, AllocationUpdate,
            Blob, CreateAllocationRequest, DeleteAllocationRequest,
            ExecutionPlanUpdates as FEExecutionPlanUpdates, FunctionCall as FEFunctionCall,
            FunctionInputs, WatchAllocationStateRequest,
        },
        FunctionExecutor,
    },
    function_executor_controller::blob_utils::presign_write_only_blob,
};

const CREATE_ALLOCATION_TIMEOUT_SECS: u32 = 5;
const SEND_ALLOCATION_UPDATE_TIMEOUT_SECS: u32 = 5;
const DELETE_ALLOCATION_TIMEOUT_SECS: u32 = 5;

const SERVER_CALL_FUNCTION_RPC_TIMEOUT_SECS: u32 = 5;
const SERVER_CALL_FUNCTION_RPC_BACKOFF_SECS: u32 = 2;
const SERVER_CALL_FUNCTION_RPC_MAX_RETRIES: u32 = 3;

pub struct AllocationInput {
    pub function_inputs: FunctionInputs,
    pub request_error_blob_uri: String,
    pub request_error_blob_upload_uri: String,
}

impl AllocationInput {
    pub fn new(
        function_inputs: FunctionInputs,
        request_error_blob_uri: String,
        request_error_blob_upload_uri: String,
    ) -> Self {
        Self {
            function_inputs,
            request_error_blob_uri,
            request_error_blob_upload_uri,
        }
    }
}

pub struct AllocationOutput {
    pub outcome_code: AllocationOutcomeCode,
    pub failure_reason: Option<AllocationFailureReason>,
    pub fe_result: Option<AllocationResult>,
    pub function_outputs_blob_uri: Option<String>,
    pub execution_duration_ms: Option<u64>,
}

impl AllocationOutput {
    pub fn new(
        fe_result: Option<AllocationResult>,
        function_outputs_blob_uri: Option<String>,
        execution_duration_ms: Option<u64>,
    ) -> Self {
        let outcome_code = if let Some(result) = fe_result {
            Some(result.outcome_code())
        } else {
            None
        };
        let failure_reason = if let Some(result) = fe_result {
            Some(result.failure_reason())
        } else {
            None
        };
        Self {
            outcome_code,
            failure_reason,
            fe_result,
            function_outputs_blob_uri,
            execution_duration_ms,
        }
    }

    pub fn internal_error(&self, execution_duration_ms: Option<u64>) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::InternalError),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn function_timeout(&self, execution_duration_ms: Option<u64>) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::FunctionTimeout),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn function_error_with_healthy_function_executor(
        &self,
        execution_duration_ms: Option<u64>,
    ) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::FunctionError),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn function_executor_is_in_undefined_state_after_running_allocation(
        &self,
        execution_duration_ms: Option<u64>,
    ) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::FunctionError),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn allocation_cancelled(&self, execution_duration_ms: Option<u64>) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::AllocationCancelled),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn allocation_didn_run_because_function_executor_terminated(&self) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::FunctionExecutorTerminated),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms: None,
        }
    }

    pub fn allocation_ran_out_of_memory(
        &self,
        execution_duration_ms: Option<u64>,
    ) -> AllocationOutput {
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason: Some(AllocationFailureReason::Oom),
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms,
        }
    }

    pub fn allocation_didn_run_because_function_executor_startup_failed(
        &self,
        fe_termination_reason: FunctionExecutorTerminationReason,
    ) -> AllocationOutput {
        let failure_reason = match fe_termination_reason {
            FunctionExecutorTerminationReason::Unknown => Some(AllocationFailureReason::Unknown),
            FunctionExecutorTerminationReason::StartupFailedInternalError => {
                Some(AllocationFailureReason::InternalError)
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionError => {
                Some(AllocationFailureReason::FunctionError)
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionTimeout => {
                Some(AllocationFailureReason::FunctionTimeout)
            }
            FunctionExecutorTerminationReason::Unhealthy => {
                Some(AllocationFailureReason::Unhealthy)
            }
            FunctionExecutorTerminationReason::FunctionCancelled => {
                Some(AllocationFailureReason::FunctionCancelled)
            }
            FunctionExecutorTerminationReason::InternalError => {
                Some(AllocationFailureReason::InternalError)
            }
            FunctionExecutorTerminationReason::FunctionTimeout => {
                Some(AllocationFailureReason::FunctionTimeout)
            }
            FunctionExecutorTerminationReason::Oom => Some(AllocationFailureReason::Oom),
        };
        Self {
            outcome_code: Some(AllocationOutcomeCode::Failure),
            failure_reason,
            fe_result: None,
            function_outputs_blob_uri: None,
            execution_duration_ms: None,
        }
    }
}

fn to_server_alloc_outcome(code: FEAllocationOutcomeCode) -> AllocationOutcomeCode {
    match code {
        FEAllocationOutcomeCode::Unknown => AllocationOutcomeCode::Unknown,
        FEAllocationOutcomeCode::Success => AllocationOutcomeCode::Success,
        FEAllocationOutcomeCode::Failure => AllocationOutcomeCode::Failure,
    }
}

fn to_server_alloc_failure_reason(
    code: FEAllocationFailureReason,
) -> Option<AllocationFailureReason> {
    match code {
        FEAllocationFailureReason::Unknown => Some(AllocationFailureReason::Unknown),
        FEAllocationFailureReason::InternalError => Some(AllocationFailureReason::InternalError),
        FEAllocationFailureReason::FunctionError => Some(AllocationFailureReason::FunctionError),
        FEAllocationFailureReason::RequestError => Some(AllocationFailureReason::RequestError),
    }
}

pub struct AllocationInfo {
    pub allocation: Allocation,
    pub allocation_timeout_ms: u64,
    pub start_time: f64,
    pub prepared_time: f64,
    pub is_cancelled: bool,
    pub input: Option<AllocationInput>,
    pub output: Option<AllocationOutput>,
    pub is_completed: bool,
}

impl AllocationInfo {
    pub fn new(allocation: Allocation, allocation_timeout_ms: u64, start_time: f64) -> Self {
        Self {
            allocation,
            allocation_timeout_ms,
            start_time,
            prepared_time: 0.0,
            is_cancelled: false,
            input: None,
            output: None,
            is_completed: false,
        }
    }
}

struct BlobInfo {
    id: String,
    uri: String,
    upload_id: String,
}

struct RunAllocationOnFunctionExecutorResult {
    fe_result: AllocationResult,
    execution_end_time: f64,
    function_outputs_blob_uri: Option<String>,
}

pub struct AllocationRunner {
    alloc_info: AllocationInfo,
    function_executor: FunctionExecutor,
    blob_store: BlobStore,
    state_reporter: ExecutorStateReporter,
    // state_reconciler: ExecutorStateReconciler,
    channel_manager: ChannelManager,
    pending_output_blobs: HashMap<String, BlobInfo>,
    // append-only
    // TODO: use a bitvec
    started_function_call_ids: Vec<String>,
    // pending_function_call_watchers: HashMap<String, FunctionCallWatcher>,
    pending_request_state_read_ops: Vec<String>,
    // pending_request_state_write_ops: HashMap<String, _FunctionCallWatcherInfo>,
}

impl AllocationRunner {
    pub fn new(
        alloc_info: AllocationInfo,
        function_executor: FunctionExecutor,
        blob_store: BlobStore,
        state_reporter: ExecutorStateReporter,
        // state_reconciler: ExecutorStateReconciler,
        channel_manager: ChannelManager,
    ) -> Self {
        Self {
            alloc_info,
            function_executor,
            blob_store,
            state_reporter,
            // state_reconciler,
            channel_manager,
            pending_output_blobs: HashMap::new(),
            // append-only
            // TODO: use a bitvec
            started_function_call_ids: Vec::new(),
            // pending_function_call_watchers: HashMap::new(),
            pending_request_state_read_ops: Vec::new(),
            // pending_request_state_write_ops: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<AllocationInfo, String> {
        Ok(self.alloc_info.clone())
    }

    async fn run_alloc_on_fe(
        &mut self,
    ) -> Result<RunAllocationOnFunctionExecutorResult, Box<dyn std::error::Error>> {
        let no_progress_timeout = Duration::from_millis(self.alloc_info.allocation_timeout_ms);

        let channel = self
            .function_executor
            .channel()
            .ok_or("Channel is None")?
            .clone();

        let fe_alloc = FEAllocation {
            request_id: self.alloc_info.allocation.request_id.clone(),
            function_call_id: self.alloc_info.allocation.function_call_id.clone(),
            allocation_id: self.alloc_info.allocation.allocation_id.clone(),
            inputs: self.alloc_info.input.function_inputs.clone(),
            result: None,
        };

        let mut client = FunctionExecutorClient::new(channel);

        let create_request = CreateAllocationRequest {
            allocation: Some(fe_alloc),
        };

        tokio::time::timeout(
            Duration::from_secs(CREATE_ALLOCATION_TIMEOUT_SECS),
            client.create_allocation(create_request),
        )
        .await?;

        let watch_request = WatchAllocationStateRequest {
            allocation_id: self.alloc_info.allocation.allocation_id.clone(),
        };

        let mut stream = client
            .watch_allocation_state(watch_request)
            .await?
            .into_inner();

        let mut previous_progress: Option<AllocationProgress> = None;
        let mut allocation_result: Option<AllocationResult> = None;
        let mut deadline = Instant::now() + no_progress_timeout;

        loop {
            let timeout_duration = deadline.saturating_duration_since(Instant::now());

            let response = match tokio::time::timeout(timeout_duration, stream.message()).await {
                Ok(Ok(Some(state))) => state,
                Ok(Ok(None)) => break, // EOF
                Ok(Err(e)) => {
                    // return Err(AllocationError::FailedLeavingFEInUndefinedState(e).into());
                }
                Err(_) => {
                    // return Err(AllocationError::Timeout.into());
                }
            };

            if let Some(progress) = &response.progress {
                if previous_progress.as_ref() != Some(progress) {
                    deadline = Instant::now() + no_progress_timeout;
                }
                previous_progress = Some(progress.clone());
            }

            let mut fe_output_blob_requests: HashMap<String, AllocationOutputBlobRequest> =
                HashMap::new();
            for output_blob_request in response.output_blob_requests {
                // TODO: validate fe output blob req
                fe_output_blob_requests
                    .insert(output_blob_request.id().to_string(), output_blob_request);
                if !self
                    .pending_output_blobs
                    .contains_key(output_blob_request.id().to_string())
                {
                    let output_blob = self.create_output_blob(output_blob_request).await?;
                    let update = AllocationUpdate {
                        allocation_id: Some(self.alloc_info.allocation.allocation_id().to_string()),
                        update: Some(Update::OutputBlob(AllocationOutputBlob {
                            status: None,
                            blob: Some(output_blob),
                        })),
                    };
                    tokio::time::timeout(
                        Duration::from_secs(SEND_ALLOCATION_UPDATE_TIMEOUT_SECS),
                        client.send_allocation_update(update),
                    )
                    .await?;
                }
            }
            let mut fe_function_calls: HashMap<String, AllocationFunctionCall> = HashMap::new();
            for function_call in response.function_calls {
                if let Some(updates) = function_call.updates {
                    if let Some(root_function_call_id) = updates.root_function_call_id {
                        fe_function_calls.insert(root_function_call_id, function_call)
                    }
                }
                if let Some(args_blob) = function_call.args_blob {
                    let blob_info = self.pending_output_blobs.get(args_blob.id());

                    if let Some(blob_info) = blob_info {
                        let mut parts_etag: Vec<String> = Vec::new();
                        for blob_chunk in args_blob.chunks {
                            parts_etag.push(blob_chunk.etag().to_string())
                        }
                        self.blob_store.complete_multipart_upload(
                            &blob_info.uri,
                            &blob_info.upload_id,
                            parts_etag,
                        );
                    }
                }
            }
            // TODO: self.reconcile_function_call_watchers(&mut client, &response.function_call_watchers)
            // .await?;

            if response.request_state_operations.is_some() {
                // TODO: self.reconcile_request_state_operations(
                //     &mut client,
                //     &response.request_state_operations,
                // )
                // .await?;
            }

            // if let Some(result) = response.result {
            //     allocation_result = Some(result);
            //     break;
            // }
        }

        let delete_request = DeleteAllocationRequest {
            allocation_id: self.alloc_info.allocation.allocation_id.clone(),
        };

        tokio::time::timeout(
            Duration::from_secs(DELETE_ALLOCATION_TIMEOUT_SECS),
            client.delete_allocation(delete_request),
        )
        .await?;

        let execution_end_time = Instant::now();

        let mut function_outputs_blob_uri: Option<String> = None;

        if let Some(result) = &allocation_result {
            if let Some(uploaded_blob) = &result.uploaded_function_outputs_blob {
                let blob_info = self
                    .pending_output_blobs
                    .get(&uploaded_blob.id)
                    .ok_or_else(|| {
                        info!("failing allocation because its outputs blob is not found");
                    })?;

                function_outputs_blob_uri = Some(blob_info.uri.clone());

                let mut parts_etag: Vec<String> = Vec::new();
                for blob_chunk in uploaded_blob.chunks {
                    parts_etag.push(blob_chunk.etag().to_string())
                }
                self.blob_store
                    .complete_multipart_upload(&blob_info.uri, &blob_info.upload_id, parts_etag)
                    .await?;
                self.pending_output_blobs.remove(uploaded_blob.id());

                if let Some(outputs) = &result.outputs {
                    if let Some(allocation_result::Outputs::Updates(updates)) = outputs {
                        to_server_execution_plan_updates(
                            updates,
                            function_outputs_blob_uri.as_ref(),
                        )
                        .map_err(|e| {
                            info!(
                            "failing allocation because its FE execution plan updates are invalid",
                        );
                        })?;
                    }
                }
            }
        }

        Ok(RunAllocationOnFunctionExecutorResult {
            fe_result: allocation_result.ok_or("No allocation result")?,
            execution_end_time,
            function_outputs_blob_uri,
        })
    }

    async fn create_output_blob(
        &self,
        fe_output_blob_request: AllocationOutputBlobRequest,
    ) -> Result<Blob, Box<dyn std::error::Error>> {
        let blob_uri = format!(
            "{}.{}.{}.output",
            self.alloc_info.allocation.request_data_payload_uri_prefix(),
            self.alloc_info.allocation.allocation_id(),
            fe_output_blob_request.id()
        );
        let function_outputs_blob_upload_id =
            self.blob_store.create_multipart_upload(&blob_uri).await?;
        let blob_info = BlobInfo {
            id: fe_output_blob_request.id().to_string(),
            uri: blob_uri,
            upload_id: function_outputs_blob_upload_id,
        };

        self.pending_output_blobs
            .insert(fe_output_blob_request.id().to_string(), blob_info);

        let blob = presign_write_only_blob(
            &blob_info.id,
            &blob_info.uri,
            &blob_info.upload_id,
            fe_output_blob_request.size(),
            self.blob_store,
        )
        .await?;
        Ok(blob)
    }
}

fn to_server_execution_plan_updates(
    fe_execution_plan_updates: FEExecutionPlanUpdates,
    args_blob_uri: Option<&str>,
) -> ExecutionPlanUpdates {
    let mut server_execution_plan_updates: Vec<ExecutionPlanUpdates> = Vec::new();

    for fe_update in fe_execution_plan_updates.updates {
        server_execution_plan_updates.push(ExecutionPlanUpdate {
            op: execution_plan_update::Op::FunctionCall(to_server_function_call(
                fe_update.op,
                args_blob_uri,
            )),
        });
    }
}

fn to_server_function_call(
    fe_function_call: FEFunctionCall,
    args_blob_uri: Option<&str>,
) -> FunctionCall {
    let mut server_args: Vec<FunctionArg> = Vec::new();
    for fe_arg in fe_function_call.args {
        if let Some(allocation_result::Outputs::Value(value)) = fe_arg {
            if let Some(manifest) = value.manifest {
                let arg = FunctionArg {
                    source: Some(Source::InlineData(DataPayload {
                        id: None,
                        uri: Some(args_blob_uri),
                        encoding: manifest.encoding,
                        encoding_version: manifest.encoding_version,
                        content_type: manifest.content_type,
                        metadata_size: manifest.metadata_size,
                        offset: value.offset,
                        size: manifest.size,
                        sha256_hash: manifest.sha256_hash,
                        source_function_call_id: manifest.source_function_call_id,
                    })),
                };
                server_args.push(value);
            }
        }
    }
    let target = if let Some(val) = fe_function_call.target {
        FunctionRef {
            namespace: val.namespace,
            application_name: val.application_name,
            application_version: val.application_version,
            function_name: val.function_name,
        }
    } else {
        None
    };
    FunctionCall {
        id: fe_function_call.id,
        target,
        args: server_args,
        call_metadata: fe_function_call.call_metadata,
    }
}
