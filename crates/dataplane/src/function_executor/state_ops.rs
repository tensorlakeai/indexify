//! Request state operations for allocations.
//!
//! Handles prepare-read, prepare-write, and commit-write state operations
//! between the FE and the blob store.

use std::collections::{HashMap, HashSet};

use proto_api::function_executor_pb::{
    self,
    AllocationRequestStateCommitWriteOperationResult,
    AllocationRequestStateOperationResult,
    AllocationRequestStatePrepareReadOperationResult,
    AllocationRequestStatePrepareWriteOperationResult,
    AllocationState,
    allocation_request_state_operation,
};
use tracing::{debug, warn};

use super::{allocation_runner, fe_client::FunctionExecutorGrpcClient};
use crate::blob_ops::{self, BlobStore};

/// Info about a pending request state write operation.
struct StateWriteOpInfo {
    blob_uri: String,
    upload_id: String,
}

/// Manages request state operations (read/write/commit) for an allocation.
pub(super) struct RequestStateHandler {
    pending_read_ops: HashSet<String>,
    pending_write_ops: HashMap<String, StateWriteOpInfo>,
}

#[allow(clippy::too_many_arguments)]
impl RequestStateHandler {
    pub fn new() -> Self {
        Self {
            pending_read_ops: HashSet::new(),
            pending_write_ops: HashMap::new(),
        }
    }

    /// Reconcile request state operations from the FE allocation state.
    pub async fn reconcile(
        &mut self,
        state: &AllocationState,
        client: &mut FunctionExecutorGrpcClient,
        blob_store: &BlobStore,
        allocation_id: &str,
        uri_prefix: &str,
    ) {
        let mut fe_read_op_ids: HashSet<String> = HashSet::new();

        for op in &state.request_state_operations {
            let op_id = op.operation_id.as_deref().unwrap_or("");
            if op_id.is_empty() {
                continue;
            }

            let state_key = op.state_key.as_deref().unwrap_or("");

            match &op.operation {
                Some(allocation_request_state_operation::Operation::PrepareRead(_)) => {
                    fe_read_op_ids.insert(op_id.to_string());
                    if !self.pending_read_ops.contains(op_id) {
                        self.pending_read_ops.insert(op_id.to_string());
                        self.handle_prepare_read(
                            op_id,
                            state_key,
                            client,
                            blob_store,
                            allocation_id,
                            uri_prefix,
                        )
                        .await;
                    }
                }
                Some(allocation_request_state_operation::Operation::PrepareWrite(pw)) => {
                    if !self.pending_write_ops.contains_key(op_id) {
                        self.handle_prepare_write(
                            op_id,
                            state_key,
                            pw.size.unwrap_or(0),
                            client,
                            blob_store,
                            allocation_id,
                            uri_prefix,
                        )
                        .await;
                    }
                }
                Some(allocation_request_state_operation::Operation::CommitWrite(cw)) => {
                    self.handle_commit_write(op_id, cw, client, blob_store, allocation_id)
                        .await;
                }
                None => {}
            }
        }

        // Clean up read ops that FE removed
        self.pending_read_ops = fe_read_op_ids;
    }

    /// Handle a prepare_read state operation: look up blob, presign read URL,
    /// send result to FE.
    async fn handle_prepare_read(
        &mut self,
        op_id: &str,
        state_key: &str,
        client: &mut FunctionExecutorGrpcClient,
        blob_store: &BlobStore,
        allocation_id: &str,
        uri_prefix: &str,
    ) {
        let blob_uri = request_state_key_blob_uri(uri_prefix, state_key);

        debug!(
            allocation_id = %allocation_id,
            operation_id = %op_id,
            state_key = %state_key,
            "Handling prepare_read state operation"
        );

        let (status, blob) = match blob_store.get_metadata(&blob_uri).await {
            Ok(metadata) => {
                match blob_ops::presign_read_only_blob(
                    op_id,
                    &blob_uri,
                    metadata.size_bytes,
                    blob_store,
                )
                .await
                {
                    Ok(blob) => (allocation_runner::ok_status(), Some(blob)),
                    Err(e) => {
                        warn!(
                            allocation_id = %allocation_id,
                            operation_id = %op_id,
                            error = %e,
                            "Failed to presign read blob for state operation"
                        );
                        (allocation_runner::error_status(13, ""), None)
                    }
                }
            }
            Err(_) => (allocation_runner::error_status(5, ""), None), // NOT_FOUND
        };

        let update = make_read_result(allocation_id, op_id, status, blob);
        if let Err(e) = client.send_allocation_update(update).await {
            warn!(
                allocation_id = %allocation_id,
                operation_id = %op_id,
                error = %e,
                "Failed to send state read operation result"
            );
        }
    }

    /// Handle a prepare_write state operation: create multipart upload,
    /// presign write URLs, store in pending ops.
    async fn handle_prepare_write(
        &mut self,
        op_id: &str,
        state_key: &str,
        write_size: u64,
        client: &mut FunctionExecutorGrpcClient,
        blob_store: &BlobStore,
        allocation_id: &str,
        uri_prefix: &str,
    ) {
        let blob_uri = request_state_key_blob_uri(uri_prefix, state_key);

        debug!(
            allocation_id = %allocation_id,
            operation_id = %op_id,
            state_key = %state_key,
            size = write_size,
            "Handling prepare_write state operation"
        );

        let (status, blob) = match blob_store.create_multipart_upload(&blob_uri).await {
            Ok(handle) => {
                match blob_ops::presign_write_only_blob(
                    op_id,
                    &blob_uri,
                    &handle.upload_id,
                    write_size,
                    blob_store,
                )
                .await
                {
                    Ok(blob) => {
                        self.pending_write_ops.insert(
                            op_id.to_string(),
                            StateWriteOpInfo {
                                blob_uri: blob_uri.clone(),
                                upload_id: handle.upload_id.clone(),
                            },
                        );
                        (allocation_runner::ok_status(), Some(blob))
                    }
                    Err(e) => {
                        warn!(
                            allocation_id = %allocation_id,
                            operation_id = %op_id,
                            error = %e,
                            "Failed to presign write blob for state operation"
                        );
                        let _ = blob_store
                            .abort_multipart_upload(&blob_uri, &handle.upload_id)
                            .await;
                        (allocation_runner::error_status(13, ""), None)
                    }
                }
            }
            Err(e) => {
                warn!(
                    allocation_id = %allocation_id,
                    operation_id = %op_id,
                    error = %e,
                    "Failed to create multipart upload for state operation"
                );
                (allocation_runner::error_status(13, ""), None)
            }
        };

        let update = make_write_result(allocation_id, op_id, status, blob);
        if let Err(e) = client.send_allocation_update(update).await {
            warn!(
                allocation_id = %allocation_id,
                operation_id = %op_id,
                error = %e,
                "Failed to send state write operation result"
            );
        }
    }

    /// Handle a commit_write state operation: complete multipart upload using
    /// collected etags.
    async fn handle_commit_write(
        &mut self,
        op_id: &str,
        cw: &function_executor_pb::AllocationRequestStateCommitWriteOperation,
        client: &mut FunctionExecutorGrpcClient,
        blob_store: &BlobStore,
        allocation_id: &str,
    ) {
        debug!(
            allocation_id = %allocation_id,
            operation_id = %op_id,
            "Handling commit_write state operation"
        );

        let write_op_id = cw
            .blob
            .as_ref()
            .and_then(|b| b.id.as_deref())
            .map(|s| s.to_string());

        let status = if let Some(ref wop_id) = write_op_id {
            if let Some(write_info) = self.pending_write_ops.remove(wop_id.as_str()) {
                let etags: Vec<String> = cw
                    .blob
                    .as_ref()
                    .map(|b| b.chunks.iter().filter_map(|c| c.etag.clone()).collect())
                    .unwrap_or_default();

                match blob_store
                    .complete_multipart_upload(&write_info.blob_uri, &write_info.upload_id, &etags)
                    .await
                {
                    Ok(()) => allocation_runner::ok_status(),
                    Err(e) => {
                        warn!(
                            allocation_id = %allocation_id,
                            operation_id = %op_id,
                            error = %e,
                            "Failed to complete multipart upload for state operation"
                        );
                        allocation_runner::error_status(13, "")
                    }
                }
            } else {
                allocation_runner::error_status(5, "") // NOT_FOUND
            }
        } else {
            allocation_runner::error_status(5, "") // NOT_FOUND
        };

        let update = make_commit_result(allocation_id, op_id, status);
        if let Err(e) = client.send_allocation_update(update).await {
            warn!(
                allocation_id = %allocation_id,
                operation_id = %op_id,
                error = %e,
                "Failed to send state commit operation result"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Builder helpers — construct complete state operation result updates
// ---------------------------------------------------------------------------

/// Build the blob URI for a request state key.
fn request_state_key_blob_uri(uri_prefix: &str, state_key: &str) -> String {
    // URL-encode the state key to make it safe for use in URIs.
    let safe_key: String = state_key
        .bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                vec![b as char]
            } else {
                format!("%{:02X}", b).chars().collect()
            }
        })
        .collect();
    format!("{}/state/{}", uri_prefix, safe_key)
}

fn make_read_result(
    allocation_id: &str,
    op_id: &str,
    status: proto_api::google_rpc::Status,
    blob: Option<function_executor_pb::Blob>,
) -> function_executor_pb::AllocationUpdate {
    allocation_runner::make_allocation_update(
        allocation_id,
        function_executor_pb::allocation_update::Update::RequestStateOperationResult(
            AllocationRequestStateOperationResult {
                operation_id: Some(op_id.to_string()),
                status: Some(status),
                result: Some(
                    function_executor_pb::allocation_request_state_operation_result::Result::PrepareRead(
                        AllocationRequestStatePrepareReadOperationResult { blob },
                    ),
                ),
            },
        ),
    )
}

fn make_write_result(
    allocation_id: &str,
    op_id: &str,
    status: proto_api::google_rpc::Status,
    blob: Option<function_executor_pb::Blob>,
) -> function_executor_pb::AllocationUpdate {
    allocation_runner::make_allocation_update(
        allocation_id,
        function_executor_pb::allocation_update::Update::RequestStateOperationResult(
            AllocationRequestStateOperationResult {
                operation_id: Some(op_id.to_string()),
                status: Some(status),
                result: Some(
                    function_executor_pb::allocation_request_state_operation_result::Result::PrepareWrite(
                        AllocationRequestStatePrepareWriteOperationResult { blob },
                    ),
                ),
            },
        ),
    )
}

fn make_commit_result(
    allocation_id: &str,
    op_id: &str,
    status: proto_api::google_rpc::Status,
) -> function_executor_pb::AllocationUpdate {
    allocation_runner::make_allocation_update(
        allocation_id,
        function_executor_pb::allocation_update::Update::RequestStateOperationResult(
            AllocationRequestStateOperationResult {
                operation_id: Some(op_id.to_string()),
                status: Some(status),
                result: Some(
                    function_executor_pb::allocation_request_state_operation_result::Result::CommitWrite(
                        AllocationRequestStateCommitWriteOperationResult {},
                    ),
                ),
            },
        ),
    )
}
