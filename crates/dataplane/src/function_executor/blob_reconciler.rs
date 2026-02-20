//! Output blob reconciliation for allocation execution.

use std::collections::HashSet;

use proto_api::function_executor_pb::{self, AllocationOutputBlob, AllocationState};
use tracing::{debug, warn};

use super::{
    allocation_runner::{error_status, make_allocation_update, ok_status},
    fe_client::FunctionExecutorGrpcClient,
};
use crate::blob_ops::{self, BlobStore, MultipartUploadHandle};

/// Handle new output blob requests from the FE.
pub(super) async fn reconcile_output_blobs(
    client: &mut FunctionExecutorGrpcClient,
    allocation_id: &str,
    uri_prefix: &str,
    blob_store: &BlobStore,
    seen_blob_ids: &mut HashSet<String>,
    output_blob_handles: &mut Vec<MultipartUploadHandle>,
    state: &AllocationState,
) {
    for blob_req in &state.output_blob_requests {
        let blob_id = blob_req.id.as_deref().unwrap_or("");
        if !blob_id.is_empty() && seen_blob_ids.insert(blob_id.to_string()) {
            debug!(
                blob_id = %blob_id,
                size = ?blob_req.size,
                "New output blob request"
            );

            let blob_size = blob_req.size.unwrap_or(0);

            // Create output blob with presigned write URLs
            match blob_ops::create_output_blob(
                allocation_id,
                blob_id,
                uri_prefix,
                blob_size,
                blob_store,
            )
            .await
            {
                Ok((handle, blob)) => {
                    output_blob_handles.push(handle);

                    let update = make_allocation_update(
                        allocation_id,
                        function_executor_pb::allocation_update::Update::OutputBlob(
                            AllocationOutputBlob {
                                status: Some(ok_status()),
                                blob: Some(blob),
                            },
                        ),
                    );
                    if let Err(e) = client.send_allocation_update(update).await {
                        warn!(
                            blob_id = %blob_id,
                            error = ?e,
                            "Failed to send output blob update"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        blob_id = %blob_id,
                        error = ?e,
                        "Failed to create output blob"
                    );
                    // Send error status back to FE
                    let update = make_allocation_update(
                        allocation_id,
                        function_executor_pb::allocation_update::Update::OutputBlob(
                            AllocationOutputBlob {
                                status: Some(error_status(13, e.to_string())),
                                blob: None,
                            },
                        ),
                    );
                    let _ = client.send_allocation_update(update).await;
                }
            }
        }
    }
}

/// Create a presigned read-only BLOB for a server DataPayload.
///
/// Used when delivering function call results from the allocation stream to the
/// FE — the server-side DataPayload blob URIs must be presigned so the FE can
/// read them.
pub(super) async fn presign_read_only_blob_for_data_payload(
    data_payload: &proto_api::executor_api_pb::DataPayload,
    blob_store: &BlobStore,
) -> anyhow::Result<function_executor_pb::Blob> {
    let blob_uri = data_payload.uri.as_deref().unwrap_or("");
    let offset = data_payload.offset.unwrap_or(0);
    let size = data_payload.size.unwrap_or(0);
    let total_size = offset + size;

    blob_ops::presign_read_only_blob("fc-result", blob_uri, total_size, blob_store).await
}
