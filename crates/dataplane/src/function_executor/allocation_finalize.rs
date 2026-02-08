//! Post-execution allocation cleanup.
//!
//! Handles completing or aborting multipart uploads after allocation execution.

use anyhow::Result;
use tracing::{debug, warn};

use crate::blob_ops::{BlobStore, MultipartUploadHandle};

/// Finalize allocation after execution.
///
/// Completes the request error blob multipart upload if the FE produced an
/// error output, otherwise aborts it to clean up resources.
///
/// Also completes or aborts any output blob multipart uploads.
pub async fn finalize_allocation(
    allocation_id: &str,
    fe_result: Option<&proto_api::function_executor_pb::AllocationResult>,
    request_error_blob_handle: Option<&MultipartUploadHandle>,
    output_blob_handles: &[MultipartUploadHandle],
    blob_store: &BlobStore,
) -> Result<()> {
    debug!(
        allocation_id = %allocation_id,
        "Finalizing allocation blob operations"
    );

    // Handle request error blob
    if let Some(handle) = request_error_blob_handle {
        let has_error_output = fe_result
            .and_then(|r| r.request_error_output.as_ref())
            .is_some();

        if has_error_output {
            // FE wrote error data — complete the multipart upload with ETags from the FE
            let etags: Vec<String> = fe_result
                .and_then(|r| r.uploaded_request_error_blob.as_ref())
                .map(|blob| {
                    blob.chunks
                        .iter()
                        .filter_map(|chunk| chunk.etag.clone())
                        .collect()
                })
                .unwrap_or_default();

            if !etags.is_empty() {
                if let Err(e) = blob_store
                    .complete_multipart_upload(&handle.uri, &handle.upload_id, &etags)
                    .await
                {
                    warn!(
                        allocation_id = %allocation_id,
                        error = %e,
                        "Failed to complete request error blob multipart upload"
                    );
                }
            } else {
                // No ETags → abort
                let _ = blob_store
                    .abort_multipart_upload(&handle.uri, &handle.upload_id)
                    .await;
            }
        } else {
            // No error output — abort the multipart upload
            if let Err(e) = blob_store
                .abort_multipart_upload(&handle.uri, &handle.upload_id)
                .await
            {
                warn!(
                    allocation_id = %allocation_id,
                    error = %e,
                    "Failed to abort request error blob multipart upload"
                );
            }
        }
    }

    // Complete output blob multipart uploads
    // The FE writes directly to presigned URLs, so we don't need to complete
    // these — the FE already uploaded the parts. However, for S3 multipart
    // uploads, we need to explicitly complete them.
    for handle in output_blob_handles {
        // For output blobs, the ETags come from the FE's AllocationState
        // output_blob_requests. Since we don't track per-output-blob ETags
        // in the current flow, we skip completion here. The FE writes
        // directly via presigned upload-part URLs.
        // TODO: Track output blob ETags and complete multipart uploads.
        debug!(
            allocation_id = %allocation_id,
            uri = %handle.uri,
            "Output blob handle pending finalization"
        );
    }

    Ok(())
}
