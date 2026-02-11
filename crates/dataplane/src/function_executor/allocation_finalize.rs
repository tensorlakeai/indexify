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

    // Handle output blob multipart uploads.
    // If no fe_result (cancelled/failed before execution produced a result),
    // abort all output blob uploads to clean up resources.
    //
    // With an fe_result, complete the multipart uploads so the data is visible
    // in S3 before the result is delivered. S3 multipart uploads are invisible
    // until completed.
    complete_output_blobs(allocation_id, fe_result, output_blob_handles, blob_store).await;

    Ok(())
}

/// Complete or abort output blob multipart uploads.
///
/// When there is an `fe_result`, extracts ETags from the uploaded blobs and
/// completes their multipart uploads. Blobs without a matching fe_result
/// entry (e.g. function call args blobs that were already completed eagerly
/// during execution) are left as-is.
///
/// When there is no `fe_result`, aborts all uploads to clean up resources.
async fn complete_output_blobs(
    allocation_id: &str,
    fe_result: Option<&proto_api::function_executor_pb::AllocationResult>,
    output_blob_handles: &[MultipartUploadHandle],
    blob_store: &BlobStore,
) {
    let Some(fe_result) = fe_result else {
        // No result — abort all output blob uploads
        for handle in output_blob_handles {
            debug!(
                allocation_id = %allocation_id,
                uri = %handle.uri,
                "Aborting output blob (no FE result)"
            );
            let _ = blob_store
                .abort_multipart_upload(&handle.uri, &handle.upload_id)
                .await;
        }
        return;
    };

    // Complete the function outputs blob multipart upload.
    complete_blob_from_fe(
        allocation_id,
        fe_result.uploaded_function_outputs_blob.as_ref(),
        output_blob_handles,
        blob_store,
        "function outputs",
    )
    .await;
}

/// Find the output blob handle matching an FE blob (by ID suffix) and complete
/// its multipart upload using the ETags from the blob's chunks.
async fn complete_blob_from_fe(
    allocation_id: &str,
    fe_blob: Option<&proto_api::function_executor_pb::Blob>,
    output_blob_handles: &[MultipartUploadHandle],
    blob_store: &BlobStore,
    label: &str,
) {
    let Some(blob) = fe_blob else {
        return;
    };

    let Some(blob_id) = blob.id.as_deref() else {
        return;
    };

    let suffix = format!(".output_{}", blob_id);
    let Some(handle) = output_blob_handles
        .iter()
        .find(|h| h.uri.ends_with(&suffix))
    else {
        warn!(
            allocation_id = %allocation_id,
            blob_id = %blob_id,
            label = %label,
            "No matching output blob handle found for completion"
        );
        return;
    };

    let etags: Vec<String> = blob.chunks.iter().filter_map(|c| c.etag.clone()).collect();

    if etags.is_empty() {
        debug!(
            allocation_id = %allocation_id,
            blob_id = %blob_id,
            label = %label,
            "No ETags in blob chunks, skipping completion"
        );
        return;
    }

    if let Err(e) = blob_store
        .complete_multipart_upload(&handle.uri, &handle.upload_id, &etags)
        .await
    {
        warn!(
            allocation_id = %allocation_id,
            blob_id = %blob_id,
            label = %label,
            error = %e,
            "Failed to complete output blob multipart upload"
        );
    } else {
        debug!(
            allocation_id = %allocation_id,
            blob_id = %blob_id,
            label = %label,
            "Completed output blob multipart upload"
        );
    }
}
