//! Allocation input preparation.
//!
//! Prepares allocation inputs by presigning read URLs for input arguments
//! and creating multipart uploads for output blobs.

use anyhow::{Context, Result};
use proto_api::{
    executor_api_pb::Allocation as ServerAllocation,
    function_executor_pb::FunctionInputs,
};
use tracing::{debug, warn};

use super::events::PreparedAllocation;
use crate::blob_ops::{self, BlobStore, MultipartUploadHandle, REQUEST_ERROR_MAX_SIZE};

/// Prepare allocation inputs for execution on the FE.
///
/// Creates the FE-format FunctionInputs from the server allocation, including:
/// - Presigned read URLs for each input argument
/// - A writable blob for capturing request errors (multipart upload)
pub async fn prepare_allocation(
    allocation: &ServerAllocation,
    blob_store: &BlobStore,
) -> Result<PreparedAllocation> {
    debug!("Preparing allocation inputs");

    // Create request error blob multipart upload
    let (request_error_blob_handle, request_error_blob) =
        match prepare_request_error_blob(allocation, blob_store).await {
            Ok((handle, blob)) => (Some(handle), Some(blob)),
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to create request error blob, continuing without it"
                );
                (None, None)
            }
        };

    // Presign read URLs for each input argument
    let mut args = Vec::new();
    let mut arg_blobs = Vec::new();

    for (idx, data_payload) in allocation.args.iter().enumerate() {
        let so_inside_blob =
            super::proto_convert::data_payload_to_serialized_object_inside_blob(data_payload);
        args.push(so_inside_blob);

        // Presign read-only blob for this argument.
        // Use offset + size from the DataPayload to determine blob size.
        // This matches the Python executor behavior exactly.
        let blob_uri = data_payload.uri.as_deref().unwrap_or("");
        let offset = data_payload.offset.unwrap_or(0);
        let data_size = data_payload.size.unwrap_or(0);
        let blob_size = offset + data_size;

        let blob = blob_ops::presign_read_only_blob(
            &format!("arg-{}", idx),
            blob_uri,
            blob_size,
            blob_store,
        )
        .await
        .with_context(|| format!("Failed to presign read blob for arg {}", idx))?;

        arg_blobs.push(blob);
    }

    let inputs = FunctionInputs {
        args,
        arg_blobs,
        request_error_blob,
        function_call_metadata: allocation.function_call_metadata.clone(),
    };

    Ok(PreparedAllocation {
        inputs,
        request_error_blob_handle,
    })
}

/// Create a multipart upload for the request error blob.
async fn prepare_request_error_blob(
    allocation: &ServerAllocation,
    blob_store: &BlobStore,
) -> Result<(MultipartUploadHandle, proto_api::function_executor_pb::Blob)> {
    let request_id = allocation.request_id.as_deref().unwrap_or("unknown");
    let uri_prefix = allocation
        .request_error_payload_uri_prefix
        .as_deref()
        .unwrap_or("file:///tmp/indexify-errors");

    let blob_uri = format!("{}.{}.req_error", uri_prefix, request_id);

    let handle = blob_store
        .create_multipart_upload(&blob_uri)
        .await
        .context("Failed to create multipart upload for request error blob")?;

    let blob = match blob_ops::presign_write_only_blob(
        "request_error_blob",
        &blob_uri,
        &handle.upload_id,
        REQUEST_ERROR_MAX_SIZE,
        blob_store,
    )
    .await
    {
        Ok(blob) => blob,
        Err(e) => {
            // Abort the multipart upload on presign failure
            let _ = blob_store
                .abort_multipart_upload(&blob_uri, &handle.upload_id)
                .await;
            return Err(e.context("Failed to presign write blob for request error"));
        }
    };

    Ok((handle, blob))
}
