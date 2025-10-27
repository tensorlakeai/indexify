use tracing::error;

use crate::{
    data_model::{FunctionRunFailureReason, FunctionRunOutcome},
    executor_api::executor_api_pb,
    state_store::in_memory_state::FunctionCallOutcome,
};

impl From<String> for executor_api_pb::DataPayloadEncoding {
    fn from(value: String) -> Self {
        match value.as_str() {
            "application/json" => executor_api_pb::DataPayloadEncoding::Utf8Json,
            "application/python-pickle" => executor_api_pb::DataPayloadEncoding::BinaryPickle,
            "application/zip" => executor_api_pb::DataPayloadEncoding::BinaryZip,
            "text/plain" => executor_api_pb::DataPayloadEncoding::Utf8Text,
            // User supplied content type for tensorlake.File.
            _ => executor_api_pb::DataPayloadEncoding::Raw,
        }
    }
}

pub fn fn_call_outcome_to_pb(
    function_call_outcome: &FunctionCallOutcome,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> executor_api_pb::FunctionCallResult {
    let outcome_code = match function_call_outcome.outcome {
        FunctionRunOutcome::Success => executor_api_pb::AllocationOutcomeCode::Success,
        FunctionRunOutcome::Failure(_) => executor_api_pb::AllocationOutcomeCode::Failure,
        FunctionRunOutcome::Unknown => executor_api_pb::AllocationOutcomeCode::Unknown,
    };
    let failure_reason =
        function_call_outcome
            .failure_reason
            .map(|failure_reason| match failure_reason {
                FunctionRunFailureReason::Unknown => {
                    executor_api_pb::AllocationFailureReason::Unknown
                }
                FunctionRunFailureReason::InternalError => {
                    executor_api_pb::AllocationFailureReason::InternalError
                }
                FunctionRunFailureReason::FunctionError => {
                    executor_api_pb::AllocationFailureReason::FunctionError
                }
                FunctionRunFailureReason::FunctionTimeout => {
                    executor_api_pb::AllocationFailureReason::FunctionTimeout
                }
                FunctionRunFailureReason::RequestError => {
                    executor_api_pb::AllocationFailureReason::RequestError
                }
                FunctionRunFailureReason::FunctionRunCancelled => {
                    executor_api_pb::AllocationFailureReason::AllocationCancelled
                }
                FunctionRunFailureReason::FunctionExecutorTerminated => {
                    executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated
                }
                FunctionRunFailureReason::ConstraintUnsatisfiable => {
                    executor_api_pb::AllocationFailureReason::ConstraintUnsatisfiable
                }
                FunctionRunFailureReason::OutOfMemory => {
                    executor_api_pb::AllocationFailureReason::Oom
                }
                FunctionRunFailureReason::ExecutorRemoved => {
                    executor_api_pb::AllocationFailureReason::ExecutorRemoved
                }
            });

    let return_value = function_call_outcome
        .return_value
        .clone()
        .map(|return_value| {
            // TODO Eugene - Can you check this conversion is correct?
            let encoding: executor_api_pb::DataPayloadEncoding = return_value.encoding.into();
            executor_api_pb::DataPayload {
                uri: Some(blob_store_path_to_url(
                    &return_value.path,
                    blob_store_url_scheme,
                    blob_store_url,
                )),
                encoding: Some(encoding.into()),
                encoding_version: Some(0),
                content_type: None,
                metadata_size: Some(return_value.metadata_size),
                offset: Some(return_value.offset),
                size: Some(return_value.size),
                sha256_hash: Some(return_value.sha256_hash),
                source_function_call_id: None,
                id: Some(return_value.id),
            }
        });

    let request_error = function_call_outcome
        .request_error
        .clone()
        .map(|request_error| {
            let encoding: executor_api_pb::DataPayloadEncoding = request_error.encoding.into();
            executor_api_pb::DataPayload {
                uri: Some(blob_store_path_to_url(
                    &request_error.path,
                    blob_store_url_scheme,
                    blob_store_url,
                )),
                encoding: Some(encoding.into()),
                encoding_version: Some(0),
                content_type: None,
                metadata_size: Some(request_error.metadata_size),
                offset: Some(request_error.offset),
                size: Some(request_error.size),
                sha256_hash: Some(request_error.sha256_hash),
                source_function_call_id: None,
                id: Some(request_error.id),
            }
        });

    executor_api_pb::FunctionCallResult {
        namespace: Some(function_call_outcome.namespace.clone()),
        request_id: Some(function_call_outcome.request_id.clone()),
        function_call_id: Some(function_call_outcome.function_call_id.to_string()),
        outcome_code: Some(outcome_code.into()),
        failure_reason: failure_reason.map(|failure_reason| failure_reason.into()),
        return_value,
        request_error,
    }
}

pub fn blob_store_path_to_url(
    path: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        // Local file blob store implementation is always using absolute paths without
        // "/"" prefix. The paths are not relative to the configure blob_store_url path.
        format!("{blob_store_url_scheme}:///{path}")
    } else if blob_store_url_scheme == "s3" {
        // S3 blob store implementation uses paths relative to its bucket from
        // blob_store_url.
        format!(
            "{}://{}/{}",
            blob_store_url_scheme,
            bucket_name_from_s3_blob_store_url(blob_store_url),
            path
        )
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

pub fn blob_store_url_to_path(
    url: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        // Local file blob store implementation is always using absolute paths without
        // "/"" prefix. The paths are not relative to the configure blob_store_url path.
        url.strip_prefix(&format!("{blob_store_url_scheme}:///").to_string())
            // The url doesn't include blob_store_scheme if this payload was uploaded to server
            // instead of directly to blob storage.
            .unwrap_or(url)
            .to_string()
    } else if blob_store_url_scheme == "s3" {
        // S3 blob store implementation uses paths relative to its bucket from
        // blob_store_url.
        url.strip_prefix(
            &format!(
                "{}://{}/",
                blob_store_url_scheme,
                bucket_name_from_s3_blob_store_url(blob_store_url)
            )
            .to_string(),
        )
        // The url doesn't include blob_store_url if this payload was uploaded to server instead
        // of directly to blob storage.
        .unwrap_or(url)
        .to_string()
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

fn bucket_name_from_s3_blob_store_url(blob_store_url: &str) -> String {
    match url::Url::parse(blob_store_url) {
        Ok(url) => match url.host_str() {
            Some(bucket) => bucket.into(),
            None => {
                error!("Didn't find bucket name in S3 url: {}", blob_store_url);
                String::new()
            }
        },
        Err(e) => {
            error!(
                "Failed to parse blob_store_url: {}. Error: {}",
                blob_store_url, e
            );
            String::new()
        }
    }
}
