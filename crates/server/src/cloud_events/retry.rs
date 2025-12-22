use std::{
    hash::{DefaultHasher, Hasher},
    time::Duration,
};

use chrono::Utc;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest,
    logs_service_client::LogsServiceClient,
};
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic_types::StatusExt;
use tracing::{error, warn};

/// Classification of errors for retry purposes.
#[derive(Debug, Clone, PartialEq)]
pub enum RetryErrorType {
    /// Error is not retryable (e.g., authentication failure, bad request).
    NonRetryable,
    /// Error is retryable with exponential backoff (e.g., server error, network
    /// timeout).
    Retryable,
    /// Error indicates throttling - wait for the specified duration before
    /// retrying. This overrides exponential backoff timing.
    Throttled(Duration),
}

/// Configuration for retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_retries: usize,
    /// Initial delay in milliseconds before the first retry.
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds between retries.
    pub max_delay_ms: u64,
    /// Maximum jitter in milliseconds to add to the delay.
    pub jitter_ms: u64,
}

/// Classifies a tonic::Status error
pub fn classify_tonic_status(status: &tonic::Status) -> RetryErrorType {
    // Use tonic-types to extract RetryInfo - this is the proper way!
    let retry_info_seconds = status
        .get_details_retry_info()
        .and_then(|retry_info| retry_info.retry_delay)
        .map(|duration| duration.as_secs());

    classify_grpc_error(status.code(), retry_info_seconds)
}

/// Classifies gRPC errors based on status code and metadata.
///
/// Implements the OpenTelemetry OTLP specification for error handling:
/// https://opentelemetry.io/docs/specs/otlp/
/// https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures
///
/// # Arguments
/// * `grpc_code` - gRPC status code as tonic::Code enum
/// * `retry_info_seconds` - Parsed retry delay from RetryInfo metadata, if
///   present
fn classify_grpc_error(grpc_code: tonic::Code, retry_info_seconds: Option<u64>) -> RetryErrorType {
    match grpc_code {
        // RESOURCE_EXHAUSTED: Special case per OTLP spec
        // Retryable only if server provides RetryInfo indicating recovery is possible
        tonic::Code::ResourceExhausted => {
            if let Some(seconds) = retry_info_seconds {
                // Server signals recovery is possible - use throttled retry
                let capped_seconds = seconds.min(600); // Cap at 10 minutes. TODO - what's sensible here?
                return RetryErrorType::Throttled(std::time::Duration::from_secs(capped_seconds));
            }
            // No RetryInfo - treat as non-retryable per OTLP spec
            RetryErrorType::NonRetryable
        }

        // Retryable errors per OTLP specification
        tonic::Code::Cancelled |
        tonic::Code::DeadlineExceeded |
        tonic::Code::Aborted |
        tonic::Code::OutOfRange |
        tonic::Code::Unavailable |
        tonic::Code::DataLoss => RetryErrorType::Retryable,

        // Non-retryable errors per OTLP specification
        tonic::Code::Unknown |
        tonic::Code::InvalidArgument |
        tonic::Code::NotFound |
        tonic::Code::AlreadyExists |
        tonic::Code::PermissionDenied |
        tonic::Code::FailedPrecondition |
        tonic::Code::Unimplemented |
        tonic::Code::Internal |
        tonic::Code::Unauthenticated => RetryErrorType::NonRetryable,

        // OK should never reach here in error scenarios, but handle gracefully
        tonic::Code::Ok => RetryErrorType::NonRetryable,
    }
}

pub async fn export_with_retry(
    client: &mut LogsServiceClient<Channel>,
    policy: &RetryPolicy,
    request: &ExportLogsServiceRequest,
) -> Result<(), anyhow::Error> {
    let mut attempt = 0;
    let mut delay = policy.initial_delay_ms;

    loop {
        match client.export(request.clone()).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let error_type = classify_tonic_status(&err);

                match error_type {
                    RetryErrorType::NonRetryable => {
                        error!(?err, "CloudEvent push failed with non-retryable error");
                        return Err(err.into());
                    }
                    RetryErrorType::Retryable if attempt < policy.max_retries => {
                        attempt += 1;
                        // Use exponential backoff with jitter
                        warn!(?err, "Retrying CloudEvent push due to retryable error");
                        let jitter = generate_jitter(policy.jitter_ms);
                        let delay_with_jitter = std::cmp::min(delay + jitter, policy.max_delay_ms);
                        sleep(Duration::from_millis(delay_with_jitter)).await;
                        delay = std::cmp::min(delay * 2, policy.max_delay_ms); // Exponential backoff
                    }
                    RetryErrorType::Throttled(server_delay) if attempt < policy.max_retries => {
                        attempt += 1;
                        // Use server-specified delay (overrides exponential backoff)
                        warn!(
                            ?err,
                            ?server_delay,
                            "Retrying CloudEvent push after server-specified throttling delay"
                        );
                        sleep(server_delay).await;
                    }
                    _ => {
                        // Max retries reached
                        error!(
                            ?err,
                            attempt, "CloudEvent push failed after using all attempts"
                        );
                        return Err(err.into());
                    }
                }
            }
        }
    }
}

fn generate_jitter(max_jitter: u64) -> u64 {
    let nanos = Utc::now().timestamp_subsec_nanos();

    let mut hasher = DefaultHasher::default();
    hasher.write_u32(nanos);
    hasher.finish() % (max_jitter + 1)
}
