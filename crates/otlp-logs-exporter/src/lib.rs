use opentelemetry_proto::{
    tonic::collector::logs::v1::{
        ExportLogsServiceRequest,
        logs_service_client::LogsServiceClient,
    },
    transform::{
        common::tonic::ResourceAttributesWithSchema,
        logs::tonic::group_logs_by_resource_and_scope,
    },
};

pub mod error;
use error::Error;
pub use opentelemetry_proto;

pub mod retry;
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    logs::{LogBatch, LogExporter},
};
use retry::RetryPolicy;
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, transport::Channel};

use crate::retry::export_with_retry;

/// OtlpLogsExporter is a log exporter for OpenTelemetry that uses Tonic to send
/// logs to a collector.
///
/// It allows you to specify a retry policy for retrying failed requests based
/// on whether the Otel errors are retryable or not.
#[derive(Debug)]
pub struct OtlpLogsExporter {
    client: Mutex<LogsServiceClient<Channel>>,
    retry_policy: RetryPolicy,
    resource: ResourceAttributesWithSchema,
}

impl OtlpLogsExporter {
    pub async fn with_default_retry(endpoint: &str) -> Result<Self, Error> {
        let retry_policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        Self::new(endpoint, retry_policy).await
    }

    pub fn with_channel(channel: Channel, retry_policy: RetryPolicy) -> Self {
        let client = LogsServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);

        Self {
            retry_policy,
            client: Mutex::new(client),
            resource: Default::default(),
        }
    }

    pub async fn new(endpoint: &str, retry_policy: RetryPolicy) -> Result<Self, Error> {
        let channel_builder = Channel::from_shared(endpoint.to_string())?;
        let channel = channel_builder.connect().await?;
        Ok(Self::with_channel(channel, retry_policy))
    }

    /// Export a single logs request.
    ///
    /// This function will retry if the request fails based on the exporter's
    /// retry policy.
    pub async fn send_request(&mut self, request: ExportLogsServiceRequest) -> Result<(), Error> {
        let mut client = self.client.lock().await;
        export_with_retry(&mut client, &self.retry_policy, &request).await
    }
}

impl LogExporter for OtlpLogsExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let resource_logs = group_logs_by_resource_and_scope(batch, &self.resource);
        let request = ExportLogsServiceRequest { resource_logs };

        let mut client = self.client.lock().await;

        match export_with_retry(&mut client, &self.retry_policy, &request).await {
            Ok(_) => Ok(()),
            Err(error) => Err(OTelSdkError::InternalFailure(format!(
                "export error: {error:?}"
            ))),
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}
