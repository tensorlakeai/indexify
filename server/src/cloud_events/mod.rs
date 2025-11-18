use anyhow::Context;
use chrono::Utc;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::error;
use uuid::Uuid;

use crate::{
    config::CloudEventsConfig,
    data_model::{RequestCtx, RequestOutcome},
    state_store::requests::SchedulerUpdateRequest,
};

mod retry;
use retry::*;

#[derive(Clone)]
pub struct CloudEventsManager {
    cancellation_token: CancellationToken,
    tx: Sender<RequestCtx>,
}

impl Drop for CloudEventsManager {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl CloudEventsManager {
    pub async fn new(config: &CloudEventsConfig) -> Result<Self, anyhow::Error> {
        let channel_builder = Channel::from_shared(config.writer_endpoint.clone())
            .context("building OTLP channel builder")?;

        let channel = channel_builder
            .connect()
            .await
            .context("building OTLP channel")?;

        let cancellation_token = CancellationToken::new();
        let tx = Self::start_collector(cancellation_token.clone(), channel.clone()).await;

        Ok(CloudEventsManager {
            cancellation_token,
            tx,
        })
    }

    pub async fn process_completed_requests(&self, update: &SchedulerUpdateRequest) {
        for request_ctx in update.completed_requests() {
            self.send_request_completed(request_ctx).await;
        }
    }

    async fn send_request_completed(&self, request_ctx: &RequestCtx) {
        if let Err(error) = self.tx.send(request_ctx.clone()).await {
            error!(
                ?error,
                "Failed to send request completed event, the Request completed event channel is closed."
            );
        }
    }

    async fn start_collector(cancel: CancellationToken, channel: Channel) -> Sender<RequestCtx> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RequestCtx>(100);
        let mut exporter = CloudEventsExporter::new(channel.clone());

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    ev = rx.recv() => match ev {
                        Some(request_ctx) => {
                            let res = exporter.emit_request_completed(request_ctx).await;
                            if let Err(err) = res {
                                error!(?err, "Failed to emit request completed event");
                            }
                        }
                        None => { break }
                    }
                }
            }
        });

        tx
    }
}

pub struct CloudEventsExporter {
    client: LogsServiceClient<Channel>,
    retry_policy: RetryPolicy,
}

impl CloudEventsExporter {
    fn new(channel: Channel) -> Self {
        let retry_policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let client = LogsServiceClient::new(channel)
            .send_compressed(tonic::codec::CompressionEncoding::Zstd);

        Self {
            client,
            retry_policy,
        }
    }

    pub async fn emit_request_completed(&mut self, ctx: RequestCtx) -> Result<(), anyhow::Error> {
        let attributes = vec![
            key_value_string("ai.tensorlake.namespace", &ctx.namespace),
            key_value_string("ai.tensorlake.application.name", &ctx.application_name),
            key_value_string(
                "ai.tensorlake.application.version",
                &ctx.application_version,
            ),
        ];
        let resource = Resource {
            attributes,
            ..Default::default()
        };

        let (outcome, message) = match &ctx.outcome {
            Some(RequestOutcome::Success) => ("success", "The request completed successfully"),
            Some(RequestOutcome::Failure(_)) => ("failure", "The request failed to complete"),
            Some(RequestOutcome::Unknown) | None => {
                ("unknown", "The request outcome has not been determined yet")
            }
        };

        let data_attributes = KeyValueList {
            values: vec![
                key_value_string("level", "info"),
                key_value_string("outcome", outcome),
                key_value_string("message", message),
            ],
        };

        let now = Utc::now();
        let timestamp = now.timestamp_nanos_opt().expect("DateTime out of range") as u64;

        let attributes = vec![
            key_value_string("specversion", "1.0"),
            key_value_string("id", &Uuid::new_v4().to_string()),
            key_value_string("type", "ai.tensorlake.progress-update"),
            key_value_string("source", "/tensorlake/indexify_server"),
            key_value_string("timestamp", &now.to_rfc3339()),
            key_value_string("ai.tensorlake.status", "completed"),
            KeyValue {
                key: "data".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(data_attributes)),
                }),
            },
        ];

        let log_record = LogRecord {
            time_unix_nano: timestamp,
            observed_time_unix_nano: timestamp,
            body: Some(AnyValue {
                value: Some(Value::StringValue(message.to_string())),
            }),
            attributes,
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: format!("ai.tensorlake.request.id:{}", ctx.request_id),
            ..Default::default()
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope),
                    log_records: vec![log_record],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        export_with_retry(&mut self.client, &self.retry_policy, &request).await
    }
}

fn key_value_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}
