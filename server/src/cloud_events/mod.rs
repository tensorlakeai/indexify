use anyhow::Context;
use chrono::Utc;
use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
    common::v1::{
        AnyValue,
        ArrayValue,
        InstrumentationScope,
        KeyValue,
        KeyValueList,
        any_value::Value,
    },
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::error;
use uuid::Uuid;

use crate::{config::CloudEventsConfig, state_store::request_events::RequestStateChangeEvent};

mod retry;
use retry::*;

#[derive(Clone)]
pub struct CloudEventsExporter {
    cancellation_token: CancellationToken,
    tx: Sender<RequestStateChangeEvent>,
}

impl Drop for CloudEventsExporter {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl CloudEventsExporter {
    pub async fn new(config: &CloudEventsConfig) -> Result<Self, anyhow::Error> {
        let channel_builder = Channel::from_shared(config.writer_endpoint.clone())
            .context("building OTLP channel builder")?;

        let channel = channel_builder
            .connect()
            .await
            .context("building OTLP channel")?;

        let cancellation_token = CancellationToken::new();
        let tx = Self::start_collector(cancellation_token.clone(), channel.clone()).await;

        Ok(CloudEventsExporter {
            cancellation_token,
            tx,
        })
    }

    pub async fn send_request_state_change_event(&self, update: &RequestStateChangeEvent) {
        if let Err(error) = self.tx.send(update.clone()).await {
            error!(
                ?error,
                "Failed to send request completed event, the Request completed event channel is closed."
            );
        }
    }

    async fn start_collector(
        cancel: CancellationToken,
        channel: Channel,
    ) -> Sender<RequestStateChangeEvent> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<RequestStateChangeEvent>(100);
        let mut exporter = CloudEventsExporterClient::new(channel.clone());

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    ev = rx.recv() => match ev {
                        Some(update) => {
                            let res = exporter.emit_request_state_update(update).await;
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

pub struct CloudEventsExporterClient {
    client: LogsServiceClient<Channel>,
    retry_policy: RetryPolicy,
}

impl CloudEventsExporterClient {
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

    pub async fn emit_request_state_update(
        &mut self,
        update: RequestStateChangeEvent,
    ) -> Result<(), anyhow::Error> {
        let request = create_export_request(&update)?;
        export_with_retry(&mut self.client, &self.retry_policy, &request).await
    }
}

fn create_export_request(
    update: &RequestStateChangeEvent,
) -> Result<ExportLogsServiceRequest, anyhow::Error> {
    let attributes = vec![
        key_value_string("ai.tensorlake.namespace", update.namespace()),
        key_value_string("ai.tensorlake.application.name", update.application_name()),
        key_value_string(
            "ai.tensorlake.application.version",
            update.application_version(),
        ),
    ];
    let resource = Resource {
        attributes,
        ..Default::default()
    };

    let message = update.message();
    let data = update_to_any_value(update)?;

    let now = Utc::now();
    let timestamp = now.timestamp_nanos_opt().expect("DateTime out of range") as u64;

    let attributes = vec![
        key_value_string("specversion", "1.0"),
        key_value_string("id", &Uuid::new_v4().to_string()),
        key_value_string("type", "ai.tensorlake.progress-update"),
        key_value_string("source", "/tensorlake/indexify_server"),
        key_value_string("timestamp", &now.to_rfc3339()),
        key_value_string("ai.tensorlake.status", update.request_status()),
        KeyValue {
            key: "data".to_string(),
            value: Some(data),
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
        name: format!("ai.tensorlake.request.id:{}", update.request_id()),
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

    Ok(request)
}

fn update_to_any_value(update: &RequestStateChangeEvent) -> Result<AnyValue, anyhow::Error> {
    let json_data: JsonValue = serde_json::to_value(update)
        .context("failed to serialize request state change update to JSON")?;
    let data = serde_json_to_otel_any_value(&json_data);

    Ok(data)
}

fn key_value_string(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

pub fn serde_json_to_otel_value(value: &JsonValue) -> Option<Value> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(b) => Some(Value::BoolValue(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::IntValue(i))
            } else {
                n.as_f64().map(Value::DoubleValue)
            }
        }
        JsonValue::String(s) => Some(Value::StringValue(s.clone())),
        JsonValue::Array(arr) => {
            let values = arr
                .iter()
                .map(|v| AnyValue {
                    value: serde_json_to_otel_value(v),
                })
                .collect();
            Some(Value::ArrayValue(ArrayValue { values }))
        }
        JsonValue::Object(obj) => {
            let values = obj
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(AnyValue {
                        value: serde_json_to_otel_value(v),
                    }),
                })
                .collect();
            Some(Value::KvlistValue(KeyValueList { values }))
        }
    }
}

pub fn serde_json_to_otel_any_value(value: &JsonValue) -> AnyValue {
    AnyValue {
        value: serde_json_to_otel_value(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_model::{RequestFailureReason, RequestOutcome},
        state_store::request_events::{
            FunctionRunAssigned,
            FunctionRunCompleted,
            FunctionRunCreated,
            FunctionRunMatchedCache,
            FunctionRunOutcomeSummary,
            RequestCreatedEvent,
            RequestStartedEvent,
        },
    };

    fn string_value(s: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        })
    }

    fn extract_inner_kvlist(any_value: &AnyValue) -> KeyValueList {
        let Some(Value::KvlistValue(kvlist)) = &any_value.value else {
            panic!("Expected outer KvlistValue");
        };

        // The enum wrapper should be the only key at this level
        assert_eq!(kvlist.values.len(), 1);
        let Some(Value::KvlistValue(inner_kvlist)) = &kvlist.values[0]
            .value
            .as_ref()
            .and_then(|av| av.value.as_ref())
        else {
            panic!("Expected inner KvlistValue");
        };

        inner_kvlist.clone()
    }

    #[test]
    fn test_request_created_event_to_any_value() {
        let event = RequestStateChangeEvent::RequestCreated(RequestCreatedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-123".to_string(),
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);

        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-123")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_request_started_event_to_any_value() {
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.1".to_string(),
            request_id: "req-456".to_string(),
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.1")),
                "request_id" => assert_eq!(kv.value, string_value("req-456")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_request_finished_event_to_any_value() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.2",
            "req-789",
            RequestOutcome::Success,
        );

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.2")),
                "request_id" => assert_eq!(kv.value, string_value("req-789")),
                "outcome" => assert_eq!(kv.value, string_value("Success")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_created_event_to_any_value() {
        let event = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.0".to_string(),
            request_id: "req-001".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-123".to_string(),
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("function_name"));
        assert!(keys.contains("function_run_id"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-001")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-123")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_assigned_event_to_any_value() {
        let event = RequestStateChangeEvent::FunctionRunAssigned(FunctionRunAssigned {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.1".to_string(),
            request_id: "req-002".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-456".to_string(),
            allocation_id: "alloc-789".to_string(),
            executor_id: "executor-001".to_string(),
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("function_name"));
        assert!(keys.contains("function_run_id"));
        assert!(keys.contains("allocation_id"));
        assert!(keys.contains("executor_id"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.1")),
                "request_id" => assert_eq!(kv.value, string_value("req-002")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-456")),
                "allocation_id" => assert_eq!(kv.value, string_value("alloc-789")),
                "executor_id" => assert_eq!(kv.value, string_value("executor-001")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_completed_event_to_any_value() {
        let event = RequestStateChangeEvent::FunctionRunCompleted(FunctionRunCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            allocation_id: "alloc-456".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("function_name"));
        assert!(keys.contains("function_run_id"));
        assert!(keys.contains("allocation_id"));
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.2")),
                "request_id" => assert_eq!(kv.value, string_value("req-003")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-789")),
                "allocation_id" => assert_eq!(kv.value, string_value("alloc-456")),
                "outcome" => assert_eq!(kv.value, string_value("success")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_matched_cache_event_to_any_value() {
        let event = RequestStateChangeEvent::FunctionRunMatchedCache(FunctionRunMatchedCache {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.3".to_string(),
            request_id: "req-004".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-111".to_string(),
        });

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("function_name"));
        assert!(keys.contains("function_run_id"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.3")),
                "request_id" => assert_eq!(kv.value, string_value("req-004")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-111")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_failure_outcome_to_any_value() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-fail",
            RequestOutcome::Failure(RequestFailureReason::InternalError),
        );

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-fail")),
                "outcome" => assert_eq!(
                    kv.value,
                    Some(AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "Failure".to_string(),
                                value: string_value("InternalError")
                            }]
                        }))
                    })
                ),
                _ => {}
            }
        }
    }

    #[test]
    fn test_unknown_outcome_to_any_value() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-unknown",
            RequestOutcome::Unknown,
        );

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-unknown")),
                "outcome" => assert_eq!(kv.value, string_value("Unknown")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_success_outcome_to_any_value() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-unknown",
            RequestOutcome::Success,
        );

        let result = super::update_to_any_value(&event);
        assert!(result.is_ok());

        let any_value = result.unwrap();
        let inner_kvlist = extract_inner_kvlist(&any_value);
        let keys: std::collections::HashSet<_> = inner_kvlist
            .values
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains("namespace"));
        assert!(keys.contains("application_name"));
        assert!(keys.contains("application_version"));
        assert!(keys.contains("request_id"));
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("1.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-unknown")),
                "outcome" => assert_eq!(kv.value, string_value("Success")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_create_export_request_structure() {
        let event = RequestStateChangeEvent::RequestCreated(RequestCreatedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-123".to_string(),
        });

        let result = super::create_export_request(&event);
        assert!(result.is_ok());

        let request = result.unwrap();
        assert_eq!(request.resource_logs.len(), 1);

        let resource_logs = &request.resource_logs[0];
        let Some(resource) = &resource_logs.resource else {
            panic!("Expected Resource");
        };

        // Verify resource attributes
        assert_eq!(resource.attributes.len(), 3);
        let resource_keys: std::collections::HashSet<_> = resource
            .attributes
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(resource_keys.contains("ai.tensorlake.namespace"));
        assert!(resource_keys.contains("ai.tensorlake.application.name"));
        assert!(resource_keys.contains("ai.tensorlake.application.version"));

        for kv in &resource.attributes {
            match kv.key.as_str() {
                "ai.tensorlake.namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "ai.tensorlake.application.name" => assert_eq!(kv.value, string_value("test-app")),
                "ai.tensorlake.application.version" => assert_eq!(kv.value, string_value("1.0.0")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_create_export_request_log_record() {
        let event = RequestStateChangeEvent::RequestCreated(RequestCreatedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-456".to_string(),
        });

        let result = super::create_export_request(&event);
        assert!(result.is_ok());

        let request = result.unwrap();
        let resource_logs = &request.resource_logs[0];
        assert_eq!(resource_logs.scope_logs.len(), 1);

        let scope_logs = &resource_logs.scope_logs[0];
        assert_eq!(scope_logs.log_records.len(), 1);

        let log_record = &scope_logs.log_records[0];

        // Verify log record body is a string
        let Some(body) = &log_record.body else {
            panic!("Expected log record body");
        };
        assert!(matches!(body.value, Some(Value::StringValue(_))));

        // Verify log record attributes
        let log_attrs_keys: std::collections::HashSet<_> = log_record
            .attributes
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(log_attrs_keys.contains("specversion"));
        assert!(log_attrs_keys.contains("id"));
        assert!(log_attrs_keys.contains("type"));
        assert!(log_attrs_keys.contains("source"));
        assert!(log_attrs_keys.contains("timestamp"));
        assert!(log_attrs_keys.contains("ai.tensorlake.status"));
        assert!(log_attrs_keys.contains("data"));

        for kv in &log_record.attributes {
            match kv.key.as_str() {
                "specversion" => assert_eq!(kv.value, string_value("1.0")),
                "type" => assert_eq!(kv.value, string_value("ai.tensorlake.progress-update")),
                "source" => assert_eq!(kv.value, string_value("/tensorlake/indexify_server")),
                "ai.tensorlake.status" => assert!(matches!(
                    &kv.value,
                    Some(av) if matches!(&av.value, Some(Value::StringValue(_)))
                )),
                "id" => assert!(matches!(
                    &kv.value,
                    Some(av) if matches!(&av.value, Some(Value::StringValue(_)))
                )),
                "timestamp" => assert!(matches!(
                    &kv.value,
                    Some(av) if matches!(&av.value, Some(Value::StringValue(_)))
                )),
                "data" => assert!(matches!(&kv.value, Some(_))),
                _ => {}
            }
        }
    }

    #[test]
    fn test_create_export_request_scope() {
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.1".to_string(),
            request_id: "req-789".to_string(),
        });

        let result = super::create_export_request(&event);
        assert!(result.is_ok());

        let request = result.unwrap();
        let resource_logs = &request.resource_logs[0];
        let scope_logs = &resource_logs.scope_logs[0];

        let Some(scope) = &scope_logs.scope else {
            panic!("Expected InstrumentationScope");
        };

        assert_eq!(scope.name, "ai.tensorlake.request.id:req-789");
    }

    #[test]
    fn test_create_export_request_with_failure_outcome() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.2",
            "req-fail-123",
            RequestOutcome::Failure(RequestFailureReason::InternalError),
        );

        let result = super::create_export_request(&event);
        assert!(result.is_ok());

        let request = result.unwrap();
        let resource_logs = &request.resource_logs[0];
        let log_record = &resource_logs.scope_logs[0].log_records[0];

        // Find ai.tensorlake.status attribute
        let status_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == "ai.tensorlake.status")
            .expect("Expected ai.tensorlake.status attribute");

        assert!(matches!(
            &status_attr.value,
            Some(av) if matches!(&av.value, Some(Value::StringValue(s)) if s == "Finished")
        ));
    }

    #[test]
    fn test_create_export_request_data_field() {
        let event = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.0".to_string(),
            request_id: "req-001".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-123".to_string(),
        });

        let result = super::create_export_request(&event);
        assert!(result.is_ok());

        let request = result.unwrap();
        let log_record = &request.resource_logs[0].scope_logs[0].log_records[0];

        // Find data attribute
        let data_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == "data")
            .expect("Expected data attribute");

        // Data should contain the serialized event as a KvlistValue
        let Some(data_value) = &data_attr.value else {
            panic!("Expected data value");
        };
        assert!(matches!(&data_value.value, Some(Value::KvlistValue(_))));

        // Verify the data contains expected fields from FunctionRunCreated
        if let Some(Value::KvlistValue(kvlist)) = &data_value.value {
            // The outer kvlist has enum wrapper, get the inner kvlist
            assert_eq!(kvlist.values.len(), 1);
            let Some(Value::KvlistValue(inner_kvlist)) = &kvlist.values[0]
                .value
                .as_ref()
                .and_then(|av| av.value.as_ref())
            else {
                panic!("Expected inner KvlistValue");
            };

            let keys: std::collections::HashSet<_> = inner_kvlist
                .values
                .iter()
                .map(|kv| kv.key.as_str())
                .collect();
            assert!(keys.contains("namespace"));
            assert!(keys.contains("application_name"));
            assert!(keys.contains("function_name"));
            assert!(keys.contains("function_run_id"));
        } else {
            panic!("Expected KvlistValue in data");
        }
    }
}
