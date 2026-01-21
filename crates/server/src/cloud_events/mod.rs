use std::collections::HashMap;

use anyhow::Context;
use chrono::Utc;
use otlp_logs_exporter::opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
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
use uuid::Uuid;

use crate::state_store::request_events::{
    PersistedRequestStateChangeEvent,
    RequestStateChangeEvent,
};

pub struct CloudEventBatch {
    pub export_request: ExportLogsServiceRequest,
    pub event_keys: Vec<Vec<u8>>,
}

pub fn create_batch_export_request(
    updates: &[PersistedRequestStateChangeEvent],
) -> Result<Vec<CloudEventBatch>, anyhow::Error> {
    let mut grouped: HashMap<&str, Vec<&PersistedRequestStateChangeEvent>> = HashMap::new();
    for update in updates {
        grouped
            .entry(update.event.request_id())
            .or_default()
            .push(update);
    }

    let mut requests = Vec::with_capacity(grouped.len());
    for (request_id, group) in grouped {
        let Some(first) = group.first() else { continue };

        let resource = create_resource(&first.event);
        let scope = create_scope(request_id);

        let mut keys = Vec::with_capacity(group.len());
        let mut log_records = Vec::with_capacity(group.len());
        for update in group {
            keys.push(update.key());
            log_records.push(create_log_record(&update.event)?);
        }

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope),
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = CloudEventBatch {
            export_request: request,
            event_keys: keys,
        };

        requests.push(batch);
    }

    Ok(requests)
}

fn create_resource(update: &RequestStateChangeEvent) -> Resource {
    let attributes = vec![
        key_value_string("ai.tensorlake.namespace", update.namespace()),
        key_value_string("ai.tensorlake.application.name", update.application_name()),
        key_value_string(
            "ai.tensorlake.application.version",
            update.application_version(),
        ),
    ];
    Resource {
        attributes,
        ..Default::default()
    }
}

fn create_scope(request_id: &str) -> InstrumentationScope {
    InstrumentationScope {
        name: format!("ai.tensorlake.request.id:{}", request_id),
        ..Default::default()
    }
}

fn create_log_record(update: &RequestStateChangeEvent) -> Result<LogRecord, anyhow::Error> {
    let message = update.message();
    let data = update_to_any_value(update)?;

    let now = Utc::now();
    let timestamp = now.timestamp_nanos_opt().expect("DateTime out of range") as u64;

    let attributes = vec![
        key_value_string("specversion", "1.0"),
        key_value_string("id", &Uuid::now_v7().to_string()),
        key_value_string("type", "ai.tensorlake.progress_update"),
        key_value_string("source", "/tensorlake/indexify_server"),
        key_value_string("timestamp", &now.to_rfc3339()),
        KeyValue {
            key: "data".to_string(),
            value: Some(data),
        },
    ];

    Ok(LogRecord {
        time_unix_nano: timestamp,
        observed_time_unix_nano: timestamp,
        body: Some(AnyValue {
            value: Some(Value::StringValue(message.to_string())),
        }),
        attributes,
        ..Default::default()
    })
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
    use chrono::{DateTime, SecondsFormat};

    use super::*;
    use crate::{
        data_model::{RequestCtxBuilder, RequestFailureReason, RequestOutcome},
        state_store::request_events::{
            AllocationCompleted,
            AllocationCreated,
            FunctionRunCompleted,
            FunctionRunCreated,
            FunctionRunMatchedCache,
            FunctionRunOutcomeSummary,
            PersistedRequestStateChangeEvent,
            RequestStartedEvent,
            RequestStateChangeEventId,
        },
    };

    fn string_value(s: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(Value::StringValue(s.to_string())),
        })
    }

    fn date_value(v: &DateTime<Utc>) -> Option<AnyValue> {
        // Serde serializes times with different precissions depending on the target os.
        #[cfg(not(target_os = "linux"))]
        let format = SecondsFormat::Micros;
        #[cfg(target_os = "linux")]
        let format = SecondsFormat::Nanos;
        string_value(&v.to_rfc3339_opts(format, true))
    }

    fn make_test_ctx(
        namespace: &str,
        app: &str,
        version: &str,
        request_id: &str,
        outcome: RequestOutcome,
    ) -> crate::data_model::RequestCtx {
        RequestCtxBuilder::default()
            .namespace(namespace.to_string())
            .application_name(app.to_string())
            .application_version(version.to_string())
            .request_id(request_id.to_string())
            .outcome(Some(outcome))
            .function_calls(Default::default())
            .function_runs(Default::default())
            .build()
            .unwrap()
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
    fn test_request_started_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.1".to_string(),
            request_id: "req-456".to_string(),
            created_at: now,
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
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_request_finished_event_to_any_value() {
        let ctx = make_test_ctx(
            "test-ns",
            "test-app",
            "1.0.2",
            "req-789",
            RequestOutcome::Success,
        );
        let event = RequestStateChangeEvent::finished(&ctx, &RequestOutcome::Success, None);
        let now = match event {
            RequestStateChangeEvent::RequestFinished(ref event) => event.created_at,
            _ => panic!("unexpected event type"),
        };

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
                "outcome" => assert_eq!(kv.value, string_value("success")),
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_created_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.0".to_string(),
            request_id: "req-001".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-123".to_string(),
            created_at: now,
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
        assert!(keys.contains("created_at"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.0")),
                "request_id" => assert_eq!(kv.value, string_value("req-001")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-123")),
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_allocation_created_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::AllocationCreated(AllocationCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.1".to_string(),
            request_id: "req-002".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-456".to_string(),
            allocation_id: "alloc-789".to_string(),
            executor_id: "executor-001".to_string(),
            created_at: now,
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
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_allocation_completed_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::AllocationCompleted(AllocationCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            allocation_id: "alloc-456".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
            created_at: now,
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
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_completed_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::FunctionRunCompleted(FunctionRunCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
            created_at: now,
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
        assert!(keys.contains("outcome"));

        for kv in &inner_kvlist.values {
            match kv.key.as_str() {
                "namespace" => assert_eq!(kv.value, string_value("test-ns")),
                "application_name" => assert_eq!(kv.value, string_value("test-app")),
                "application_version" => assert_eq!(kv.value, string_value("2.0.2")),
                "request_id" => assert_eq!(kv.value, string_value("req-003")),
                "function_name" => assert_eq!(kv.value, string_value("my-function")),
                "function_run_id" => assert_eq!(kv.value, string_value("run-789")),
                "outcome" => assert_eq!(kv.value, string_value("success")),
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_function_run_matched_cache_event_to_any_value() {
        let now = Utc::now();
        let event = RequestStateChangeEvent::FunctionRunMatchedCache(FunctionRunMatchedCache {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.3".to_string(),
            request_id: "req-004".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-111".to_string(),
            created_at: now,
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
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_failure_outcome_to_any_value() {
        let ctx = make_test_ctx(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-fail",
            RequestOutcome::Failure(RequestFailureReason::InternalError),
        );
        let event = RequestStateChangeEvent::finished(
            &ctx,
            &RequestOutcome::Failure(RequestFailureReason::InternalError),
            None,
        );
        let now = match event {
            RequestStateChangeEvent::RequestFinished(ref event) => event.created_at,
            _ => panic!("unexpected event type"),
        };

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
                "created_at" => {
                    assert_eq!(kv.value, date_value(&now))
                }
                "outcome" => assert_eq!(
                    kv.value,
                    Some(AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "failure".to_string(),
                                value: string_value("internal_error")
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
        let ctx = make_test_ctx(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-unknown",
            RequestOutcome::Unknown,
        );
        let event = RequestStateChangeEvent::finished(&ctx, &RequestOutcome::Unknown, None);

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
                "outcome" => assert_eq!(kv.value, string_value("unknown")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_success_outcome_to_any_value() {
        let ctx = make_test_ctx(
            "test-ns",
            "test-app",
            "1.0.0",
            "req-unknown",
            RequestOutcome::Success,
        );
        let event = RequestStateChangeEvent::finished(&ctx, &RequestOutcome::Success, None);

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
                "outcome" => assert_eq!(kv.value, string_value("success")),
                _ => {}
            }
        }
    }

    #[test]
    fn test_create_export_request_structure() {
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-123".to_string(),
            created_at: Utc::now(),
        });

        let persisted =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event);
        let result = super::create_batch_export_request(&[persisted]);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];

        let resource_logs = &request.export_request.resource_logs[0];
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
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-456".to_string(),
            created_at: Utc::now(),
        });

        let persisted =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event);
        let result = super::create_batch_export_request(&[persisted]);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];

        let resource_logs = &request.export_request.resource_logs[0];
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
        assert!(log_attrs_keys.contains("data"));

        for kv in &log_record.attributes {
            match kv.key.as_str() {
                "specversion" => assert_eq!(kv.value, string_value("1.0")),
                "type" => assert_eq!(kv.value, string_value("ai.tensorlake.progress_update")),
                "source" => assert_eq!(kv.value, string_value("/tensorlake/indexify_server")),
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
            created_at: Utc::now(),
        });

        let persisted =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event);
        let result = super::create_batch_export_request(&[persisted]);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];

        let resource_logs = &request.export_request.resource_logs[0];
        let scope_logs = &resource_logs.scope_logs[0];

        let Some(scope) = &scope_logs.scope else {
            panic!("Expected InstrumentationScope");
        };

        assert_eq!(scope.name, "ai.tensorlake.request.id:req-789");
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
            created_at: Utc::now(),
        });

        let persisted =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event);
        let result = super::create_batch_export_request(&[persisted]);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];

        let log_record = &request.export_request.resource_logs[0].scope_logs[0].log_records[0];

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

    #[test]
    fn test_create_batch_export_request_groups_by_request_id() {
        let now = Utc::now();
        let event1 = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-A".to_string(),
            created_at: now,
        });
        let event2 = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-A".to_string(),
            function_name: "fn1".to_string(),
            function_run_id: "run-1".to_string(),
            created_at: now,
        });
        let event3 = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-B".to_string(),
            created_at: now,
        });

        let persisted1 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event1);
        let persisted2 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(2), event2);
        let persisted3 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(3), event3);

        let updates: Vec<PersistedRequestStateChangeEvent> =
            vec![persisted1, persisted2, persisted3];
        let result = super::create_batch_export_request(&updates);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 2);

        let mut req_a_count = 0;
        let mut req_b_count = 0;
        for req in &requests {
            let scope = req.export_request.resource_logs[0].scope_logs[0]
                .scope
                .as_ref()
                .unwrap();
            let log_count = req.export_request.resource_logs[0].scope_logs[0]
                .log_records
                .len();
            if scope.name == "ai.tensorlake.request.id:req-A" {
                req_a_count = log_count;
            } else if scope.name == "ai.tensorlake.request.id:req-B" {
                req_b_count = log_count;
            }
        }
        assert_eq!(req_a_count, 2);
        assert_eq!(req_b_count, 1);
    }

    #[test]
    fn test_create_batch_export_request_empty_input() {
        let updates: Vec<PersistedRequestStateChangeEvent> = vec![];
        let result = super::create_batch_export_request(&updates);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_create_batch_export_request_single_event() {
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-single".to_string(),
            created_at: Utc::now(),
        });

        let persisted =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event);
        let updates = vec![persisted];
        let result = super::create_batch_export_request(&updates);
        assert!(result.is_ok());

        let requests = result.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].export_request.resource_logs[0].scope_logs[0]
                .log_records
                .len(),
            1
        );

        let scope = requests[0].export_request.resource_logs[0].scope_logs[0]
            .scope
            .as_ref()
            .unwrap();
        assert_eq!(scope.name, "ai.tensorlake.request.id:req-single");
    }

    #[test]
    fn test_create_batch_export_request_multiple_events_multiple_request_ids() {
        let now = Utc::now();

        // Request A: 3 events
        let event_a1 = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-A".to_string(),
            created_at: now,
        });

        let event_a2 = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-A".to_string(),
            function_name: "func-1".to_string(),
            function_run_id: "run-1".to_string(),
            created_at: now,
        });

        let event_a3 = RequestStateChangeEvent::AllocationCreated(AllocationCreated {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-A".to_string(),
            function_name: "func-1".to_string(),
            function_run_id: "run-1".to_string(),
            allocation_id: "alloc-1".to_string(),
            executor_id: "executor-1".to_string(),
            created_at: now,
        });

        // Request B: 2 events
        let event_b1 = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-B".to_string(),
            created_at: now,
        });

        let event_b2 = RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
            namespace: "ns1".to_string(),
            application_name: "app1".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-B".to_string(),
            function_name: "func-2".to_string(),
            function_run_id: "run-2".to_string(),
            created_at: now,
        });

        // Request C: 1 event
        let event_c1 = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns2".to_string(),
            application_name: "app2".to_string(),
            application_version: "2.0.0".to_string(),
            request_id: "req-C".to_string(),
            created_at: now,
        });

        let persisted_a1 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(1), event_a1);
        let persisted_a2 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(2), event_a2);
        let persisted_a3 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(3), event_a3);
        let persisted_b1 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(4), event_b1);
        let persisted_b2 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(5), event_b2);
        let persisted_c1 =
            PersistedRequestStateChangeEvent::new(RequestStateChangeEventId::new(6), event_c1);

        let updates = vec![
            persisted_a1,
            persisted_a2,
            persisted_a3,
            persisted_b1,
            persisted_b2,
            persisted_c1,
        ];
        let result = super::create_batch_export_request(&updates);
        assert!(result.is_ok());

        let requests = result.unwrap();
        // Should produce 3 export requests (one per request ID)
        assert_eq!(requests.len(), 3);

        // Collect results by request ID
        let mut request_data: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for req in &requests {
            let scope = req.export_request.resource_logs[0].scope_logs[0]
                .scope
                .as_ref()
                .unwrap();
            let scope_name = scope.name.clone();
            let log_count = req.export_request.resource_logs[0].scope_logs[0]
                .log_records
                .len();

            // Extract request ID from scope name
            if let Some(req_id) = scope_name.strip_prefix("ai.tensorlake.request.id:") {
                request_data.insert(req_id.to_string(), log_count);
            }
        }

        // Verify event counts for each request ID
        assert_eq!(request_data.get("req-A"), Some(&3));
        assert_eq!(request_data.get("req-B"), Some(&2));
        assert_eq!(request_data.get("req-C"), Some(&1));

        // Verify that req-A and req-B have the same namespace but req-C is different
        let mut ns_data: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for req in &requests {
            let scope = req.export_request.resource_logs[0].scope_logs[0]
                .scope
                .as_ref()
                .unwrap();
            if let Some(req_id) = scope.name.strip_prefix("ai.tensorlake.request.id:") {
                let resource = req.export_request.resource_logs[0]
                    .resource
                    .as_ref()
                    .unwrap();
                let namespace = resource
                    .attributes
                    .iter()
                    .find(|kv| kv.key == "ai.tensorlake.namespace")
                    .and_then(|kv| {
                        if let Some(av) = &kv.value {
                            if let Some(Value::StringValue(s)) = &av.value {
                                Some(s.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                ns_data.insert(req_id.to_string(), namespace);
            }
        }

        assert_eq!(ns_data.get("req-A"), Some(&"ns1".to_string()));
        assert_eq!(ns_data.get("req-B"), Some(&"ns1".to_string()));
        assert_eq!(ns_data.get("req-C"), Some(&"ns2".to_string()));
    }
}
