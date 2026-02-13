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

use crate::state_store::request_events::RequestStateChangeEvent;

/// Create a batched export request from multiple events.
/// Events are grouped by request_id (which is unique system-wide) to create
/// efficient batches. Returns the ExportLogsServiceRequest that contains all
/// events.
pub fn create_batched_export_request(
    events: &[RequestStateChangeEvent],
) -> Result<ExportLogsServiceRequest, anyhow::Error> {
    if events.is_empty() {
        return Ok(ExportLogsServiceRequest {
            resource_logs: vec![],
        });
    }

    // Group events by request_id (unique system-wide)
    let mut request_groups: HashMap<String, Vec<&RequestStateChangeEvent>> = HashMap::new();
    for event in events {
        request_groups
            .entry(event.request_id().to_string())
            .or_default()
            .push(event);
    }

    // Create one ResourceLogs per request_id
    let mut resource_logs = Vec::new();
    for (request_id, events) in request_groups {
        // Get metadata from the first event (all events in a request share the same
        // metadata)
        let first = events.first().unwrap();
        let attributes = vec![
            key_value_string("ai.tensorlake.namespace", first.namespace()),
            key_value_string("ai.tensorlake.application.name", first.application_name()),
            key_value_string(
                "ai.tensorlake.application.version",
                first.application_version(),
            ),
        ];

        let scope = InstrumentationScope {
            name: format!("ai.tensorlake.request.id:{}", request_id),
            ..Default::default()
        };

        let mut log_records = Vec::new();
        for event in events {
            log_records.push(create_log_record(event)?);
        }

        let resource = Resource {
            attributes,
            ..Default::default()
        };

        resource_logs.push(ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope),
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        });
    }

    Ok(ExportLogsServiceRequest { resource_logs })
}

/// Create a log record from a request state change event.
fn create_log_record(update: &RequestStateChangeEvent) -> Result<LogRecord, anyhow::Error> {
    let message = update.message();
    let data = update_to_any_value(update)?;

    let now = Utc::now();
    let timestamp = now.timestamp_nanos_opt().expect("DateTime out of range") as u64;

    let mut attributes = vec![
        key_value_string("specversion", "1.0"),
        key_value_string("id", &Uuid::new_v4().to_string()),
        key_value_string("type", "ai.tensorlake.progress_update"),
        key_value_string("source", "/tensorlake/indexify_server"),
        key_value_string("timestamp", &now.to_rfc3339()),
        KeyValue {
            key: "data".to_string(),
            value: Some(data),
        },
    ];

    let mut meta_values = Vec::new();

    if let Some(id) = update.allocation_id() {
        meta_values.push(key_value_string_array("allocations", id));
    } else {
        meta_values.push(key_value_array("allocations", vec![]));
    }

    if let Some(id) = update.function_run_id() {
        meta_values.push(key_value_string_array("function_runs", id));
    } else {
        meta_values.push(key_value_array("function_runs", vec![]));
    }

    // Add function_executor_id to function_run_metadata so each event can have its
    // own. This will be separated later by log writer.
    if let Some(id) = update.function_executor_id() {
        meta_values.push(key_value_string("function_executor_id", id));
    }

    attributes.push(KeyValue {
        key: "ai.tensorlake.function_run_metadata".to_string(),
        value: Some(AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: meta_values,
            })),
        }),
    });

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

fn key_value_string_array(key: &str, value: &str) -> KeyValue {
    key_value_array(
        key,
        vec![AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }],
    )
}

fn key_value_array(key: &str, values: Vec<AnyValue>) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::ArrayValue(ArrayValue { values })),
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
            RequestStartedEvent,
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
            container_id: "container-001".to_string(),
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
            container_id: "container-001".to_string(),
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
}
