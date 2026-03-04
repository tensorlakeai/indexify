use std::{borrow::Cow, collections::HashMap, time::Instant};

use opentelemetry::{
    global,
    logs::{AnyValue as LogsAnyValue, Severity},
    metrics::{Counter, Histogram},
};
use opentelemetry_proto::{
    tonic::{
        collector::logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
        common::v1::{AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList},
        logs::v1::{LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        resource::v1::Resource as ProtoResource,
    },
    transform::common::tonic::ResourceAttributesWithSchema,
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
};
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, transport::Channel};
use tracing::{debug, warn};

use crate::{
    error::Error,
    log_batch::LogBatch,
    log_exporter::LogExporter,
    log_record::LogRecord,
    retry::{RetryPolicy, export_with_retry},
};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub(crate) struct ExporterMetrics {
    pub(crate) export_requests: Counter<u64>,
    pub(crate) export_errors: Counter<u64>,
    pub(crate) export_duration_ms: Histogram<f64>,
    pub(crate) retry_attempts: Counter<u64>,
}

impl ExporterMetrics {
    fn new() -> Self {
        let meter = global::meter("otel_logs_sdk");
        Self {
            export_requests: meter
                .u64_counter("otel_logs.exporter.export_requests_total")
                .with_description("Total OTLP export requests initiated")
                .build(),
            export_errors: meter
                .u64_counter("otel_logs.exporter.export_errors_total")
                .with_description("Total failed OTLP export requests")
                .build(),
            export_duration_ms: meter
                .f64_histogram("otel_logs.exporter.export_duration_ms")
                .with_description("Duration of OTLP export requests in milliseconds")
                .with_unit("ms")
                .build(),
            retry_attempts: meter
                .u64_counter("otel_logs.exporter.retry_attempts_total")
                .with_description("Total number of retry attempts made during OTLP export")
                .build(),
        }
    }
}

/// OtlpLogsExporter is a log exporter for OpenTelemetry that uses Tonic to send
/// logs to a collector.
///
/// It allows you to specify a retry policy for retrying failed requests based
/// on whether the Otel errors are retryable or not.
pub struct OtlpLogsExporter {
    client: Mutex<LogsServiceClient<Channel>>,
    retry_policy: RetryPolicy,
    resource: ResourceAttributesWithSchema,
    metrics: ExporterMetrics,
}

impl std::fmt::Debug for OtlpLogsExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpLogsExporter").finish()
    }
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
            metrics: ExporterMetrics::new(),
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
        export_with_retry(
            &mut client,
            &self.retry_policy,
            &request,
            &self.metrics.retry_attempts,
        )
        .await
    }
}

impl LogExporter for OtlpLogsExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let record_count = batch.0.len();
        debug!(record_count, "Exporting records to OTLP endpoint");
        self.metrics.export_requests.add(1, &[]);

        let resource_logs = group_logs_by_resource_and_scope(batch, &self.resource);
        let request = ExportLogsServiceRequest { resource_logs };

        let mut client = self.client.lock().await;

        let start = Instant::now();
        let result = export_with_retry(
            &mut client,
            &self.retry_policy,
            &request,
            &self.metrics.retry_attempts,
        )
        .await;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        self.metrics.export_duration_ms.record(elapsed_ms, &[]);

        match result {
            Ok(_) => {
                debug!(record_count, elapsed_ms, "OTLP export successful");
                Ok(())
            }
            Err(error) => {
                warn!(?error, record_count, "OTLP export failed");
                self.metrics.export_errors.add(1, &[]);
                Err(OTelSdkError::InternalFailure(format!(
                    "OTLP export error: {error:?}"
                )))
            }
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}

// ---------------------------------------------------------------------------
// Proto conversion helpers
// ---------------------------------------------------------------------------

fn to_nanos(time: std::time::SystemTime) -> u64 {
    time.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

fn anyvalue_to_proto_value(
    value: LogsAnyValue,
) -> opentelemetry_proto::tonic::common::v1::any_value::Value {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match value {
        LogsAnyValue::Double(f) => Value::DoubleValue(f),
        LogsAnyValue::Int(i) => Value::IntValue(i),
        LogsAnyValue::String(s) => Value::StringValue(s.into()),
        LogsAnyValue::Boolean(b) => Value::BoolValue(b),
        LogsAnyValue::ListAny(v) => Value::ArrayValue(ArrayValue {
            values: v
                .into_iter()
                .map(|v| AnyValue {
                    value: Some(anyvalue_to_proto_value(v)),
                })
                .collect(),
        }),
        LogsAnyValue::Map(m) => Value::KvlistValue(KeyValueList {
            values: m
                .into_iter()
                .map(|(key, value)| KeyValue {
                    key: key.into(),
                    value: Some(AnyValue {
                        value: Some(anyvalue_to_proto_value(value)),
                    }),
                })
                .collect(),
        }),
        LogsAnyValue::Bytes(v) => Value::BytesValue(*v),
        _ => unreachable!("Nonexistent value type"),
    }
}

fn log_record_to_proto(record: &LogRecord) -> ProtoLogRecord {
    let severity_number = match record.severity_number {
        Some(Severity::Trace) => SeverityNumber::Trace,
        Some(Severity::Trace2) => SeverityNumber::Trace2,
        Some(Severity::Trace3) => SeverityNumber::Trace3,
        Some(Severity::Trace4) => SeverityNumber::Trace4,
        Some(Severity::Debug) => SeverityNumber::Debug,
        Some(Severity::Debug2) => SeverityNumber::Debug2,
        Some(Severity::Debug3) => SeverityNumber::Debug3,
        Some(Severity::Debug4) => SeverityNumber::Debug4,
        Some(Severity::Info) => SeverityNumber::Info,
        Some(Severity::Info2) => SeverityNumber::Info2,
        Some(Severity::Info3) => SeverityNumber::Info3,
        Some(Severity::Info4) => SeverityNumber::Info4,
        Some(Severity::Warn) => SeverityNumber::Warn,
        Some(Severity::Warn2) => SeverityNumber::Warn2,
        Some(Severity::Warn3) => SeverityNumber::Warn3,
        Some(Severity::Warn4) => SeverityNumber::Warn4,
        Some(Severity::Error) => SeverityNumber::Error,
        Some(Severity::Error2) => SeverityNumber::Error2,
        Some(Severity::Error3) => SeverityNumber::Error3,
        Some(Severity::Error4) => SeverityNumber::Error4,
        Some(Severity::Fatal) => SeverityNumber::Fatal,
        Some(Severity::Fatal2) => SeverityNumber::Fatal2,
        Some(Severity::Fatal3) => SeverityNumber::Fatal3,
        Some(Severity::Fatal4) => SeverityNumber::Fatal4,
        None => SeverityNumber::Unspecified,
    };

    ProtoLogRecord {
        time_unix_nano: record.timestamp.map(to_nanos).unwrap_or_default(),
        observed_time_unix_nano: record.observed_timestamp.map(to_nanos).unwrap_or_default(),
        severity_number: severity_number.into(),
        severity_text: record.severity_text.clone().unwrap_or_default(),
        body: record.body.clone().map(|v| AnyValue {
            value: Some(anyvalue_to_proto_value(v)),
        }),
        attributes: record
            .attributes
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: Some(AnyValue {
                    value: Some(anyvalue_to_proto_value(v.clone())),
                }),
            })
            .collect(),
        dropped_attributes_count: 0,
        flags: 0,
        span_id: vec![],
        trace_id: vec![],
        event_name: String::new(),
    }
}

pub(crate) fn group_logs_by_resource_and_scope(
    batch: LogBatch<'_>,
    resource: &ResourceAttributesWithSchema,
) -> Vec<ResourceLogs> {
    let mut scope_map: HashMap<String, (InstrumentationScope, Vec<ProtoLogRecord>)> =
        HashMap::new();

    for (record, scope) in batch.0 {
        let scope_name = scope.name().to_owned();
        let entry = scope_map.entry(scope_name).or_insert_with(|| {
            let proto_scope = InstrumentationScope::from((*scope, None::<Cow<'static, str>>));
            (proto_scope, Vec::new())
        });
        entry.1.push(log_record_to_proto(record));
    }

    let scope_logs = scope_map
        .into_values()
        .map(|(proto_scope, log_records)| ScopeLogs {
            scope: Some(proto_scope),
            schema_url: resource.schema_url.clone().unwrap_or_default(),
            log_records,
        })
        .collect();

    vec![ResourceLogs {
        resource: Some(ProtoResource {
            attributes: resource.attributes.0.clone(),
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_logs,
        schema_url: resource.schema_url.clone().unwrap_or_default(),
    }]
}

#[cfg(test)]
mod tests {
    use opentelemetry::logs::AnyValue;
    use opentelemetry_sdk::Resource;

    use super::*;

    fn make_scope(name: &str) -> opentelemetry::InstrumentationScope {
        opentelemetry::InstrumentationScope::builder(name.to_owned()).build()
    }

    #[test]
    fn test_group_logs_single_scope() {
        let resource: ResourceAttributesWithSchema = (&Resource::builder_empty().build()).into();
        let mut record = LogRecord::default();
        record.set_body(AnyValue::String("msg".into()));
        let scope = make_scope("my-scope");
        let pairs = [(&record, &scope)];
        let batch = LogBatch::new(&pairs);

        let resource_logs = group_logs_by_resource_and_scope(batch, &resource);
        assert_eq!(1, resource_logs.len());
        assert_eq!(1, resource_logs[0].scope_logs.len());
        assert_eq!(1, resource_logs[0].scope_logs[0].log_records.len());
    }

    #[test]
    fn test_group_logs_multiple_scopes() {
        let resource: ResourceAttributesWithSchema = (&Resource::builder_empty().build()).into();
        let record_a = LogRecord::default();
        let record_b = LogRecord::default();
        let scope_a = make_scope("scope-a");
        let scope_b = make_scope("scope-b");
        let pairs = [(&record_a, &scope_a), (&record_b, &scope_b)];
        let batch = LogBatch::new(&pairs);

        let resource_logs = group_logs_by_resource_and_scope(batch, &resource);
        assert_eq!(1, resource_logs.len());
        assert_eq!(2, resource_logs[0].scope_logs.len());
    }

    #[test]
    fn test_group_logs_same_scope_merged() {
        let resource: ResourceAttributesWithSchema = (&Resource::builder_empty().build()).into();
        let record_a = LogRecord::default();
        let record_b = LogRecord::default();
        let scope = make_scope("scope");
        let pairs = [(&record_a, &scope), (&record_b, &scope)];
        let batch = LogBatch::new(&pairs);

        let resource_logs = group_logs_by_resource_and_scope(batch, &resource);
        assert_eq!(1, resource_logs[0].scope_logs.len());
        assert_eq!(2, resource_logs[0].scope_logs[0].log_records.len());
    }

    #[test]
    fn test_log_record_to_proto_severity() {
        let mut record = LogRecord::default();
        record.set_severity_number(Severity::Error);
        record.set_severity_text("ERROR");

        let proto = log_record_to_proto(&record);
        assert_eq!(proto.severity_text, "ERROR");
        assert_eq!(proto.severity_number, SeverityNumber::Error as i32);
    }

    #[test]
    fn test_log_record_to_proto_timestamp() {
        use std::time::{Duration, UNIX_EPOCH};

        let ts = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let mut record = LogRecord::default();
        record.set_timestamp(ts);

        let proto = log_record_to_proto(&record);
        assert_eq!(proto.time_unix_nano, 1_000_000 * 1_000_000_000);
    }

    #[test]
    fn test_anyvalue_string_conversion() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let val = LogsAnyValue::String("hello".into());
        let proto = anyvalue_to_proto_value(val);
        assert!(matches!(proto, Value::StringValue(s) if s == "hello"));
    }

    #[test]
    fn test_anyvalue_list_conversion() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let val = LogsAnyValue::ListAny(Box::new(vec![LogsAnyValue::Int(1), LogsAnyValue::Int(2)]));
        let proto = anyvalue_to_proto_value(val);
        assert!(matches!(proto, Value::ArrayValue(arr) if arr.values.len() == 2));
    }
}
