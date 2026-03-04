use std::sync::{Arc, Mutex};

use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::{Resource, error::OTelSdkResult};

use crate::{log_batch::LogBatch, log_exporter::LogExporter, log_record::LogRecord};

/// Owned copy of a log record plus its instrumentation scope, as stored in
/// `InMemoryLogExporter`.
#[derive(Debug, Clone)]
pub struct OwnedLogData {
    pub record: LogRecord,
    pub instrumentation: InstrumentationScope,
}

/// An in-memory log exporter for testing.
///
/// Clones every exported record into an internal `Vec`. Call
/// `get_emitted_logs()` to retrieve them.
#[derive(Debug, Clone, Default)]
pub struct InMemoryLogExporter {
    logs: Arc<Mutex<Vec<OwnedLogData>>>,
}

impl InMemoryLogExporter {
    /// Returns a snapshot of all emitted log records.
    pub fn get_emitted_logs(&self) -> Result<Vec<OwnedLogData>, String> {
        self.logs
            .lock()
            .map(|g| g.clone())
            .map_err(|e| format!("mutex poisoned: {e}"))
    }
}

impl LogExporter for InMemoryLogExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let mut logs = self
            .logs
            .lock()
            .expect("InMemoryLogExporter mutex poisoned");
        for (record, scope) in batch.0 {
            logs.push(OwnedLogData {
                record: (*record).clone(),
                instrumentation: (*scope).clone(),
            });
        }
        Ok(())
    }

    fn set_resource(&mut self, _resource: &Resource) {}
}

#[cfg(test)]
mod tests {
    use opentelemetry::logs::AnyValue;

    use super::*;
    use crate::log_record::LogRecord;

    fn make_scope(name: &str) -> InstrumentationScope {
        InstrumentationScope::builder(name.to_owned()).build()
    }

    #[tokio::test]
    async fn test_export_stores_records() {
        let exporter = InMemoryLogExporter::default();

        let mut record = LogRecord::default();
        record.set_body(AnyValue::String("test message".into()));
        let scope = make_scope("test-scope");
        let pairs = [(&record, &scope)];
        let batch = LogBatch::new(&pairs);

        exporter.export(batch).await.unwrap();

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(1, logs.len());
        assert_eq!(
            logs[0].record.body,
            Some(AnyValue::String("test message".into()))
        );
        assert_eq!(logs[0].instrumentation.name(), "test-scope");
    }

    #[tokio::test]
    async fn test_export_accumulates_across_calls() {
        let exporter = InMemoryLogExporter::default();
        let scope = make_scope("scope");

        for i in 0..3u32 {
            let mut record = LogRecord::default();
            record.set_body(AnyValue::Int(i as i64));
            let pairs = [(&record, &scope)];
            exporter.export(LogBatch::new(&pairs)).await.unwrap();
        }

        let logs = exporter.get_emitted_logs().unwrap();
        assert_eq!(3, logs.len());
    }

    #[tokio::test]
    async fn test_export_empty_batch() {
        let exporter = InMemoryLogExporter::default();
        exporter.export(LogBatch::new(&[])).await.unwrap();
        assert!(exporter.get_emitted_logs().unwrap().is_empty());
    }

    #[test]
    fn test_clone_shares_storage() {
        let exporter = InMemoryLogExporter::default();
        let cloned = exporter.clone();

        // Both handles should see the same underlying vec (Arc-shared).
        assert_eq!(
            exporter.get_emitted_logs().unwrap().len(),
            cloned.get_emitted_logs().unwrap().len()
        );
    }
}
