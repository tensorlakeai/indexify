use opentelemetry::InstrumentationScope;

use crate::log_record::LogRecord;

/// A batch of log records to be exported.
///
/// This is our own `LogBatch` type, replacing
/// `opentelemetry_sdk::logs::LogBatch` to work with our `LogRecord` type.
pub struct LogBatch<'a>(pub &'a [(&'a LogRecord, &'a InstrumentationScope)]);

impl<'a> LogBatch<'a> {
    pub fn new(data: &'a [(&'a LogRecord, &'a InstrumentationScope)]) -> Self {
        Self(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_batch() {
        let batch = LogBatch::new(&[]);
        assert!(batch.0.is_empty());
    }

    #[test]
    fn test_batch_length() {
        let record_a = LogRecord::default();
        let record_b = LogRecord::default();
        let scope_a = InstrumentationScope::builder("scope-a").build();
        let scope_b = InstrumentationScope::builder("scope-b").build();
        let pairs = [(&record_a, &scope_a), (&record_b, &scope_b)];

        let batch = LogBatch::new(&pairs);
        assert_eq!(batch.0.len(), 2);
    }

    #[test]
    fn test_batch_preserves_scope_names() {
        let record = LogRecord::default();
        let scope = InstrumentationScope::builder("my-scope").build();
        let pairs = [(&record, &scope)];

        let batch = LogBatch::new(&pairs);
        assert_eq!(batch.0[0].1.name(), "my-scope");
    }
}
