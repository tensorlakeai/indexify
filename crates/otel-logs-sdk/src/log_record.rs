use std::time::SystemTime;

use opentelemetry::{
    Key,
    logs::{AnyValue, Severity},
};

/// Our own log record type, independent of
/// `opentelemetry_sdk::logs::SdkLogRecord`.
///
/// `SdkLogRecord` has a `pub(crate)` constructor, making it impossible to
/// create from outside the SDK. This type provides the same fields with public
/// constructors and a `Default` impl for use with `AsyncBatchLogProcessor`.
#[derive(Debug, Clone, Default)]
pub struct LogRecord {
    pub timestamp: Option<SystemTime>,
    pub observed_timestamp: Option<SystemTime>,
    pub severity_text: Option<String>,
    pub severity_number: Option<Severity>,
    pub body: Option<AnyValue>,
    pub attributes: Vec<(Key, AnyValue)>,
}

impl LogRecord {
    pub fn set_timestamp(&mut self, t: SystemTime) {
        self.timestamp = Some(t);
    }

    pub fn set_observed_timestamp(&mut self, t: SystemTime) {
        self.observed_timestamp = Some(t);
    }

    pub fn set_body(&mut self, b: AnyValue) {
        self.body = Some(b);
    }

    pub fn set_severity_text(&mut self, s: &str) {
        self.severity_text = Some(s.to_owned());
    }

    pub fn set_severity_number(&mut self, n: Severity) {
        self.severity_number = Some(n);
    }

    pub fn add_attribute<K: Into<Key>, V: Into<AnyValue>>(&mut self, k: K, v: V) {
        self.attributes.push((k.into(), v.into()));
    }

    pub fn body(&self) -> Option<&AnyValue> {
        self.body.as_ref()
    }

    pub fn attributes_iter(&self) -> impl Iterator<Item = (Key, AnyValue)> + '_ {
        self.attributes.iter().map(|(k, v)| (k.clone(), v.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    #[test]
    fn test_default_is_empty() {
        let record = LogRecord::default();
        assert!(record.timestamp.is_none());
        assert!(record.observed_timestamp.is_none());
        assert!(record.severity_text.is_none());
        assert!(record.severity_number.is_none());
        assert!(record.body.is_none());
        assert!(record.attributes.is_empty());
    }

    #[test]
    fn test_set_timestamp() {
        let ts = UNIX_EPOCH + Duration::from_secs(100);
        let mut record = LogRecord::default();
        record.set_timestamp(ts);
        assert_eq!(record.timestamp, Some(ts));
    }

    #[test]
    fn test_set_observed_timestamp() {
        let ts = UNIX_EPOCH + Duration::from_secs(200);
        let mut record = LogRecord::default();
        record.set_observed_timestamp(ts);
        assert_eq!(record.observed_timestamp, Some(ts));
    }

    #[test]
    fn test_set_body() {
        let mut record = LogRecord::default();
        record.set_body(AnyValue::String("hello".into()));
        assert_eq!(record.body, Some(AnyValue::String("hello".into())));
    }

    #[test]
    fn test_set_severity_text() {
        let mut record = LogRecord::default();
        record.set_severity_text("WARN");
        assert_eq!(record.severity_text, Some("WARN".to_string()));
    }

    #[test]
    fn test_set_severity_number() {
        let mut record = LogRecord::default();
        record.set_severity_number(Severity::Error);
        assert_eq!(record.severity_number, Some(Severity::Error));
    }

    #[test]
    fn test_add_attribute() {
        let mut record = LogRecord::default();
        record.add_attribute("key1", AnyValue::String("val1".into()));
        record.add_attribute("key2", AnyValue::Int(42));
        assert_eq!(record.attributes.len(), 2);
        assert_eq!(record.attributes[0].0.as_str(), "key1");
        assert_eq!(record.attributes[1].0.as_str(), "key2");
    }

    #[test]
    fn test_clone() {
        let mut record = LogRecord::default();
        record.set_body(AnyValue::String("body".into()));
        record.add_attribute("k", AnyValue::Boolean(true));

        let cloned = record.clone();
        assert_eq!(cloned.body, record.body);
        assert_eq!(cloned.attributes.len(), record.attributes.len());
    }
}
