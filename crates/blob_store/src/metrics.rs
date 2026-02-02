//! Metrics for blob store operations.

use std::time::Instant;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};

/// Metrics for blob storage operations.
#[derive(Clone)]
pub struct BlobMetrics {
    /// Histogram for operation latencies.
    pub operations: Histogram<f64>,

    /// Counter for operation errors.
    pub errors: Counter<u64>,
}

impl BlobMetrics {
    /// Create new metrics from a meter.
    pub fn new(meter: &Meter) -> Self {
        let operations = meter
            .f64_histogram("blob_storage_operation_duration_seconds")
            .with_description("Duration of blob storage operations in seconds")
            .build();

        let errors = meter
            .u64_counter("blob_storage_errors_total")
            .with_description("Total number of blob storage errors")
            .build();

        Self { operations, errors }
    }
}

/// Timer for measuring operation duration.
pub struct Timer {
    start: Instant,
    histogram: Histogram<f64>,
    labels: Vec<KeyValue>,
}

impl Timer {
    /// Start a new timer with labels.
    pub fn start_with_labels(histogram: &Histogram<f64>, labels: &[KeyValue]) -> Self {
        Self {
            start: Instant::now(),
            histogram: histogram.clone(),
            labels: labels.to_vec(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let duration = self.start.elapsed().as_secs_f64();
        self.histogram.record(duration, &self.labels);
    }
}
