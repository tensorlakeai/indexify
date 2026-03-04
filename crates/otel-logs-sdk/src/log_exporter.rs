use std::fmt;

use opentelemetry_sdk::{Resource, error::OTelSdkResult};

use crate::log_batch::LogBatch;

/// Log exporter trait for use with `AsyncBatchLogProcessor`.
///
/// This replaces `opentelemetry_sdk::logs::LogExporter` and works with our
/// own `LogBatch` type. All implementors must be `Send + Sync` so they can
/// be owned by a background Tokio task.
pub trait LogExporter: Send + Sync + fmt::Debug {
    fn export(
        &self,
        batch: LogBatch<'_>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send;

    fn set_resource(&mut self, resource: &Resource);

    fn shutdown(&self) -> OTelSdkResult {
        Ok(())
    }
}
