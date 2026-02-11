//! State reporter that collects allocation results from FE controllers
//! and includes them in heartbeat updates.
//!
//! Results are buffered and sent in the next heartbeat. When new results
//! arrive, the heartbeat loop is notified to send immediately (matching Python
//! executor behavior with schedule_state_report).
//!
//! Message size limiting: allocation results are fragmented across multiple
//! heartbeat RPCs if the total message would exceed 10 MB. Results are only
//! removed from the buffer after successful delivery. This matches the Python
//! executor's `_collect_report_state_request()` fragmentation logic.

use std::sync::Arc;

use prost::Message;
use proto_api::executor_api_pb::AllocationResult;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

/// Maximum state report message size in bytes (10 MB).
/// Matches Python executor's `_STATE_REPORT_MAX_MESSAGE_SIZE_MB`.
const STATE_REPORT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Collects allocation results from FE controllers for inclusion in heartbeats.
pub struct StateReporter {
    /// Buffered results waiting to be sent in next heartbeat.
    pending_results: Arc<Mutex<Vec<AllocationResult>>>,
    /// Notify when new results are available (wakes up heartbeat loop).
    results_notify: Arc<Notify>,
}

impl StateReporter {
    /// Create a new state reporter and spawn a background task that drains
    /// the result channel and notifies the heartbeat loop.
    pub fn new(result_rx: mpsc::UnboundedReceiver<AllocationResult>) -> Self {
        let pending_results = Arc::new(Mutex::new(Vec::new()));
        let results_notify = Arc::new(Notify::new());

        // Spawn background drainer task
        let pending = pending_results.clone();
        let notify = results_notify.clone();
        tokio::spawn(async move {
            drain_results(result_rx, pending, notify).await;
        });

        Self {
            pending_results,
            results_notify,
        }
    }

    /// Get a handle to the notify for waking up the heartbeat loop.
    pub fn results_notify(&self) -> Arc<Notify> {
        self.results_notify.clone()
    }

    /// Collect allocation results that fit within the message size limit.
    ///
    /// `base_message_size` is the encoded size of the request without any
    /// allocation results (executor_state + empty executor_update).
    ///
    /// Returns `(results, has_remaining)`. Results are cloned from the buffer
    /// and NOT removed — call [`remove_reported_results`] after a successful
    /// RPC.
    pub async fn collect_results(&self, base_message_size: usize) -> (Vec<AllocationResult>, bool) {
        let pending = self.pending_results.lock().await;
        if pending.is_empty() {
            return (Vec::new(), false);
        }

        let mut results = Vec::new();
        let mut current_size = base_message_size;

        for result in pending.iter() {
            let result_size = result.encoded_len();

            if results.is_empty() {
                // Always include at least one result to make forward progress.
                results.push(result.clone());
                current_size += result_size;

                if current_size >= STATE_REPORT_MAX_MESSAGE_SIZE {
                    tracing::warn!(
                        size = current_size,
                        limit = STATE_REPORT_MAX_MESSAGE_SIZE,
                        "Single allocation result exceeds message size limit"
                    );
                }
            } else if current_size + result_size < STATE_REPORT_MAX_MESSAGE_SIZE {
                results.push(result.clone());
                current_size += result_size;
            } else {
                // Would exceed limit — stop and let remaining results be sent
                // in the next heartbeat.
                debug!(
                    included = results.len(),
                    remaining = pending.len() - results.len(),
                    message_size = current_size,
                    "Fragmenting state report due to message size limit"
                );
                break;
            }
        }

        let has_remaining = results.len() < pending.len();

        debug!(
            count = results.len(),
            has_remaining, "Collected allocation results for heartbeat"
        );

        (results, has_remaining)
    }

    /// Remove allocation results that were successfully reported to the server.
    /// Called after a successful heartbeat RPC.
    pub async fn remove_reported_results(&self, allocation_ids: &[String]) {
        if allocation_ids.is_empty() {
            return;
        }
        let mut pending = self.pending_results.lock().await;
        pending.retain(|r| {
            r.allocation_id
                .as_ref()
                .map(|id| !allocation_ids.contains(id))
                .unwrap_or(true)
        });
    }
}

/// Background task: drains the result channel and notifies the heartbeat loop.
async fn drain_results(
    mut result_rx: mpsc::UnboundedReceiver<AllocationResult>,
    pending_results: Arc<Mutex<Vec<AllocationResult>>>,
    results_notify: Arc<Notify>,
) {
    while let Some(result) = result_rx.recv().await {
        let mut pending = pending_results.lock().await;
        pending.push(result);
        drop(pending);
        // Wake up heartbeat loop to send results immediately
        results_notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a StateReporter and push results directly into the
    /// buffer.
    async fn setup_reporter(results: Vec<AllocationResult>) -> StateReporter {
        let (_tx, rx) = mpsc::unbounded_channel::<AllocationResult>();
        let reporter = StateReporter::new(rx);
        {
            let mut pending = reporter.pending_results.lock().await;
            pending.extend(results);
        }
        reporter
    }

    fn make_result(id: &str) -> AllocationResult {
        AllocationResult {
            allocation_id: Some(id.to_string()),
            ..Default::default()
        }
    }

    /// Create an AllocationResult with a large request_id to control encoded
    /// size.
    fn make_large_result(id: &str, payload_size: usize) -> AllocationResult {
        AllocationResult {
            allocation_id: Some(id.to_string()),
            request_id: Some("x".repeat(payload_size)),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_collect_results_empty_buffer() {
        let reporter = setup_reporter(vec![]).await;
        let (results, has_remaining) = reporter.collect_results(100).await;
        assert!(results.is_empty());
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_results_all_fit() {
        let reporter = setup_reporter(vec![
            make_result("a1"),
            make_result("a2"),
            make_result("a3"),
        ])
        .await;

        let (results, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 3);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_results_does_not_drain_buffer() {
        let reporter = setup_reporter(vec![make_result("a1"), make_result("a2")]).await;

        // Collect twice — both should return the same results since buffer is not
        // drained.
        let (first, _) = reporter.collect_results(0).await;
        let (second, _) = reporter.collect_results(0).await;
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn test_collect_results_fragmentation() {
        // Each large result is ~4 MB. With a 10 MB limit, only 2 should fit (8 MB < 10
        // MB), but not 3 (12 MB > 10 MB).
        let four_mb = 4 * 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_result("a1", four_mb),
            make_large_result("a2", four_mb),
            make_large_result("a3", four_mb),
        ])
        .await;

        let (results, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 2, "Only 2 of 3 results should fit in 10 MB");
        assert!(has_remaining);
        assert_eq!(results[0].allocation_id.as_deref(), Some("a1"));
        assert_eq!(results[1].allocation_id.as_deref(), Some("a2"));
    }

    #[tokio::test]
    async fn test_collect_results_always_includes_at_least_one() {
        // Single result larger than 10 MB — must still be included for progress.
        let twelve_mb = 12 * 1024 * 1024;
        let reporter = setup_reporter(vec![make_large_result("big", twelve_mb)]).await;

        let (results, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 1, "Must include at least one result");
        assert!(!has_remaining);
        assert_eq!(results[0].allocation_id.as_deref(), Some("big"));
    }

    #[tokio::test]
    async fn test_collect_results_respects_base_message_size() {
        // Base is 9 MB, so even a small result pushes past the 10 MB limit after the
        // first.
        let base_size = 9 * 1024 * 1024;
        let one_mb = 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_result("a1", one_mb),
            make_large_result("a2", one_mb),
        ])
        .await;

        let (results, has_remaining) = reporter.collect_results(base_size).await;
        assert_eq!(
            results.len(),
            1,
            "Second result should not fit with 9 MB base"
        );
        assert!(has_remaining);
    }

    #[tokio::test]
    async fn test_remove_reported_results() {
        let reporter = setup_reporter(vec![
            make_result("a1"),
            make_result("a2"),
            make_result("a3"),
        ])
        .await;

        reporter
            .remove_reported_results(&["a1".to_string(), "a3".to_string()])
            .await;

        let (results, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 1);
        assert!(!has_remaining);
        assert_eq!(results[0].allocation_id.as_deref(), Some("a2"));
    }

    #[tokio::test]
    async fn test_remove_then_collect_returns_remaining() {
        // Simulates the full fragmentation cycle:
        // 1. Collect first batch
        // 2. Remove reported results (RPC succeeded)
        // 3. Collect remaining batch
        let four_mb = 4 * 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_result("a1", four_mb),
            make_large_result("a2", four_mb),
            make_large_result("a3", four_mb),
        ])
        .await;

        // First collect: a1 + a2 fit (~8 MB < 10 MB), a3 remains
        let (batch1, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(batch1.len(), 2);
        assert!(has_remaining);

        // Simulate successful RPC: remove a1 and a2
        reporter
            .remove_reported_results(&["a1".to_string(), "a2".to_string()])
            .await;

        // Second collect: only a3 left
        let (batch2, has_remaining) = reporter.collect_results(0).await;
        assert_eq!(batch2.len(), 1);
        assert!(!has_remaining);
        assert_eq!(batch2[0].allocation_id.as_deref(), Some("a3"));
    }

    #[tokio::test]
    async fn test_remove_empty_ids_is_noop() {
        let reporter = setup_reporter(vec![make_result("a1")]).await;
        reporter.remove_reported_results(&[]).await;
        let (results, _) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_drain_from_channel() {
        let (tx, rx) = mpsc::unbounded_channel::<AllocationResult>();
        let reporter = StateReporter::new(rx);

        // Send results through the channel
        tx.send(make_result("c1")).unwrap();
        tx.send(make_result("c2")).unwrap();

        // Give the drain task a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (results, _) = reporter.collect_results(0).await;
        assert_eq!(results.len(), 2);
    }
}
