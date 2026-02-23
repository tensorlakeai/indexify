//! State reporter that collects command responses, allocation log entries,
//! and allocation outcomes for inclusion in heartbeat RPCs.
//!
//! Command responses (ContainerStarted/ContainerTerminated,
//! AllocationScheduled) are buffered as `CommandResponse` messages.
//!
//! Allocation log entries (CallFunction) are buffered as `AllocationLogEntry`
//! messages and sent via the heartbeat's `allocation_log_entries` field.
//!
//! Allocation outcomes (AllocationCompleted/AllocationFailed) are buffered as
//! `AllocationOutcome` messages and sent via the heartbeat's
//! `allocation_outcomes` field.
//!
//! All buffers use the same safe collect/drain_sent pattern: items are
//! **cloned** on collect and only removed after successful delivery. This
//! prevents data loss when a heartbeat RPC fails.
//!
//! Message size limiting: items are fragmented across multiple RPCs if the
//! total message would exceed 10 MB.

use std::sync::Arc;

use prost::Message;
use proto_api::executor_api_pb::{AllocationLogEntry, AllocationOutcome, CommandResponse};
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

/// Maximum state report message size in bytes (10 MB).
/// Matches Python executor's `_STATE_REPORT_MAX_MESSAGE_SIZE_MB`.
const STATE_REPORT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Generic pending buffer shared by responses and activities
// ---------------------------------------------------------------------------

/// A size-aware append-only buffer that supports collect-then-drain semantics.
///
/// Items arrive via an `mpsc::UnboundedReceiver` and are buffered in a `Vec`
/// behind a `Mutex`. The heartbeat loop collects a batch that fits within the
/// message size limit, sends the RPC, and then drains exactly the sent items.
struct PendingBuffer<T> {
    items: Arc<Mutex<Vec<T>>>,
    notify: Arc<Notify>,
}

impl<T: Message + Clone + Send + 'static> PendingBuffer<T> {
    fn new() -> Self {
        Self {
            items: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Spawn a background task that drains `rx` into the buffer and wakes
    /// the heartbeat loop via `notify`.
    fn spawn_drainer(&self, mut rx: mpsc::UnboundedReceiver<T>) {
        let items = self.items.clone();
        let notify = self.notify.clone();
        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                let mut buf = items.lock().await;
                buf.push(item);
                drop(buf);
                notify.notify_one();
            }
        });
    }

    /// Collect items that fit within the message size limit.
    ///
    /// `base_message_size` is the encoded size of the request envelope.
    ///
    /// Returns `(items, count, has_remaining)`. Items are **cloned** from the
    /// buffer. On success, call [`drain_sent`] with the returned `count` to
    /// remove them. On failure, do nothing — items stay for retry.
    async fn collect(&self, base_message_size: usize, label: &str) -> (Vec<T>, usize, bool) {
        let pending = self.items.lock().await;
        if pending.is_empty() {
            return (Vec::new(), 0, false);
        }

        let mut batch = Vec::new();
        let mut current_size = base_message_size;

        for item in pending.iter() {
            let item_size = item.encoded_len();

            if batch.is_empty() {
                // Always include at least one item to make forward progress.
                batch.push(item.clone());
                current_size += item_size;

                if current_size >= STATE_REPORT_MAX_MESSAGE_SIZE {
                    tracing::warn!(
                        size = current_size,
                        limit = STATE_REPORT_MAX_MESSAGE_SIZE,
                        label,
                        "Single item exceeds message size limit"
                    );
                }
            } else if current_size + item_size < STATE_REPORT_MAX_MESSAGE_SIZE {
                batch.push(item.clone());
                current_size += item_size;
            } else {
                debug!(
                    included = batch.len(),
                    remaining = pending.len() - batch.len(),
                    message_size = current_size,
                    label,
                    "Fragmenting report due to message size limit"
                );
                break;
            }
        }

        let count = batch.len();
        let has_remaining = count < pending.len();

        debug!(count, has_remaining, label, "Collected items for report");

        (batch, count, has_remaining)
    }

    /// Remove the first `count` items from the buffer after a successful RPC.
    ///
    /// Safe because new items are always appended to the end, so the first
    /// `count` items are exactly the ones returned by [`collect`].
    async fn drain_sent(&self, count: usize) {
        if count == 0 {
            return;
        }
        let mut pending = self.items.lock().await;
        let n = count.min(pending.len());
        pending.drain(..n);
    }
}

// ---------------------------------------------------------------------------
// StateReporter
// ---------------------------------------------------------------------------

/// Collects command responses, allocation activities, and allocation outcomes
/// for reporting.
pub struct StateReporter {
    responses: PendingBuffer<CommandResponse>,
    activities: PendingBuffer<AllocationLogEntry>,
    /// Allocation outcomes (AllocationCompleted/AllocationFailed) sent via the
    /// heartbeat's `allocation_outcomes` field for guaranteed delivery.
    outcomes: PendingBuffer<AllocationOutcome>,
}

impl StateReporter {
    /// Create a new state reporter and spawn background tasks that drain
    /// the response, container response, activity, and outcome channels and
    /// notify the respective loops.
    pub fn new(
        response_rx: mpsc::UnboundedReceiver<CommandResponse>,
        container_response_rx: mpsc::UnboundedReceiver<CommandResponse>,
        activity_rx: mpsc::UnboundedReceiver<AllocationLogEntry>,
        outcome_rx: mpsc::UnboundedReceiver<AllocationOutcome>,
    ) -> Self {
        let responses = PendingBuffer::new();
        let activities = PendingBuffer::new();
        let outcomes = PendingBuffer::new();

        // Two drainers feed the same response buffer (acks + container events).
        responses.spawn_drainer(response_rx);
        responses.spawn_drainer(container_response_rx);

        activities.spawn_drainer(activity_rx);
        outcomes.spawn_drainer(outcome_rx);

        Self {
            responses,
            activities,
            outcomes,
        }
    }

    /// Get a handle to the notify for waking up the result loop.
    pub fn results_notify(&self) -> Arc<Notify> {
        self.responses.notify.clone()
    }

    /// Get a handle to the notify for waking up when activities arrive.
    pub fn activities_notify(&self) -> Arc<Notify> {
        self.activities.notify.clone()
    }

    // ---------------------------------------------------------------
    // Command responses (report_command_responses)
    // ---------------------------------------------------------------

    /// Collect command responses that fit within the message size limit.
    pub async fn collect_responses(
        &self,
        base_message_size: usize,
    ) -> (Vec<CommandResponse>, usize, bool) {
        self.responses
            .collect(base_message_size, "command_responses")
            .await
    }

    /// Remove the first `count` responses from the buffer after a successful
    /// RPC.
    pub async fn drain_sent(&self, count: usize) {
        self.responses.drain_sent(count).await;
    }

    // ---------------------------------------------------------------
    // Allocation log entries (heartbeat)
    // ---------------------------------------------------------------

    /// Collect allocation log entries that fit within the message size limit.
    ///
    /// Items are **cloned** from the buffer. On successful delivery, call
    /// [`drain_sent_log_entries`] with the returned `count` to remove them.
    /// On failure, do nothing — items stay in the buffer for retry.
    pub async fn collect_log_entries(
        &self,
        base_message_size: usize,
    ) -> (Vec<AllocationLogEntry>, usize, bool) {
        self.activities
            .collect(base_message_size, "allocation_log_entries")
            .await
    }

    /// Remove the first `count` log entries from the buffer after a successful
    /// heartbeat RPC.
    pub async fn drain_sent_log_entries(&self, count: usize) {
        self.activities.drain_sent(count).await;
    }

    // ---------------------------------------------------------------
    // Allocation outcomes (report_allocation_activities)
    // ---------------------------------------------------------------

    /// Get a handle to the notify for waking up the outcome report loop.
    pub fn outcomes_notify(&self) -> Arc<Notify> {
        self.outcomes.notify.clone()
    }

    /// Collect allocation outcomes that fit within the message size limit.
    ///
    /// Items are **cloned** from the buffer. On successful delivery, call
    /// [`drain_sent_outcomes`] with the returned `count` to remove them.
    /// On failure, do nothing — items stay in the buffer for retry.
    pub async fn collect_outcomes(
        &self,
        base_message_size: usize,
    ) -> (Vec<AllocationOutcome>, usize, bool) {
        self.outcomes
            .collect(base_message_size, "allocation_outcomes")
            .await
    }

    /// Remove the first `count` outcomes from the buffer after a successful
    /// RPC.
    pub async fn drain_sent_outcomes(&self, count: usize) {
        self.outcomes.drain_sent(count).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a StateReporter and push responses directly into the
    /// buffer.
    async fn setup_reporter(responses: Vec<CommandResponse>) -> StateReporter {
        let (_tx, rx) = mpsc::unbounded_channel::<CommandResponse>();
        let (_cs_tx, cs_rx) = mpsc::unbounded_channel::<CommandResponse>();
        let (_act_tx, act_rx) = mpsc::unbounded_channel::<AllocationLogEntry>();
        let (_out_tx, out_rx) = mpsc::unbounded_channel::<AllocationOutcome>();
        let reporter = StateReporter::new(rx, cs_rx, act_rx, out_rx);
        {
            let mut pending = reporter.responses.items.lock().await;
            pending.extend(responses);
        }
        reporter
    }

    fn make_response(id: &str) -> CommandResponse {
        CommandResponse {
            command_seq: Some(1),
            response: Some(
                proto_api::executor_api_pb::command_response::Response::AllocationScheduled(
                    proto_api::executor_api_pb::AllocationScheduled {
                        allocation_id: id.to_string(),
                    },
                ),
            ),
        }
    }

    /// Create a CommandResponse with a large allocation_id to control encoded
    /// size.
    fn make_large_response(id: &str, payload_size: usize) -> CommandResponse {
        CommandResponse {
            command_seq: Some(1),
            response: Some(
                proto_api::executor_api_pb::command_response::Response::AllocationScheduled(
                    proto_api::executor_api_pb::AllocationScheduled {
                        allocation_id: format!("{}{}", id, "x".repeat(payload_size)),
                    },
                ),
            ),
        }
    }

    #[tokio::test]
    async fn test_collect_responses_empty_buffer() {
        let reporter = setup_reporter(vec![]).await;
        let (responses, count, has_remaining) = reporter.collect_responses(100).await;
        assert!(responses.is_empty());
        assert_eq!(count, 0);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_responses_all_fit() {
        let reporter = setup_reporter(vec![
            make_response("a1"),
            make_response("a2"),
            make_response("a3"),
        ])
        .await;

        let (responses, count, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 3);
        assert_eq!(count, 3);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_does_not_drain_buffer() {
        let reporter = setup_reporter(vec![make_response("a1"), make_response("a2")]).await;

        // Collect twice without draining — both should return the same results.
        let (first, ..) = reporter.collect_responses(0).await;
        let (second, ..) = reporter.collect_responses(0).await;
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
    }

    #[tokio::test]
    async fn test_drain_sent_removes_from_front() {
        let reporter = setup_reporter(vec![
            make_response("a1"),
            make_response("a2"),
            make_response("a3"),
        ])
        .await;

        // Collect all 3.
        let (batch, _count, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 3);

        // Drain 2 (simulate: only first 2 were sent in a fragmented RPC).
        reporter.drain_sent(2).await;

        // Only a3 remains.
        let (remaining, ..) = reporter.collect_responses(0).await;
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_collect_responses_fragmentation() {
        // Each large response is ~4 MB. With a 10 MB limit, only 2 should fit (8 MB <
        // 10 MB), but not 3 (12 MB > 10 MB).
        let four_mb = 4 * 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_response("a1", four_mb),
            make_large_response("a2", four_mb),
            make_large_response("a3", four_mb),
        ])
        .await;

        let (responses, count, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(
            responses.len(),
            2,
            "Only 2 of 3 responses should fit in 10 MB"
        );
        assert!(has_remaining);

        // Drain the sent batch.
        reporter.drain_sent(count).await;

        // Remaining response is still in the buffer.
        let (remaining, ..) = reporter.collect_responses(0).await;
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_collect_responses_always_includes_at_least_one() {
        // Single response larger than 10 MB — must still be included for progress.
        let twelve_mb = 12 * 1024 * 1024;
        let reporter = setup_reporter(vec![make_large_response("big", twelve_mb)]).await;

        let (responses, _, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 1, "Must include at least one response");
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_responses_respects_base_message_size() {
        // Base is 9 MB, so even a small response pushes past the 10 MB limit after the
        // first.
        let base_size = 9 * 1024 * 1024;
        let one_mb = 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_response("a1", one_mb),
            make_large_response("a2", one_mb),
        ])
        .await;

        let (responses, _, has_remaining) = reporter.collect_responses(base_size).await;
        assert_eq!(
            responses.len(),
            1,
            "Second response should not fit with 9 MB base"
        );
        assert!(has_remaining);
    }

    #[tokio::test]
    async fn test_failure_keeps_items_in_buffer() {
        let reporter = setup_reporter(vec![make_response("a1"), make_response("a2")]).await;

        // Collect returns clones, but buffer is untouched.
        let (batch, ..) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 2);

        // Don't call drain_sent (simulating RPC failure).

        // Items are still in the buffer.
        let (retry, ..) = reporter.collect_responses(0).await;
        assert_eq!(retry.len(), 2);
    }

    #[tokio::test]
    async fn test_drain_sent_does_not_affect_new_items() {
        let reporter = setup_reporter(vec![make_response("a1"), make_response("a2")]).await;

        // Collect 2 items.
        let (_, count, _) = reporter.collect_responses(0).await;
        assert_eq!(count, 2);

        // Simulate new item arriving between collect and drain.
        {
            let mut pending = reporter.responses.items.lock().await;
            pending.push(make_response("a3"));
        }

        // Drain only the 2 that were collected.
        reporter.drain_sent(count).await;

        // Only a3 should remain.
        let (remaining, ..) = reporter.collect_responses(0).await;
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_fragmentation_cycle() {
        // Simulates the full fragmentation cycle:
        // 1. Collect first batch
        // 2. Drain sent (RPC succeeded)
        // 3. Collect remaining batch
        let four_mb = 4 * 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_response("a1", four_mb),
            make_large_response("a2", four_mb),
            make_large_response("a3", four_mb),
        ])
        .await;

        // First collect: a1 + a2 fit (~8 MB < 10 MB), a3 remains
        let (batch1, count1, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(batch1.len(), 2);
        assert!(has_remaining);

        // RPC succeeded — drain sent.
        reporter.drain_sent(count1).await;

        // Second collect: only a3 left
        let (batch2, _, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(batch2.len(), 1);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_drain_from_channel() {
        let (tx, rx) = mpsc::unbounded_channel::<CommandResponse>();
        let (_cs_tx, cs_rx) = mpsc::unbounded_channel::<CommandResponse>();
        let (_act_tx, act_rx) = mpsc::unbounded_channel::<AllocationLogEntry>();
        let (_out_tx, out_rx) = mpsc::unbounded_channel::<AllocationOutcome>();
        let reporter = StateReporter::new(rx, cs_rx, act_rx, out_rx);

        // Send responses through the channel
        tx.send(make_response("c1")).unwrap();
        tx.send(make_response("c2")).unwrap();

        // Give the drain task a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (responses, ..) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 2);
    }

    #[tokio::test]
    async fn test_container_terminated_sent_and_drained() {
        let container_terminated = CommandResponse {
            command_seq: None,
            response: Some(
                proto_api::executor_api_pb::command_response::Response::ContainerTerminated(
                    proto_api::executor_api_pb::ContainerTerminated {
                        container_id: "fe-1".to_string(),
                        reason: proto_api::executor_api_pb::ContainerTerminationReason::Unhealthy
                            .into(),
                    },
                ),
            ),
        };

        let reporter = setup_reporter(vec![
            make_response("a1"),
            container_terminated,
            make_response("a2"),
        ])
        .await;

        // Collect all 3.
        let (batch, count, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 3);

        // Drain sent — all 3 removed.
        reporter.drain_sent(count).await;

        // Buffer is empty.
        let (remaining, ..) = reporter.collect_responses(0).await;
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_container_started_sent_and_drained() {
        // ContainerStarted responses are collected and drained like everything else.
        let container_started = CommandResponse {
            command_seq: None,
            response: Some(
                proto_api::executor_api_pb::command_response::Response::ContainerStarted(
                    proto_api::executor_api_pb::ContainerStarted {
                        container_id: "fe-1".to_string(),
                    },
                ),
            ),
        };

        let reporter = setup_reporter(vec![container_started]).await;

        let (batch, count, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 1);

        reporter.drain_sent(count).await;

        // Buffer is empty — ContainerStarted was drained.
        let (remaining, ..) = reporter.collect_responses(0).await;
        assert!(remaining.is_empty());
    }
}
