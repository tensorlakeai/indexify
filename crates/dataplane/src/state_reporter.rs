//! State reporter that collects command responses for inclusion in
//! `report_command_responses` RPCs.
//!
//! Allocation results (AllocationCompleted/AllocationFailed) and container
//! lifecycle events (ContainerTerminated/ContainerStarted) are buffered as
//! `CommandResponse` messages and sent when new responses arrive.
//!
//! Message size limiting: responses are fragmented across multiple RPCs if
//! the total message would exceed 10 MB. Responses are drained from the
//! buffer on collect and only re-added on RPC failure, eliminating race
//! conditions between the drain task and the heartbeat loop.

use std::sync::Arc;

use prost::Message;
use proto_api::executor_api_pb::CommandResponse;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

/// Maximum state report message size in bytes (10 MB).
/// Matches Python executor's `_STATE_REPORT_MAX_MESSAGE_SIZE_MB`.
const STATE_REPORT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Collects command responses for `report_command_responses`.
pub struct StateReporter {
    /// Buffered responses waiting to be sent (allocation results +
    /// container lifecycle events).
    pending_responses: Arc<Mutex<Vec<CommandResponse>>>,
    /// Notify when new responses are available.
    results_notify: Arc<Notify>,
}

impl StateReporter {
    /// Create a new state reporter and spawn background tasks that drain
    /// the response and container response channels and notify the result
    /// loop.
    pub fn new(
        response_rx: mpsc::UnboundedReceiver<CommandResponse>,
        container_response_rx: mpsc::UnboundedReceiver<CommandResponse>,
    ) -> Self {
        let pending_responses = Arc::new(Mutex::new(Vec::new()));
        let results_notify = Arc::new(Notify::new());

        // Spawn background drainer for allocation responses
        {
            let pending = pending_responses.clone();
            let notify = results_notify.clone();
            tokio::spawn(async move {
                drain_responses(response_rx, pending, notify).await;
            });
        }

        // Spawn background drainer for container lifecycle responses
        {
            let pending = pending_responses.clone();
            let notify = results_notify.clone();
            tokio::spawn(async move {
                drain_responses(container_response_rx, pending, notify).await;
            });
        }

        Self {
            pending_responses,
            results_notify,
        }
    }

    /// Get a handle to the notify for waking up the result loop.
    pub fn results_notify(&self) -> Arc<Notify> {
        self.results_notify.clone()
    }

    /// Collect command responses that fit within the message size limit.
    ///
    /// `base_message_size` is the encoded size of the request without any
    /// responses (executor_id + overhead).
    ///
    /// Returns `(responses, has_remaining)`. Responses are **drained** from
    /// the buffer — on RPC success they are simply dropped. On RPC failure,
    /// call [`re_add_failed_responses`] to put them back for retry.
    pub async fn collect_responses(
        &self,
        base_message_size: usize,
    ) -> (Vec<CommandResponse>, bool) {
        let mut pending = self.pending_responses.lock().await;
        if pending.is_empty() {
            return (Vec::new(), false);
        }

        let mut count = 0;
        let mut current_size = base_message_size;

        for response in pending.iter() {
            let response_size = response.encoded_len();

            if count == 0 {
                // Always include at least one response to make forward progress.
                count += 1;
                current_size += response_size;

                if current_size >= STATE_REPORT_MAX_MESSAGE_SIZE {
                    tracing::warn!(
                        size = current_size,
                        limit = STATE_REPORT_MAX_MESSAGE_SIZE,
                        "Single command response exceeds message size limit"
                    );
                }
            } else if current_size + response_size < STATE_REPORT_MAX_MESSAGE_SIZE {
                count += 1;
                current_size += response_size;
            } else {
                // Would exceed limit — stop and let remaining responses be sent
                // in the next heartbeat.
                debug!(
                    included = count,
                    remaining = pending.len() - count,
                    message_size = current_size,
                    "Fragmenting state report due to message size limit"
                );
                break;
            }
        }

        let has_remaining = count < pending.len();

        // Drain the first `count` items from the buffer. New items pushed
        // by drain_responses go at the end, so this is safe even under
        // concurrent access (the lock is held for the full operation).
        let responses: Vec<CommandResponse> = pending.drain(..count).collect();

        debug!(
            count = responses.len(),
            has_remaining, "Collected command responses for report_command_responses"
        );

        (responses, has_remaining)
    }

    /// Re-add responses to the buffer after a failed RPC.
    ///
    /// Responses are prepended to the front of the buffer so they are sent
    /// first on the next attempt.
    pub async fn re_add_failed_responses(&self, responses: Vec<CommandResponse>) {
        if responses.is_empty() {
            return;
        }
        let mut pending = self.pending_responses.lock().await;
        // Splice the failed responses back to the front.
        let tail = pending.split_off(0);
        *pending = responses;
        pending.extend(tail);
    }
}

/// Background task: drains a command response channel into the shared buffer.
async fn drain_responses(
    mut rx: mpsc::UnboundedReceiver<CommandResponse>,
    pending: Arc<Mutex<Vec<CommandResponse>>>,
    notify: Arc<Notify>,
) {
    while let Some(response) = rx.recv().await {
        let mut buf = pending.lock().await;
        buf.push(response);
        drop(buf);
        notify.notify_one();
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
        let reporter = StateReporter::new(rx, cs_rx);
        {
            let mut pending = reporter.pending_responses.lock().await;
            pending.extend(responses);
        }
        reporter
    }

    fn make_response(id: &str) -> CommandResponse {
        CommandResponse {
            command_seq: 0,
            response: Some(
                proto_api::executor_api_pb::command_response::Response::AllocationFailed(
                    proto_api::executor_api_pb::AllocationFailed {
                        allocation_id: id.to_string(),
                        reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError
                            .into(),
                        ..Default::default()
                    },
                ),
            ),
        }
    }

    /// Create a CommandResponse with a large request_id to control encoded
    /// size.
    fn make_large_response(id: &str, payload_size: usize) -> CommandResponse {
        CommandResponse {
            command_seq: 0,
            response: Some(
                proto_api::executor_api_pb::command_response::Response::AllocationFailed(
                    proto_api::executor_api_pb::AllocationFailed {
                        allocation_id: id.to_string(),
                        reason: proto_api::executor_api_pb::AllocationFailureReason::InternalError
                            .into(),
                        request_id: Some("x".repeat(payload_size)),
                        ..Default::default()
                    },
                ),
            ),
        }
    }

    #[tokio::test]
    async fn test_collect_responses_empty_buffer() {
        let reporter = setup_reporter(vec![]).await;
        let (responses, has_remaining) = reporter.collect_responses(100).await;
        assert!(responses.is_empty());
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

        let (responses, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 3);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_collect_drains_buffer() {
        let reporter = setup_reporter(vec![make_response("a1"), make_response("a2")]).await;

        // First collect drains the buffer.
        let (first, _) = reporter.collect_responses(0).await;
        assert_eq!(first.len(), 2);

        // Second collect returns empty — buffer was drained.
        let (second, _) = reporter.collect_responses(0).await;
        assert!(second.is_empty());
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

        let (responses, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(
            responses.len(),
            2,
            "Only 2 of 3 responses should fit in 10 MB"
        );
        assert!(has_remaining);

        // Remaining response is still in the buffer.
        let (remaining, _) = reporter.collect_responses(0).await;
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_collect_responses_always_includes_at_least_one() {
        // Single response larger than 10 MB — must still be included for progress.
        let twelve_mb = 12 * 1024 * 1024;
        let reporter = setup_reporter(vec![make_large_response("big", twelve_mb)]).await;

        let (responses, has_remaining) = reporter.collect_responses(0).await;
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

        let (responses, has_remaining) = reporter.collect_responses(base_size).await;
        assert_eq!(
            responses.len(),
            1,
            "Second response should not fit with 9 MB base"
        );
        assert!(has_remaining);
    }

    #[tokio::test]
    async fn test_re_add_failed_responses() {
        let reporter = setup_reporter(vec![]).await;

        // Simulate: collect drains, RPC fails, re-add.
        {
            let mut pending = reporter.pending_responses.lock().await;
            pending.push(make_response("a1"));
            pending.push(make_response("a2"));
        }

        let (batch, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 2);

        // Buffer should be empty now.
        let (empty, _) = reporter.collect_responses(0).await;
        assert!(empty.is_empty());

        // Re-add on failure.
        reporter.re_add_failed_responses(batch).await;

        // Buffer should have them back.
        let (retry, _) = reporter.collect_responses(0).await;
        assert_eq!(retry.len(), 2);
    }

    #[tokio::test]
    async fn test_re_add_preserves_order_with_new_items() {
        let reporter = setup_reporter(vec![]).await;

        // Add initial items and collect.
        {
            let mut pending = reporter.pending_responses.lock().await;
            pending.push(make_response("a1"));
            pending.push(make_response("a2"));
        }
        let (batch, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 2);

        // Simulate new items arriving while RPC is in flight.
        {
            let mut pending = reporter.pending_responses.lock().await;
            pending.push(make_response("a3"));
        }

        // Re-add failed batch — should go to front.
        reporter.re_add_failed_responses(batch).await;

        let (retry, _) = reporter.collect_responses(0).await;
        assert_eq!(retry.len(), 3);
        // a1 and a2 should be before a3.
        let ids: Vec<String> = retry
            .iter()
            .filter_map(|r| {
                crate::function_executor::proto_convert::command_response_allocation_id(r)
                    .map(|s| s.to_string())
            })
            .collect();
        assert_eq!(ids, vec!["a1", "a2", "a3"]);
    }

    #[tokio::test]
    async fn test_fragmentation_cycle() {
        // Simulates the full fragmentation cycle:
        // 1. Collect first batch (drains)
        // 2. Drop batch (RPC succeeded)
        // 3. Collect remaining batch
        let four_mb = 4 * 1024 * 1024;
        let reporter = setup_reporter(vec![
            make_large_response("a1", four_mb),
            make_large_response("a2", four_mb),
            make_large_response("a3", four_mb),
        ])
        .await;

        // First collect: a1 + a2 fit (~8 MB < 10 MB), a3 remains
        let (batch1, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(batch1.len(), 2);
        assert!(has_remaining);

        // Drop batch1 (RPC succeeded).

        // Second collect: only a3 left
        let (batch2, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(batch2.len(), 1);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_drain_from_channel() {
        let (tx, rx) = mpsc::unbounded_channel::<CommandResponse>();
        let (_cs_tx, cs_rx) = mpsc::unbounded_channel::<CommandResponse>();
        let reporter = StateReporter::new(rx, cs_rx);

        // Send responses through the channel
        tx.send(make_response("c1")).unwrap();
        tx.send(make_response("c2")).unwrap();

        // Give the drain task a moment to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (responses, _) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 2);
    }

    #[tokio::test]
    async fn test_container_terminated_drained_with_collect() {
        let container_terminated = CommandResponse {
            command_seq: 0,
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

        // Collect drains everything.
        let (batch, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 3);

        // Buffer is empty — no stale ContainerTerminated.
        let (remaining, _) = reporter.collect_responses(0).await;
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_container_started_drained_with_collect() {
        // ContainerStarted responses should also be drained by collect
        // (previously they accumulated forever).
        let container_started = CommandResponse {
            command_seq: 0,
            response: Some(
                proto_api::executor_api_pb::command_response::Response::ContainerStarted(
                    proto_api::executor_api_pb::ContainerStarted {
                        container_id: "fe-1".to_string(),
                    },
                ),
            ),
        };

        let reporter = setup_reporter(vec![container_started]).await;

        let (batch, _) = reporter.collect_responses(0).await;
        assert_eq!(batch.len(), 1);

        // Buffer is empty — ContainerStarted was drained.
        let (remaining, _) = reporter.collect_responses(0).await;
        assert!(remaining.is_empty());
    }
}
