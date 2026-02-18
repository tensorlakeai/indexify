//! State reporter that collects command responses for inclusion in
//! `report_command_responses` RPCs.
//!
//! Allocation results (AllocationCompleted/AllocationFailed) and container
//! lifecycle events (ContainerTerminated) are buffered as `CommandResponse`
//! messages and sent when new responses arrive.
//!
//! Message size limiting: responses are fragmented across multiple RPCs if
//! the total message would exceed 10 MB. Allocation responses are only
//! removed from the buffer after successful delivery. ContainerTerminated
//! responses are always drained (no retry-buffer needed since they're small
//! and idempotent).

use std::sync::Arc;

use prost::Message;
use proto_api::executor_api_pb::CommandResponse;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

use crate::function_executor::proto_convert;

/// Maximum state report message size in bytes (10 MB).
/// Matches Python executor's `_STATE_REPORT_MAX_MESSAGE_SIZE_MB`.
const STATE_REPORT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Collects command responses for `report_command_responses`.
pub struct StateReporter {
    /// Buffered responses waiting to be sent (allocation results +
    /// container terminated events).
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
    /// Returns `(responses, has_remaining)`. Responses are cloned from the
    /// buffer and NOT removed — call [`remove_reported_responses`] after a
    /// successful RPC.
    pub async fn collect_responses(
        &self,
        base_message_size: usize,
    ) -> (Vec<CommandResponse>, bool) {
        let pending = self.pending_responses.lock().await;
        if pending.is_empty() {
            return (Vec::new(), false);
        }

        let mut responses = Vec::new();
        let mut current_size = base_message_size;

        for response in pending.iter() {
            let response_size = response.encoded_len();

            if responses.is_empty() {
                // Always include at least one response to make forward progress.
                responses.push(response.clone());
                current_size += response_size;

                if current_size >= STATE_REPORT_MAX_MESSAGE_SIZE {
                    tracing::warn!(
                        size = current_size,
                        limit = STATE_REPORT_MAX_MESSAGE_SIZE,
                        "Single command response exceeds message size limit"
                    );
                }
            } else if current_size + response_size < STATE_REPORT_MAX_MESSAGE_SIZE {
                responses.push(response.clone());
                current_size += response_size;
            } else {
                // Would exceed limit — stop and let remaining responses be sent
                // in the next heartbeat.
                debug!(
                    included = responses.len(),
                    remaining = pending.len() - responses.len(),
                    message_size = current_size,
                    "Fragmenting state report due to message size limit"
                );
                break;
            }
        }

        let has_remaining = responses.len() < pending.len();

        debug!(
            count = responses.len(),
            has_remaining, "Collected command responses for report_command_responses"
        );

        (responses, has_remaining)
    }

    /// Remove command responses that were successfully reported to the server.
    /// Called after a successful `report_command_responses` RPC.
    ///
    /// Allocation responses (AllocationCompleted/AllocationFailed) are removed
    /// by allocation_id. ContainerTerminated responses are always drained
    /// (removed unconditionally) since they're small and idempotent.
    pub async fn remove_reported_responses(&self, reported_allocation_ids: &[String]) {
        let mut pending = self.pending_responses.lock().await;
        pending.retain(|r| {
            // ContainerTerminated responses are always drained
            if matches!(
                &r.response,
                Some(
                    proto_api::executor_api_pb::command_response::Response::ContainerTerminated(_)
                )
            ) {
                return false;
            }

            // Allocation responses are removed by ID
            if let Some(alloc_id) = proto_convert::command_response_allocation_id(r) {
                return !reported_allocation_ids.contains(&alloc_id.to_string());
            }

            // Unknown responses — keep them (shouldn't happen)
            true
        });
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
    async fn test_collect_responses_does_not_drain_buffer() {
        let reporter = setup_reporter(vec![make_response("a1"), make_response("a2")]).await;

        // Collect twice — both should return the same results since buffer is not
        // drained.
        let (first, _) = reporter.collect_responses(0).await;
        let (second, _) = reporter.collect_responses(0).await;
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
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
    async fn test_remove_reported_responses() {
        let reporter = setup_reporter(vec![
            make_response("a1"),
            make_response("a2"),
            make_response("a3"),
        ])
        .await;

        reporter
            .remove_reported_responses(&["a1".to_string(), "a3".to_string()])
            .await;

        let (responses, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 1);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_remove_then_collect_returns_remaining() {
        // Simulates the full fragmentation cycle:
        // 1. Collect first batch
        // 2. Remove reported responses (RPC succeeded)
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

        // Simulate successful RPC: remove a1 and a2
        reporter
            .remove_reported_responses(&["a1".to_string(), "a2".to_string()])
            .await;

        // Second collect: only a3 left
        let (batch2, has_remaining) = reporter.collect_responses(0).await;
        assert_eq!(batch2.len(), 1);
        assert!(!has_remaining);
    }

    #[tokio::test]
    async fn test_remove_empty_ids_is_noop() {
        let reporter = setup_reporter(vec![make_response("a1")]).await;
        reporter.remove_reported_responses(&[]).await;
        let (responses, _) = reporter.collect_responses(0).await;
        assert_eq!(responses.len(), 1);
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
    async fn test_container_terminated_responses_drained_on_remove() {
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

        // Remove reported: a1 was reported, container terminated should also be drained
        reporter
            .remove_reported_responses(&["a1".to_string()])
            .await;

        let (responses, _) = reporter.collect_responses(0).await;
        // Only a2 should remain — a1 was removed by ID, container_terminated was
        // drained
        assert_eq!(responses.len(), 1);
    }
}
