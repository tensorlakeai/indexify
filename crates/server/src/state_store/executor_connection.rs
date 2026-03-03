use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};

use prost::Message;
use tokio::sync::Notify;

use crate::executor_api::executor_api_pb;

/// Server-side connection state for a single executor.
/// Created on registration, destroyed on deregistration.
///
/// Holds buffered commands/results for long-poll delivery. Command emission is
/// update-driven by scheduler batches and full-sync reconnects.
#[derive(Clone)]
pub struct ExecutorConnection {
    /// Serializes command emission + outbox enqueue per executor.
    /// Prevents concurrent emission tasks from interleaving sequence updates.
    pub command_emit_lock: Arc<tokio::sync::Mutex<()>>,

    /// Buffered commands for poll_commands delivery.
    pending_commands: Arc<tokio::sync::Mutex<Vec<executor_api_pb::Command>>>,
    /// Wakes a held poll_commands request when new commands arrive.
    commands_notify: Arc<Notify>,

    /// Buffered results for poll_allocation_results delivery.
    pending_results: Arc<tokio::sync::Mutex<Vec<executor_api_pb::SequencedAllocationResult>>>,
    /// Monotonic counter for result sequence numbers.
    next_result_seq: Arc<AtomicU64>,
    /// Highest command sequence number acked by the dataplane.
    /// Used to detect executor restarts (ack regression to 0/None).
    last_acked_command_seq: Arc<AtomicU64>,
    /// One-shot flag consumed by heartbeat to request a full-state sync.
    request_full_state: Arc<atomic::AtomicBool>,
    /// Wakes a held poll_allocation_results request when new results arrive.
    results_notify: Arc<Notify>,
}

impl ExecutorConnection {
    fn capped_clone_by_encoded_size<T: Message + Clone>(items: &[T], limit_bytes: usize) -> Vec<T> {
        if items.is_empty() {
            return Vec::new();
        }

        let mut selected = Vec::new();
        let mut total = 0usize;
        for item in items {
            let item_size = item.encoded_len();
            if selected.is_empty() || total.saturating_add(item_size) <= limit_bytes {
                total = total.saturating_add(item_size);
                selected.push(item.clone());
            } else {
                break;
            }
        }
        selected
    }

    /// Create a new connection (executor just registered).
    pub fn new() -> Self {
        Self {
            command_emit_lock: Arc::new(tokio::sync::Mutex::new(())),
            pending_commands: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            commands_notify: Arc::new(Notify::new()),
            pending_results: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            next_result_seq: Arc::new(AtomicU64::new(1)),
            last_acked_command_seq: Arc::new(AtomicU64::new(0)),
            request_full_state: Arc::new(atomic::AtomicBool::new(false)),
            results_notify: Arc::new(Notify::new()),
        }
    }

    /// Append commands to the pending buffer and wake any waiting poll.
    pub async fn push_commands(&self, cmds: Vec<executor_api_pb::Command>) {
        if cmds.is_empty() {
            return;
        }
        let mut buf = self.pending_commands.lock().await;
        buf.extend(cmds);
        self.commands_notify.notify_one();
    }

    /// Clone all pending commands (does NOT remove them).
    #[cfg(test)]
    pub async fn clone_commands(&self) -> Vec<executor_api_pb::Command> {
        self.pending_commands.lock().await.clone()
    }

    /// Clone a size-bounded prefix of pending commands (does NOT remove them).
    pub async fn clone_commands_capped(&self, limit_bytes: usize) -> Vec<executor_api_pb::Command> {
        let buf = self.pending_commands.lock().await;
        Self::capped_clone_by_encoded_size(&buf, limit_bytes)
    }

    /// Replace buffered commands (used on reconnect/restart hydration).
    pub async fn replace_commands(&self, cmds: Vec<executor_api_pb::Command>) {
        let mut buf = self.pending_commands.lock().await;
        *buf = cmds;
        if !buf.is_empty() {
            self.commands_notify.notify_one();
        }
    }

    /// Remove commands with seq <= `acked_seq`.
    pub async fn drain_commands_up_to(&self, acked_seq: u64) {
        let mut buf = self.pending_commands.lock().await;
        buf.retain(|cmd| cmd.seq > acked_seq);
    }

    /// Observe command ack progression and detect regressions.
    ///
    /// Returns true when a regression is detected (e.g. ack resets from N>0 to
    /// 0/None), which usually indicates dataplane local-state loss/restart.
    pub fn observe_command_ack(&self, acked_seq: Option<u64>) -> bool {
        let observed = acked_seq.unwrap_or(0);
        let prev = self.last_acked_command_seq.load(atomic::Ordering::Relaxed);
        if observed < prev {
            self.last_acked_command_seq
                .store(0, atomic::Ordering::Relaxed);
            self.request_full_state
                .store(true, atomic::Ordering::SeqCst);
            return true;
        }
        if observed > prev {
            self.last_acked_command_seq
                .store(observed, atomic::Ordering::Relaxed);
        }
        false
    }

    /// Consume and clear the one-shot full-state request flag.
    pub fn take_full_state_request(&self) -> bool {
        self.request_full_state
            .swap(false, atomic::Ordering::SeqCst)
    }

    /// Restore ack cursor from persistent storage.
    pub fn restore_acked_command_seq(&self, seq: u64) {
        self.last_acked_command_seq
            .store(seq, atomic::Ordering::Relaxed);
    }

    /// Restore next result sequence number for this executor connection.
    pub fn restore_next_result_seq(&self, next_seq: u64) {
        self.next_result_seq
            .store(next_seq.max(1), atomic::Ordering::Relaxed);
    }

    /// Read the next result sequence number that will be assigned.
    pub fn next_result_seq(&self) -> u64 {
        self.next_result_seq.load(atomic::Ordering::Relaxed)
    }

    /// Buffer a new allocation log entry as a sequenced result and wake any
    /// waiting poll.
    pub async fn push_result(&self, entry: executor_api_pb::AllocationLogEntry) {
        let seq = self.next_result_seq.fetch_add(1, atomic::Ordering::Relaxed);
        let result = executor_api_pb::SequencedAllocationResult {
            seq,
            entry: Some(entry),
        };
        let mut buf = self.pending_results.lock().await;
        buf.push(result);
        self.results_notify.notify_one();
    }

    /// Clone all pending results (does NOT remove them).
    #[cfg(test)]
    pub async fn clone_results(&self) -> Vec<executor_api_pb::SequencedAllocationResult> {
        self.pending_results.lock().await.clone()
    }

    /// Clone a size-bounded prefix of pending results (does NOT remove them).
    pub async fn clone_results_capped(
        &self,
        limit_bytes: usize,
    ) -> Vec<executor_api_pb::SequencedAllocationResult> {
        let buf = self.pending_results.lock().await;
        Self::capped_clone_by_encoded_size(&buf, limit_bytes)
    }

    /// Remove results with seq <= `acked_seq`.
    pub async fn drain_results_up_to(&self, acked_seq: u64) {
        let mut buf = self.pending_results.lock().await;
        buf.retain(|r| r.seq > acked_seq);
    }

    /// Get a clone of the commands notify handle (for long-poll waiters).
    pub fn commands_notify(&self) -> Arc<Notify> {
        self.commands_notify.clone()
    }

    /// Get a clone of the results notify handle (for long-poll waiters).
    pub fn results_notify(&self) -> Arc<Notify> {
        self.results_notify.clone()
    }

    /// Reset command emission state for a reconnect/full-sync handshake.
    ///
    /// Clears the pending command outbox and resets command cursors.
    pub async fn reset_for_full_sync(&self) {
        self.pending_commands.lock().await.clear();
        self.pending_results.lock().await.clear();
        self.next_result_seq.store(1, atomic::Ordering::Relaxed);
        self.last_acked_command_seq
            .store(0, atomic::Ordering::Relaxed);
        self.request_full_state
            .store(false, atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_command(seq: u64, payload_size: usize) -> executor_api_pb::Command {
        executor_api_pb::Command {
            seq,
            command: Some(executor_api_pb::command::Command::KillAllocation(
                executor_api_pb::KillAllocation {
                    allocation_id: "x".repeat(payload_size),
                },
            )),
        }
    }

    #[tokio::test]
    async fn test_clone_commands_capped_respects_limit() {
        let conn = ExecutorConnection::new();
        let c1 = make_command(1, 32);
        let c2 = make_command(2, 32);
        let c3 = make_command(3, 32);
        let limit = c1.encoded_len() + c2.encoded_len();
        conn.push_commands(vec![c1, c2, c3]).await;

        let batch = conn.clone_commands_capped(limit).await;
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].seq, 1);
        assert_eq!(batch[1].seq, 2);
    }

    #[tokio::test]
    async fn test_clone_results_capped_always_includes_first() {
        let conn = ExecutorConnection::new();
        conn.push_result(executor_api_pb::AllocationLogEntry {
            allocation_id: "a".repeat(128),
            clock: 0,
            entry: None,
        })
        .await;
        conn.push_result(executor_api_pb::AllocationLogEntry {
            allocation_id: "b".repeat(128),
            clock: 0,
            entry: None,
        })
        .await;

        let batch = conn.clone_results_capped(1).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].seq, 1);
    }
}
