use std::time::Duration;

use tracing::warn;

use super::{ExecutorId, IndexifyState, executor_api_pb};
use crate::state_store::executor_connection::MAX_POLL_RESPONSE_BYTES;

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// Long-poll helper for the commands buffer.
///
/// Both `poll_commands` and `poll_allocation_results` share this structure:
/// 1. Drain acked items.
/// 2. Clone current items; return early if non-empty.
/// 3. Wait on notify or timeout.
/// 4. Re-clone and return.
pub async fn long_poll_commands(
    indexify_state: &IndexifyState,
    executor_id: &ExecutorId,
    acked_seq: Option<u64>,
) -> Vec<executor_api_pb::Command> {
    let connections = indexify_state.executor_connections.read().await;
    let Some(conn) = connections.get(executor_id) else {
        return vec![];
    };

    if conn.observe_command_ack(acked_seq) {
        warn!(
            executor_id = executor_id.get(),
            observed_ack = ?acked_seq,
            "detected command ack regression; requesting full state sync"
        );
    }

    if let Some(seq) = acked_seq {
        match indexify_state.ack_executor_commands(executor_id, seq).await {
            Ok(()) => conn.drain_commands_up_to(seq).await,
            Err(err) => {
                warn!(
                    executor_id = executor_id.get(),
                    acked_seq = seq,
                    error = ?err,
                    "failed to persist command ack; keeping in-memory outbox for retry"
                );
            }
        }
    }

    let items = conn.clone_commands_capped(MAX_POLL_RESPONSE_BYTES).await;
    if !items.is_empty() {
        return items;
    }

    let notify = conn.commands_notify();
    drop(connections);

    tokio::select! {
        _ = notify.notified() => {},
        _ = tokio::time::sleep(LONG_POLL_TIMEOUT) => {},
    }

    let connections = indexify_state.executor_connections.read().await;
    if let Some(conn) = connections.get(executor_id) {
        conn.clone_commands_capped(MAX_POLL_RESPONSE_BYTES).await
    } else {
        vec![]
    }
}

pub async fn long_poll_results(
    indexify_state: &IndexifyState,
    executor_id: &ExecutorId,
    acked_seq: Option<u64>,
) -> Vec<executor_api_pb::SequencedAllocationResult> {
    let connections = indexify_state.executor_connections.read().await;
    let Some(conn) = connections.get(executor_id) else {
        return vec![];
    };

    if let Some(seq) = acked_seq {
        conn.drain_results_up_to(seq).await;
    }

    let items = conn.clone_results_capped(MAX_POLL_RESPONSE_BYTES).await;
    if !items.is_empty() {
        return items;
    }

    let notify = conn.results_notify();
    drop(connections);

    tokio::select! {
        _ = notify.notified() => {},
        _ = tokio::time::sleep(LONG_POLL_TIMEOUT) => {},
    }

    let connections = indexify_state.executor_connections.read().await;
    if let Some(conn) = connections.get(executor_id) {
        conn.clone_results_capped(MAX_POLL_RESPONSE_BYTES).await
    } else {
        vec![]
    }
}
