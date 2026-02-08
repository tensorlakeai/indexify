//! State reporter that collects allocation results from FE controllers
//! and includes them in heartbeat updates.
//!
//! Results are buffered and sent in the next heartbeat. When new results
//! arrive, the heartbeat loop is notified to send immediately (matching Python
//! executor behavior with schedule_state_report).

use std::sync::Arc;

use proto_api::executor_api_pb::AllocationResult;
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::debug;

use crate::function_executor::events::CompletedAllocation;

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
    pub fn new(result_rx: mpsc::UnboundedReceiver<CompletedAllocation>) -> Self {
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

    /// Take allocation results for inclusion in the next heartbeat.
    pub async fn take_results(&self) -> Vec<AllocationResult> {
        let mut pending = self.pending_results.lock().await;
        if pending.is_empty() {
            return Vec::new();
        }

        let results = std::mem::take(&mut *pending);

        debug!(
            count = results.len(),
            "Taking allocation results for heartbeat"
        );

        results
    }
}

/// Background task: drains the result channel and notifies the heartbeat loop.
async fn drain_results(
    mut result_rx: mpsc::UnboundedReceiver<CompletedAllocation>,
    pending_results: Arc<Mutex<Vec<AllocationResult>>>,
    results_notify: Arc<Notify>,
) {
    while let Some(completed) = result_rx.recv().await {
        let mut pending = pending_results.lock().await;
        pending.push(completed.result);
        drop(pending);
        // Wake up heartbeat loop to send results immediately
        results_notify.notify_one();
    }
}
