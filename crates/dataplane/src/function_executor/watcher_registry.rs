//! Function call watcher registry.
//!
//! Tracks function call watchers registered by allocation runners and provides
//! routing of function call results from the server to the appropriate FE
//! allocation runners.
//!
//! This is the Rust equivalent of the Python executor's
//! FunctionCallWatchDispatcher and the watcher tracking in state_reconciler.

use std::{collections::HashMap, sync::Arc};

use proto_api::executor_api_pb::{
    AllocationOutcomeCode,
    FunctionCallResult as ServerFunctionCallResult,
    FunctionCallWatch,
};
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{debug, warn};

/// A registered watcher with a channel to receive results.
struct WatcherEntry {
    /// The FunctionCallWatch to include in heartbeats.
    watch: FunctionCallWatch,
    /// Senders for delivering results. Multiple allocations can watch the same
    /// function call.
    result_senders: Vec<mpsc::UnboundedSender<ServerFunctionCallResult>>,
}

/// Thread-safe registry for function call watchers.
#[derive(Clone)]
pub struct WatcherRegistry {
    inner: Arc<Mutex<WatcherRegistryInner>>,
    /// Notify when new watchers are registered (wakes up heartbeat loop).
    watcher_notify: Arc<Notify>,
}

struct WatcherRegistryInner {
    /// Maps watch key (namespace.request_id.function_call_id) to watcher
    /// entries.
    watchers: HashMap<String, WatcherEntry>,
}

impl WatcherRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(WatcherRegistryInner {
                watchers: HashMap::new(),
            })),
            watcher_notify: Arc::new(Notify::new()),
        }
    }

    /// Get a handle to the notify for waking up the heartbeat loop when watches
    /// change.
    pub fn watcher_notify(&self) -> Arc<Notify> {
        self.watcher_notify.clone()
    }

    /// Register a function call watcher. Returns a receiver for the result.
    ///
    /// The watch will be included in heartbeats so the server knows to send
    /// function call results for this function call.
    pub async fn register_watcher(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
        function_call_id: &str,
    ) -> mpsc::UnboundedReceiver<ServerFunctionCallResult> {
        let key = watch_key(namespace, request_id, function_call_id);
        let (tx, rx) = mpsc::unbounded_channel();

        let mut inner = self.inner.lock().await;
        let entry = inner.watchers.entry(key).or_insert_with(|| WatcherEntry {
            watch: FunctionCallWatch {
                namespace: Some(namespace.to_string()),
                application: Some(application.to_string()),
                request_id: Some(request_id.to_string()),
                function_call_id: Some(function_call_id.to_string()),
            },
            result_senders: Vec::new(),
        });
        entry.result_senders.push(tx);

        debug!(
            namespace = %namespace,
            request_id = %request_id,
            function_call_id = %function_call_id,
            "Registered function call watcher"
        );

        // Wake up heartbeat loop to report new watches immediately
        self.watcher_notify.notify_one();

        rx
    }

    /// Unregister a function call watcher.
    ///
    /// Removes all senders for this watch key that are closed (receiver
    /// dropped). If no senders remain, the watch entry is removed entirely.
    pub async fn unregister_watcher(
        &self,
        namespace: &str,
        request_id: &str,
        function_call_id: &str,
    ) {
        let key = watch_key(namespace, request_id, function_call_id);
        let mut inner = self.inner.lock().await;

        if let Some(entry) = inner.watchers.get_mut(&key) {
            // Remove closed senders
            entry.result_senders.retain(|tx| !tx.is_closed());
            if entry.result_senders.is_empty() {
                inner.watchers.remove(&key);
                debug!(
                    namespace = %namespace,
                    request_id = %request_id,
                    function_call_id = %function_call_id,
                    "Unregistered function call watcher (no more listeners)"
                );
            }
        }
    }

    /// Route function call results to registered watchers.
    ///
    /// Filters non-terminal results and results without return values
    /// (matching Python executor behavior).
    pub async fn deliver_results(&self, results: &[ServerFunctionCallResult]) {
        let mut inner = self.inner.lock().await;

        for result in results {
            // Filter out non-terminal outcomes (same logic as Python)
            let outcome = result.outcome_code();
            if outcome != AllocationOutcomeCode::Success &&
                outcome != AllocationOutcomeCode::Failure
            {
                continue;
            }

            // If success but no return_value, skip (waiting for tail call resolution)
            if outcome == AllocationOutcomeCode::Success && result.return_value.is_none() {
                continue;
            }

            let namespace = result.namespace.as_deref().unwrap_or("");
            let request_id = result.request_id.as_deref().unwrap_or("");
            let function_call_id = result.function_call_id.as_deref().unwrap_or("");
            let key = watch_key(namespace, request_id, function_call_id);

            if let Some(entry) = inner.watchers.get_mut(&key) {
                // Deliver to all registered senders
                entry.result_senders.retain(|tx| {
                    if tx.is_closed() {
                        return false;
                    }
                    if let Err(e) = tx.send(result.clone()) {
                        warn!(
                            function_call_id = %function_call_id,
                            "Failed to deliver function call result to watcher"
                        );
                        false
                    } else {
                        true
                    }
                });

                debug!(
                    function_call_id = %function_call_id,
                    outcome = ?outcome,
                    "Delivered function call result to watchers"
                );
            } else {
                debug!(
                    function_call_id = %function_call_id,
                    "No watchers found for function call result, ignoring"
                );
            }
        }
    }

    /// Get all active function call watches for inclusion in heartbeats.
    /// Also performs garbage collection of closed watchers.
    pub async fn get_function_call_watches(&self) -> Vec<FunctionCallWatch> {
        let mut inner = self.inner.lock().await;

        // Prune closed senders and empty entries
        inner.watchers.retain(|key, entry| {
            entry.result_senders.retain(|tx| !tx.is_closed());
            if entry.result_senders.is_empty() {
                debug!(
                    key = %key,
                    "Removing stale function call watcher (no more listeners)"
                );
                false
            } else {
                true
            }
        });

        inner
            .watchers
            .values()
            .map(|entry| entry.watch.clone())
            .collect()
    }
}

fn watch_key(namespace: &str, request_id: &str, function_call_id: &str) -> String {
    format!("{}.{}.{}", namespace, request_id, function_call_id)
}
