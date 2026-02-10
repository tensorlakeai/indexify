//! Function call watcher registry.
//!
//! Tracks function call watchers registered by allocation runners and provides
//! routing of function call results from the server to the appropriate
//! allocation runner's channel.
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

/// A watcher result delivered to an allocation runner's channel.
pub struct WatcherResult {
    pub watcher_id: String,
    pub watched_function_call_id: String,
    pub fc_result: ServerFunctionCallResult,
}

/// A registered watcher tied to a specific allocation runner.
struct RegisteredWatcher {
    watcher_id: String,
    watched_function_call_id: String,
    result_tx: mpsc::UnboundedSender<WatcherResult>,
}

/// Entry for a watch key (namespace.request_id.function_call_id).
struct WatcherEntry {
    /// The FunctionCallWatch to include in heartbeats.
    watch: FunctionCallWatch,
    /// Registered watchers. Multiple allocations can watch the same function
    /// call.
    watchers: Vec<RegisteredWatcher>,
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

impl Default for WatcherRegistry {
    fn default() -> Self {
        Self::new()
    }
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

    /// Register a function call watcher.
    ///
    /// Results will be delivered to `result_tx` as `WatcherResult` values.
    /// The watch will be included in heartbeats so the server knows to send
    /// function call results for this function call.
    pub async fn register_watcher(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
        function_call_id: &str,
        watcher_id: &str,
        result_tx: mpsc::UnboundedSender<WatcherResult>,
    ) {
        let key = watch_key(namespace, request_id, function_call_id);

        let mut inner = self.inner.lock().await;
        let entry = inner.watchers.entry(key).or_insert_with(|| WatcherEntry {
            watch: FunctionCallWatch {
                namespace: Some(namespace.to_string()),
                application: Some(application.to_string()),
                request_id: Some(request_id.to_string()),
                function_call_id: Some(function_call_id.to_string()),
            },
            watchers: Vec::new(),
        });
        entry.watchers.push(RegisteredWatcher {
            watcher_id: watcher_id.to_string(),
            watched_function_call_id: function_call_id.to_string(),
            result_tx,
        });

        debug!(
            namespace = %namespace,
            request_id = %request_id,
            function_call_id = %function_call_id,
            watcher_id = %watcher_id,
            "Registered function call watcher"
        );

        // Wake up heartbeat loop to report new watches immediately
        self.watcher_notify.notify_one();
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
                // Deliver to all registered watchers, removing closed ones
                entry.watchers.retain(|w| {
                    if w.result_tx.is_closed() {
                        return false;
                    }
                    let watcher_result = WatcherResult {
                        watcher_id: w.watcher_id.clone(),
                        watched_function_call_id: w.watched_function_call_id.clone(),
                        fc_result: result.clone(),
                    };
                    if w.result_tx.send(watcher_result).is_err() {
                        warn!(
                            function_call_id = %function_call_id,
                            watcher_id = %w.watcher_id,
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
            entry.watchers.retain(|w| !w.result_tx.is_closed());
            if entry.watchers.is_empty() {
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
