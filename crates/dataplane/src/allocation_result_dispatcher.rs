//! Routes incoming FunctionCallResults from the allocation stream to the
//! allocation runner handling the target allocation.
//!
//! The dispatcher maintains a map of `allocation_id → channel sender`. Each
//! allocation runner registers on start and deregisters on completion. The
//! service's allocation stream handler calls `dispatch()` to forward results.

use std::{collections::HashMap, sync::Arc};

use proto_api::executor_api_pb;
use tokio::sync::{RwLock, mpsc};

/// Routes incoming `AllocationStreamResponse` messages to the allocation runner
/// handling each allocation.
pub struct AllocationResultDispatcher {
    senders:
        RwLock<HashMap<String, mpsc::UnboundedSender<executor_api_pb::AllocationStreamResponse>>>,
}

impl AllocationResultDispatcher {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            senders: RwLock::new(HashMap::new()),
        })
    }

    /// Register an allocation runner for receiving results. Returns the
    /// receiver end of the channel.
    ///
    /// Called by the allocation runner when it starts execution.
    pub async fn register(
        &self,
        allocation_id: String,
    ) -> mpsc::UnboundedReceiver<executor_api_pb::AllocationStreamResponse> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.senders.write().await.insert(allocation_id, tx);
        rx
    }

    /// Remove an allocation runner's registration.
    ///
    /// Called by the allocation runner on completion. Also safe to call if the
    /// runner was never registered (no-op).
    pub async fn deregister(&self, allocation_id: &str) {
        self.senders.write().await.remove(allocation_id);
    }

    /// Dispatch a result to the allocation runner for the given allocation.
    ///
    /// Returns `true` if the result was delivered, `false` if no runner is
    /// registered for the allocation or the runner's channel is closed.
    ///
    /// Called by the service's allocation stream handler.
    pub async fn dispatch(
        &self,
        allocation_id: &str,
        response: executor_api_pb::AllocationStreamResponse,
    ) -> bool {
        if let Some(tx) = self.senders.read().await.get(allocation_id) {
            tx.send(response).is_ok()
        } else {
            false
        }
    }
}
