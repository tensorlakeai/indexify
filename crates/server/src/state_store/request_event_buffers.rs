use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::{RwLock, broadcast};
use tracing::debug;

use super::request_events::RequestStateChangeEvent;

const BROADCAST_CHANNEL_CAPACITY: usize = 100;

/// Unique identifier for a subscription
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
}

impl SubscriptionKey {
    pub fn new(namespace: &str, application: &str, request_id: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            application: application.to_string(),
            request_id: request_id.to_string(),
        }
    }
}

/// Per-subscription state containing the broadcast channel
struct SubscriptionState {
    tx: broadcast::Sender<RequestStateChangeEvent>,
    /// Number of active receivers (reference count)
    receiver_count: AtomicU64,
}

impl SubscriptionState {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        Self {
            tx,
            receiver_count: AtomicU64::new(0),
        }
    }

    fn send(&self, event: RequestStateChangeEvent) {
        let _ = self.tx.send(event);
    }

    fn subscribe(&self) -> broadcast::Receiver<RequestStateChangeEvent> {
        self.receiver_count.fetch_add(1, Ordering::Relaxed);
        self.tx.subscribe()
    }

    fn release(&self) -> bool {
        let prev = self.receiver_count.fetch_sub(1, Ordering::Relaxed);
        prev == 1
    }
}

/// Manages per-request broadcast channels for request state change events
pub struct RequestEventBuffers {
    /// Map of subscription key to subscription state
    subscriptions: RwLock<HashMap<SubscriptionKey, SubscriptionState>>,
    /// Counter for total subscriptions (for metrics)
    subscription_count: AtomicU64,
}

impl Default for RequestEventBuffers {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestEventBuffers {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            subscription_count: AtomicU64::new(0),
        }
    }

    /// Subscribe to events for a specific request.
    /// Returns a broadcast receiver for this request's events.
    pub async fn subscribe(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> broadcast::Receiver<RequestStateChangeEvent> {
        let key = SubscriptionKey::new(namespace, application, request_id);

        let mut subscriptions = self.subscriptions.write().await;
        let state = subscriptions.entry(key.clone()).or_insert_with(|| {
            self.subscription_count.fetch_add(1, Ordering::Relaxed);
            debug!(
                namespace,
                application, request_id, "new subscription created"
            );
            SubscriptionState::new()
        });

        state.subscribe()
    }

    /// Push an event to the matching subscription's broadcast channel.
    /// Called when a request state change event occurs.
    pub async fn push_event(&self, event: RequestStateChangeEvent) {
        let key = SubscriptionKey::new(
            event.namespace(),
            event.application_name(),
            event.request_id(),
        );

        let subscriptions = self.subscriptions.read().await;
        if let Some(state) = subscriptions.get(&key) {
            state.send(event);
        }
    }

    /// Release a subscription. Only removes when the last client disconnects.
    pub async fn unsubscribe(&self, namespace: &str, application: &str, request_id: &str) {
        let key = SubscriptionKey::new(namespace, application, request_id);

        // First, decrement the receiver count
        let should_remove = {
            let subscriptions = self.subscriptions.read().await;
            if let Some(state) = subscriptions.get(&key) {
                state.release()
            } else {
                false
            }
        };

        // Only remove if this was the last receiver
        if should_remove {
            let mut subscriptions = self.subscriptions.write().await;
            // Double-check: another subscriber might have joined between read and write
            if let Some(state) = subscriptions.get(&key) &&
                state.receiver_count.load(Ordering::Relaxed) == 0
            {
                subscriptions.remove(&key);
                self.subscription_count.fetch_sub(1, Ordering::Relaxed);
                debug!(
                    namespace,
                    application, request_id, "subscription removed (last client disconnected)"
                );
            }
        }
    }

    #[cfg(test)]
    pub fn subscription_count(&self) -> u64 {
        self.subscription_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::state_store::request_events::RequestStartedEvent;

    #[tokio::test]
    async fn test_subscribe_and_receive() {
        let buffers = RequestEventBuffers::new();

        // Subscribe to a request
        let mut rx = buffers.subscribe("ns", "app", "req1").await;

        // Push an event
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns".to_string(),
            application_name: "app".to_string(),
            application_version: "1.0".to_string(),
            request_id: "req1".to_string(),
            created_at: Utc::now().timestamp_millis(),
        });

        buffers.push_event(event.clone()).await;

        // Should receive the event
        let received = rx.recv().await.unwrap();
        assert!(matches!(
            received,
            RequestStateChangeEvent::RequestStarted(_)
        ));
    }

    #[tokio::test]
    async fn test_no_subscription_drops_event() {
        let buffers = RequestEventBuffers::new();

        // Push an event without subscription - should not panic
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns".to_string(),
            application_name: "app".to_string(),
            application_version: "1.0".to_string(),
            request_id: "req1".to_string(),
            created_at: Utc::now().timestamp_millis(),
        });

        buffers.push_event(event).await;
        // No assertion needed - just verify it doesn't panic
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let buffers = RequestEventBuffers::new();

        // Subscribe
        let _rx = buffers.subscribe("ns", "app", "req1").await;
        assert_eq!(buffers.subscription_count(), 1);

        // Unsubscribe
        buffers.unsubscribe("ns", "app", "req1").await;
        assert_eq!(buffers.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_multiple_subscribers_same_request() {
        let buffers = RequestEventBuffers::new();

        // Two subscribers for the same request
        let mut rx1 = buffers.subscribe("ns", "app", "req1").await;
        let mut rx2 = buffers.subscribe("ns", "app", "req1").await;

        // Push an event
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns".to_string(),
            application_name: "app".to_string(),
            application_version: "1.0".to_string(),
            request_id: "req1".to_string(),
            created_at: Utc::now().timestamp_millis(),
        });

        buffers.push_event(event).await;

        // Both should receive
        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }

    #[tokio::test]
    async fn test_ref_counting_first_unsubscribe_keeps_channel() {
        let buffers = RequestEventBuffers::new();

        // Two subscribers for the same request
        let rx1 = buffers.subscribe("ns", "app", "req1").await;
        let mut rx2 = buffers.subscribe("ns", "app", "req1").await;
        assert_eq!(buffers.subscription_count(), 1); // Same subscription

        // First client unsubscribes
        buffers.unsubscribe("ns", "app", "req1").await;
        assert_eq!(buffers.subscription_count(), 1); // Still there!

        // Push an event - rx2 should still receive it
        let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
            namespace: "ns".to_string(),
            application_name: "app".to_string(),
            application_version: "1.0".to_string(),
            request_id: "req1".to_string(),
            created_at: Utc::now().timestamp_millis(),
        });
        buffers.push_event(event).await;
        assert!(rx2.recv().await.is_ok());

        // rx1 was unsubscribed, but the channel is still alive
        // (rx1 would get Closed if we tried to receive, but that's fine)
        drop(rx1);

        // Second client unsubscribes - now it should be removed
        buffers.unsubscribe("ns", "app", "req1").await;
        assert_eq!(buffers.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_lagged_receiver() {
        let buffers = RequestEventBuffers::new();

        // Subscribe
        let mut rx = buffers.subscribe("ns", "app", "req1").await;

        // Push more events than the channel capacity
        for i in 0..(BROADCAST_CHANNEL_CAPACITY + 100) {
            let event = RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
                namespace: "ns".to_string(),
                application_name: "app".to_string(),
                application_version: format!("{}", i),
                request_id: "req1".to_string(),
                created_at: Utc::now().timestamp_millis(),
            });
            buffers.push_event(event).await;
        }

        // Receiver should get a Lagged error
        let result = rx.recv().await;
        assert!(matches!(
            result,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
    }
}
