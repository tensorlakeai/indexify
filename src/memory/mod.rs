mod simple;
mod window;
mod lru;

use super::server_config::{
    self, MemoryPolicyKind::Simple, MemoryPolicyKind::Window, MemoryPolicyKind::Lru
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::info;

use simple::SimpleConversationHistory;
use window::WindowConversationHistory;
use lru::LRUCache;


/// An enumeration of possible errors that can occur while adding to or retrieving from conversation history.
#[derive(Error, Debug)]
pub enum ConversationHistoryError {
    /// An error that occurs when the requested policy is not found.
    #[error("policy `{0}` not found")]
    PolicyNotFound(String),

    /// An internal error that occurs during the operation.
    #[error("internal error: `{0}`")]
    InternalError(String),

    /// An error that occurs when the required configuration is missing for a memory policy.
    #[error("configuration `{0}`, missing for memory policy `{1}`")]
    ConfigurationError(String, String),

    /// An error that occurs with Mutex.
    #[error("mutex lock error: `{0}`")]
    MutexLockError(String)
}

pub type ConversationHistoryTS = Arc<Mutex<dyn ConversationHistory + Sync + Send>>;

/// A trait that defines the interface for interacting with conversation history store.
pub trait ConversationHistory {
    /// Adds turn to Conversation History store.
    fn add_turn(
        &mut self,
        memory_policy: String,
        turn: String,
    ) -> Result<(), ConversationHistoryError>;

    // Retrieves records from Conversation History.
    fn retrieve_history(
        &mut self,
        memory_policy: String,
        query: String,
    ) -> Result<Vec<String>, ConversationHistoryError>;
}

/// A struct that represents a router for storing and retrieving from Conversation History.
///
/// This struct provides methods for storing and retrieving from Conversation History using different policies.
/// It maintains a mapping between memory policies and their corresponding `ConversationHistory`
/// implementations, allowing it to conversation history creation requests to the appropriate method.
pub struct ConversationHistoryRouter {
    router: HashMap<String, ConversationHistoryTS>,

    policy_names: Vec<String>,
}

impl ConversationHistoryRouter {
    /// Creates a new instance of `ConversationHistoryRouter` with the specified server configuration.
    ///
    /// This method initializes the router with the available memory policies and their corresponding
    /// `ConversationHistory` implementations.
    ///
    /// # Arguments
    ///
    /// * `config` - The server configuration specifying the available models and their settings.
    ///
    /// # Returns
    ///
    /// * A result containing an instance of `ConversationHistoryRouter` if successful, or a
    ///   `ConversationHistoryError` if an error occurs.
    pub fn new(config: Arc<server_config::ServerConfig>) -> Result<Self, ConversationHistoryError> {
        let mut router: HashMap<String, ConversationHistoryTS> = HashMap::new();
        let mut policy_names = Vec::new();
        for policy in config.memory_policies.clone() {
            policy_names.push(policy.policy_kind.to_string());
            info!(
                "loading store for policy: {:?}",
                policy.policy_kind.to_string()
            );
            match policy.policy_kind {
                Simple => {
                    let simple_memory_router = Arc::new(Mutex::new(SimpleConversationHistory::new()));
                    router.insert(policy.policy_kind.to_string(), simple_memory_router);
                }
                Window => {
                    let window_memory_router = Arc::new(Mutex::new(WindowConversationHistory::new(policy.window_size)));
                    router.insert(policy.policy_kind.to_string(), window_memory_router);
                }
                Lru => {
                    let lru_memory_router = Arc::new(Mutex::new(LRUCache::new(policy.capacity)));
                    router.insert(policy.policy_kind.to_string(), lru_memory_router);
                }
            }
        }
        Ok(Self {
            router,
            policy_names,
        })
    }

    /// Returns a list of available memory policies.
    ///
    /// # Returns
    ///
    /// * A vector of strings representing the names of the supported memory policies.
    pub fn list_policies(&self) -> Vec<String> {
        self.policy_names.clone()
    }
}

impl ConversationHistory for ConversationHistoryRouter {

    fn add_turn(
        &mut self,
        policy: String,
        turn: String,
    ) -> Result<(), ConversationHistoryError> {
        let conversation_history_mutex = self
            .router
            .get(&policy)
            .ok_or_else(|| ConversationHistoryError::PolicyNotFound(policy.clone()))?
            .clone();

        let mut conversation_history_lock = conversation_history_mutex
            .lock()
            .map_err(|e| ConversationHistoryError::MutexLockError(e.to_string()))?;

        conversation_history_lock.add_turn(policy, turn)
            .map_err(|e| ConversationHistoryError::InternalError(e.to_string()))?;

        Ok(())
    }

    fn retrieve_history(
        &mut self,
        policy: String,
        query: String,
    ) -> Result<Vec<String>, ConversationHistoryError> {
        let conversation_history_mutex = self
            .router
            .get(&policy)
            .ok_or_else(|| ConversationHistoryError::PolicyNotFound(policy.clone()))?
            .clone();

        let mut conversation_history_lock = conversation_history_mutex
            .lock()
            .map_err(|e| ConversationHistoryError::MutexLockError(e.to_string()))?;

        conversation_history_lock.retrieve_history(policy, query)
    }
}
