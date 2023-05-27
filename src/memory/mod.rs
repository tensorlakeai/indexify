mod indefinite;
mod window;
mod lru;

use std::option::Option;

use crate::{MemoryStoragePolicyKind, MemoryStoragePolicy};

use super::server_config::{
    self
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::info;

use indefinite::IndefiniteMemorySession;
use uuid::Uuid;
use window::WindowMemorySession;
use lru::LRUCache;


/// An enumeration of possible errors that can occur while adding to or retrieving from conversation history.
#[derive(Error, Debug)]
pub enum MemorySessionError {
    /// An error that occurs when the requested policy is not found.
    #[error("policy `{0}` not found")]
    PolicyNotFound(String),

    /// An internal error that occurs during the operation.
    #[error("internal error: `{0}`")]
    InternalError(String),

    /// An error that occurs when the required configuration is missing for a memory policy.
    #[error("configuration `{0}`, missing for memory policy `{1}`")]
    ConfigurationError(String, String),

    /// An error that occurs when requested session is not found.
    #[error("session `{0}` not found")]
    SessionNotFound(Uuid),

    /// An error that occurs with Mutex.
    #[error("mutex lock error: `{0}`")]
    MutexLockError(String)
}

pub type MemorySessionTS = Arc<Mutex<dyn MemorySession + Sync + Send>>;

/// A trait that defines the interface for interacting with conversation history store.
pub trait MemorySession {
    /// Adds turn to store.
    fn add_turn(
        &mut self,
        turn: String,
    ) -> Result<(), MemorySessionError>;

    // Retrieves records from session.
    fn retrieve_history(
        &mut self,
        query: String,
    ) -> Result<Vec<String>, MemorySessionError>;
}

/// A struct that represents a router for storing and retrieving from memory.
///
/// This struct provides methods for storing and retrieving from memory sessions.
/// It maintains a mapping between session ids and their corresponding `MemorySession`
/// implementations, allowing it to route requests to the appropriate session.
pub struct MemorySessionRouter {
    router: HashMap<Uuid, MemorySessionTS>,
}

impl MemorySessionRouter {
    /// Creates a new instance of `MemorySessionRouter` with the specified server configuration.
    ///
    /// This method initializes the router with the available memory policies and their corresponding
    /// `MemorySession` implementations.
    ///
    /// # Arguments
    ///
    /// * `config` - The server configuration specifying the available models and their settings.
    ///
    /// # Returns
    ///
    /// * A result containing an instance of `MemorySessionRouter` if successful, or a
    ///   `MemorySessionError` if an error occurs.
    pub fn new(_config: Arc<server_config::ServerConfig>) -> Result<Self, MemorySessionError> {
        let router: HashMap<Uuid, MemorySessionTS> = HashMap::new();
        Ok(Self {
            router,
        })
    }

    fn does_session_exist(&self, session_id: Option<Uuid>) -> bool {
        return session_id.is_some() && self.router.contains_key(&session_id.unwrap())
    }

    fn get_session(&self, session_id: Uuid) -> Option<MemorySessionTS> {
        if self.does_session_exist(Some(session_id)) == false {
            return None
        }
        self.router.get(&session_id).cloned()
    }

    fn create_memory_session(&mut self, memory_storage_policy: MemoryStoragePolicy) ->  Result<MemorySessionTS, MemorySessionError> {
        let session: MemorySessionTS = match memory_storage_policy.policy_kind {
            MemoryStoragePolicyKind::Indefinite =>
                Arc::new(Mutex::new(IndefiniteMemorySession::new())),
            MemoryStoragePolicyKind::Window =>
                Arc::new(Mutex::new(WindowMemorySession::new(memory_storage_policy.window_size))),
            MemoryStoragePolicyKind::Lru =>
                Arc::new(Mutex::new(LRUCache::new(memory_storage_policy.capacity))),
        };
        return Ok(session);
    }

    pub fn create_session(&mut self, session_id: Option<Uuid>, memory_storage_policy: MemoryStoragePolicy) -> Result<Uuid, MemorySessionError> {
        if self.does_session_exist(session_id) {
            info!("Session already exists");
            return Ok(session_id.unwrap());
        }
        let session_id = session_id.unwrap_or(Uuid::new_v4());
        let session = self.create_memory_session(memory_storage_policy);
        self.router.insert(session_id, session.unwrap());
        return Ok(session_id);
    }

    pub fn add_turn(
        &mut self,
        session_id: Uuid,
        turn: String,
    ) -> Result<(), MemorySessionError> {
        let session = self.get_session(session_id);

        if session.is_none() {
            return Err(MemorySessionError::SessionNotFound(session_id))
        }

        let binding = session
            .unwrap();
        let mut conversation_history = binding
            .lock()
            .map_err(|e| MemorySessionError::MutexLockError(e.to_string()))?;

        conversation_history.add_turn(turn)
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))?;

        Ok(())
    }

    pub fn retrieve_history(
        &mut self,
        session_id: Uuid,
        query: String,
    ) -> Result<Vec<String>, MemorySessionError> {
        let session = self.get_session(session_id);

        if session.is_none() {
            return Err(MemorySessionError::SessionNotFound(session_id))
        }

        let binding = session
            .unwrap();
        let mut conversation_history = binding
            .lock()
            .map_err(|e| MemorySessionError::MutexLockError(e.to_string()))?;

        conversation_history.retrieve_history(query)
    }
}
