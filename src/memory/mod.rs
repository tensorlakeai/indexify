mod indefinite;
mod lru;
mod window;

use std::{option::Option, sync::Arc};

use crate::{MemoryStoragePolicy, MemoryStoragePolicyKind};

use dashmap::{mapref::one::RefMut, DashMap};
use thiserror::Error;
use tracing::info;

use indefinite::IndefiniteMemorySession;
use lru::LRUCache;
use uuid::Uuid;
use window::WindowMemorySession;

use super::server_config::{self};

/// An enumeration of possible errors that can occur while adding to or retrieving from memory.
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
}

pub type MemorySessionTS = Arc<dyn MemorySession + Sync + Send>;

/// A trait that defines the interface for interacting with memory store.
pub trait MemorySession {
    /// Adds turn to store.
    fn add_turn(&mut self, turn: String) -> Result<(), MemorySessionError>;

    // Retrieves records from session.
    fn retrieve_history(&mut self, query: String) -> Result<Vec<String>, MemorySessionError>;

    fn id(&self) -> Uuid;
}

/// A struct that represents a router for storing and retrieving from memory.
///
/// This struct provides methods for storing and retrieving from memory sessions.
/// It maintains a mapping between session ids and their corresponding `MemorySession`
/// implementations, allowing it to route requests to the appropriate session.
pub struct MemorySessionRouter {
    router: DashMap<Uuid, MemorySessionTS>,
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
        let router: DashMap<Uuid, MemorySessionTS> = DashMap::new();

        // for (policy_name, policy) in config.memory_policies.iter() {
        //     let memory_storage_policy = MemoryStoragePolicy::new(policy_name, policy)?;
        //     let session = self.create_memory_session(memory_storage_policy)?;
        //     router.insert(session.id(), session);
        // }

        Ok(Self { router })
    }

    fn does_session_exist(&self, session_id: Option<Uuid>) -> bool {
        return session_id.is_some() && self.router.contains_key(&session_id.unwrap());
    }

    fn get_session(
        &self,
        session_id: Uuid,
    ) -> Option<RefMut<Uuid, Arc<dyn MemorySession + Send + Sync>>> {
        if self.does_session_exist(Some(session_id)) == false {
            return None;
        }
        return self.router.get_mut(&session_id);
    }

    fn create_memory_session(
        &self,
        session_id: Uuid,
        memory_storage_policy: MemoryStoragePolicy,
    ) -> Result<MemorySessionTS, MemorySessionError> {
        let session: MemorySessionTS = match memory_storage_policy.policy_kind {
            MemoryStoragePolicyKind::Indefinite => {
                Arc::new(IndefiniteMemorySession::new(session_id))
            }
            MemoryStoragePolicyKind::Window => Arc::new(WindowMemorySession::new(
                session_id,
                memory_storage_policy.window_size,
            )),
            MemoryStoragePolicyKind::Lru => {
                Arc::new(LRUCache::new(session_id, memory_storage_policy.capacity))
            }
        };
        return Ok(session);
    }

    pub fn create_session(
        &self,
        session_id: Option<Uuid>,
        memory_storage_policy: MemoryStoragePolicy,
    ) -> Result<Uuid, MemorySessionError> {
        if self.does_session_exist(session_id) {
            info!("Session already exists");
            return Ok(session_id.unwrap());
        }
        let session_id = session_id.unwrap_or(Uuid::new_v4());
        let session = self.create_memory_session(session_id, memory_storage_policy);
        self.router.insert(session_id, session.unwrap());
        return Ok(session_id);
    }

    pub fn add_turn(&self, session_id: Uuid, turn: String) -> Result<(), MemorySessionError> {
        let session_option = self.get_session(session_id);

        if session_option.is_none() {
            return Err(MemorySessionError::SessionNotFound(session_id));
        }

        let mut session_arc = session_option.unwrap().clone();
        let session_ref = Arc::get_mut(&mut session_arc).ok_or_else(|| {
            MemorySessionError::InternalError(
                "Failed to obtain mutable reference to session".into(),
            )
        })?;

        session_ref
            .add_turn(turn)
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))?;

        Ok(())
    }

    pub fn retrieve_history(
        &self,
        session_id: Uuid,
        query: String,
    ) -> Result<Vec<String>, MemorySessionError> {
        let session_option = self.get_session(session_id);

        if session_option.is_none() {
            return Err(MemorySessionError::SessionNotFound(session_id));
        }

        let mut session_arc = session_option.unwrap().clone();
        let session_ref = Arc::get_mut(&mut session_arc).ok_or_else(|| {
            MemorySessionError::InternalError(
                "Failed to obtain mutable reference to session".into(),
            )
        })?;

        let history = session_ref
            .retrieve_history(query)
            .map_err(|e| MemorySessionError::InternalError(e.to_string()))?;

        Ok(history)
    }
}
