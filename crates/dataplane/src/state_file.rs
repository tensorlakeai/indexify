//! State file for persisting container state across dataplane restarts.
//!
//! This module provides functionality to save and load container state to/from
//! a JSON file, enabling the dataplane to recover running containers after
//! restart.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use prost::Message;
use proto_api::executor_api_pb::FunctionExecutorDescription;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Persisted state for a single container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedContainer {
    /// Container ID (same as sandbox ID for sandboxes).
    pub container_id: String,
    /// Process/container handle ID (container name for Docker, PID for
    /// ForkExec).
    pub handle_id: String,
    /// gRPC daemon address (host:port).
    pub daemon_addr: String,
    /// HTTP daemon address (host:port).
    pub http_addr: String,
    /// Container's internal IP address.
    pub container_ip: String,
    /// Timestamp when the container was started (epoch ms).
    pub started_at: u64,
    /// Base64-encoded protobuf of FunctionExecutorDescription.
    /// Contains the full container description including function ref.
    #[serde(default)]
    pub description_proto: Option<String>,
}

impl PersistedContainer {
    /// Decode the FunctionExecutorDescription from the stored protobuf.
    pub fn decode_description(&self) -> Option<FunctionExecutorDescription> {
        let proto_b64 = self.description_proto.as_ref()?;
        let proto_bytes = BASE64.decode(proto_b64).ok()?;
        FunctionExecutorDescription::decode(proto_bytes.as_slice()).ok()
    }

    /// Encode a FunctionExecutorDescription to base64 protobuf.
    pub fn encode_description(desc: &FunctionExecutorDescription) -> String {
        BASE64.encode(desc.encode_to_vec())
    }
}

/// State file contents.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StateFileContents {
    /// Map of function executor ID to persisted container state.
    containers: HashMap<String, PersistedContainer>,
}

/// Manages persistence of container state to a file.
pub struct StateFile {
    path: PathBuf,
    state: Mutex<StateFileContents>,
}

impl StateFile {
    /// Create a new StateFile manager.
    ///
    /// If the file exists, its contents are loaded. Otherwise, starts with
    /// empty state.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = if path.exists() {
            match Self::load_from_file(&path).await {
                Ok(contents) => {
                    info!(
                        path = %path.display(),
                        containers = contents.containers.len(),
                        "Loaded state file"
                    );
                    contents
                }
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to load state file, starting fresh"
                    );
                    StateFileContents::default()
                }
            }
        } else {
            info!(path = %path.display(), "State file does not exist, starting fresh");
            StateFileContents::default()
        };

        Ok(Self {
            path,
            state: Mutex::new(state),
        })
    }

    /// Load state from file.
    async fn load_from_file(path: &Path) -> Result<StateFileContents> {
        let contents = tokio::fs::read_to_string(path)
            .await
            .context("Failed to read state file")?;
        let state: StateFileContents =
            serde_json::from_str(&contents).context("Failed to parse state file")?;
        Ok(state)
    }

    /// Save current state to file.
    async fn save_to_file(&self) -> Result<()> {
        let state = self.state.lock().await;
        let contents =
            serde_json::to_string_pretty(&*state).context("Failed to serialize state")?;
        tokio::fs::write(&self.path, contents)
            .await
            .context("Failed to write state file")?;
        Ok(())
    }

    /// Add or update a container in the state file.
    pub async fn upsert(&self, container: PersistedContainer) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            state
                .containers
                .insert(container.container_id.clone(), container);
        }
        self.save_to_file().await
    }

    /// Remove a container from the state file.
    pub async fn remove(&self, container_id: &str) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            state.containers.remove(container_id);
        }
        self.save_to_file().await
    }

    /// Get all persisted containers.
    pub async fn get_all(&self) -> Vec<PersistedContainer> {
        let state = self.state.lock().await;
        state.containers.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_state_file_create_and_persist() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");

        // Create state file and add a container
        let state_file = StateFile::new(&path).await.unwrap();
        state_file
            .upsert(PersistedContainer {
                container_id: "fe-1".to_string(),
                handle_id: "container-123".to_string(),
                daemon_addr: "127.0.0.1:9500".to_string(),
                http_addr: "127.0.0.1:9501".to_string(),
                container_ip: "172.17.0.2".to_string(),
                started_at: 1234567890,
                description_proto: None,
            })
            .await
            .unwrap();

        // Verify file exists
        assert!(path.exists());

        // Load state file again and verify contents
        let state_file2 = StateFile::new(&path).await.unwrap();
        let containers = state_file2.get_all().await;
        assert_eq!(containers.len(), 1);
        assert_eq!(containers[0].container_id, "fe-1");
        assert_eq!(containers[0].handle_id, "container-123");
    }

    #[tokio::test]
    async fn test_state_file_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");

        let state_file = StateFile::new(&path).await.unwrap();
        state_file
            .upsert(PersistedContainer {
                container_id: "fe-1".to_string(),
                handle_id: "container-123".to_string(),
                daemon_addr: "127.0.0.1:9500".to_string(),
                http_addr: "127.0.0.1:9501".to_string(),
                container_ip: "172.17.0.2".to_string(),
                started_at: 1234567890,
                description_proto: None,
            })
            .await
            .unwrap();

        state_file.remove("fe-1").await.unwrap();

        let containers = state_file.get_all().await;
        assert!(containers.is_empty());
    }

    #[tokio::test]
    async fn test_state_file_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let state_file = StateFile::new(&path).await.unwrap();
        let containers = state_file.get_all().await;
        assert!(containers.is_empty());
    }

    #[test]
    fn test_description_encode_decode() {
        use proto_api::executor_api_pb::FunctionRef;

        let desc = FunctionExecutorDescription {
            id: Some("test-container".to_string()),
            function: Some(FunctionRef {
                namespace: Some("test-ns".to_string()),
                application_name: Some("test-app".to_string()),
                function_name: Some("test-fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            ..Default::default()
        };

        let encoded = PersistedContainer::encode_description(&desc);
        let container = PersistedContainer {
            container_id: "test".to_string(),
            handle_id: "h1".to_string(),
            daemon_addr: "127.0.0.1:9500".to_string(),
            http_addr: "127.0.0.1:9501".to_string(),
            container_ip: "127.0.0.1".to_string(),
            started_at: 0,
            description_proto: Some(encoded),
        };

        let decoded = container.decode_description().expect("should decode");
        assert_eq!(decoded.id, Some("test-container".to_string()));
        assert!(decoded.function.is_some());
        let func = decoded.function.unwrap();
        assert_eq!(func.namespace, Some("test-ns".to_string()));
        assert_eq!(func.application_name, Some("test-app".to_string()));
        assert_eq!(func.function_name, Some("test-fn".to_string()));
    }

    #[test]
    fn test_description_decode_none() {
        let container = PersistedContainer {
            container_id: "test".to_string(),
            handle_id: "h1".to_string(),
            daemon_addr: "127.0.0.1:9500".to_string(),
            http_addr: "127.0.0.1:9501".to_string(),
            container_ip: "127.0.0.1".to_string(),
            started_at: 0,
            description_proto: None,
        };

        assert!(container.decode_description().is_none());
    }
}
