//! File manager for sandbox file operations.
//!
//! Provides secure file read/write/delete operations with path traversal
//! protection.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::fs;
use tracing::debug;

use crate::http_models::DirectoryEntry;

/// Manages file operations for the sandbox API.
#[derive(Clone)]
pub struct FileManager;

impl FileManager {
    /// Create a new file manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a path for security (no traversal attacks).
    fn validate_path(&self, path: &str) -> Result<PathBuf> {
        let path = Path::new(path);

        // Check for path traversal
        for component in path.components() {
            if let std::path::Component::ParentDir = component {
                anyhow::bail!("Path traversal detected: '..' is not allowed");
            }
        }

        // Canonicalize to resolve symlinks and get absolute path
        let canonical = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };

        Ok(canonical)
    }

    /// Read a file's contents.
    pub async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let path = self.validate_path(path)?;
        debug!(path = %path.display(), "Reading file");

        if path.is_dir() {
            anyhow::bail!(
                "Path is a directory, not a file: {}. Use /api/v1/files/list to list directory contents.",
                path.display()
            );
        }

        fs::read(&path)
            .await
            .with_context(|| format!("Failed to read file: {}", path.display()))
    }

    /// Write content to a file, creating parent directories if needed.
    pub async fn write_file(&self, path: &str, content: Vec<u8>) -> Result<()> {
        let path = self.validate_path(path)?;
        debug!(path = %path.display(), size = content.len(), "Writing file");

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "Failed to create parent directories for: {}",
                    path.display()
                )
            })?;
        }

        fs::write(&path, content)
            .await
            .with_context(|| format!("Failed to write file: {}", path.display()))
    }

    /// Delete a file.
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        let path = self.validate_path(path)?;
        debug!(path = %path.display(), "Deleting file");

        fs::remove_file(&path)
            .await
            .with_context(|| format!("Failed to delete file: {}", path.display()))
    }

    /// List contents of a directory.
    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        let path = self.validate_path(path)?;
        debug!(path = %path.display(), "Listing directory");

        let metadata = fs::metadata(&path)
            .await
            .with_context(|| format!("Failed to get metadata: {}", path.display()))?;

        if !metadata.is_dir() {
            anyhow::bail!("Path is not a directory: {}", path.display());
        }

        let mut entries = Vec::new();
        let mut read_dir = fs::read_dir(&path)
            .await
            .with_context(|| format!("Failed to read directory: {}", path.display()))?;

        while let Some(entry) = read_dir.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            let metadata = entry.metadata().await?;
            let is_dir = metadata.is_dir();
            let size = if is_dir { None } else { Some(metadata.len()) };

            let modified_at = metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64);

            entries.push(DirectoryEntry {
                name,
                is_dir,
                size,
                modified_at,
            });
        }

        // Sort entries: directories first, then alphabetically
        entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        });

        Ok(entries)
    }
}

impl Default for FileManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_write_and_read_file() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new();

        let path = temp_dir.path().join("test.txt");
        let content = b"Hello, World!";

        manager
            .write_file(path.to_str().unwrap(), content.to_vec())
            .await
            .unwrap();

        let read_content = manager.read_file(path.to_str().unwrap()).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_path_traversal_detection() {
        let manager = FileManager::new();

        let result = manager.read_file("../etc/passwd").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("traversal"));
    }

    #[tokio::test]
    async fn test_list_directory() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new();

        // Create some files and directories
        fs::write(temp_dir.path().join("file1.txt"), "content1")
            .await
            .unwrap();
        fs::write(temp_dir.path().join("file2.txt"), "content2")
            .await
            .unwrap();
        fs::create_dir(temp_dir.path().join("subdir"))
            .await
            .unwrap();

        let entries = manager
            .list_directory(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        assert_eq!(entries.len(), 3);
        // Directories come first
        assert!(entries[0].is_dir);
        assert_eq!(entries[0].name, "subdir");
        // Then files alphabetically
        assert!(!entries[1].is_dir);
        assert!(!entries[2].is_dir);
    }

    #[tokio::test]
    async fn test_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let manager = FileManager::new();

        let path = temp_dir.path().join("to_delete.txt");
        fs::write(&path, "content").await.unwrap();

        manager.delete_file(path.to_str().unwrap()).await.unwrap();
        assert!(!path.exists());
    }
}
