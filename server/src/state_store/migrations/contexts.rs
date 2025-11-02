use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use serde_json::Value;

use crate::{
    data_model::StateMachineMetadata,
    metrics::StateStoreMetrics,
    state_store::{
        self,
        driver::{
            Reader,
            Transaction,
            rocksdb::{RocksDBConfig, RocksDBDriver},
        },
        state_machine::IndexifyObjectsColumns,
    },
};

/// Context for database preparation phase of migrations
pub struct PrepareContext {
    pub path: PathBuf,
    pub db_opts: Options,
    pub config: RocksDBConfig,
}

impl PrepareContext {
    pub fn new(path: PathBuf, config: RocksDBConfig) -> Self {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        Self {
            path,
            db_opts,
            config,
        }
    }

    /// Open database with all existing column families
    pub fn open_db(&self) -> Result<RocksDBDriver> {
        let cfs = match DB::list_cf(&self.db_opts, &self.path) {
            Ok(cfs) => cfs,
            Err(e) => return Err(anyhow!("Failed to list column families: {e}")),
        }
        .into_iter()
        .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        let metrics = Arc::new(StateStoreMetrics::new());
        state_store::open_database(self.path.clone(), self.config.clone(), cfs, metrics)
    }

    /// Helper to perform column family operations and reopen DB
    #[allow(dead_code)]
    pub fn reopen_with_cf_operations<F>(&self, operations: F) -> Result<RocksDBDriver>
    where
        F: FnOnce(&mut RocksDBDriver) -> Result<()>,
    {
        // Open DB
        let mut db = self.open_db()?;

        // Apply operations
        operations(&mut db)?;

        // Close DB to finalize CF changes
        drop(db);

        // Reopen with updated CFs
        self.open_db()
    }

    /// Get list of all column families
    pub fn list_cfs(&self) -> Result<Vec<String>> {
        DB::list_cf(&self.db_opts, &self.path)
            .map_err(|e| anyhow!("Failed to list column families: {e}"))
    }
}

/// Context for applying migration logic
pub struct MigrationContext {
    pub db: RocksDBDriver,
    pub txn: Transaction,
}

impl MigrationContext {
    pub fn new(db: RocksDBDriver, txn: Transaction) -> Self {
        Self { db, txn }
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()?;
        Ok(())
    }

    /// Write state machine metadata to the database
    pub fn write_sm_meta(&self, sm_meta: &StateMachineMetadata) -> Result<()> {
        state_store::write_sm_meta(&self.txn, sm_meta)
    }

    /// Iterate over all entries in a column family
    pub fn iterate<F>(&self, column_family: &IndexifyObjectsColumns, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let iter = self.db.iter(column_family.as_ref(), Default::default());

        for kv in iter {
            let (key, value) = kv?;
            callback(&key, &value)?;
        }

        Ok(())
    }

    /// Parse JSON from bytes
    pub fn _parse_json(&self, bytes: &[u8]) -> Result<Value> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!("Error deserializing JSON: {e}"))
    }

    /// Encode JSON to bytes
    pub fn _encode_json(&self, json: &Value) -> Result<Vec<u8>> {
        serde_json::to_vec(json).map_err(|e| anyhow!("Error serializing JSON: {e}"))
    }

    /// Helper for common field renames in JSON objects
    #[cfg(test)]
    pub fn rename_json_field(
        &self,
        json: &mut Value,
        old_field: &str,
        new_field: &str,
    ) -> Result<bool> {
        if let Some(obj) = json.as_object_mut() {
            if let Some(value) = obj.remove(old_field) {
                obj.insert(new_field.to_string(), value);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Helper to ensure a field exists with a default value
    #[cfg(test)]
    pub fn ensure_json_field(
        &self,
        json: &mut Value,
        field: &str,
        default_value: Value,
    ) -> Result<bool> {
        if let Some(obj) = json.as_object_mut() {
            if !obj.contains_key(field) {
                obj.insert(field.to_string(), default_value);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Helper to update a JSON object and write it back
    pub fn _update_json<F>(
        &self,
        column_family: &IndexifyObjectsColumns,
        key: &[u8],
        updater: F,
    ) -> Result<bool>
    where
        F: FnOnce(&mut Value) -> Result<bool>,
    {
        if let Some(value_bytes) = self.db.get(column_family.as_ref(), key)? {
            let mut json = self._parse_json(&value_bytes)?;

            if updater(&mut json)? {
                let updated_bytes = self._encode_json(&json)?;
                self.txn.put(column_family.as_ref(), key, &updated_bytes)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Get a string value from a JSON object
    pub fn _get_string_val(&self, val: &Value, key: &str) -> Result<String> {
        val.get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Missing {key} in JSON value"))
    }

    /// Truncate all entries in a column family
    pub fn _truncate_cf(&self, column_family: &IndexifyObjectsColumns) -> Result<usize> {
        let mut count = 0;

        self.iterate(column_family, |key, _| {
            self.txn.delete(column_family.as_ref(), key)?;
            count += 1;
            Ok(())
        })?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::{ColumnFamilyDescriptor, TransactionDB, TransactionDBOptions};
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::state_store::driver::Writer;

    #[test]
    fn test_prepare_context() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();

        let ctx = PrepareContext::new(path, RocksDBConfig::default());

        // Test creating DB
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let cf_names = ["test_cf".to_string(), "default".to_string()];
        let cf_descriptors: Vec<_> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        TransactionDB::<rocksdb::MultiThreaded>::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            &ctx.path,
            cf_descriptors,
        )?;

        // Test listing CFs
        let cfs = ctx.list_cfs()?;
        assert!(cfs.contains(&"test_cf".to_string()));
        assert!(cfs.contains(&"default".to_string()));

        // Test reopen with CF operations
        let db = ctx.reopen_with_cf_operations(|db| {
            db.drop("test_cf")?;
            db.create("new_cf", &Default::default())?;
            Ok(())
        })?;

        drop(db);

        let new_cfs = DB::list_cf(&db_opts, &ctx.path)?;
        assert!(!new_cfs.contains(&"test_cf".to_string()));
        assert!(new_cfs.contains(&"new_cf".to_string()));

        Ok(())
    }

    #[test]
    fn test_migration_context() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        let cf_name = IndexifyObjectsColumns::RequestCtx.as_ref();
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(cf_name, Options::default()),
        ];

        let metrics = Arc::new(StateStoreMetrics::new());
        let db = state_store::open_database(
            path.to_path_buf(),
            RocksDBConfig::default(),
            cf_descriptors.into_iter(),
            metrics,
        )?;

        // Add test data
        let test_json = json!({
            "id": "test1",
            "old_field": "value",
            "preserved": true
        });

        let key = b"test_key";
        let value = serde_json::to_vec(&test_json)?;

        let cf = IndexifyObjectsColumns::RequestCtx.as_ref();
        db.put(cf, key, &value)?;

        // Create migration context
        let txn = db.transaction();
        let ctx = MigrationContext::new(db.clone(), txn);

        // Test JSON operations
        ctx._update_json(&IndexifyObjectsColumns::RequestCtx, key, |json| {
            // Rename field
            ctx.rename_json_field(json, "old_field", "new_field")?;

            // Add field with default
            ctx.ensure_json_field(json, "added_field", json!(42))?;

            Ok(true)
        })?;

        ctx.commit()?;

        // Verify changes
        let cf = IndexifyObjectsColumns::RequestCtx.as_ref();
        let updated_bytes = db.get(cf, key)?.unwrap();
        let updated_json: Value = serde_json::from_slice(&updated_bytes)?;

        assert_eq!(updated_json["new_field"], "value");
        assert_eq!(updated_json["added_field"], 42);
        assert_eq!(updated_json["preserved"], true);
        assert!(updated_json.get("old_field").is_none());

        Ok(())
    }
}
