use anyhow::Result;
use async_trait::async_trait;

use super::contexts::{MigrationContext, PrepareContext};
use crate::state_store::driver::rocksdb::RocksDBDriver;

/// Trait defining a database migration
#[async_trait]
pub trait Migration: Send + Sync {
    /// The version this migration upgrades TO
    fn version(&self) -> u64;

    /// Name for logging purposes
    fn name(&self) -> &'static str;

    /// DB preparation - column family operations before transaction
    /// Default implementation simply opens the DB with existing column families
    async fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        ctx.open_db()
    }

    /// Apply migration using provided context
    async fn apply(&self, ctx: &MigrationContext) -> Result<()>;

    fn box_clone(&self) -> Box<dyn Migration>;
}
