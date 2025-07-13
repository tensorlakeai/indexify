use anyhow::Result;
use rocksdb::TransactionDB;

use super::contexts::{MigrationContext, PrepareContext};

/// Trait defining a database migration
pub trait Migration {
    /// The version this migration upgrades TO
    fn version(&self) -> u64;

    /// Name for logging purposes
    fn name(&self) -> &'static str;

    /// DB preparation - column family operations before transaction
    /// Default implementation simply opens the DB with existing column families
    fn prepare(&self, ctx: &PrepareContext) -> Result<TransactionDB> {
        ctx.open_db()
    }

    /// Apply migration using provided context
    fn apply(&self, ctx: &MigrationContext) -> Result<()>;

    fn box_clone(&self) -> Box<dyn Migration>;
}
