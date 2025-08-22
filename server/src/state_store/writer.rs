trait Transaction {
    fn write(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

trait Write {
    fn start_transaction(&self) -> Result<Arc<dyn Transaction>>;
    fn compare_and_set(
        &self,
        transaction: Arc<dyn Transaction>,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool>;
}
