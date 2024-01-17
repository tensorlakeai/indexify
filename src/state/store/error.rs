use std::{
    backtrace::Backtrace,
    error::Error,
    fmt::{Display, Formatter},
    string::FromUtf8Error,
};

use anyerror::AnyError;
use openraft::{StorageError, StorageIOError};

use super::SledStoreTree;
use crate::state::NodeId;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, strum::Display, Clone, Copy)]
#[strum(serialize_all = "title_case")]
pub enum StoreErrorKind {
    ParseError,
    IoError,
    Uncategorized,

    // raft errors
    ReadStateMachine,
    WriteStateMachine,
    ReadSnapshot,
    WriteSnapshot,
    ReadVote,
    WriteVote,
    ReadLogs,
    WriteLogs,
}

impl StoreErrorKind {
    pub fn build_with_source(&self, msg: impl Into<String>, source: Box<dyn Error>) -> StoreError {
        StoreError::new(self.clone(), msg).with_source(source)
    }

    pub fn build_with_tree(
        &self,
        msg: impl Into<String>,
        source: Box<dyn Error>,
        tree: SledStoreTree,
    ) -> StoreError {
        StoreError::new(self.clone(), msg)
            .with_source(source)
            .with_tree(tree)
    }

    pub fn build_with_tree_and_key(
        &self,
        msg: impl Into<String>,
        source: Box<dyn Error>,
        tree: SledStoreTree,
        key: impl Into<String>,
    ) -> StoreError {
        StoreError::new(self.clone(), msg)
            .with_source(source)
            .with_tree(tree)
            .with_key(key)
    }
}

#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
    msg: String,
    source: Option<Box<dyn Error + 'static>>,
    tree: Option<SledStoreTree>,
    key: Option<String>,
    backtrace: Option<Backtrace>,
}

impl StoreError {
    pub fn new(kind: StoreErrorKind, msg: impl Into<String>) -> Self {
        StoreError {
            kind,
            msg: msg.into(),
            source: None,
            tree: None,
            key: None,
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn with_source(mut self, source: Box<dyn Error>) -> Self {
        self.source = Some(source);
        self
    }

    pub fn with_tree(mut self, tree: SledStoreTree) -> Self {
        self.tree = Some(tree);
        self
    }

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

impl From<AnyError> for StoreError {
    fn from(e: AnyError) -> Self {
        StoreError {
            kind: StoreErrorKind::Uncategorized,
            msg: format!("{}", e),
            source: Some(Box::new(e)),
            tree: None,
            key: None,
            backtrace: Some(Backtrace::capture()),
        }
    }
}

impl Error for StoreError {}

impl From<StoreError> for AnyError {
    fn from(e: StoreError) -> Self {
        AnyError::new(&e).add_context(|| "sled store error")
    }
}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "sled store {} error: {}", self.kind, self.msg)?;
        if let Some(source) = &self.source {
            write!(f, ": {}", source)?;
        }
        if let Some(tree) = &self.tree {
            write!(f, " (tree: {})", tree)?;
        }
        if let Some(key) = &self.key {
            write!(f, " (key: {})", key)?;
        }
        if let Some(backtrace) = &self.backtrace {
            write!(f, "\nBacktrace:\n{:?}", backtrace)?;
        }
        Ok(())
    }
}

impl From<FromUtf8Error> for StoreError {
    fn from(e: FromUtf8Error) -> Self {
        StoreError {
            kind: StoreErrorKind::ParseError,
            msg: format!("{}", e),
            source: Some(Box::new(e)),
            tree: None,
            key: None,
            backtrace: Some(Backtrace::capture()),
        }
    }
}

impl From<sled::Error> for StoreError {
    fn from(e: sled::Error) -> Self {
        StoreError {
            kind: StoreErrorKind::IoError,
            msg: format!("{}", e),
            source: Some(Box::new(e)),
            tree: None,
            key: None,
            backtrace: Some(Backtrace::capture()),
        }
    }
}

impl From<StoreError> for StorageIOError<NodeId> {
    fn from(e: StoreError) -> Self {
        match e.kind {
            StoreErrorKind::ReadStateMachine => StorageIOError::read_state_machine(&e),
            StoreErrorKind::WriteStateMachine => StorageIOError::write_state_machine(&e),
            StoreErrorKind::ReadSnapshot => StorageIOError::read(&e),
            StoreErrorKind::WriteSnapshot => StorageIOError::write(&e),
            StoreErrorKind::ReadVote => StorageIOError::read_vote(&e),
            StoreErrorKind::WriteVote => StorageIOError::write_vote(&e),
            StoreErrorKind::ReadLogs => StorageIOError::read_logs(&e),
            StoreErrorKind::WriteLogs => StorageIOError::write_logs(&e),

            // other type transformations aren't supported
            _ => {
                tracing::error!("unsupported sled store error: {}", e);
                // cast to a generic read error
                StorageIOError::read(&e)
            }
        }
    }
}

impl From<StoreError> for StorageError<NodeId> {
    fn from(e: StoreError) -> Self {
        let io_error: StorageIOError<NodeId> = e.into();
        io_error.into()
    }
}
