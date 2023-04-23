use std::{collections::HashMap, sync::Arc, hash::Hash};

use async_trait::async_trait;
use anyhow::Result;
use thiserror::Error;

use crate::{MilviusConfig, server_config};


#[derive(Error, Debug)]
enum VectorDbError {
    #[error("config not present")]
    ConfigNotPresent,
}

pub type VectorDBTS = Arc<dyn VectorDb + Sync + Send>;

#[async_trait]
pub trait VectorDb {

    async fn create_index(&self, name: String, model: String) -> Result<()>;

    async fn add_texts(&self, texts: Vec<String>, attrs: HashMap<String, String>) -> Result<()>;
}

pub fn create_vectordb(config: server_config::ServerConfig) -> Result<VectorDBTS, VectorDbError> {
    let index_config = config.index_config.ok_or(|| VectorDbError::ConfigNotPresent)?;
    Ok(Arc::new(Milvius::new(config.index_config)))
}


pub struct Milvius {

}

impl Milvius {
    pub fn new(config: MilviusConfig) -> Result<Self> {
        Ok(Self {  })
    }
}

#[async_trait]
impl VectorDb for Milvius {
    async fn create_index(&self, name: String, model: String) -> Result<()> {
        Ok(())
    }

    async fn add_texts(&self, texts: Vec<String>, attrs: HashMap<String, String>) -> Result<()> {
        Ok(())
    }
}
