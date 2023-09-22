use crate::PgEmbeddingConfig;

use super::{CreateIndexParams, VectorDbError};

/// PgEmbeddingDb
pub struct PgEmbeddingDb {
    pg_embedding_config: PgEmbeddingConfig,
}

pub struct PgEmbeddingPayload {
    pub text: String,
    pub chunk_id: String,
    pub metadata: serde_json::Value,
}

impl PgEmbeddingDb {
    pub fn new(config: PgEmbeddingConfig) -> PgEmbeddingDb {
        Self {
            pg_embedding_config: config,
        }
    }

    // Create a client, I suppose this will just be a postgres client (?)
    // pub fn create_client(&self) -> Result<>
}

#[async_trait]
impl VectorDb for PgEmbeddingDb {
    fn name(&self) -> String {
        "pg_embedding".into()
    }

    /// Because this is a separate VectorDb index (i.e. modularly replacdable by qdrant),
    /// we create a new table for each index.
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError> {
        // TODO: Assert or choose optional parameters
        let index_name = index.vectordb_index_name;
        let vector_dim = index.vector_dim;
        let (m, efconstruction, efsearch) = match index.unique_params {
            Some(unique_params) => {
                // Return error if the default parameters are not.
                // We can also return some default's instead, but I suppose the "Option<>" wrapper implies that either all are set, or none are
                unique_params.collect_tuple().unwrap()
            }
            None => (3, 5, 5),
        };

        // TODO: Depending on the distance, we have to create a different index ...
        // can use r# #
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "",
            crate::vectordbs::IndexDistance::Cosine => "ann_cos_ops",
            crate::vectordbs::IndexDistance::Dot => "ann_manhattan_ops",
        };

        let mut query = r#"
            CREATE TABLE {index_name}(id integer PRIMARY KEY, embedding real[]);
            CREATE INDEX ON {index_name} USING hnsw(embedding {distance_extension}) WITH (dims={dims}, m={m}, efconstruction={efconstruction}, efsearch={efsearch});
            SET enable_seqscan = off;
        )"#;
        // TODO: run the query againts self.client
    }

    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError> {
        // Insert an array into the index
        let mut query = r#"
            SELECT id FROM {index} ORDER BY embedding <-> array[3,3,3] LIMIT {k};
        )"#;
        // TODO: run the query againts self.client
    }

    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>, VectorDbError> {
        // TODO: How to turn query_embedding into an array[...]
        let mut query = r#"
            SELECT id FROM {index} ORDER BY embedding <-> array[3,3,3] LIMIT {k};
        )"#;
        // TODO: run the query againts self.client
        todo!()
    }

    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let mut query = r#"
            DROP TABLE {index_name};
        )"#;
        // TODO: run the query againts self.client
    }

    async fn num_vectors(&self, index: &str) -> Result<u64, VectorDbError> {
        let mut query = r#"
            SELECT COUNT(*) FROM TABLE {index_name};
        )"#;
        // TODO: run the query againts self.client
        todo!()
    }
}
