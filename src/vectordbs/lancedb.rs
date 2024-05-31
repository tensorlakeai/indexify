use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use arrow_array::{
    cast::as_string_array,
    types::Float32Type,
    Array,
    FixedSizeListArray,
    PrimitiveArray,
    RecordBatch,
    RecordBatchIterator,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use itertools::izip;
use lance::dataset::{BatchUDF, WriteParams};
use lancedb::{
    query::{ExecutableQuery, QueryBase},
    table::{NewColumnTransform, WriteOptions},
    Connection,
    Table,
};
use tracing;

use super::{CreateIndexParams, Filter, FilterOperator, SearchResult, VectorChunk, VectorDb};
use crate::server_config::LancedbConfig;

fn from_filter_to_str(filters: Vec<Filter>) -> String {
    filters
        .into_iter()
        .map(|f| match f.operator {
            FilterOperator::Eq => format!("{} = '{}'", f.key, f.value),
            FilterOperator::Neq => format!("{} != '{}'", f.key, f.value),
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub struct LanceDb {
    conn: Arc<Connection>,
}

impl Debug for LanceDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceDb")
    }
}

async fn vector_chunk_from_batch(
    batch: RecordBatch,
    schema: SchemaRef,
) -> Result<Vec<VectorChunk>> {
    let mut results = Vec::<VectorChunk>::new();
    let mut ids = Vec::new();
    let mut embeddings = Vec::new();
    let mut content_metadatas = Vec::new();
    let mut root_content_metadatas: Vec<Option<String>> = Vec::new();
    let mut metadatas: Vec<HashMap<String, serde_json::Value>> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        if field_name == "id" {
            ids = as_string_array(batch.column_by_name(field_name).unwrap())
                .iter()
                .map(|x| x.unwrap().to_string())
                .collect();
        } else if field_name == "vector" {
            // Looks like we are not using the embedding
            // so we are just doing empty Vec to make the structs happy
        } else if field_name == "content_metadata" {
            for row in as_string_array(batch.column_by_name(field_name).unwrap()) {
                let row = row.ok_or(anyhow!("content_metadata is null"))?;
                content_metadatas.push(row.to_string());
            }
        } else if field_name == "root_content_metadata" {
            for row in as_string_array(batch.column_by_name(field_name).unwrap()) {
                root_content_metadatas.push(row.map(|x| x.to_string()));
            }
        } else {
            let column = batch.column_by_name(field_name).unwrap();
            // if metadatas are not allocated yet, allocate them
            if metadatas.is_empty() {
                metadatas = vec![HashMap::new(); column.len()];
            }
            // FIXME SHould be a better way to enumerate with index
            let mut i = 0;
            for row in as_string_array(batch.column_by_name(field_name).unwrap()) {
                let row = row.ok_or(anyhow!("metadata is null"))?;
                let value: serde_json::Value = serde_json::json!(row);
                metadatas[i].insert(field_name.to_string(), value);
                i += 1;
            }
        }
    }
    // Looks like we are not using the embedding
    // so we are just doing empty Vec to make the structs happy
    if embeddings.is_empty() {
        embeddings = vec![Vec::new(); ids.len()];
    }
    if metadatas.is_empty() {
        metadatas = vec![HashMap::new(); ids.len()];
    }
    for (id, embedding, metadata, root_content_metadata, content_metadata) in izip!(
        ids,
        embeddings,
        metadatas,
        root_content_metadatas,
        content_metadatas
    ) {
        let root_content_metadata: Option<indexify_internal_api::ContentMetadata> =
            match root_content_metadata {
                Some(metadata) => serde_json::from_str(&metadata)?,
                None => None,
            };
        let content_metadata = serde_json::from_str(&content_metadata)?;
        results.push(VectorChunk {
            content_id: id,
            embedding,
            metadata,
            content_metadata,
            root_content_metadata,
        });
    }
    Ok(results)
}

impl LanceDb {
    pub async fn new(config: &LancedbConfig) -> Result<Self> {
        let conn = lancedb::connect(&config.path)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to create db: {}", e))?;
        Ok(LanceDb {
            conn: Arc::new(conn),
        })
    }
}

// Update the schema of the table with the missing fields from the metadata keys
async fn update_schema_with_missing_fields(
    tbl: &Table,
    metadata: HashMap<String, serde_json::Value>,
) -> Result<Arc<Schema>, anyhow::Error> {
    let mut new_fields = Vec::new();
    let mut schema = tbl.schema().await?;
    // Find the fields that has to be added
    for (key, _) in metadata.iter() {
        if schema.field_with_name(key).is_err() {
            new_fields.push(Arc::new(Field::new(key, DataType::Utf8, true)));
        }
    }
    if !new_fields.is_empty() {
        let mut all_fields = schema.fields().to_vec();
        all_fields.extend(new_fields.clone());
        let new_schema = Arc::new(Schema::new(new_fields.clone()));
        let new_schema_clone = new_schema.clone();
        //add nulls to record batch
        let mapper = move |record_batch: &RecordBatch| {
            let mut arrays = record_batch.columns().to_vec();
            for _ in &new_fields {
                let null_array = Arc::new(arrow_array::NullArray::new(record_batch.num_rows()));
                arrays.push(null_array);
            }

            let ret = RecordBatch::try_new(new_schema_clone.clone(), arrays)?;
            Ok(ret)
        };

        let udf = BatchUDF {
            mapper: Box::new(mapper),
            output_schema: new_schema,
            result_checkpoint: None,
        };
        tbl.add_columns(NewColumnTransform::BatchUDF(udf), None)
            .await
            .map_err(|e| anyhow!("unable to add columns to table {}", e))?;

        schema = tbl.schema().await?;
    }

    Ok(schema)
}

#[async_trait]
impl VectorDb for LanceDb {
    fn name(&self) -> String {
        "lancedb".into()
    }

    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    index.vector_dim as i32,
                ),
                true,
            ),
            Field::new("root_content_metadata", DataType::Utf8, true),
            Field::new("content_metadata", DataType::Utf8, false),
        ]));
        let batches = RecordBatchIterator::new(vec![], schema.clone());
        let table_names = self.conn.table_names().execute().await?;
        if table_names.contains(&index.vectordb_index_name) {
            return Ok(());
        }
        let _ = self
            .conn
            .create_table(&index.vectordb_index_name, Box::new(batches))
            .write_options(WriteOptions {
                lance_write_params: Some(WriteParams {
                    mode: lance::dataset::WriteMode::Append,
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .map_err(|e| anyhow!("unable to create table: {}", e))?;
        Ok(())
    }

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        let tbl = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;
        let ids = StringArray::from_iter_values(chunks.iter().map(|c| c.content_id.clone()));
        let vector_dim = chunks[0].embedding.len() as i32;
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            chunks
                .iter()
                .map(|c| Some(c.embedding.iter().map(|e| Some(*e)))),
            vector_dim,
        );
        let metadata = chunks
            .first()
            .map(|c| c.metadata.clone())
            .unwrap_or_default();

        let mut content_metadatas = Vec::<String>::new();
        let mut root_content_metadatas = Vec::<Option<String>>::new();
        for chunk in &chunks {
            let content_metadata = serde_json::to_string(&chunk.content_metadata)?;
            content_metadatas.push(content_metadata);
            let root_content_metadata = match &chunk.root_content_metadata {
                Some(metadata) => Some(serde_json::to_string(metadata)?),
                None => None,
            };
            root_content_metadatas.push(root_content_metadata);
        }

        let content_metadata_array = StringArray::from_iter_values(content_metadatas.iter());
        let root_content_metadata_array = StringArray::from(root_content_metadatas.to_vec());

        let schema = update_schema_with_missing_fields(&tbl, metadata).await?;

        let mut arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(ids),
            Arc::new(vectors),
            Arc::new(root_content_metadata_array),
            Arc::new(content_metadata_array),
        ];
        for field in schema.fields() {
            if ["id", "vector", "root_content_metadata", "content_metadata"]
                .contains(&field.name().as_str())
            {
                continue;
            }
            let values = chunks.iter().map(|c| {
                c.metadata
                    .get(field.name())
                    .map(|v| serde_json::to_string(&v).unwrap())
            });
            let array = values.collect::<StringArray>();
            arrays.push(Arc::new(array));
        }

        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(schema.clone(), arrays).unwrap()]
                .into_iter()
                .map(Ok),
            schema,
        );

        let mut merge_insert = tbl.merge_insert(&["id"]);
        merge_insert
            .when_matched_update_all(None)
            .when_not_matched_insert_all();

        merge_insert
            .execute(Box::new(batches))
            .await
            .map_err(|e| anyhow!("unable to add to table {}", e))
    }

    async fn get_points(&self, index: &str, ids: Vec<String>) -> Result<Vec<VectorChunk>> {
        let tbl = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;

        let ids_str = ids
            .into_iter()
            .map(|id| format!("'{}'", id))
            .collect::<Vec<_>>()
            .join(", ");
        let condition = format!("id IN ({})", ids_str);
        let mut results = Vec::<VectorChunk>::new();
        let schema = tbl.schema().await?;
        let mut stream = tbl
            .query()
            .only_if(condition)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to select records: {}", e))?;
        while let Some(Ok(batch)) = stream.next().await {
            let result_batch = vector_chunk_from_batch(batch, schema.clone()).await?;
            results.extend(result_batch);
        }

        Ok(results)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let tbl = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;

        update_schema_with_missing_fields(&tbl, metadata.clone()).await?;

        let mut update_op = tbl.update().only_if(format!("id = '{}'", content_id));
        for (key, value) in metadata {
            update_op = update_op.column(key, value.to_string());
        }
        update_op
            .execute()
            .await
            .map_err(|e| anyhow!("unable to update metadata: {}", e))
    }

    #[tracing::instrument]
    #[tracing::instrument]
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()> {
        // Open the table
        let tbl = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;

        // Delete the rows where content_id is the key
        tbl.delete(&format!("id = '{}'", content_id))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to remove embeddings from lance db table for content_id {}: {}",
                    content_id,
                    e
                )
            })
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
        filters: Vec<Filter>,
    ) -> Result<Vec<SearchResult>> {
        // FIXME remove the hardcoding to cosine
        // We need to pass the distance metric from
        // data manager to the vector db
        let tbl = self.conn.open_table(&index).execute().await?;
        let mut query = tbl
            .vector_search(query_embedding)
            .map_err(|e| anyhow!("unable to create vector search query: {}", e))?
            .distance_type(lancedb::DistanceType::Cosine);
        if !filters.is_empty() {
            query = query.only_if(from_filter_to_str(filters));
        }
        let res = query
            .column("vector")
            .limit(k as usize)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut results = vec![];
        for rb in &res {
            let distances = rb.column_by_name("_distance").unwrap();
            let distance_values = distances
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()
                .unwrap()
                .values()
                .iter();
            let vector_chunks = vector_chunk_from_batch(rb.clone(), tbl.schema().await.unwrap())
                .await
                .unwrap();

            for (chunk, distance) in izip!(vector_chunks, distance_values) {
                results.push(SearchResult {
                    content_id: chunk.content_id,
                    confidence_score: *distance,
                    metadata: chunk.metadata,
                    content_metadata: chunk.content_metadata,
                    root_content_metadata: chunk.root_content_metadata,
                });
            }
        }
        Ok(results)
    }

    #[tracing::instrument]
    async fn drop_index(&self, index: &str) -> Result<()> {
        self.conn
            .drop_table(&index)
            .await
            .map_err(|e| anyhow!("unable to drop index: {}", e))
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let table = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table {} ", e))?;
        let rows = table.count_rows(None).await?;
        Ok(rows as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::vectordbs::{
        tests::{
            basic_search,
            crud_operations,
            insertion_idempotent,
            search_filters,
            store_metadata,
        },
        IndexDistance,
        VectorDBTS,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: crate::vectordbs::IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        basic_search(lance, "hello-index").await;
    }

    // FIXME: This test is failing
    // Come back to thtis
    #[tokio::test]
    #[ignore]
    async fn test_store_metadata() {
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        let index_name = "metadata-index";
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: crate::vectordbs::IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        store_metadata(lance, index_name).await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "idempotent-index";
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        insertion_idempotent(lance, index_name).await;
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_deletion() {
        let index_name = "delete-index";
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        crud_operations(lance, index_name).await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore]
    async fn test_search_filters() {
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: crate::vectordbs::IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        search_filters(lance, "hello-index").await;
    }
}
