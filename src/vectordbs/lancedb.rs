use std::{
    fmt::{self, Debug},
    iter::zip,
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
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use lance::dataset::BatchUDF;
use lancedb::{
    query::{ExecutableQuery, QueryBase},
    table::NewColumnTransform,
    Connection,
    Table,
};
use serde_json::Value;
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

fn to_column(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
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

fn add_nulls_to_record_batch(
    record_batch: &RecordBatch,
    schema: arrow_schema::SchemaRef,
    new_fields: &[Field],
) -> lance::Result<RecordBatch> {
    let mut arrays = record_batch.columns().to_vec();

    for _ in new_fields {
        let null_array = Arc::new(arrow_array::NullArray::new(record_batch.num_rows()));
        arrays.push(null_array);
    }

    let ret = RecordBatch::try_new(schema, arrays)?;
    Ok(ret)
}

// Update the schema of the table with the missing fields from the metadata keys
async fn update_schema_with_missing_fields(
    tbl: &Table,
    metadata: &serde_json::Value,
) -> Result<Arc<Schema>, anyhow::Error> {
    let mut new_fields = Vec::new();
    let mut schema = tbl.schema().await?;
    for (key, _) in metadata.as_object().unwrap().iter() {
        if schema.field_with_name(key).is_err() {
            new_fields.push(Field::new(key, DataType::Utf8, true));
        }
    }
    if !new_fields.is_empty() {
        println!("updating schema with new fields: {:?}", new_fields);

        let new_schema = Arc::new(Schema::new(new_fields.clone()));
        let cloned_schema = new_schema.clone();

        //add nulls to record batch
        let mapper = move |record_batch: &RecordBatch| {
            add_nulls_to_record_batch(record_batch, cloned_schema.clone(), &new_fields)
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
        ]));
        let batches = RecordBatchIterator::new(vec![], schema.clone());
        let _ = self
            .conn
            .create_table(&index.vectordb_index_name, Box::new(batches))
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

        let schema = update_schema_with_missing_fields(&tbl, &metadata).await?;

        let mut arrays: Vec<Arc<dyn Array>> = vec![Arc::new(ids), Arc::new(vectors)];
        for field in schema.fields() {
            if field.name() == "id" || field.name() == "vector" {
                continue;
            }
            let values = chunks.iter().map(|c| {
                let metadata = c.metadata.as_object().unwrap();
                let value = metadata.get(field.name());
                value.map(to_column)
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
        while let Some(batch) = stream.next().await {
            let b = batch.expect("should be Ok");
            let ids = b.column_by_name("id").unwrap();
            let id_values = as_string_array(&ids);

            for i in 0..id_values.len() {
                let mut metadata = serde_json::Map::new();
                for field in schema.fields() {
                    let field_name = field.name();
                    if field_name != "id" && field_name != "vector" {
                        if field.data_type() != &DataType::Utf8 {
                            return Err(anyhow!("field {} is not of type Utf8", field_name));
                        }
                        let column = b.column_by_name(field_name).unwrap();
                        let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                        if !string_array.is_null(i) {
                            let value = string_array.value(i);
                            metadata.insert(field_name.to_string(), serde_json::json!(value));
                        }
                    }
                }

                results.push(VectorChunk {
                    content_id: id_values.value(i).to_string(),
                    embedding: Vec::new(),
                    metadata: serde_json::Value::Object(metadata),
                });
            }
        }

        Ok(results)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: serde_json::Value,
    ) -> Result<()> {
        let tbl = self
            .conn
            .open_table(index)
            .execute()
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;

        update_schema_with_missing_fields(&tbl, &metadata).await?;

        let mut update_op = tbl.update().only_if(format!("id = '{}'", content_id));
        for (key, value) in metadata.as_object().unwrap() {
            update_op = update_op.column(key, format!("'{}'", to_column(value)));
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
        let tbl = self.conn.open_table(&index).execute().await?;
        let mut query = tbl.query();
        if filters.len() > 0 {
            query = query.only_if(from_filter_to_str(filters));
        }
        let res = query
            .nearest_to(query_embedding)?
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
            let ids = rb.column_by_name("id").unwrap();
            let id_values = as_string_array(&ids);
            let distances = rb.column_by_name("_distance").unwrap();
            let distance_values = distances
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()
                .unwrap();
            for (id, distance) in zip(id_values, distance_values) {
                results.push(SearchResult {
                    content_id: id.unwrap().to_string(),
                    confidence_score: distance.unwrap(),
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

    use serde_json::json;

    use super::*;
    use crate::{
        data_manager::DataManager,
        vectordbs::{IndexDistance, VectorDBTS},
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
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "id1".into(),
            embedding: vec![0., 2.],
            metadata: metadata1,
        };
        let metadata2 = json!({"key1": "value3", "key2": "value4"});
        let chunk1 = VectorChunk {
            content_id: "id2".into(),
            embedding: vec![0., 3.],
            metadata: metadata2,
        };
        lance
            .add_embedding("hello-index", vec![chunk, chunk1])
            .await
            .unwrap();

        assert_eq!(lance.num_vectors("hello-index").await.unwrap(), 2);

        assert_eq!(
            lance
                .search("hello-index".to_string(), vec![0., 2.], 1, vec![])
                .await
                .unwrap()
                .len(),
            1
        );
    }

    fn make_id() -> String {
        DataManager::make_id()
    }

    #[tokio::test]
    async fn test_store_metadata() {
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
                vectordb_index_name: "metadata-index".into(),
                vector_dim: 2,
                distance: crate::vectordbs::IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let content_ids = vec![make_id(), make_id()];
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk1 = VectorChunk {
            content_id: content_ids[0].clone(),
            embedding: vec![0.1, 0.2],
            metadata: metadata1.clone(),
        };
        lance
            .add_embedding("metadata-index", vec![chunk1])
            .await
            .unwrap();
        let metadata2 = json!({"key1": "value3", "key2": "value4"});
        let chunk2 = VectorChunk {
            content_id: content_ids[1].clone(),
            embedding: vec![0.3, 0.4],
            metadata: metadata2.clone(),
        };
        lance
            .add_embedding("metadata-index", vec![chunk2])
            .await
            .unwrap();

        let result = lance
            .get_points("metadata-index", content_ids.clone())
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for chunk in result {
            if chunk.content_id == content_ids[0] {
                assert_eq!(chunk.metadata, metadata1);
            } else if chunk.content_id == content_ids[1] {
                assert_eq!(chunk.metadata, metadata2);
            } else {
                panic!("unexpected content_id: {}", chunk.content_id);
            }
        }

        let new_metadata = json!({"key1": "value5", "key2": "value6"});
        lance
            .update_metadata(
                "metadata-index",
                content_ids[0].clone(),
                new_metadata.clone(),
            )
            .await
            .unwrap();
        let result = lance
            .get_points("metadata-index", vec![content_ids[0].clone()])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata, new_metadata);

        let new_metadata = json!({"key1": "value5", "key2": 20});
        lance
            .update_metadata(
                "metadata-index",
                content_ids[0].clone(),
                new_metadata.clone(),
            )
            .await
            .unwrap();
        let result = lance
            .get_points("metadata-index", vec![content_ids[0].clone()])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let result_map = result[0].metadata.as_object().unwrap();
        assert_eq!(result_map.get("key1").unwrap().as_str().unwrap(), "value5");
        assert_eq!(result_map.get("key2").unwrap().as_str().unwrap(), "20");
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
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1,
        };
        lance
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        lance.add_embedding(index_name, vec![chunk]).await.unwrap();
        let num_elements = lance.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
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
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: json!({"key1": "value1", "key2": "value2"}),
        };
        lance
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        let num_elements = lance.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 1);
        lance.remove_embedding(index_name, "0").await.unwrap();
        let num_elements = lance.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 0);
    }
}
