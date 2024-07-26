use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use arrow_array::{
    cast::{as_boolean_array, as_string_array},
    types::{self, Float32Type},
    Array,
    BooleanArray,
    FixedSizeListArray,
    PrimitiveArray,
    RecordBatch,
    RecordBatchIterator,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use filter::Operator;
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

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};
use crate::server_config::LancedbConfig;

fn from_filter_to_str(filter: &filter::LabelsFilter) -> String {
    filter
        .expressions()
        .iter()
        .map(|f| {
            let value = from_filter_value_to_str(&f.value);
            match f.operator {
                Operator::Eq => format!("{} = {}", f.key, value),
                Operator::Neq => format!("{} != {}", f.key, value),
                Operator::Gt => format!("{} > {}", f.key, value),
                Operator::Lt => format!("{} < {}", f.key, value),
                Operator::GtEq => format!("{} >= {}", f.key, value),
                Operator::LtEq => format!("{} <= {}", f.key, value),
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn from_filter_value_to_str(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("'{s}'"),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => "".to_string(),
    }
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
                let row = row.map(|s| s.to_string()).unwrap_or_default();
                content_metadatas.push(row);
            }
        } else if field_name == "root_content_metadata" {
            for row in as_string_array(batch.column_by_name(field_name).unwrap()) {
                root_content_metadatas.push(row.map(|x| x.to_string()));
            }
        } else {
            let column = batch.column_by_name(field_name).unwrap();
            let rows = from_arrow_column_to_json_values(column)?;

            // if metadatas are not allocated yet, allocate them
            if metadatas.is_empty() {
                metadatas = vec![HashMap::new(); column.len()];
            }

            let mut i = 0;
            for row in rows {
                metadatas[i].insert(field_name.to_string(), row);
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
    for (key, value) in metadata.iter() {
        if schema.field_with_name(key).is_err() {
            match value {
                serde_json::Value::Number(n) => {
                    if n.is_f64() {
                        new_fields.push(Field::new(key, DataType::Float64, true));
                    } else {
                        new_fields.push(Field::new(key, DataType::Int64, true));
                    }
                }
                serde_json::Value::String(_) => {
                    new_fields.push(Field::new(key, DataType::Utf8, true));
                }
                serde_json::Value::Bool(_) => {
                    new_fields.push(Field::new(key, DataType::Boolean, true));
                }
                _ => {}
            }
        }
    }

    let new_fields = new_fields.into_iter().map(Arc::new).collect::<Vec<_>>();

    if !new_fields.is_empty() {
        let mut all_fields = schema.fields().to_vec();
        all_fields.extend(new_fields.clone());
        let new_schema = Arc::new(Schema::new(new_fields.clone()));
        let new_schema_clone = new_schema.clone();
        //add nulls to record batch
        let mapper = move |record_batch: &RecordBatch| {
            let mut arrays = Vec::<Arc<dyn Array>>::new();
            for _ in &new_fields {
                let num_rows = record_batch.num_rows();
                let null_string_array =
                    StringArray::from_iter((0..num_rows).map(|_| Option::<String>::None));
                arrays.push(Arc::new(null_string_array));
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

            let all_values: Vec<Option<&serde_json::Value>> = chunks
                .iter()
                .map(|c| c.metadata.get(field.name()))
                .collect();

            for value in &all_values {
                if let Some(value) = value {
                    validate_value_with_column_type(value, field)?;
                }
            }

            arrays.push(from_serde_json_to_arrow_array(
                field.data_type(),
                all_values,
            )?);
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
        filter: filter::LabelsFilter,
    ) -> Result<Vec<SearchResult>> {
        // FIXME remove the hardcoding to cosine
        // We need to pass the distance metric from
        // data manager to the vector db
        let tbl = self.conn.open_table(&index).execute().await?;
        let mut query = tbl
            .vector_search(query_embedding)
            .map_err(|e| anyhow!("unable to create vector search query: {}", e))?
            .distance_type(lancedb::DistanceType::Cosine);
        if !filter.is_empty() {
            query = query.only_if(from_filter_to_str(&filter));
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

            let table_schema = tbl
                .schema()
                .await
                .map_err(|e| anyhow!("unable to get schema of table: {}", e))?;
            let vector_chunks = vector_chunk_from_batch(rb.clone(), table_schema)
                .await
                .map_err(|e| anyhow!("unable to get vector chunks from batch: {}", e))?;

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

fn from_serde_json_to_arrow_array(
    datatype: &DataType,
    values: Vec<Option<&serde_json::Value>>,
) -> Result<Arc<dyn Array>> {
    let iterator = values.iter();
    match datatype {
        DataType::Float64 => {
            let arr = iterator
                .map(|v| match v {
                    Some(serde_json::Value::Number(n)) => Some(n.as_f64()?),
                    _ => None,
                })
                .collect::<PrimitiveArray<types::Float64Type>>();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let arr = iterator
                .map(|v| match v {
                    Some(serde_json::Value::Number(n)) => Some(n.as_i64()?),
                    _ => None,
                })
                .collect::<PrimitiveArray<types::Int64Type>>();
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let arr = iterator
                .map(|v| match v {
                    Some(serde_json::Value::String(s)) => Some(s.to_string()),
                    _ => None,
                })
                .collect::<StringArray>();
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let arr = iterator
                .map(|v| match v {
                    Some(serde_json::Value::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect::<BooleanArray>();
            Ok(Arc::new(arr))
        }
        _ => Err(anyhow!("unsupported metadata type for field")),
    }
}

fn from_arrow_column_to_json_values(column: &Arc<dyn Array>) -> Result<Vec<serde_json::Value>> {
    let mut values = vec![];

    match column.data_type() {
        DataType::Utf8 => {
            for row in as_string_array(column).iter() {
                let row = row.unwrap_or_default();
                values.push(row.into());
            }
        }
        DataType::Int64 => {
            let column = column
                .as_any()
                .downcast_ref::<PrimitiveArray<types::Int64Type>>()
                .unwrap();
            for row in column.values().iter() {
                values.push(serde_json::json!(row));
            }
        }
        DataType::Float64 => {
            let column = column
                .as_any()
                .downcast_ref::<PrimitiveArray<types::Float64Type>>()
                .unwrap();
            for row in column.values().iter() {
                values.push(serde_json::json!(row));
            }
        }
        DataType::Boolean => {
            for row in as_boolean_array(column).iter() {
                let row = row.unwrap_or_default();
                values.push(serde_json::json!(row));
            }
        }
        _ => {
            // This should not happen as we don't have
            // data type of this kind in our schema
        }
    }

    Ok(values)
}

fn validate_value_with_column_type(
    value: &serde_json::Value,
    field: &Field,
) -> Result<(), anyhow::Error> {
    let field_name = field.name();
    let datatype = field.data_type();
    let err = Err(anyhow!(
        "Type of value {value:?} mismatch for field {field_name} ({datatype:?})"
    ));

    match value {
        serde_json::Value::Number(_) => match datatype {
            DataType::Float64 => {}
            DataType::Int64 => {}
            _ => return err,
        },
        serde_json::Value::String(_) => {
            if *field.data_type() != DataType::Utf8 {
                return err;
            }
        }
        serde_json::Value::Bool(_) => {
            if *field.data_type() != DataType::Boolean {
                return err;
            }
        }
        _ => return err,
    };

    Ok(())
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
