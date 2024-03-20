use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use gluesql::{
    core::{
        data::{HashMapJsonExt, Key, Schema, ValueError},
        error::Result,
        store::{
            AlterTable,
            CustomFunction,
            CustomFunctionMut,
            DataRow,
            Index,
            IndexMut,
            Metadata,
            RowIter,
            Store,
            StoreMut,
            Transaction,
        },
    },
    prelude::Glue,
};
use indexify_internal_api::{SchemaColumnType, StructuredDataSchema};
use serde::{Deserialize, Serialize};

use super::MetadataReaderTS;

pub async fn run_query(
    query: String,
    metadata_reader: MetadataReaderTS,
    schemas: Vec<StructuredDataSchema>,
    namespace: String,
) -> anyhow::Result<Vec<StructuredDataRow>> {
    let q_engine = QueryEngine::new(metadata_reader, schemas, &namespace);
    let mut glue_query = Glue::new(q_engine);
    let payloads = glue_query
        .execute(query)
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    let mut out_rows = vec![];
    for payload in payloads {
        let result = payload.select();
        if let Some(payload_iter) = result {
            for row in payload_iter {
                let mut out_row = HashMap::new();
                for (col, val) in row {
                    let val: serde_json::Value = val.clone().try_into()?;
                    out_row.insert(col.to_string(), val);
                }
                out_rows.push(StructuredDataRow { data: out_row });
            }
        }
    }
    Ok(out_rows)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StructuredDataRow {
    pub data: HashMap<String, serde_json::Value>,
}

pub struct QueryEngine {
    storage: MetadataReaderTS,
    schemas: Vec<StructuredDataSchema>,
    namespace: String,
}

impl QueryEngine {
    pub fn new(
        storage: MetadataReaderTS,
        schemas: Vec<StructuredDataSchema>,
        namespace: &str,
    ) -> Self {
        Self {
            storage,
            schemas,
            namespace: namespace.to_string(),
        }
    }

    fn schema_to_sql_ddl(&self, schema: &StructuredDataSchema) -> Result<String> {
        let mut columns = vec![r#""content_id" TEXT NULL"#.to_string()];
        for (column_name, dtype) in schema.columns.iter() {
            let dtype = match dtype {
                SchemaColumnType::Null => "OBJECT",
                SchemaColumnType::Array => "LIST",
                SchemaColumnType::BigInt => "BIGINT",
                SchemaColumnType::Bool => "BOOLEAN",
                SchemaColumnType::Float => "FLOAT",
                SchemaColumnType::Int => "INT",
                SchemaColumnType::Text => "TEXT",
                SchemaColumnType::Object => "JSON",
            };
            columns.push(format!(r#""{}" {} NULL"#, column_name, dtype));
        }
        let column_str = columns.join(",");
        let schema_str = format!(
            r#"CREATE TABLE "{}" ({});"#,
            schema.content_source, column_str
        );
        Ok(schema_str)
    }
}

#[async_trait(?Send)]
impl Store for QueryEngine {
    async fn fetch_all_schemas(&self) -> Result<Vec<Schema>> {
        let mut schema_ddls = vec![];
        for schema in &self.schemas {
            let schema_str = self.schema_to_sql_ddl(schema)?;
            schema_ddls.push(Schema::from_ddl(&schema_str)?);
        }
        Ok(schema_ddls)
    }

    async fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        for schema in &self.schemas {
            if schema.content_source == table_name {
                let schema_str = self.schema_to_sql_ddl(schema)?;
                return Ok(Some(Schema::from_ddl(&schema_str)?));
            }
        }
        Ok(None)
    }

    async fn fetch_data(&self, _table_name: &str, key: &Key) -> Result<Option<DataRow>> {
        if let Key::Str(key) = key {
            let metadata = self
                .storage
                .get_metadata_for_id(&self.namespace, key)
                .await
                .map_err(|e| gluesql::core::error::Error::StorageMsg(e.to_string()))?;
            if let Some(metadata) = metadata {
                let mut out_rows: HashMap<String, gluesql::core::data::Value> = HashMap::new();
                out_rows.insert(
                    "content_id".to_string(),
                    gluesql::core::data::Value::Str(metadata.content_id.clone()),
                );
                let meta = match metadata.metadata.clone() {
                    serde_json::Value::Object(json_map) => HashMap::try_from_json_map(json_map),
                    _ => Err(ValueError::JsonObjectTypeRequired.into()),
                };
                let meta =
                    meta.map_err(|e| gluesql::core::error::Error::StorageMsg(e.to_string()))?;
                out_rows.extend(meta);
                return Ok(Some(DataRow::Map(out_rows)));
            }
            return Ok(None);
        }
        Err(gluesql::core::error::Error::StorageMsg(format!(
            "inavlid key {:?}",
            key
        )))
    }

    async fn scan_data(&self, table_name: &str) -> Result<RowIter> {
        let _ = self
            .schemas
            .iter()
            .find(|schema| schema.content_source == table_name)
            .ok_or_else(|| {
                gluesql::core::error::Error::StorageMsg(format!("table {} not found", table_name))
            })?;

        return self
            .storage
            .scan_metadata(&self.namespace, table_name)
            .await;
    }
}

impl StoreMut for QueryEngine {}
impl Metadata for QueryEngine {}
impl AlterTable for QueryEngine {}
impl Transaction for QueryEngine {}
impl CustomFunctionMut for QueryEngine {}
impl Index for QueryEngine {}
impl IndexMut for QueryEngine {}
impl CustomFunction for QueryEngine {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gluesql::prelude::Glue;
    use nanoid::nanoid;
    use serde_json::json;

    use super::{super::MetadataStorageTS, *};
    use crate::metadata_storage::{sqlite::SqliteIndexManager, ExtractedMetadata};

    async fn create_sqlite_metadata_store() -> Arc<SqliteIndexManager> {
        std::fs::remove_file("/tmp/foo").unwrap_or(());
        SqliteIndexManager::new("/tmp/foo").unwrap()
    }

    fn create_schema(
        ns: &str,
        columns: Vec<(&str, SchemaColumnType)>,
        cs: &str,
    ) -> StructuredDataSchema {
        StructuredDataSchema {
            namespace: ns.to_string(),
            columns: columns
                .into_iter()
                .map(|(name, dtype)| (name.to_string(), dtype))
                .collect(),
            content_source: cs.to_string(),
            id: nanoid!(16),
        }
    }

    #[tokio::test]
    async fn test_schema_to_sql_ddl() {
        let cols1 = vec![
            ("id", SchemaColumnType::Text),
            ("name", SchemaColumnType::Text),
        ];
        let schema = create_schema("test", cols1, "User");
        let cols2 = vec![
            ("id", SchemaColumnType::Text),
            ("foo", SchemaColumnType::Text),
        ];
        let schema2 = create_schema("test", cols2, "User2");
        let query_engine = QueryEngine::new(
            create_sqlite_metadata_store().await,
            vec![schema, schema2],
            "test",
        );
        let glue_query = Glue::new(query_engine);
        let schemas = glue_query.storage.fetch_all_schemas().await.unwrap();
        assert_eq!(schemas.len(), 2);
    }

    #[tokio::test]
    async fn test_query_data() {
        let sqlite_index_manager = create_sqlite_metadata_store().await;
        let ns = "mynamespace";
        let meta1 = ExtractedMetadata::new(
            "test_content_id",
            "test_parent_content_id",
            "test_content_source",
            json!({"name": "diptanu", "role": "founder"}),
            "test_extractor",
            "test_extractor_policy",
        );
        let meta2 = ExtractedMetadata::new(
            "test_content_id",
            "test_parent_content_id",
            "test_content_source",
            json!({"name": "lucas", "role": "engineer"}),
            "test_extractor",
            "test_extractor_policy",
        );
        let meta3 = ExtractedMetadata::new(
            "test_content_id",
            "test_parent_content_id",
            "test_content_source",
            json!({"name": "zaid", "role": "engineer"}),
            "test_extractor",
            "test_extractor_policy",
        );
        (sqlite_index_manager.clone() as MetadataStorageTS)
            .add_metadata(ns, meta1)
            .await
            .unwrap();
        (sqlite_index_manager.clone() as MetadataStorageTS)
            .add_metadata(ns, meta2)
            .await
            .unwrap();
        (sqlite_index_manager.clone() as MetadataStorageTS)
            .add_metadata(ns, meta3)
            .await
            .unwrap();
        let cols1 = vec![
            ("name", SchemaColumnType::Text),
            ("role", SchemaColumnType::Text),
        ];
        let schema = create_schema(ns, cols1, "test_content_source");
        let result = run_query(
            "SELECT * FROM test_content_source;".to_string(),
            sqlite_index_manager,
            vec![schema],
            ns.to_string(),
        )
        .await
        .unwrap();
        assert_eq!(result.len(), 3);
        for res in result {
            assert_eq!(res.data.get("content_id").unwrap(), "test_content_id");
            assert!(
                ["founder", "engineer"].contains(&res.data.get("role").unwrap().as_str().unwrap())
            );
        }
    }
}
