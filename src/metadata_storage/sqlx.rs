use std::collections::BTreeMap;

use gluesql::core::{
    data::{Key, Value},
    error::{Error::StorageMsg as GlueStorageError, Result as GlueResult},
    store::DataRow,
};
use itertools::Itertools;

use super::ExtractedMetadata;

pub fn row_to_extracted_metadata<'a, T: sqlx::Row>(row: &'a T) -> ExtractedMetadata
where
    usize: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, T::Database> + sqlx::Type<T::Database>,
    sqlx::types::Json<serde_json::Value>: sqlx::Decode<'a, T::Database> + sqlx::Type<T::Database>,
{
    let id: String = row.get(0);
    let extractor: String = row.get(2);
    let extraction_policy: String = row.get(3);
    let content_source: String = row.get(4);
    let data: serde_json::Value = row.get(6);
    let content_id: String = row.get(7);
    let parent_content_id: String = row.get(8);
    ExtractedMetadata {
        id,
        content_id,
        parent_content_id,
        content_source,
        metadata: data,
        extractor_name: extractor,
        extraction_policy,
    }
}

pub fn row_to_metadata_scan_item<'a, T: sqlx::Row>(row: &'a T) -> GlueResult<(Key, DataRow)>
where
    usize: sqlx::ColumnIndex<T>,
    String: sqlx::Decode<'a, T::Database> + sqlx::Type<T::Database>,
    sqlx::types::Json<serde_json::Value>: sqlx::Decode<'a, T::Database> + sqlx::Type<T::Database>,
{
    let content_id: String = row.get(0);
    let mut out_rows: Vec<Value> = Vec::new();
    out_rows.push(Value::Str(content_id.clone()));

    let data: serde_json::Value = row.get(1);
    let data = match data {
        serde_json::Value::Object(json_map) => json_map
            .into_iter()
            .map(|(key, value)| {
                let value = Value::try_from(value)?;

                Ok((key, value))
            })
            .collect::<GlueResult<BTreeMap<String, Value>>>()
            .map_err(|e| GlueStorageError(format!("invalid metadata: {}", e)))?,
        _ => return Err(GlueStorageError("expected JSON object".to_string())),
    };
    out_rows.extend(data.values().cloned().collect_vec());

    Ok((Key::Str(content_id), DataRow::Vec(out_rows)))
}
