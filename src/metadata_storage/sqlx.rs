use sqlx::Row;

use super::ExtractedMetadata;

pub fn row_to_extracted_metadata<'a, T: Row>(row: &'a T) -> ExtractedMetadata
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
