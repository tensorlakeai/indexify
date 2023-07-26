use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let _ = manager
            .create_table(
                Table::create()
                    .table(Index::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Index::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Index::ExtractorName).string().not_null())
                    .col(ColumnDef::new(Index::EmbeddingModel).string().not_null())
                    .col(ColumnDef::new(Index::IndexType).string().not_null())
                    .col(ColumnDef::new(Index::TextSplitter).string().not_null())
                    .col(ColumnDef::new(Index::VectorDb).string().not_null())
                    .col(ColumnDef::new(Index::VectorDbParams).json())
                    .col(ColumnDef::new(Index::UniqueParams).json())
                    .col(ColumnDef::new(Index::RepositoryId).string().not_null())
                    .to_owned(),
            )
            .await?;

        let _ = manager
            .create_table(
                Table::create()
                    .table(Content::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Content::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Content::Text).text().not_null())
                    .col(ColumnDef::new(Content::ContentType).string().not_null())
                    .col(ColumnDef::new(Content::Metadata).json())
                    .col(ColumnDef::new(Content::RepositoryId).string().not_null())
                    .col(ColumnDef::new(Content::MemorySessionId).string())
                    .col(ColumnDef::new(Content::ExtractorsState).json_binary())
                    .to_owned(),
            )
            .await;

        let _ = manager
            .create_table(
                Table::create()
                    .table(IndexChunks::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IndexChunks::ChunkId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(IndexChunks::ContentId).string().not_null())
                    .col(ColumnDef::new(IndexChunks::Text).text().not_null())
                    .col(ColumnDef::new(IndexChunks::IndexName).string().not_null())
                    .to_owned(),
            )
            .await;
        let _ = manager
            .create_table(
                Table::create()
                    .table(MemorySessions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(MemorySessions::SessionId)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(MemorySessions::RepositoryId)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(MemorySessions::Metadata).json())
                    .to_owned(),
            )
            .await;
        let _ = manager
            .create_table(
                Table::create()
                    .table(ExtractionEvent::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ExtractionEvent::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ExtractionEvent::Payload).json().not_null())
                    .col(
                        ColumnDef::new(ExtractionEvent::AllocationInfo)
                            .json()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ExtractionEvent::ProcessedAt)
                            .big_unsigned()
                            .null(),
                    )
                    .to_owned(),
            )
            .await;

        let _ = manager
            .create_table(
                Table::create()
                    .table(Work::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Work::Id).string().not_null().primary_key())
                    .col(ColumnDef::new(Work::State).string().not_null())
                    .col(ColumnDef::new(Work::WorkerId).string())
                    .col(ColumnDef::new(Work::ContentId).string().not_null())
                    .col(ColumnDef::new(Work::IndexName).string().not_null())
                    .col(ColumnDef::new(Work::Extractor).string().not_null())
                    .col(ColumnDef::new(Work::ExtractorParams).json_binary())
                    .col(ColumnDef::new(Work::RepositoryId).string().not_null())
                    .to_owned(),
            )
            .await;

        let _ = manager
            .create_table(
                Table::create()
                    .table(ExtractedData::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ExtractedData::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(ExtractedData::RepositoryId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExtractedData::ExtractorID)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ExtractedData::Data).json_binary().not_null())
                    .col(
                        ColumnDef::new(ExtractedData::CreatedAt)
                            .big_unsigned()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await;

        let _ = manager
            .create_table(
                Table::create()
                    .table(Extractors::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Extractors::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Extractors::ExtractorType)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Extractors::Config).json_binary())
                    .to_owned(),
            )
            .await;

        manager
            .create_table(
                Table::create()
                    .table(DataRepository::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(DataRepository::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(DataRepository::ExtractorBindings).json())
                    .col(ColumnDef::new(DataRepository::Metadata).json())
                    .col(ColumnDef::new(DataRepository::DataConnectors).json())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let _ = manager
            .drop_table(Table::drop().table(Index::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(IndexChunks::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(Content::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(MemorySessions::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(ExtractionEvent::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(DataRepository::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(Work::Table).to_owned())
            .await;
        let _ = manager
            .drop_table(Table::drop().table(ExtractedData::Table).to_owned())
            .await;
        manager
            .drop_table(Table::drop().table(Extractors::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum Index {
    Table,
    Name,
    ExtractorName,
    IndexType,
    TextSplitter,
    EmbeddingModel,
    VectorDb,
    VectorDbParams,
    UniqueParams,
    RepositoryId,
}

#[derive(Iden)]
enum IndexChunks {
    Table,
    ContentId,
    ChunkId,
    Text,
    IndexName,
}

#[derive(Iden)]
enum Content {
    Table,
    Id,
    ContentType,
    Text,
    Metadata,
    RepositoryId,
    MemorySessionId,
    ExtractorsState,
}

#[derive(Iden)]
enum MemorySessions {
    Table,
    SessionId,
    RepositoryId,
    Metadata,
}

#[derive(Iden)]
enum ExtractionEvent {
    Table,
    Id,
    Payload,
    AllocationInfo,
    ProcessedAt,
}

#[derive(Iden)]
enum DataRepository {
    Table,
    Name,
    ExtractorBindings,
    Metadata,
    DataConnectors,
}

#[derive(Iden)]
enum Work {
    Table,
    Id,
    State,
    WorkerId,
    ContentId,
    IndexName,
    Extractor,
    ExtractorParams,
    RepositoryId,
}

#[derive(Iden)]
enum ExtractedData {
    Table,
    Id,
    RepositoryId,
    ExtractorID,
    Data,
    CreatedAt,
}

#[derive(Iden)]
enum Extractors {
    Table,
    Id,
    ExtractorType,
    Config,
}
