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
                    .col(ColumnDef::new(Index::VectorIndexName).string())
                    .col(ColumnDef::new(Index::ExtractorName).string().not_null())
                    .col(ColumnDef::new(Index::IndexType).string().not_null())
                    .col(ColumnDef::new(Index::IndexSchema).json_binary().not_null())
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
                    .col(ColumnDef::new(Content::Metadata).json_binary())
                    .col(ColumnDef::new(Content::RepositoryId).string().not_null())
                    .col(ColumnDef::new(Content::ExtractorBindingsState).json_binary())
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
                    .table(Events::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Events::Id).string().not_null().primary_key())
                    .col(ColumnDef::new(Events::RepositoryId).string().not_null())
                    .col(ColumnDef::new(Events::Message).string().not_null())
                    .col(
                        ColumnDef::new(Events::UnixTimeStamp)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Events::Metadata).json_binary())
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
                    .col(
                        ColumnDef::new(ExtractionEvent::Payload)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExtractionEvent::AllocationInfo)
                            .json_binary()
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
                    .col(
                        ColumnDef::new(Work::ExtractorParams)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Work::RepositoryId).string().not_null())
                    .to_owned(),
            )
            .await;

        let _ = manager
            .create_table(
                Table::create()
                    .table(AttributesIndex::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(AttributesIndex::Id)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::RepositoryId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::ExtractorID)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::IndexName)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::Data)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::ContentId)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AttributesIndex::CreatedAt)
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
                    .col(ColumnDef::new(Extractors::Description).string().not_null())
                    .col(
                        ColumnDef::new(Extractors::InputParams)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Extractors::OutputSchema)
                            .json_binary()
                            .not_null(),
                    )
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
                    .col(ColumnDef::new(DataRepository::ExtractorBindings).json_binary())
                    .col(ColumnDef::new(DataRepository::Metadata).json_binary())
                    .col(ColumnDef::new(DataRepository::DataConnectors).json_binary())
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
            .drop_table(Table::drop().table(Events::Table).to_owned())
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
            .drop_table(Table::drop().table(AttributesIndex::Table).to_owned())
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
    VectorIndexName,
    ExtractorName,
    IndexType,
    IndexSchema,
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
    ExtractorBindingsState,
}

#[derive(Iden)]
enum Events {
    Table,
    Id,
    RepositoryId,
    Message,
    UnixTimeStamp,
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
enum AttributesIndex {
    Table,
    Id,
    RepositoryId,
    ExtractorID,
    Data,
    IndexName,
    ContentId,
    CreatedAt,
}

#[derive(Iden)]
enum Extractors {
    Table,
    Id,
    Description,
    InputParams,
    OutputSchema,
}
