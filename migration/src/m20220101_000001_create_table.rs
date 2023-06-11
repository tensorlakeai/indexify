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
                    .col(ColumnDef::new(Index::EmbeddingModel).string().not_null())
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
                    .col(ColumnDef::new(Content::ExtractorsState).json())
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
                    .col(ColumnDef::new(DataRepository::Extractors).json())
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
        manager
            .drop_table(Table::drop().table(DataRepository::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum Index {
    Table,
    Name,
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
enum DataRepository {
    Table,
    Name,
    Extractors,
    Metadata,
    DataConnectors,
}
