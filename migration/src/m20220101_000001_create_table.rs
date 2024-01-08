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

        manager
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
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let _ = manager
            .drop_table(Table::drop().table(Index::Table).to_owned())
            .await;
        manager
            .drop_table(Table::drop().table(AttributesIndex::Table).to_owned())
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
