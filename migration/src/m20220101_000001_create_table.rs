use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Index::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Index::Name).string().primary_key())
                    .col(ColumnDef::new(Index::EmbeddingModel).string().not_null())
                    .col(ColumnDef::new(Index::TextSplitter).string().not_null())
                    .col(ColumnDef::new(Index::VectorDb).string().not_null())
                    .col(ColumnDef::new(Index::VectorDbParams).json())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Index::Table).to_owned())
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
}
