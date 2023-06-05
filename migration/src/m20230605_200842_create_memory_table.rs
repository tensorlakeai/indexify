use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
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
                    .col(ColumnDef::new(MemorySessions::IndexName).string().not_null())
                    .col(ColumnDef::new(MemorySessions::Metadata).json())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(MemorySessions::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum MemorySessions {
    Table,
    SessionId,
    IndexName,
    Metadata
}
