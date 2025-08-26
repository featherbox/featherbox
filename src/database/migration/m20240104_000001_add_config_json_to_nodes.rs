use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FboxNodes::Table)
                    .add_column(ColumnDef::new(FboxNodes::ConfigJson).text().null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FboxNodes::Table)
                    .drop_column(FboxNodes::ConfigJson)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum FboxNodes {
    #[sea_orm(iden = "__fbox_nodes")]
    Table,
    ConfigJson,
}
