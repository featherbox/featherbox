use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxNodes::Table)
                    .add_column(ColumnDef::new(FeatherboxNodes::ConfigJson).text().null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxNodes::Table)
                    .drop_column(FeatherboxNodes::ConfigJson)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum FeatherboxNodes {
    #[sea_orm(iden = "__featherbox_nodes")]
    Table,
    ConfigJson,
}
