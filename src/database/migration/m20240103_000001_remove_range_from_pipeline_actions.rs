use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxPipelineActions::Table)
                    .drop_column(FeatherboxPipelineActions::Since)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxPipelineActions::Table)
                    .drop_column(FeatherboxPipelineActions::Until)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxPipelineActions::Table)
                    .add_column(
                        ColumnDef::new(FeatherboxPipelineActions::Since)
                            .date_time()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(FeatherboxPipelineActions::Table)
                    .add_column(
                        ColumnDef::new(FeatherboxPipelineActions::Until)
                            .date_time()
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum FeatherboxPipelineActions {
    #[sea_orm(iden = "__featherbox_pipeline_actions")]
    Table,
    Since,
    Until,
}
