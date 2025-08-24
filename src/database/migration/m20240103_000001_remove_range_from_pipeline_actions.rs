use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FboxPipelineActions::Table)
                    .drop_column(FboxPipelineActions::Since)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(FboxPipelineActions::Table)
                    .drop_column(FboxPipelineActions::Until)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(FboxPipelineActions::Table)
                    .add_column(
                        ColumnDef::new(FboxPipelineActions::Since)
                            .date_time()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(FboxPipelineActions::Table)
                    .add_column(
                        ColumnDef::new(FboxPipelineActions::Until)
                            .date_time()
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum FboxPipelineActions {
    #[sea_orm(iden = "__fbox_pipeline_actions")]
    Table,
    Since,
    Until,
}
