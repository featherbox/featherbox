use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .add_column(
                        ColumnDef::new(Pipelines::Status)
                            .string()
                            .not_null()
                            .default("PENDING"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .add_column(ColumnDef::new(Pipelines::StartedAt).date_time())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .add_column(ColumnDef::new(Pipelines::CompletedAt).date_time())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .add_column(
                        ColumnDef::new(PipelineActions::Status)
                            .string()
                            .not_null()
                            .default("PENDING"),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .add_column(ColumnDef::new(PipelineActions::StartedAt).date_time())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .add_column(ColumnDef::new(PipelineActions::CompletedAt).date_time())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .add_column(ColumnDef::new(PipelineActions::ErrorMessage).text())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .drop_column(Pipelines::CompletedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .drop_column(Pipelines::StartedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Pipelines::Table)
                    .drop_column(Pipelines::Status)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .drop_column(PipelineActions::ErrorMessage)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .drop_column(PipelineActions::CompletedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .drop_column(PipelineActions::StartedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(PipelineActions::Table)
                    .drop_column(PipelineActions::Status)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Pipelines {
    #[sea_orm(iden = "__fbox_pipelines")]
    Table,
    Status,
    StartedAt,
    CompletedAt,
}

#[derive(DeriveIden)]
enum PipelineActions {
    #[sea_orm(iden = "__fbox_pipeline_actions")]
    Table,
    Status,
    StartedAt,
    CompletedAt,
    ErrorMessage,
}
