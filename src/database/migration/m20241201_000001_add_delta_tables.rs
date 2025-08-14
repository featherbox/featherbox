use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Deltas::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Deltas::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Deltas::ActionId).integer().not_null())
                    .col(ColumnDef::new(Deltas::InsertDeltaPath).string().not_null())
                    .col(ColumnDef::new(Deltas::UpdateDeltaPath).string().not_null())
                    .col(ColumnDef::new(Deltas::DeleteDeltaPath).string().not_null())
                    .col(ColumnDef::new(Deltas::CreatedAt).date_time().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_deltas_action_id")
                            .from(Deltas::Table, Deltas::ActionId)
                            .to(PipelineActions::Table, PipelineActions::Id),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Deltas::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Deltas {
    #[sea_orm(iden = "__fbox_deltas")]
    Table,
    Id,
    ActionId,
    InsertDeltaPath,
    UpdateDeltaPath,
    DeleteDeltaPath,
    CreatedAt,
}

#[derive(DeriveIden)]
enum PipelineActions {
    #[sea_orm(iden = "__fbox_pipeline_actions")]
    Table,
    Id,
}
