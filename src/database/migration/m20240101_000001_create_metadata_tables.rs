use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(FboxGraphs::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FboxGraphs::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FboxGraphs::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FboxPipelines::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FboxPipelines::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(FboxPipelines::GraphId).integer().not_null())
                    .col(
                        ColumnDef::new(FboxPipelines::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("graph_id")
                            .from(FboxPipelines::Table, FboxPipelines::GraphId)
                            .to(FboxGraphs::Table, FboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FboxNodes::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FboxNodes::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(FboxNodes::GraphId).integer().not_null())
                    .col(ColumnDef::new(FboxNodes::Name).string().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("graph_id")
                            .from(FboxNodes::Table, FboxNodes::GraphId)
                            .to(FboxGraphs::Table, FboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FboxEdges::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FboxEdges::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(FboxEdges::GraphId).integer().not_null())
                    .col(ColumnDef::new(FboxEdges::FromNode).string().not_null())
                    .col(ColumnDef::new(FboxEdges::ToNode).string().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("graph_id")
                            .from(FboxEdges::Table, FboxEdges::GraphId)
                            .to(FboxGraphs::Table, FboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FboxPipelineActions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FboxPipelineActions::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FboxPipelineActions::PipelineId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FboxPipelineActions::TableName)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FboxPipelineActions::ExecutionOrder)
                            .integer()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("pipeline_id")
                            .from(FboxPipelineActions::Table, FboxPipelineActions::PipelineId)
                            .to(FboxPipelines::Table, FboxPipelines::Id),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(FboxPipelineActions::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FboxEdges::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FboxNodes::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FboxPipelines::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FboxGraphs::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum FboxGraphs {
    #[sea_orm(iden = "__fbox_graphs")]
    Table,
    Id,
    CreatedAt,
}

#[derive(DeriveIden)]
enum FboxPipelines {
    #[sea_orm(iden = "__fbox_pipelines")]
    Table,
    Id,
    GraphId,
    CreatedAt,
}

#[derive(DeriveIden)]
enum FboxNodes {
    #[sea_orm(iden = "__fbox_nodes")]
    Table,
    Id,
    GraphId,
    Name,
}

#[derive(DeriveIden)]
enum FboxEdges {
    #[sea_orm(iden = "__fbox_edges")]
    Table,
    Id,
    GraphId,
    FromNode,
    ToNode,
}

#[derive(DeriveIden)]
enum FboxPipelineActions {
    #[sea_orm(iden = "__fbox_pipeline_actions")]
    Table,
    Id,
    PipelineId,
    TableName,
    ExecutionOrder,
}
