use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(FeatherboxGraphs::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FeatherboxGraphs::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxGraphs::CreatedAt)
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
                    .table(FeatherboxPipelines::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FeatherboxPipelines::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxPipelines::GraphId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxPipelines::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_pipelines_graph_id")
                            .from(FeatherboxPipelines::Table, FeatherboxPipelines::GraphId)
                            .to(FeatherboxGraphs::Table, FeatherboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FeatherboxNodes::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FeatherboxNodes::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxNodes::GraphId)
                            .integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FeatherboxNodes::Name).string().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_nodes_graph_id")
                            .from(FeatherboxNodes::Table, FeatherboxNodes::GraphId)
                            .to(FeatherboxGraphs::Table, FeatherboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FeatherboxEdges::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FeatherboxEdges::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxEdges::GraphId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxEdges::FromNode)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FeatherboxEdges::ToNode).string().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_edges_graph_id")
                            .from(FeatherboxEdges::Table, FeatherboxEdges::GraphId)
                            .to(FeatherboxGraphs::Table, FeatherboxGraphs::Id),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(FeatherboxPipelineActions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(FeatherboxPipelineActions::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxPipelineActions::PipelineId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxPipelineActions::TableName)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(FeatherboxPipelineActions::ExecutionOrder)
                            .integer()
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_actions_pipeline_id")
                            .from(
                                FeatherboxPipelineActions::Table,
                                FeatherboxPipelineActions::PipelineId,
                            )
                            .to(FeatherboxPipelines::Table, FeatherboxPipelines::Id),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(FeatherboxPipelineActions::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(FeatherboxEdges::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FeatherboxNodes::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FeatherboxPipelines::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(FeatherboxGraphs::Table).to_owned())
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum FeatherboxGraphs {
    #[sea_orm(iden = "__featherbox_graphs")]
    Table,
    Id,
    CreatedAt,
}

#[derive(DeriveIden)]
enum FeatherboxPipelines {
    #[sea_orm(iden = "__featherbox_pipelines")]
    Table,
    Id,
    GraphId,
    CreatedAt,
}

#[derive(DeriveIden)]
enum FeatherboxNodes {
    #[sea_orm(iden = "__featherbox_nodes")]
    Table,
    Id,
    GraphId,
    Name,
}

#[derive(DeriveIden)]
enum FeatherboxEdges {
    #[sea_orm(iden = "__featherbox_edges")]
    Table,
    Id,
    GraphId,
    FromNode,
    ToNode,
}

#[derive(DeriveIden)]
enum FeatherboxPipelineActions {
    #[sea_orm(iden = "__featherbox_pipeline_actions")]
    Table,
    Id,
    PipelineId,
    TableName,
    ExecutionOrder,
}
