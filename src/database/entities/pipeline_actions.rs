use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "__featherbox_pipeline_actions")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub pipeline_id: i32,
    pub table_name: String,
    pub execution_order: i32,
    pub status: String,
    pub started_at: Option<DateTime>,
    pub completed_at: Option<DateTime>,
    pub error_message: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipelines::Entity",
        from = "Column::PipelineId",
        to = "super::pipelines::Column::Id"
    )]
    Pipeline,
}

impl Related<super::pipelines::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Pipeline.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
