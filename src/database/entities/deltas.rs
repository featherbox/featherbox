use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "__fbox_deltas")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub action_id: i32,
    pub insert_delta_path: String,
    pub update_delta_path: String,
    pub delete_delta_path: String,
    pub created_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pipeline_actions::Entity",
        from = "Column::ActionId",
        to = "super::pipeline_actions::Column::Id"
    )]
    PipelineAction,
}

impl Related<super::pipeline_actions::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::PipelineAction.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
