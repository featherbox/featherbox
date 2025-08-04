use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "__fbox_nodes")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub graph_id: i32,
    pub name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::graphs::Entity",
        from = "Column::GraphId",
        to = "super::graphs::Column::Id"
    )]
    Graph,
}

impl Related<super::graphs::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Graph.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
