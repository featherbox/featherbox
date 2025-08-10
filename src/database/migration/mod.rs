use sea_orm_migration::prelude::*;

mod m20240101_000001_create_metadata_tables;
mod m20240102_000001_add_range_to_pipeline_actions;
mod m20241201_000001_add_delta_tables;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20240101_000001_create_metadata_tables::Migration),
            Box::new(m20240102_000001_add_range_to_pipeline_actions::Migration),
            Box::new(m20241201_000001_add_delta_tables::Migration),
        ]
    }
}
