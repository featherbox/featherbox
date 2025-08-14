use crate::{
    config::Config,
    pipeline::{
        adapter::Adapter, build::Pipeline, delta::DeltaManager, ducklake::DuckLake, model::Model,
    },
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

impl Pipeline {
    async fn get_latest_pipeline_action_ids(
        &self,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<Vec<i32>> {
        use crate::database::entities::{pipeline_actions, pipelines};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

        let latest_pipeline = pipelines::Entity::find()
            .order_by_desc(pipelines::Column::CreatedAt)
            .one(app_db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No pipeline found in database"))?;

        let mut action_ids = Vec::new();

        for action in &self.actions {
            let pipeline_action = pipeline_actions::Entity::find()
                .filter(pipeline_actions::Column::PipelineId.eq(latest_pipeline.id))
                .filter(pipeline_actions::Column::TableName.eq(&action.table_name))
                .one(app_db)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Action for table '{}' not found in latest pipeline",
                        action.table_name
                    )
                })?;

            action_ids.push(pipeline_action.id);
        }

        Ok(action_ids)
    }

    pub async fn execute(
        &self,
        config: &Config,
        ducklake: &DuckLake,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<()> {
        let shared_ducklake = Arc::new(ducklake.clone());
        let shared_delta_manager = Arc::new(DeltaManager::new(
            &config.project_root,
            Arc::clone(&shared_ducklake),
        )?);

        let mut adapters = HashMap::new();
        for (table_name, adapter_config) in &config.adapters {
            let adapter = Adapter::new(adapter_config.clone(), Arc::clone(&shared_delta_manager));
            adapters.insert(table_name.clone(), adapter);
        }

        let mut models = HashMap::new();
        for (table_name, model_config) in &config.models {
            let model = Model::new(
                model_config.clone(),
                Arc::clone(&shared_ducklake),
                Arc::clone(&shared_delta_manager),
            );
            models.insert(table_name.clone(), model);
        }

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;

        for (idx, action) in self.actions.iter().enumerate() {
            let action_id = action_ids[idx];

            if let Some(adapter) = adapters.get(&action.table_name) {
                adapter
                    .execute_import(
                        &action.table_name,
                        action.time_range.clone(),
                        app_db,
                        action_id,
                        Some(&config.project.connections),
                    )
                    .await?;
            } else if let Some(model) = models.get(&action.table_name) {
                model
                    .execute_transform(&action.table_name, app_db, action_id, config)
                    .await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
    }
}
