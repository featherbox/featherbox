use crate::{
    config::Config,
    pipeline::{
        build::Pipeline,
        delta::{DeltaManager, DeltaMetadata},
        ducklake::DuckLake,
        file_processor::FileProcessor,
    },
};
use anyhow::{Context, Result};
use std::collections::HashMap;

// Re-export types from build module for backward compatibility
// Note: These are already imported in the use statement above

impl Pipeline {
    pub async fn execute(&self, config: &Config, ducklake: &DuckLake) -> Result<()> {
        for action in &self.actions {
            if let Some(adapter) = config.adapters.get(&action.table_name) {
                let file_paths = crate::pipeline::file_processor::FileProcessor::process_pattern(
                    &adapter.file.path,
                    adapter,
                )?;

                if !file_paths.is_empty() {
                    let sql = ducklake.build_create_and_load_sql_multiple(
                        &action.table_name,
                        adapter,
                        &file_paths,
                    )?;
                    ducklake.execute_batch(&sql).with_context(|| {
                        format!(
                            "Failed to execute adapter SQL for table '{}'",
                            action.table_name
                        )
                    })?;
                }
            } else if let Some(model) = config.models.get(&action.table_name) {
                ducklake.transform(model, &action.table_name).await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
    }

    pub async fn execute_with_delta(
        &self,
        config: &Config,
        ducklake: &DuckLake,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<()> {
        let delta_manager = DeltaManager::new(&config.project_root)?;

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;

        for (idx, action) in self.actions.iter().enumerate() {
            let action_id = action_ids[idx];

            if let Some(adapter) = config.adapters.get(&action.table_name) {
                let file_paths =
                    FileProcessor::files_for_processing(adapter, action.time_range.clone())?;

                if !file_paths.is_empty() {
                    ducklake
                        .process_delta(
                            adapter,
                            &action.table_name,
                            &file_paths,
                            &delta_manager,
                            app_db,
                            action_id,
                        )
                        .await?;
                }
            } else if let Some(model) = config.models.get(&action.table_name) {
                let dependency_deltas = self
                    .collect_dependency_deltas(&action.table_name, &delta_manager, app_db, config)
                    .await?;

                ducklake
                    .transform_with_delta(
                        model,
                        &action.table_name,
                        &delta_manager,
                        app_db,
                        action_id,
                        &dependency_deltas,
                    )
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

    async fn collect_dependency_deltas(
        &self,
        model_table_name: &str,
        delta_manager: &DeltaManager,
        app_db: &sea_orm::DatabaseConnection,
        config: &Config,
    ) -> Result<HashMap<String, DeltaMetadata>> {
        use crate::dependency::graph::from_table;

        let model = config
            .models
            .get(model_table_name)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model_table_name))?;

        let dependencies = from_table(&model.sql);
        let mut dependency_deltas = HashMap::new();

        for dep_table in dependencies {
            if config.adapters.contains_key(&dep_table) {
                if let Some(delta_metadata) = delta_manager
                    .latest_delta_metadata(app_db, &dep_table)
                    .await?
                {
                    dependency_deltas.insert(dep_table, delta_metadata);
                }
            }
        }

        Ok(dependency_deltas)
    }

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
}
