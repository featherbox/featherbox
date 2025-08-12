use crate::{
    config::Config,
    pipeline::{
        build::Pipeline, delta::DeltaManager, ducklake::DuckLake, file_processor::FileSystem,
        importer::Importer, transformer::Transformer,
    },
};
use anyhow::Result;

impl Pipeline {
    async fn create_filesystem_for_adapter(
        config: &Config,
        adapter_connection: &str,
    ) -> Result<FileSystem> {
        if let Some(connection) = config.project.connections.get(adapter_connection) {
            FileSystem::from_connection(connection).await
        } else {
            Ok(FileSystem::new_local(None))
        }
    }

    pub async fn execute(&self, config: &Config, ducklake: &DuckLake) -> Result<()> {
        let import_processor = Importer::new(ducklake);
        let transform_processor = Transformer::new(ducklake);

        for action in &self.actions {
            if let Some(adapter) = config.adapters.get(&action.table_name) {
                let filesystem =
                    Self::create_filesystem_for_adapter(config, &adapter.connection).await?;
                import_processor
                    .import_adapter_with_filesystem(adapter, &action.table_name, &filesystem)
                    .await?;
            } else if let Some(model) = config.models.get(&action.table_name) {
                transform_processor.transform_model(model, &action.table_name)?;
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
        tracing::info!(
            "Starting execute_with_delta with {} actions",
            self.actions.len()
        );
        let delta_manager = DeltaManager::new(&config.project_root)?;
        let import_processor = Importer::new(ducklake);
        let transform_processor = Transformer::new(ducklake);

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;
        tracing::info!(
            "Retrieved {} action IDs: {:?}",
            action_ids.len(),
            action_ids
        );

        for (idx, action) in self.actions.iter().enumerate() {
            let action_id = action_ids[idx];
            tracing::info!(
                "Processing action {}: table_name = '{}', action_id = {}",
                idx,
                action.table_name,
                action_id
            );

            if let Some(adapter) = config.adapters.get(&action.table_name) {
                tracing::info!(
                    "Found adapter for table '{}', starting import",
                    action.table_name
                );
                let filesystem =
                    Self::create_filesystem_for_adapter(config, &adapter.connection).await?;
                import_processor
                    .import_adapter_with_delta_and_filesystem(
                        adapter,
                        &action.table_name,
                        action.time_range.clone(),
                        &delta_manager,
                        app_db,
                        action_id,
                        &filesystem,
                    )
                    .await?;
                tracing::info!("Completed import for table '{}'", action.table_name);
            } else if let Some(model) = config.models.get(&action.table_name) {
                tracing::info!(
                    "Found model for table '{}', starting transform",
                    action.table_name
                );
                let dependency_deltas = transform_processor
                    .collect_dependency_deltas(&action.table_name, &delta_manager, app_db, config)
                    .await?;

                transform_processor
                    .transform_model_with_delta(
                        model,
                        &action.table_name,
                        &delta_manager,
                        app_db,
                        action_id,
                        &dependency_deltas,
                    )
                    .await?;
            } else {
                tracing::error!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                );
                tracing::info!(
                    "Available adapters: {:?}",
                    config.adapters.keys().collect::<Vec<_>>()
                );
                tracing::info!(
                    "Available models: {:?}",
                    config.models.keys().collect::<Vec<_>>()
                );
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
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
