use crate::{
    config::Config,
    pipeline::{
        build::Pipeline, delta::DeltaManager, ducklake::DuckLake, importer::Importer,
        transformer::Transformer,
    },
};
use anyhow::Result;

impl Pipeline {
    pub async fn execute(&self, config: &Config, ducklake: &DuckLake) -> Result<()> {
        let import_processor = Importer::new(ducklake);
        let transform_processor = Transformer::new(ducklake);

        for action in &self.actions {
            if let Some(adapter) = config.adapters.get(&action.table_name) {
                import_processor.import_adapter(adapter, &action.table_name)?;
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
        let delta_manager = DeltaManager::new(&config.project_root)?;
        let import_processor = Importer::new(ducklake);
        let transform_processor = Transformer::new(ducklake);

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;

        for (idx, action) in self.actions.iter().enumerate() {
            let action_id = action_ids[idx];

            if let Some(adapter) = config.adapters.get(&action.table_name) {
                import_processor
                    .import_adapter_with_delta(
                        adapter,
                        &action.table_name,
                        action.time_range.clone(),
                        &delta_manager,
                        app_db,
                        action_id,
                    )
                    .await?;
            } else if let Some(model) = config.models.get(&action.table_name) {
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
