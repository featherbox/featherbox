use crate::{
    config::{Config, model::ModelConfig},
    pipeline::{
        delta::{DeltaManager, DeltaMetadata},
        ducklake::DuckLake,
    },
};
use anyhow::{Context, Result};
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Model {
    config: ModelConfig,
    ducklake: Arc<DuckLake>,
    delta_manager: Arc<DeltaManager>,
}

impl Model {
    pub fn new(
        config: ModelConfig,
        ducklake: Arc<DuckLake>,
        delta_manager: Arc<DeltaManager>,
    ) -> Self {
        Self {
            config,
            ducklake,
            delta_manager,
        }
    }

    pub async fn execute_transform(
        &self,
        table_name: &str,
        app_db: &DatabaseConnection,
        action_id: i32,
        project_config: &Config,
    ) -> Result<Option<DeltaMetadata>> {
        let dependency_deltas = self
            .collect_dependency_deltas(table_name, app_db, project_config)
            .await?;

        let delta_metadata = self
            .transform_model_with_delta(table_name, app_db, action_id, &dependency_deltas)
            .await?;

        Ok(delta_metadata)
    }

    fn transform_model(&self, model_name: &str) -> Result<()> {
        let create_table_sql = format!(
            "CREATE OR REPLACE TABLE {} AS ({});",
            model_name, self.config.sql
        );

        self.ducklake
            .execute_batch(&create_table_sql)
            .with_context(|| {
                format!("Failed to execute model transformation. SQL: {create_table_sql}")
            })?;

        Ok(())
    }

    async fn transform_model_with_delta(
        &self,
        model_name: &str,
        app_db: &DatabaseConnection,
        action_id: i32,
        dependency_deltas: &HashMap<String, DeltaMetadata>,
    ) -> Result<Option<DeltaMetadata>> {
        if dependency_deltas.is_empty() {
            self.transform_model(model_name)?;
            return Ok(None);
        }

        let modified_sql = self.rewrite_sql_for_deltas(&self.config.sql, dependency_deltas)?;

        let delta_metadata = self
            .delta_manager
            .process_delta_for_model(model_name, &modified_sql, app_db, action_id)
            .await?;

        Ok(Some(delta_metadata))
    }

    fn rewrite_sql_for_deltas(
        &self,
        sql: &str,
        dependency_deltas: &HashMap<String, DeltaMetadata>,
    ) -> Result<String> {
        use sqlparser::{dialect::DuckDbDialect, parser::Parser};

        let dialect = DuckDbDialect {};
        let parsed = Parser::parse_sql(&dialect, sql)
            .with_context(|| format!("Failed to parse SQL: {sql}"))?;

        if parsed.is_empty() {
            return Err(anyhow::anyhow!("Empty SQL statement"));
        }

        let mut modified_sql = sql.to_string();

        for (table_name, delta_metadata) in dependency_deltas {
            let delta_function_call =
                format!("read_parquet('{}')", delta_metadata.insert_delta_path);

            let patterns = vec![
                (
                    format!(r"(?i)\bFROM\s+{}\b", regex::escape(table_name)),
                    format!("FROM {delta_function_call}"),
                ),
                (
                    format!(r"(?i)\bJOIN\s+{}\b", regex::escape(table_name)),
                    format!("JOIN {delta_function_call}"),
                ),
                (
                    format!(r"(?i)\b{}\s+AS\b", regex::escape(table_name)),
                    format!("{delta_function_call} AS"),
                ),
                (
                    format!(r",\s*{}\b", regex::escape(table_name)),
                    format!(", {delta_function_call}"),
                ),
            ];

            for (pattern_str, replacement) in patterns {
                let re = regex::Regex::new(&pattern_str).with_context(|| {
                    format!("Failed to create regex for pattern: {pattern_str}")
                })?;

                modified_sql = re
                    .replace_all(&modified_sql, replacement.as_str())
                    .to_string();
            }
        }

        let _validated = Parser::parse_sql(&dialect, &modified_sql)
            .with_context(|| format!("Modified SQL is not valid: {modified_sql}"))?;

        Ok(modified_sql)
    }

    async fn collect_dependency_deltas(
        &self,
        model_table_name: &str,
        app_db: &DatabaseConnection,
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
                if let Some(delta_metadata) = self
                    .delta_manager
                    .latest_delta_metadata(app_db, &dep_table)
                    .await?
                {
                    dependency_deltas.insert(dep_table, delta_metadata);
                }
            }
        }

        Ok(dependency_deltas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_model_config() -> ModelConfig {
        ModelConfig {
            sql: "SELECT * FROM test_table".to_string(),
            description: None,
            max_age: Some(3600),
        }
    }

    #[tokio::test]
    async fn test_model_creation() {
        use crate::pipeline::delta::DeltaManager;
        use crate::pipeline::ducklake::{CatalogConfig, StorageConfig};
        use std::path::Path;

        let config = create_test_model_config();

        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_catalog.sqlite".to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let delta_manager =
            Arc::new(DeltaManager::new(Path::new("/tmp"), ducklake.clone()).unwrap());

        let model = Model::new(config, ducklake, delta_manager);
        assert_eq!(model.config.sql, "SELECT * FROM test_table");
    }
}
