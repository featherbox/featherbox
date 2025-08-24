use crate::{config::model::ModelConfig, pipeline::ducklake::DuckLake};
use anyhow::{Context, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct Model {
    config: ModelConfig,
    ducklake: Arc<DuckLake>,
}

impl Model {
    pub fn new(config: ModelConfig, ducklake: Arc<DuckLake>) -> Self {
        Self { config, ducklake }
    }

    pub async fn execute_transform(&self, table_name: &str) -> Result<()> {
        self.transform_model(table_name)?;
        Ok(())
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
        use crate::pipeline::ducklake::{CatalogConfig, StorageConfig};

        let config = create_test_model_config();

        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_catalog.sqlite".to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let model = Model::new(config, ducklake);
        assert_eq!(model.config.sql, "SELECT * FROM test_table");
    }
}
