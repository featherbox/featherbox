use crate::{
    config::{adapter::AdapterConfig, project::ConnectionConfig},
    pipeline::{
        build::TimeRange,
        delta::{DeltaManager, DeltaMetadata},
        file_processor::{FileProcessor, FileSystem},
    },
};
use anyhow::Result;
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Adapter {
    config: AdapterConfig,
    delta_manager: Arc<DeltaManager>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, delta_manager: Arc<DeltaManager>) -> Self {
        Self {
            config,
            delta_manager,
        }
    }

    pub async fn execute_import(
        &self,
        table_name: &str,
        time_range: Option<TimeRange>,
        app_db: &DatabaseConnection,
        action_id: i32,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<Option<DeltaMetadata>> {
        let filesystem = self.create_filesystem(connections).await?;

        let file_paths = FileProcessor::files_for_processing_with_filesystem(
            &self.config,
            time_range,
            &filesystem,
        )
        .await?;

        if file_paths.is_empty() {
            return Ok(None);
        }

        let delta_metadata = self
            .delta_manager
            .process_delta_for_adapter(&self.config, table_name, &file_paths, app_db, action_id)
            .await?;

        Ok(Some(delta_metadata))
    }

    async fn create_filesystem(
        &self,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<FileSystem> {
        if let Some(connections) = connections {
            if let Some(connection) = connections.get(&self.config.connection) {
                return FileSystem::from_connection(connection).await;
            }
        }
        Ok(FileSystem::new_local(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig};
    use crate::pipeline::delta::DeltaManager;
    use crate::pipeline::ducklake::{CatalogConfig, DuckLake, StorageConfig};
    use std::path::Path;

    fn create_test_adapter_config() -> AdapterConfig {
        AdapterConfig {
            connection: "local".to_string(),
            description: None,
            file: FileConfig {
                path: "test_data/*.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: Some(true),
            },
            columns: vec![],
            limits: None,
        }
    }

    #[tokio::test]
    async fn test_adapter_creation() {
        let config = create_test_adapter_config();

        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_catalog.sqlite".to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let delta_manager =
            Arc::new(DeltaManager::new(Path::new("/tmp"), ducklake.clone()).unwrap());

        let adapter = Adapter::new(config, delta_manager);
        assert_eq!(adapter.config.connection, "local");
    }
}
