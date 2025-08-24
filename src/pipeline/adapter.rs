use crate::{
    config::{
        adapter::{AdapterConfig, AdapterSource},
        project::ConnectionConfig,
    },
    pipeline::{
        build::TimeRange,
        database::DatabaseSystem,
        ducklake::DuckLake,
        file_processor::{FileProcessor, FileSystem},
    },
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Adapter {
    config: AdapterConfig,
    ducklake: Arc<DuckLake>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, ducklake: Arc<DuckLake>) -> Self {
        Self { config, ducklake }
    }

    pub async fn execute_import(
        &self,
        table_name: &str,
        time_range: Option<TimeRange>,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        match &self.config.source {
            AdapterSource::File { .. } => {
                self.execute_file_import(table_name, time_range, connections)
                    .await
            }
            AdapterSource::Database {
                table_name: source_table,
            } => {
                self.execute_database_import(source_table, table_name, connections)
                    .await
            }
        }
    }

    async fn execute_file_import(
        &self,
        table_name: &str,
        time_range: Option<TimeRange>,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        let filesystem = self.create_filesystem(connections).await?;

        let file_paths =
            FileProcessor::files_for_processing(&self.config, time_range, &filesystem).await?;

        if file_paths.is_empty() {
            return Ok(());
        }

        // let delta_metadata = self
        //     .delta_manager
        //     .process_delta_for_adapter(&self.config, table_name, &file_paths, app_db, action_id)
        //     .await?;
        let query = self.build_import_query_multiple(&self.config, &file_paths)?;

        self.ducklake.create_table_from_query(table_name, &query)?;

        Ok(())
    }

    fn build_import_query_multiple(
        &self,
        adapter: &AdapterConfig,
        file_paths: &[String],
    ) -> Result<String> {
        if file_paths.is_empty() {
            return Err(anyhow::anyhow!("No files to load"));
        }

        if file_paths.len() == 1 {
            let file_path = &file_paths[0];
            return self.build_import_query_single(adapter, file_path);
        }

        let file_paths_str = file_paths
            .iter()
            .map(|p| format!("'{p}'"))
            .collect::<Vec<_>>()
            .join(", ");

        match &adapter.source {
            crate::config::adapter::AdapterSource::File { format, .. } => {
                match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        let query = format!(
                            "SELECT * FROM read_csv_auto([{file_paths_str}], header={has_header})"
                        );
                        Ok(query)
                    }
                    "parquet" => {
                        let query = format!("SELECT * FROM read_parquet([{file_paths_str}])");
                        Ok(query)
                    }
                    "json" => {
                        let query = format!("SELECT * FROM read_json_auto([{file_paths_str}])");
                        Ok(query)
                    }
                    _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Only file sources are supported in delta processing"
            )),
        }
    }

    fn build_import_query_single(
        &self,
        adapter: &AdapterConfig,
        file_path: &str,
    ) -> Result<String> {
        match &adapter.source {
            crate::config::adapter::AdapterSource::File { format, .. } => {
                match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        let query = format!(
                            "SELECT * FROM read_csv_auto('{file_path}', header={has_header})"
                        );
                        Ok(query)
                    }
                    "parquet" => {
                        let query = format!("SELECT * FROM read_parquet('{file_path}')");
                        Ok(query)
                    }
                    "json" => {
                        let query = format!("SELECT * FROM read_json_auto('{file_path}')");
                        Ok(query)
                    }
                    _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Only file sources are supported in delta processing"
            )),
        }
    }

    async fn execute_database_import(
        &self,
        source_table: &str,
        target_table: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        let connection = self.get_connection(connections)?;
        let db_system = DatabaseSystem::from_connection(connection)?;

        let db_alias = &db_system.generate_alias();
        let attach_result = self.attach_and_validate_database(db_alias, &db_system, source_table);
        if let Err(e) = attach_result {
            if let Err(detach_err) = self.detach_database(db_alias, &db_system) {
                eprintln!("Warning: Failed to detach database during cleanup: {detach_err}");
            }
            return Err(e);
        }

        let query = db_system.build_read_query(db_alias, source_table);

        self.ducklake
            .create_table_from_query(target_table, &query)?;

        Ok(())
    }

    fn detach_database(&self, db_alias: &str, db_system: &DatabaseSystem) -> Result<()> {
        self.ducklake
            .execute_batch(&db_system.build_detach_query(db_alias)?)
            .with_context(|| format!("Failed to detach SQLite database: {db_alias}"))
    }

    fn attach_and_validate_database(
        &self,
        db_alias: &str,
        db_system: &DatabaseSystem,
        source_table: &str,
    ) -> Result<()> {
        let attach_query = db_system.build_attach_query(db_alias)?;

        self.ducklake
            .execute_batch(&attach_query)
            .with_context(|| format!("Failed to attach SQLite database. Query: {attach_query}"))?;

        let validation_query = db_system.validate_table_exists(db_alias, source_table)?;
        let validation_result = self
            .ducklake
            .query(&validation_query)
            .with_context(|| format!("Failed to validate table existence for: {source_table}"))?;

        let table_exists = !validation_result.is_empty()
            && !validation_result[0].is_empty()
            && validation_result[0][0] != "0";

        if !table_exists {
            return Err(anyhow::anyhow!(
                "Table '{}' does not exist in the source database",
                source_table
            ));
        }

        Ok(())
    }

    fn get_connection<'a>(
        &self,
        connections: Option<&'a HashMap<String, ConnectionConfig>>,
    ) -> Result<&'a ConnectionConfig> {
        if let Some(connections) = connections {
            if let Some(connection) = connections.get(&self.config.connection) {
                Ok(connection)
            } else {
                Err(anyhow::anyhow!(
                    "Connection '{}' not found",
                    self.config.connection
                ))
            }
        } else {
            Err(anyhow::anyhow!("No connections provided"))
        }
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
    use crate::pipeline::ducklake::{CatalogConfig, DuckLake, StorageConfig};

    fn create_test_adapter_config() -> AdapterConfig {
        AdapterConfig {
            connection: "local".to_string(),
            description: None,
            source: AdapterSource::File {
                file: FileConfig {
                    path: "test_data/*.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
            },
            update_strategy: None,
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

        let adapter = Adapter::new(config, ducklake);
        assert_eq!(adapter.config.connection, "local");
    }
}
