use crate::config::adapter::AdapterConfig;
use crate::config::project::ConnectionConfig;
use crate::pipeline::{
    build::TimeRange,
    delta::{DeltaManager, DeltaMetadata, DeltaProcessor},
    ducklake::DuckLake,
    file_processor::{FileProcessor, FileSystem},
};
use anyhow::{Context, Result};
use sea_orm::DatabaseConnection;
use std::collections::HashMap;

pub struct Importer<'a> {
    ducklake: &'a DuckLake,
    delta_processor: DeltaProcessor<'a>,
}

impl<'a> Importer<'a> {
    pub fn new(ducklake: &'a DuckLake) -> Self {
        let delta_processor = DeltaProcessor::new(ducklake);
        Self {
            ducklake,
            delta_processor,
        }
    }

    pub fn build_import_query_multiple(
        &self,
        adapter: &AdapterConfig,
        file_paths: &[String],
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<String> {
        if file_paths.is_empty() {
            return Err(anyhow::anyhow!("No files to load"));
        }

        if file_paths.len() == 1 {
            let file_path = &file_paths[0];
            return self.build_import_query_single(adapter, file_path, connections);
        }

        let file_paths_str =
            self.build_file_paths_string(file_paths, &adapter.connection, connections)?;

        match adapter.format.ty.as_str() {
            "csv" => {
                let has_header = adapter.format.has_header.unwrap_or(true);
                let query =
                    format!("SELECT * FROM read_csv_auto([{file_paths_str}], header={has_header})");
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
            _ => Err(anyhow::anyhow!("Unsupported format: {}", adapter.format.ty)),
        }
    }

    pub fn build_import_query_single(
        &self,
        adapter: &AdapterConfig,
        file_path: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<String> {
        let resolved_path = self.resolve_file_path(file_path, &adapter.connection, connections)?;

        match adapter.format.ty.as_str() {
            "csv" => {
                let has_header = adapter.format.has_header.unwrap_or(true);
                let query =
                    format!("SELECT * FROM read_csv_auto('{resolved_path}', header={has_header})");
                Ok(query)
            }
            "parquet" => {
                let query = format!("SELECT * FROM read_parquet('{resolved_path}')");
                Ok(query)
            }
            "json" => {
                let query = format!("SELECT * FROM read_json_auto('{resolved_path}')");
                Ok(query)
            }
            _ => Err(anyhow::anyhow!("Unsupported format: {}", adapter.format.ty)),
        }
    }

    fn build_file_paths_string(
        &self,
        file_paths: &[String],
        connection_name: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<String> {
        let resolved_paths: Result<Vec<String>> = file_paths
            .iter()
            .map(|p| self.resolve_file_path(p, connection_name, connections))
            .collect();

        let resolved_paths = resolved_paths?;
        let file_paths_str = resolved_paths
            .iter()
            .map(|p| format!("'{p}'"))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(file_paths_str)
    }

    fn resolve_file_path(
        &self,
        file_path: &str,
        connection_name: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<String> {
        let Some(connections) = connections else {
            return Ok(file_path.to_string());
        };

        let Some(connection) = connections.get(connection_name) else {
            return Ok(file_path.to_string());
        };

        match connection {
            ConnectionConfig::LocalFile { base_path } => {
                if file_path.starts_with('/') {
                    Ok(file_path.to_string())
                } else {
                    Ok(format!("{base_path}/{file_path}"))
                }
            }
            ConnectionConfig::S3 { bucket, .. } => {
                if file_path.starts_with("s3://") {
                    Ok(file_path.to_string())
                } else {
                    Ok(format!("s3://{bucket}/{file_path}"))
                }
            }
        }
    }

    fn create_empty_table_if_needed(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
    ) -> Result<()> {
        if !adapter.columns.is_empty() {
            let columns: Vec<(String, String)> = adapter
                .columns
                .iter()
                .map(|col| (col.name.clone(), col.ty.clone()))
                .collect();

            tracing::info!(
                "Creating empty table '{}' with {} columns",
                table_name,
                columns.len()
            );
            self.ducklake.create_table(table_name, &columns)?;
        } else {
            tracing::warn!(
                "Cannot create empty table '{}' - no column definitions found",
                table_name
            );
        }
        Ok(())
    }

    pub async fn import_adapter_with_filesystem(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
        filesystem: &FileSystem,
    ) -> Result<()> {
        tracing::info!(
            "Starting import for table '{}' with pattern: {}",
            table_name,
            adapter.file.path
        );
        let file_paths =
            FileProcessor::process_pattern_with_filesystem(&adapter.file.path, adapter, filesystem)
                .await?;

        tracing::info!(
            "Found {} files for table '{}': {:?}",
            file_paths.len(),
            table_name,
            file_paths
        );

        if file_paths.is_empty() {
            tracing::info!(
                "No files found for table '{}', creating empty table if needed",
                table_name
            );
            return self.create_empty_table_if_needed(adapter, table_name);
        }

        let import_query = self.build_import_query_multiple(adapter, &file_paths, None)?;

        tracing::info!("Executing import query: {}", import_query);
        self.ducklake
            .create_table_from_query(table_name, &import_query)
            .with_context(|| format!("Failed to import data into table '{table_name}'"))?;

        tracing::info!("Successfully imported data into table '{table_name}'");
        Ok(())
    }

    pub async fn import_adapter_with_delta_and_filesystem(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
        time_range: Option<TimeRange>,
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
        action_id: i32,
        filesystem: &FileSystem,
    ) -> Result<Option<DeltaMetadata>> {
        let file_paths =
            FileProcessor::files_for_processing_with_filesystem(adapter, time_range, filesystem)
                .await?;
        println!(
            "Found {} files for delta processing: {:?}",
            file_paths.len(),
            file_paths
        );

        if file_paths.is_empty() {
            tracing::info!("No files to process for table '{}'", table_name);
            return Ok(None);
        }

        let delta_metadata = self
            .delta_processor
            .process_delta_for_adapter(
                adapter,
                table_name,
                &file_paths,
                delta_manager,
                app_db,
                action_id,
            )
            .await?;

        Ok(Some(delta_metadata))
    }

    pub fn validate_source_files_schema(
        &self,
        table_name: &str,
        file_paths: &[String],
    ) -> Result<()> {
        self.delta_processor
            .validate_source_files_schema(table_name, file_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig};
    use crate::config::project::S3AuthMethod;
    use crate::pipeline::ducklake::{CatalogConfig, StorageConfig};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_import_adapter() {
        let test_dir = "/tmp/import_processor_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();
        let processor = Importer::new(&ducklake);

        let temp_dir = tempdir().unwrap();
        let test_csv = temp_dir.path().join("test_data.csv");
        std::fs::write(&test_csv, "id,name,age\n1,Alice,25\n2,Bob,30").unwrap();

        let adapter = AdapterConfig {
            connection: "test_table".to_string(),
            description: None,
            file: FileConfig {
                path: test_csv.to_string_lossy().to_string(),
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
        };

        let filesystem = FileSystem::new_local(None);
        processor
            .import_adapter_with_filesystem(&adapter, "test_table", &filesystem)
            .await
            .unwrap();

        let verify_sql = "SELECT * FROM test_table ORDER BY id";
        let results = ducklake.query(verify_sql).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec!["1".to_string(), "Alice".to_string(), "25".to_string()]
        );
        assert_eq!(
            results[1],
            vec!["2".to_string(), "Bob".to_string(), "30".to_string()]
        );
    }

    #[tokio::test]
    async fn test_validate_source_files_schema() {
        let test_dir = "/tmp/import_processor_schema_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();
        let processor = Importer::new(&ducklake);

        ducklake
            .execute_batch(
                "CREATE TABLE test_table (id BIGINT, name VARCHAR, age BIGINT);
                 INSERT INTO test_table VALUES (1, 'Alice', 25);",
            )
            .unwrap();

        let temp_dir = tempdir().unwrap();
        let test_csv = temp_dir.path().join("test_data.csv");
        std::fs::write(&test_csv, "id,name,age\n1,Alice,25\n2,Bob,30").unwrap();

        let file_paths = vec![test_csv.to_string_lossy().to_string()];

        let result = processor.validate_source_files_schema("test_table", &file_paths);
        if let Err(e) = &result {
            println!("Schema validation error: {e}");
        }
        assert!(result.is_ok(), "Schema validation should succeed");

        let invalid_csv = temp_dir.path().join("invalid_data.csv");
        std::fs::write(&invalid_csv, "id,name\n1,Alice\n2,Bob").unwrap();

        let invalid_paths = vec![invalid_csv.to_string_lossy().to_string()];
        let invalid_result = processor.validate_source_files_schema("test_table", &invalid_paths);
        assert!(
            invalid_result.is_err(),
            "Schema validation should fail for mismatched columns"
        );
    }

    #[tokio::test]
    async fn test_import_adapter_with_s3_connection() {
        let test_dir = "/tmp/import_processor_s3_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                auth_method: S3AuthMethod::Explicit,
                access_key_id: "test_key".to_string(),
                secret_access_key: "test_secret".to_string(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections.clone())
                .await
                .unwrap();
        let processor = Importer::new(&ducklake);

        let adapter = AdapterConfig {
            connection: "s3_connection".to_string(),
            description: None,
            file: FileConfig {
                path: "data/test.csv".to_string(),
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
        };

        let result =
            processor.build_import_query_single(&adapter, "data/test.csv", Some(&connections));
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("s3://test-bucket/data/test.csv"));
        assert!(query.contains("read_csv_auto"));
    }

    #[tokio::test]
    async fn test_import_adapter_with_local_connection() {
        let test_dir = "/tmp/import_processor_local_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "local_connection".to_string(),
            ConnectionConfig::LocalFile {
                base_path: "/data/local".to_string(),
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections.clone())
                .await
                .unwrap();
        let processor = Importer::new(&ducklake);

        let adapter = AdapterConfig {
            connection: "local_connection".to_string(),
            description: None,
            file: FileConfig {
                path: "test.csv".to_string(),
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
        };

        let result = processor.build_import_query_single(&adapter, "test.csv", Some(&connections));
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("/data/local/test.csv"));
        assert!(query.contains("read_csv_auto"));
    }

    #[tokio::test]
    async fn test_process_pattern_with_s3_connection() {
        let test_dir = "/tmp/import_processor_pattern_s3_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                auth_method: S3AuthMethod::Explicit,
                access_key_id: "test_key".to_string(),
                secret_access_key: "test_secret".to_string(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections.clone())
                .await
                .unwrap();
        let processor = Importer::new(&ducklake);

        let adapter = AdapterConfig {
            connection: "s3_connection".to_string(),
            description: None,
            file: FileConfig {
                path: "data/{YYYY}-{MM}-{DD}/events.json".to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: FormatConfig {
                ty: "json".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
            columns: vec![],
            limits: None,
        };

        let result = processor.build_import_query_single(
            &adapter,
            "data/2024-01-01/events.json",
            Some(&connections),
        );
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("s3://test-bucket/data/2024-01-01/events.json"));
        assert!(query.contains("read_json_auto"));
    }

    #[tokio::test]
    async fn test_build_import_query_multiple_s3() {
        let test_dir = "/tmp/import_processor_multiple_s3_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                auth_method: S3AuthMethod::Explicit,
                access_key_id: "test_key".to_string(),
                secret_access_key: "test_secret".to_string(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections.clone())
                .await
                .unwrap();
        let processor = Importer::new(&ducklake);

        let adapter = AdapterConfig {
            connection: "s3_connection".to_string(),
            description: None,
            file: FileConfig {
                path: "data/events.parquet".to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: FormatConfig {
                ty: "parquet".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
            columns: vec![],
            limits: None,
        };

        let file_paths = vec![
            "data/events-2024-01-01.parquet".to_string(),
            "data/events-2024-01-02.parquet".to_string(),
        ];

        let result =
            processor.build_import_query_multiple(&adapter, &file_paths, Some(&connections));
        assert!(result.is_ok());
        let query = result.unwrap();
        assert!(query.contains("'s3://test-bucket/data/events-2024-01-01.parquet'"));
        assert!(query.contains("'s3://test-bucket/data/events-2024-01-02.parquet'"));
        assert!(query.contains("read_parquet"));
    }
}
