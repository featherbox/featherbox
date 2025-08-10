use crate::config::adapter::AdapterConfig;
use crate::pipeline::{
    build::TimeRange,
    delta::{DeltaManager, DeltaMetadata, DeltaProcessor},
    ducklake::DuckLake,
    file_processor::FileProcessor,
};
use anyhow::{Context, Result};
use sea_orm::DatabaseConnection;

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
    ) -> Result<String> {
        match adapter.format.ty.as_str() {
            "csv" => {
                let has_header = adapter.format.has_header.unwrap_or(true);
                let query =
                    format!("SELECT * FROM read_csv_auto('{file_path}', header={has_header})");
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
            _ => Err(anyhow::anyhow!("Unsupported format: {}", adapter.format.ty)),
        }
    }

    pub fn import_adapter(&self, adapter: &AdapterConfig, table_name: &str) -> Result<()> {
        let file_paths = FileProcessor::process_pattern(&adapter.file.path, adapter)?;

        if !file_paths.is_empty() {
            let query = self.build_import_query_multiple(adapter, &file_paths)?;
            self.ducklake
                .create_table_from_query(table_name, &query)
                .with_context(|| format!("Failed to import data for table '{table_name}'"))?;
        }

        Ok(())
    }

    pub async fn import_adapter_with_delta(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
        time_range: Option<TimeRange>,
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
        action_id: i32,
    ) -> Result<Option<DeltaMetadata>> {
        let file_paths = FileProcessor::files_for_processing(adapter, time_range)?;

        if file_paths.is_empty() {
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

        processor.import_adapter(&adapter, "test_table").unwrap();

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
}
