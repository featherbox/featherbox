use crate::config::adapter::AdapterConfig;
use crate::database::entities::{deltas, pipeline_actions};
use crate::pipeline::ducklake::DuckLake;
use anyhow::{Context, Result};
use chrono::Utc;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DeltaMetadata {
    pub action_id: i32,
    pub insert_delta_path: String,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Clone)]
pub struct DeltaManager {
    deltas_dir: PathBuf,
    ducklake: Arc<DuckLake>,
}

impl DeltaManager {
    pub fn new(project_root: &Path, ducklake: Arc<DuckLake>) -> Result<Self> {
        let deltas_dir = project_root.join("deltas");
        std::fs::create_dir_all(&deltas_dir).with_context(|| {
            format!(
                "Failed to create deltas directory: {}",
                deltas_dir.display()
            )
        })?;

        Ok(Self {
            deltas_dir,
            ducklake,
        })
    }

    pub fn create_delta_path(&self, table_name: &str) -> PathBuf {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        self.deltas_dir
            .join(format!("delta_{table_name}_{timestamp}_insert.parquet"))
    }

    async fn save_delta_metadata(
        &self,
        db: &DatabaseConnection,
        action_id: i32,
        insert_path: &Path,
    ) -> Result<DeltaMetadata> {
        let insert_path_str = insert_path.to_string_lossy().to_string();
        let now = Utc::now().naive_utc();

        let active_model = deltas::ActiveModel {
            action_id: Set(action_id),
            insert_delta_path: Set(insert_path_str.clone()),
            update_delta_path: Set("".to_string()),
            delete_delta_path: Set("".to_string()),
            created_at: Set(now),
            ..Default::default()
        };

        active_model
            .insert(db)
            .await
            .with_context(|| format!("Failed to save delta metadata for action_id: {action_id}, insert_path: {insert_path_str}"))?;

        Ok(DeltaMetadata {
            action_id,
            insert_delta_path: insert_path_str,
            created_at: now,
        })
    }

    pub async fn latest_delta_metadata(
        &self,
        db: &DatabaseConnection,
        table_name: &str,
    ) -> Result<Option<DeltaMetadata>> {
        let action = pipeline_actions::Entity::find()
            .filter(pipeline_actions::Column::TableName.eq(table_name))
            .one(db)
            .await
            .with_context(|| format!("Failed to find action for table: {table_name}"))?;

        if let Some(action) = action {
            let delta = deltas::Entity::find()
                .filter(deltas::Column::ActionId.eq(action.id))
                .one(db)
                .await
                .with_context(|| format!("Failed to find delta for action: {}", action.id))?;

            if let Some(delta) = delta {
                return Ok(Some(DeltaMetadata {
                    action_id: delta.action_id,
                    insert_delta_path: delta.insert_delta_path,
                    created_at: delta.created_at,
                }));
            }
        }

        Ok(None)
    }

    pub fn cleanup_delta_files(&self, delta_metadata: &DeltaMetadata) -> Result<()> {
        let path_buf = PathBuf::from(&delta_metadata.insert_delta_path);
        if path_buf.exists() {
            std::fs::remove_file(&path_buf).with_context(|| {
                format!(
                    "Failed to remove delta file: {}",
                    delta_metadata.insert_delta_path
                )
            })?;
        }

        Ok(())
    }

    pub async fn process_delta_for_adapter(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
        file_paths: &[String],
        app_db: &DatabaseConnection,
        action_id: i32,
    ) -> Result<DeltaMetadata> {
        self.validate_source_files_schema(table_name, file_paths)?;

        let delta_path = self.create_delta_path(table_name);

        self.create_table_delta(&delta_path, adapter, file_paths)
            .await?;

        let delta_metadata = self
            .save_delta_metadata(app_db, action_id, &delta_path)
            .await?;

        self.apply_delta_to_table(table_name, &delta_path)?;

        Ok(delta_metadata)
    }

    pub async fn process_delta_for_model(
        &self,
        model_name: &str,
        modified_sql: &str,
        app_db: &DatabaseConnection,
        action_id: i32,
    ) -> Result<DeltaMetadata> {
        let delta_path = self.create_delta_path(model_name);

        self.create_model_delta(&delta_path, modified_sql)?;

        let delta_metadata = self
            .save_delta_metadata(app_db, action_id, &delta_path)
            .await?;

        self.apply_model_delta_to_table(model_name, &delta_path)?;

        Ok(delta_metadata)
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

    fn build_import_query_single(
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

    async fn create_table_delta(
        &self,
        delta_path: &Path,
        adapter: &AdapterConfig,
        file_paths: &[String],
    ) -> Result<()> {
        let temp_table_name = DuckLake::generate_temp_table_name("temp_delta");

        let query = self.build_import_query_multiple(adapter, file_paths)?;

        self.ducklake
            .create_table_from_query(&temp_table_name, &query)
            .context("Failed to create temporary delta table")?;

        let export_insert_sql = format!(
            "COPY (SELECT * FROM {temp_table_name}) TO '{}' (FORMAT PARQUET);",
            delta_path.to_string_lossy()
        );

        self.ducklake
            .execute_batch(&export_insert_sql)
            .context("Failed to export delta insert data")?;

        self.ducklake.drop_temp_table(&temp_table_name)?;

        Ok(())
    }

    fn apply_delta_to_table(&self, table_name: &str, delta_path: &Path) -> Result<()> {
        let table_exists_sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );

        let results = self.ducklake.query(&table_exists_sql)?;
        let table_exists = if let Some(row) = results.first() {
            if let Some(count_str) = row.first() {
                count_str.parse::<i64>().unwrap_or(0) > 0
            } else {
                false
            }
        } else {
            false
        };

        if !table_exists {
            let create_from_delta_sql = format!(
                "CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{}');",
                delta_path.to_string_lossy()
            );

            self.ducklake
                .execute_batch(&create_from_delta_sql)
                .context("Failed to create table from delta")?;
        } else {
            self.validate_delta_schema(table_name, delta_path)
                .with_context(|| format!("Schema validation failed for table '{table_name}'"))?;

            let insert_sql = format!(
                "INSERT INTO {table_name} SELECT * FROM read_parquet('{}');",
                delta_path.to_string_lossy()
            );

            self.ducklake
                .execute_batch(&insert_sql)
                .context("Failed to insert delta data")?;
        }

        Ok(())
    }

    fn create_model_delta(&self, delta_path: &Path, modified_sql: &str) -> Result<()> {
        let temp_table_name = DuckLake::generate_temp_table_name("temp_model_delta");

        let create_temp_sql = format!("CREATE TABLE {temp_table_name} AS ({modified_sql});");

        self.ducklake
            .execute_batch(&create_temp_sql)
            .context("Failed to create temporary model delta table")?;

        let export_insert_sql = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET);",
            temp_table_name,
            delta_path.to_string_lossy()
        );

        self.ducklake
            .execute_batch(&export_insert_sql)
            .context("Failed to export model delta insert data")?;

        self.ducklake.drop_temp_table(&temp_table_name)?;

        Ok(())
    }

    fn apply_model_delta_to_table(&self, model_name: &str, delta_path: &Path) -> Result<()> {
        let table_exists_sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{model_name}'"
        );

        let results = self.ducklake.query(&table_exists_sql)?;
        let table_exists = if let Some(row) = results.first() {
            if let Some(count_str) = row.first() {
                count_str.parse::<i64>().unwrap_or(0) > 0
            } else {
                false
            }
        } else {
            false
        };

        if !table_exists {
            let create_from_delta_sql = format!(
                "CREATE TABLE {} AS SELECT * FROM read_parquet('{}');",
                model_name,
                delta_path.to_string_lossy()
            );

            self.ducklake
                .execute_batch(&create_from_delta_sql)
                .context("Failed to create model table from delta")?;
        } else {
            let replace_sql = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}');",
                model_name,
                delta_path.to_string_lossy()
            );

            self.ducklake
                .execute_batch(&replace_sql)
                .context("Failed to replace model table with delta")?;
        }

        Ok(())
    }

    fn validate_source_files_schema(&self, table_name: &str, file_paths: &[String]) -> Result<()> {
        if !self.ducklake.table_exists(table_name)? {
            return Ok(());
        }

        let table_columns = self.ducklake.table_schema(table_name)?;

        for file_path in file_paths {
            let source_schema_sql = format!("DESCRIBE SELECT * FROM read_csv_auto('{file_path}')");

            let source_results = self.ducklake.query(&source_schema_sql)?;
            let source_columns: Vec<(String, String)> = source_results
                .into_iter()
                .filter_map(|row| {
                    if row.len() >= 2 {
                        Some((row[0].clone(), row[1].clone()))
                    } else {
                        None
                    }
                })
                .collect();

            if table_columns.len() != source_columns.len() {
                return Err(anyhow::anyhow!(
                    "Column count mismatch in file '{}': table has {} columns, file has {} columns",
                    file_path,
                    table_columns.len(),
                    source_columns.len()
                ));
            }

            for (i, ((table_col, table_type), (source_col, source_type))) in
                table_columns.iter().zip(source_columns.iter()).enumerate()
            {
                if table_col != source_col {
                    return Err(anyhow::anyhow!(
                        "Column name mismatch in file '{}' at position {}: table has '{}', file has '{}'",
                        file_path,
                        i,
                        table_col,
                        source_col
                    ));
                }
                if table_type != source_type {
                    return Err(anyhow::anyhow!(
                        "Column type mismatch in file '{}' for column '{}': table has '{}', file has '{}'",
                        file_path,
                        table_col,
                        table_type,
                        source_type
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_delta_schema(&self, table_name: &str, delta_path: &Path) -> Result<()> {
        let table_columns = self.ducklake.table_schema(table_name)?;

        let delta_schema_sql = format!(
            "DESCRIBE SELECT * FROM read_parquet('{}')",
            delta_path.to_string_lossy()
        );

        let delta_results = self.ducklake.query(&delta_schema_sql)?;
        let delta_columns: Vec<(String, String)> = delta_results
            .into_iter()
            .filter_map(|row| {
                if row.len() >= 2 {
                    Some((row[0].clone(), row[1].clone()))
                } else {
                    None
                }
            })
            .collect();

        if table_columns.len() != delta_columns.len() {
            return Err(anyhow::anyhow!(
                "Column count mismatch: table has {} columns, delta has {} columns",
                table_columns.len(),
                delta_columns.len()
            ));
        }

        for (i, ((table_col, table_type), (delta_col, delta_type))) in
            table_columns.iter().zip(delta_columns.iter()).enumerate()
        {
            if table_col != delta_col {
                return Err(anyhow::anyhow!(
                    "Column name mismatch at position {}: table has '{}', delta has '{}'",
                    i,
                    table_col,
                    delta_col
                ));
            }
            if table_type != delta_type {
                return Err(anyhow::anyhow!(
                    "Column type mismatch for '{}': table has '{}', delta has '{}'",
                    table_col,
                    table_type,
                    delta_type
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig};
    use crate::pipeline::ducklake::{CatalogConfig, StorageConfig};
    use sea_orm::ConnectionTrait;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_delta_path_creation() {
        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{}/test_catalog.sqlite", temp_dir.path().display()),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{}/test_storage", temp_dir.path().display()),
        };
        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let manager = DeltaManager::new(temp_dir.path(), ducklake).unwrap();

        let delta_path = manager.create_delta_path("users");

        assert!(delta_path.to_string_lossy().contains("delta_users_"));
        assert!(delta_path.to_string_lossy().contains("_insert.parquet"));
    }

    #[tokio::test]
    async fn test_delta_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{}/test_catalog.sqlite", temp_dir.path().display()),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{}/test_storage", temp_dir.path().display()),
        };
        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let manager = DeltaManager::new(temp_dir.path(), ducklake).unwrap();

        let deltas_dir = temp_dir.path().join("deltas");
        assert!(deltas_dir.exists());
        assert_eq!(manager.deltas_dir, deltas_dir);
    }

    #[tokio::test]
    async fn test_delta_metadata() {
        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{}/test_catalog.sqlite", temp_dir.path().display()),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{}/test_storage", temp_dir.path().display()),
        };
        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let manager = DeltaManager::new(temp_dir.path(), ducklake).unwrap();

        let db_url = format!(
            "sqlite://{}?mode=rwc",
            temp_dir.path().join("test.db").display()
        );
        let db = sea_orm::Database::connect(&db_url).await.unwrap();

        db.execute(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "CREATE TABLE __fbox_pipeline_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                execution_order INTEGER NOT NULL,
                since TEXT,
                until TEXT
            );"
            .to_string(),
        ))
        .await
        .unwrap();

        db.execute(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "CREATE TABLE __fbox_deltas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_id INTEGER NOT NULL,
                insert_delta_path TEXT NOT NULL,
                update_delta_path TEXT NOT NULL,
                delete_delta_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (action_id) REFERENCES __fbox_pipeline_actions(id)
            );"
            .to_string(),
        ))
        .await
        .unwrap();

        db.execute(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO __fbox_pipeline_actions (id, pipeline_id, table_name, execution_order) 
             VALUES (1, 1, 'test_table', 1);"
                .to_string(),
        ))
        .await
        .unwrap();

        let delta_path = manager.create_delta_path("test_table");

        let saved_metadata = manager
            .save_delta_metadata(&db, 1, &delta_path)
            .await
            .unwrap();

        assert_eq!(saved_metadata.action_id, 1);
        assert!(!saved_metadata.insert_delta_path.is_empty());

        let retrieved_metadata = manager
            .latest_delta_metadata(&db, "test_table")
            .await
            .unwrap();
        assert!(retrieved_metadata.is_some());
        let metadata = retrieved_metadata.unwrap();
        assert_eq!(metadata.action_id, saved_metadata.action_id);
        assert_eq!(metadata.insert_delta_path, saved_metadata.insert_delta_path);

        std::fs::write(&saved_metadata.insert_delta_path, "test content").unwrap();

        assert!(Path::new(&saved_metadata.insert_delta_path).exists());

        manager.cleanup_delta_files(&saved_metadata).unwrap();

        assert!(!Path::new(&saved_metadata.insert_delta_path).exists());
    }

    #[tokio::test]
    async fn test_create_and_apply_delta_table() {
        let test_dir = "/tmp/delta_processor_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let data_temp_dir = tempdir().unwrap();
        let manager = DeltaManager::new(data_temp_dir.path(), ducklake.clone()).unwrap();

        let test_csv = data_temp_dir.path().join("test_data.csv");
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

        let delta_path = manager.create_delta_path("test_table");

        let file_paths = vec![test_csv.to_string_lossy().to_string()];

        manager
            .create_table_delta(&delta_path, &adapter, &file_paths)
            .await
            .unwrap();

        assert!(delta_path.exists());

        manager
            .apply_delta_to_table("test_table", &delta_path)
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
}
