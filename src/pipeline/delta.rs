use crate::database::entities::{deltas, pipeline_actions};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DeltaMetadata {
    pub action_id: i32,
    pub insert_delta_path: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DeltaFiles {
    pub insert_path: PathBuf,
}

impl DeltaFiles {
    pub fn new(deltas_dir: &Path, table_name: &str, timestamp: &str) -> Self {
        let insert_path = deltas_dir.join(format!("delta_{table_name}_{timestamp}_insert.parquet"));

        Self { insert_path }
    }

    pub fn get_insert_path_as_string(&self) -> String {
        self.insert_path.to_string_lossy().to_string()
    }
}

pub struct DeltaManager {
    deltas_dir: PathBuf,
}

impl DeltaManager {
    pub fn new(project_root: &Path) -> Result<Self> {
        let deltas_dir = project_root.join("deltas");
        std::fs::create_dir_all(&deltas_dir).with_context(|| {
            format!(
                "Failed to create deltas directory: {}",
                deltas_dir.display()
            )
        })?;

        Ok(Self { deltas_dir })
    }

    pub fn create_delta_files(&self, table_name: &str) -> Result<DeltaFiles> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let files = DeltaFiles::new(&self.deltas_dir, table_name, &timestamp);

        Ok(files)
    }

    pub async fn save_delta_metadata(
        &self,
        db: &DatabaseConnection,
        action_id: i32,
        delta_files: &DeltaFiles,
    ) -> Result<DeltaMetadata> {
        let insert_path = delta_files.get_insert_path_as_string();
        let now = Utc::now();

        let active_model = deltas::ActiveModel {
            action_id: Set(action_id),
            insert_delta_path: Set(insert_path.clone()),
            update_delta_path: Set("".to_string()),
            delete_delta_path: Set("".to_string()),
            created_at: Set(now),
            ..Default::default()
        };

        active_model
            .insert(db)
            .await
            .with_context(|| format!("Failed to save delta metadata for action_id: {action_id}, insert_path: {insert_path}"))?;

        Ok(DeltaMetadata {
            action_id,
            insert_delta_path: insert_path,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::ConnectionTrait;
    use tempfile::tempdir;

    #[test]
    fn test_delta_files_creation() {
        let temp_dir = tempdir().unwrap();
        let delta_files = DeltaFiles::new(temp_dir.path(), "users", "20241201_120000");

        assert!(
            delta_files
                .insert_path
                .to_string_lossy()
                .contains("delta_users_20241201_120000_insert.parquet")
        );
    }

    #[test]
    fn test_delta_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let manager = DeltaManager::new(temp_dir.path()).unwrap();

        let deltas_dir = temp_dir.path().join("deltas");
        assert!(deltas_dir.exists());
        assert_eq!(manager.deltas_dir, deltas_dir);
    }

    #[test]
    fn test_create_delta_files() {
        let temp_dir = tempdir().unwrap();
        let manager = DeltaManager::new(temp_dir.path()).unwrap();

        let delta_files = manager.create_delta_files("test_table").unwrap();

        assert!(
            delta_files
                .insert_path
                .to_string_lossy()
                .contains("delta_test_table_")
        );
        assert!(
            delta_files
                .insert_path
                .to_string_lossy()
                .contains("_insert.parquet")
        );
    }

    #[tokio::test]
    async fn test_delta_metadata() {
        let temp_dir = tempdir().unwrap();
        let manager = DeltaManager::new(temp_dir.path()).unwrap();

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

        let delta_files = manager.create_delta_files("test_table").unwrap();

        let saved_metadata = manager
            .save_delta_metadata(&db, 1, &delta_files)
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
}
