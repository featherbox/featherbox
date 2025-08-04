use crate::config::project::{DatabaseType, ProjectConfig};
use crate::migration::Migrator;
use anyhow::Result;
use sea_orm::{Database, DatabaseConnection};
use sea_orm_migration::prelude::*;

pub async fn connect_app_db(project_config: &ProjectConfig) -> Result<DatabaseConnection> {
    let db_url = match &project_config.database.ty {
        DatabaseType::Sqlite => {
            let path = &project_config.database.path;
            if let Some(parent) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            format!("sqlite://{path}?mode=rwc")
        }
    };

    let db = Database::connect(&db_url).await?;
    Ok(db)
}

pub async fn check_migration_status(db: &DatabaseConnection) -> Result<()> {
    let pending = Migrator::get_pending_migrations(db).await?;
    if !pending.is_empty() {
        let pending_names: Vec<String> = pending.iter().map(|m| m.name().to_string()).collect();
        return Err(anyhow::anyhow!(
            "There are pending migrations. Please run 'fbox migrate up' first.\nPending: {}",
            pending_names.join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::{DatabaseConfig, DatabaseType};
    use tempfile;

    #[tokio::test]
    async fn test_connect_app_db() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: crate::config::project::StorageConfig {
                ty: crate::config::project::StorageType::Local,
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: db_path.to_string_lossy().to_string(),
            },
            deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        assert!(db.ping().await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_check_migration_status() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: crate::config::project::StorageConfig {
                ty: crate::config::project::StorageType::Local,
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: db_path.to_string_lossy().to_string(),
            },
            deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;

        let result_before_migration = check_migration_status(&db).await;
        assert!(result_before_migration.is_err());

        Migrator::up(&db, None).await?;

        let result_after_migration = check_migration_status(&db).await;
        assert!(result_after_migration.is_ok());

        Ok(())
    }
}
