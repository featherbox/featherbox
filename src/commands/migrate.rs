use anyhow::Result;
use sea_orm::DatabaseConnection;
use sea_orm_migration::prelude::*;
use std::path::Path;

use crate::{
    config::Config, database::connect_app_db, migration::Migrator,
    project::ensure_project_directory,
};

pub async fn execute_migrate_up(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;
    let config = Config::load_from_directory(&project_root)?;

    let app_db = connect_app_db(&config.project).await?;

    println!("Running migrations...");
    Migrator::up(&app_db, None).await?;
    println!("Migrations completed successfully!");

    Ok(())
}

pub async fn execute_migrate_status(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;
    let config = Config::load_from_directory(&project_root)?;

    let app_db = connect_app_db(&config.project).await?;

    let pending_migrations = Migrator::get_pending_migrations(&app_db).await?;

    if pending_migrations.is_empty() {
        println!("All migrations are up to date.");
    } else {
        println!("Pending migrations:");
        for migration in &pending_migrations {
            println!("  - {}", migration.name());
        }
    }

    Ok(())
}

pub async fn check_pending_migrations(db: &DatabaseConnection) -> Result<bool> {
    let pending = Migrator::get_pending_migrations(db).await?;
    Ok(!pending.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::{DatabaseConfig, DatabaseType, ProjectConfig};
    use std::fs;
    use tempfile;

    #[tokio::test]
    async fn test_execute_migrate_up() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;

        let project_yml = format!(
            r#"
storage:
  type: local
  path: {}/storage
database:
  type: sqlite
  path: {}/database.db
deployments:
  timeout: 600
connections: {{}}
"#,
            project_path.display(),
            project_path.display()
        );

        fs::write(project_path.join("project.yml"), project_yml)?;

        let result = execute_migrate_up(project_path).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_check_pending_migrations() -> Result<()> {
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

        let has_pending_before = check_pending_migrations(&db).await?;
        assert!(has_pending_before);

        Migrator::up(&db, None).await?;

        let has_pending_after = check_pending_migrations(&db).await?;
        assert!(!has_pending_after);

        Ok(())
    }
}
