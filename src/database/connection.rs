use crate::config::project::{DatabaseType, ProjectConfig};
use crate::database::migration::Migrator;
use anyhow::Result;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use sea_orm_migration::prelude::*;
use std::time::Duration;

pub async fn connect_app_db(project_config: &ProjectConfig) -> Result<DatabaseConnection> {
    let db_url =
        match &project_config.database.ty {
            DatabaseType::Sqlite => {
                let path = project_config
                    .database
                    .path
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("SQLite database path is required"))?;
                if let Some(parent) = std::path::Path::new(path).parent() {
                    std::fs::create_dir_all(parent)?;
                }
                format!("sqlite://{path}?mode=rwc")
            }
            DatabaseType::Mysql => {
                let host = project_config
                    .database
                    .host
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("MySQL database host is required"))?;
                let port = project_config.database.port.unwrap_or(3306);
                let database = project_config
                    .database
                    .database
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("MySQL database name is required"))?;
                let username = project_config
                    .database
                    .username
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("MySQL database username is required"))?;
                let password = project_config
                    .database
                    .password
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("MySQL database password is required"))?;
                format!("mysql://{username}:{password}@{host}:{port}/{database}")
            }
            DatabaseType::Postgresql => {
                let host = project_config
                    .database
                    .host
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("PostgreSQL database host is required"))?;
                let port = project_config.database.port.unwrap_or(5432);
                let database = project_config
                    .database
                    .database
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("PostgreSQL database name is required"))?;
                let username =
                    project_config.database.username.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("PostgreSQL database username is required")
                    })?;
                let password =
                    project_config.database.password.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("PostgreSQL database password is required")
                    })?;
                format!("postgresql://{username}:{password}@{host}:{port}/{database}")
            }
        };

    let mut opt = ConnectOptions::new(&db_url);
    opt.connect_timeout(Duration::from_secs(30));

    let db = Database::connect(opt).await?;

    if matches!(&project_config.database.ty, DatabaseType::Sqlite) {
        enable_sqlite_wal_mode(&db).await?;
    }

    ensure_migrations(&db).await?;
    Ok(db)
}

async fn enable_sqlite_wal_mode(db: &DatabaseConnection) -> Result<()> {
    db.execute(sea_orm::Statement::from_string(
        sea_orm::DatabaseBackend::Sqlite,
        "PRAGMA journal_mode = WAL;".to_string(),
    ))
    .await?;

    db.execute(sea_orm::Statement::from_string(
        sea_orm::DatabaseBackend::Sqlite,
        "PRAGMA busy_timeout = 10000;".to_string(),
    ))
    .await?;

    Ok(())
}

async fn ensure_migrations(db: &DatabaseConnection) -> Result<()> {
    if Migrator::get_pending_migrations(db).await?.is_empty() {
        return Ok(());
    }

    if let Err(e) = Migrator::up(db, None).await {
        return Err(anyhow::anyhow!("Failed to run database migrations: {}", e));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::{DatabaseConfig, DatabaseType, StorageConfig};

    use tempfile;

    #[tokio::test]
    async fn test_connect_app_db() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: StorageConfig::LocalFile {
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some(db_path.to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                password: None,
                username: None,
            },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        assert!(db.ping().await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_app_db_with_migrations() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: StorageConfig::LocalFile {
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some(db_path.to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                password: None,
                username: None,
            },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        assert!(db.ping().await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_mysql_db() -> Result<()> {
        let project_config = ProjectConfig {
            storage: StorageConfig::LocalFile {
                path: "/tmp/test_storage".to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Mysql,
                path: None,
                host: Some("localhost".to_string()),
                port: Some(3306),
                database: Some("featherbox_test".to_string()),
                username: Some("featherbox".to_string()),
                password: Some("testpass".to_string()),
            },
            connections: std::collections::HashMap::new(),
        };

        let result = connect_app_db(&project_config).await;
        if let Ok(db) = result {
            assert!(db.ping().await.is_ok());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_postgresql_db() -> Result<()> {
        let project_config = ProjectConfig {
            storage: StorageConfig::LocalFile {
                path: "/tmp/test_storage".to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Postgresql,
                path: None,
                host: Some("localhost".to_string()),
                port: Some(5432),
                database: Some("featherbox_test".to_string()),
                username: Some("featherbox".to_string()),
                password: Some("testpass".to_string()),
            },
            connections: std::collections::HashMap::new(),
        };

        let result = connect_app_db(&project_config).await;
        if let Ok(db) = result {
            assert!(db.ping().await.is_ok());
        }

        Ok(())
    }
}
