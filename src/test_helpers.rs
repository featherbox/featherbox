#[cfg(test)]
use crate::config::Config;
#[cfg(test)]
use crate::config::project::{
    ConnectionConfig, DatabaseConfig, DatabaseType, DeploymentsConfig, ProjectConfig,
    StorageConfig, StorageType,
};
#[cfg(test)]
use crate::database::connection::connect_app_db;
#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use sea_orm::DatabaseConnection;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use tempfile::TempDir;

#[cfg(test)]
pub fn setup_test_project() -> Result<TempDir> {
    let temp_dir = tempfile::tempdir()?;
    let project_path = temp_dir.path();

    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models"))?;
    fs::write(project_path.join("project.yml"), "test")?;

    Ok(temp_dir)
}

#[cfg(test)]
pub fn create_project_structure(project_path: &Path) -> Result<()> {
    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models").join("staging"))?;
    fs::create_dir_all(project_path.join("models").join("marts"))?;
    Ok(())
}

#[cfg(test)]
pub fn create_default_project_config() -> ProjectConfig {
    ProjectConfig {
        storage: StorageConfig {
            ty: StorageType::Local,
            path: ".fbox".to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some("test.db".to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
        },
        deployments: DeploymentsConfig { timeout: 600 },
        connections: HashMap::new(),
        secret_key_path: None,
    }
}

#[cfg(test)]
pub fn create_project_config_with_connections(
    connections: HashMap<String, ConnectionConfig>,
) -> ProjectConfig {
    ProjectConfig {
        storage: StorageConfig {
            ty: StorageType::Local,
            path: ".fbox".to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some("test.db".to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
        },
        deployments: DeploymentsConfig { timeout: 600 },
        connections,
        secret_key_path: None,
    }
}

#[cfg(test)]
pub fn setup_test_db(temp_dir: &TempDir) -> Result<String> {
    let project_path = temp_dir.path();
    let db_path = project_path.join("test.db");

    create_project_structure(project_path)?;

    fs::write(project_path.join("project.yml"), "test")?;

    Ok(db_path.to_string_lossy().to_string())
}

#[cfg(test)]
pub fn create_test_adapter_yaml(name: &str, table_name: &str) -> String {
    format!(
        r#"name: {name}
type: csv
path: data/{name}.csv
destination:
  schema: staging
  table: {table_name}
"#
    )
}

#[cfg(test)]
pub fn create_test_model_yaml(name: &str, sql: &str) -> String {
    format!(
        r#"name: {name}
sql: {sql}
"#
    )
}

#[cfg(test)]
pub fn write_test_adapter(project_path: &Path, name: &str, content: &str) -> Result<()> {
    let adapter_path = project_path.join("adapters").join(format!("{name}.yml"));
    fs::write(adapter_path, content)?;
    Ok(())
}

#[cfg(test)]
pub fn write_test_model(
    project_path: &Path,
    subdir: &str,
    name: &str,
    content: &str,
) -> Result<()> {
    let model_path = project_path
        .join("models")
        .join(subdir)
        .join(format!("{name}.yml"));
    fs::write(model_path, content)?;
    Ok(())
}

#[cfg(test)]
pub async fn setup_test_db_connection() -> Result<DatabaseConnection> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");

    let project_config = ProjectConfig {
        storage: StorageConfig {
            ty: StorageType::Local,
            path: temp_dir.path().to_string_lossy().to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some(db_path.to_string_lossy().to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
        },
        deployments: DeploymentsConfig { timeout: 600 },
        connections: HashMap::new(),
        secret_key_path: None,
    };

    let db = connect_app_db(&project_config).await?;
    std::mem::forget(temp_dir);
    Ok(db)
}

#[cfg(test)]
pub async fn setup_test_db_with_config() -> Result<(DatabaseConnection, Config)> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");

    let project_config = ProjectConfig {
        storage: StorageConfig {
            ty: StorageType::Local,
            path: temp_dir.path().to_string_lossy().to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some(db_path.to_string_lossy().to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
        },
        deployments: DeploymentsConfig { timeout: 600 },
        connections: HashMap::new(),
        secret_key_path: None,
    };

    let config = Config {
        project: project_config.clone(),
        adapters: HashMap::new(),
        models: HashMap::new(),
        project_root: temp_dir.path().to_path_buf(),
    };

    let db = connect_app_db(&project_config).await?;
    std::mem::forget(temp_dir);
    Ok((db, config))
}
