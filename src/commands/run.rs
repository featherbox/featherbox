use anyhow::Result;
use std::path::Path;

use crate::{
    config::Config,
    ducklake::{CatalogConfig, DuckLake, StorageConfig},
    graph::Graph,
    pipeline::Pipeline,
    project::ensure_project_directory,
};

pub async fn execute_run(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;

    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        println!(
            "No adapters or models found. Create some with 'fbox adapter new' or 'fbox model new'"
        );
        return Ok(());
    }

    let graph = Graph::from_config(&config)?;
    let pipeline = Pipeline::from_graph(&graph);

    let catalog_config = match &config.project.database.ty {
        crate::config::project::DatabaseType::Sqlite => CatalogConfig::Sqlite {
            path: config.project.database.path.clone(),
        },
    };

    let storage_config = match &config.project.storage.ty {
        crate::config::project::StorageType::Local => StorageConfig::LocalFile {
            path: config.project.storage.path.clone(),
        },
    };

    let ducklake = DuckLake::new(catalog_config, storage_config).await?;

    if let Err(e) = pipeline.execute(&config, &ducklake).await {
        eprintln!("Pipeline execution failed: {e}");
        return Err(e);
    }

    println!("Pipeline execution completed successfully!");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile;

    #[tokio::test]
    async fn test_execute_run_empty_project() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;

        let project_yml = r#"
storage:
  type: local
  path: ./storage
database:
  type: sqlite
  path: ./database.db
deployments:
  timeout: 600
connections: {}
"#;
        fs::write(project_path.join("project.yml"), project_yml)?;

        let result = execute_run(project_path).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_with_simple_pipeline() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
        fs::create_dir_all(project_path.join("data"))?;

        let project_yml = r#"
storage:
  type: local
  path: ./storage
database:
  type: sqlite
  path: ./database.db
deployments:
  timeout: 600
connections: {}
"#;
        fs::write(project_path.join("project.yml"), project_yml)?;

        let users_csv = project_path.join("data/users.csv");
        fs::write(&users_csv, "id,name\n1,Alice\n2,Bob")?;

        let adapter_yml = format!(
            r#"
connection: test_connection
description: "Test adapter"
file:
  path: {}
  compression: none
format:
  type: csv
  has_header: true
columns: []
"#,
            users_csv.to_string_lossy()
        );
        fs::write(project_path.join("adapters/users.yml"), adapter_yml)?;

        let model_yml = r#"
description: "Test model"
sql: "SELECT id, name FROM users WHERE id > 0"
max_age: 3600
"#;
        fs::write(project_path.join("models/active_users.yml"), model_yml)?;

        let config = Config::load_from_directory(project_path)?;
        let graph = Graph::from_config(&config)?;
        let pipeline = Pipeline::from_graph(&graph);

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{}/test.db", project_path.to_string_lossy()),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{}/storage", project_path.to_string_lossy()),
        };
        let ducklake = DuckLake::new(catalog_config, storage_config).await?;

        let result = pipeline.execute(&config, &ducklake).await;
        assert!(result.is_ok());

        let users_result = ducklake.query("SELECT * FROM users ORDER BY id")?;
        assert_eq!(users_result.len(), 2);
        assert_eq!(users_result[0], vec!["1", "Alice"]);
        assert_eq!(users_result[1], vec!["2", "Bob"]);

        let active_users_result = ducklake.query("SELECT * FROM active_users ORDER BY id")?;
        assert_eq!(active_users_result.len(), 2);
        assert_eq!(active_users_result[0], vec!["1", "Alice"]);
        assert_eq!(active_users_result[1], vec!["2", "Bob"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_missing_project_yml() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let result = execute_run(project_path).await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        println!("Error message: {error_msg}");
        assert!(
            error_msg.contains("project.yml not found")
                || error_msg.contains("This command must be run inside a FeatherBox project")
        );

        Ok(())
    }
}
