use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::secret::expand_secrets_in_text;

pub fn load_from_directory(project_path: &Path) -> Result<Config> {
    Config::load_from_directory(project_path)
}

pub mod adapter;
pub mod dashboard;
pub mod model;
pub mod project;
pub mod query;

pub use adapter::AdapterConfig;
pub use dashboard::DashboardConfig;
pub use model::ModelConfig;
pub use project::ProjectConfig;
pub use query::QueryConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub project: ProjectConfig,
    pub adapters: HashMap<String, AdapterConfig>,
    pub models: HashMap<String, ModelConfig>,
    pub queries: HashMap<String, QueryConfig>,
    pub dashboards: HashMap<String, DashboardConfig>,
    pub project_root: std::path::PathBuf,
}

impl Config {
    pub fn load_from_directory(project_path: &Path) -> Result<Self> {
        let project_config = load_project_config(project_path)?;
        let adapters = load_adapters(project_path)?;
        let models = load_models(project_path)?;
        let queries = load_queries(project_path)?;
        let dashboards = load_dashboards(project_path)?;

        Ok(Config {
            project: project_config,
            adapters,
            models,
            queries,
            dashboards,
            project_root: project_path.to_path_buf(),
        })
    }
}

fn load_project_config(project_path: &Path) -> Result<ProjectConfig> {
    let project_yml_path = project_path.join("project.yml");
    if !project_yml_path.exists() {
        return Err(anyhow::anyhow!(
            "project.yml not found. Please run 'featherbox init' first."
        ));
    }

    let content = fs::read_to_string(&project_yml_path)?;
    let _project_config: ProjectConfig = project::parse_project_config(&content)?;

    let expanded_content = expand_secrets_in_text(&content, project_path)?;

    project::parse_project_config(&expanded_content)
}

fn load_config_files_recursive<T>(
    config_dir: &Path,
    file_type: &str,
    parse_fn: fn(&str) -> Result<T>,
    project_path: &Path,
) -> Result<HashMap<String, T>> {
    let mut configs = HashMap::new();

    if !config_dir.exists() {
        return Ok(configs);
    }

    collect_config_files_recursive(
        config_dir,
        config_dir,
        file_type,
        parse_fn,
        project_path,
        &mut configs,
    )?;

    Ok(configs)
}

fn collect_config_files_recursive<T>(
    dir: &Path,
    base_dir: &Path,
    file_type: &str,
    parse_fn: fn(&str) -> Result<T>,
    project_path: &Path,
    configs: &mut HashMap<String, T>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_config_files_recursive(
                &path,
                base_dir,
                file_type,
                parse_fn,
                project_path,
                configs,
            )?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = extract_config_name(&path, base_dir, file_type)?;
            let config = parse_config_file(&path, parse_fn, project_path)?;
            configs.insert(name, config);
        }
    }

    Ok(())
}

pub fn generate_node_name_from_path(relative_path: &Path) -> String {
    relative_path
        .with_extension("")
        .to_string_lossy()
        .replace(['/', '\\'], "_")
}

fn extract_config_name(path: &Path, base_dir: &Path, file_type: &str) -> Result<String> {
    let relative_path = path
        .strip_prefix(base_dir)
        .with_context(|| format!("Failed to create relative path for {file_type}: {path:?}"))?;
    Ok(generate_node_name_from_path(relative_path))
}

fn parse_config_file<T>(
    path: &Path,
    parse_fn: fn(&str) -> Result<T>,
    project_path: &Path,
) -> Result<T> {
    let content = fs::read_to_string(path)?;
    let expanded_content = expand_secrets_in_text(&content, project_path)?;
    parse_fn(&expanded_content)
}

fn load_adapters(project_path: &Path) -> Result<HashMap<String, AdapterConfig>> {
    load_config_files_recursive(
        &project_path.join("adapters"),
        "adapter",
        adapter::parse_adapter_config,
        project_path,
    )
}

fn load_models(project_path: &Path) -> Result<HashMap<String, ModelConfig>> {
    load_config_files_recursive(
        &project_path.join("models"),
        "model",
        model::parse_model_config,
        project_path,
    )
}

fn load_queries(project_path: &Path) -> Result<HashMap<String, QueryConfig>> {
    load_config_files_recursive(
        &project_path.join("queries"),
        "query",
        query::parse_query_config,
        project_path,
    )
}

fn load_dashboards(project_path: &Path) -> Result<HashMap<String, DashboardConfig>> {
    load_config_files_recursive(
        &project_path.join("dashboards"),
        "dashboard",
        dashboard::parse_dashboard_config,
        project_path,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile;

    #[test]
    fn test_load_from_directory() -> Result<()> {
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
            connections: {}
            secret_key_path: secret.key"#;
        fs::write(project_path.join("project.yml"), project_yml)?;

        let adapter_yml = r#"
            connection: test_connection
            description: "Test adapter"
            source:
              type: file
              file:
                path: test.csv
                compression: none
                max_batch_size: 100MB
              format:
                type: csv
                has_header: true
            columns: []"#;
        fs::write(project_path.join("adapters/test_adapter.yml"), adapter_yml)?;

        let model_yml = r#"
            description: "Test model"
            sql: "SELECT * FROM test_adapter""#;
        fs::write(project_path.join("models/test_model.yml"), model_yml)?;

        let config = Config::load_from_directory(project_path)?;

        assert_eq!(config.adapters.len(), 1);
        assert!(config.adapters.contains_key("test_adapter"));

        assert_eq!(config.models.len(), 1);
        assert!(config.models.contains_key("test_model"));

        Ok(())
    }

    #[test]
    fn test_load_from_directory_missing_project_yml() {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path();

        let result = Config::load_from_directory(project_path);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("project.yml not found")
        );
    }

    #[test]
    fn test_extract_config_name_with_subdirectories() -> Result<()> {
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let base_dir = temp_dir.path();

        let staging_users_path = base_dir.join("staging").join("users.yml");
        let marts_orders_path = base_dir.join("marts").join("orders.yml");
        let production_api_path = base_dir.join("production").join("api.yml");

        assert_eq!(
            extract_config_name(&staging_users_path, base_dir, "model")?,
            "staging_users"
        );
        assert_eq!(
            extract_config_name(&marts_orders_path, base_dir, "model")?,
            "marts_orders"
        );
        assert_eq!(
            extract_config_name(&production_api_path, base_dir, "adapter")?,
            "production_api"
        );

        Ok(())
    }

    #[test]
    fn test_load_same_filename_models() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let project_yml = r#"
            storage:
              type: local
              path: ./storage
            database:
              type: sqlite
              path: ./database.db
            connections: {}"#;
        fs::write(project_path.join("project.yml"), project_yml)?;

        fs::create_dir_all(project_path.join("models/staging"))?;
        fs::create_dir_all(project_path.join("models/marts"))?;

        let staging_users_yml = r#"
            description: "Staging users model"
            sql: "SELECT * FROM raw_users""#;
        fs::write(
            project_path.join("models/staging/users.yml"),
            staging_users_yml,
        )?;

        let marts_users_yml = r#"
            description: "Marts users model"
            sql: "SELECT * FROM staging_users""#;
        fs::write(project_path.join("models/marts/users.yml"), marts_users_yml)?;

        let config = Config::load_from_directory(project_path)?;

        assert_eq!(config.models.len(), 2);
        assert!(config.models.contains_key("staging_users"));
        assert!(config.models.contains_key("marts_users"));

        assert_eq!(
            config.models["staging_users"].description.as_ref().unwrap(),
            "Staging users model"
        );
        assert_eq!(
            config.models["marts_users"].description.as_ref().unwrap(),
            "Marts users model"
        );

        Ok(())
    }
}
