use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::secret::expand_secrets_in_text;
use crate::workspace::project_dir;

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
}

pub struct FileHandle<'a, T: Serialize + Deserialize<'a>> {
    config: &'a T,
    path: PathBuf,
}

impl<'a, T: Serialize + Deserialize<'a>> FileHandle<'a, T> {
    pub fn save(&self) -> Result<()> {
        fs::create_dir_all(self.path.parent().unwrap())?;
        fs::write(&self.path, serde_yml::to_string(self.config)?)?;
        Ok(())
    }
}

fn project_config_file() -> Result<PathBuf> {
    Ok(project_dir()?.join("project.yml"))
}

fn adapters_config_directory() -> Result<PathBuf> {
    Ok(project_dir()?.join("adapters"))
}

fn models_config_directory() -> Result<PathBuf> {
    Ok(project_dir()?.join("models"))
}

fn queries_config_directory() -> Result<PathBuf> {
    Ok(project_dir()?.join("queries"))
}

fn dashboards_config_directory() -> Result<PathBuf> {
    Ok(project_dir()?.join("dashboards"))
}

impl Config {
    pub fn new() -> Self {
        Self {
            project: ProjectConfig::new(),
            adapters: HashMap::new(),
            models: HashMap::new(),
            queries: HashMap::new(),
            dashboards: HashMap::new(),
        }
    }

    pub fn load() -> Result<Self> {
        let project = load_project_config()?;
        let adapters = load_adapters()?;
        let models = load_models()?;
        let queries = load_queries()?;
        let dashboards = load_dashboards()?;

        Ok(Config {
            project,
            adapters,
            models,
            queries,
            dashboards,
        })
    }

    pub fn add_project_setting<'a>(
        &mut self,
        config: &'a ProjectConfig,
    ) -> Result<FileHandle<'a, ProjectConfig>> {
        self.project = config.clone();

        Ok(FileHandle {
            config,
            path: project_config_file()?,
        })
    }

    pub fn upsert_adapter<'a>(
        &mut self,
        path: &str,
        adapter: &'a AdapterConfig,
    ) -> Result<FileHandle<'a, AdapterConfig>> {
        self.adapters.insert(path.to_string(), adapter.clone());

        Ok(FileHandle {
            config: adapter,
            path: adapters_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn upsert_model<'a>(
        &mut self,
        path: &str,
        model: &'a ModelConfig,
    ) -> Result<FileHandle<'a, ModelConfig>> {
        self.models.insert(path.to_string(), model.clone());

        Ok(FileHandle {
            config: model,
            path: models_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn upsert_query<'a>(
        &mut self,
        path: &str,
        query: &'a QueryConfig,
    ) -> Result<FileHandle<'a, QueryConfig>> {
        self.queries.insert(path.to_string(), query.clone());

        Ok(FileHandle {
            config: query,
            path: queries_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn upsert_dashboard<'a>(
        &mut self,
        path: &str,
        dashboard: &'a DashboardConfig,
    ) -> Result<FileHandle<'a, DashboardConfig>> {
        self.dashboards.insert(path.to_string(), dashboard.clone());

        Ok(FileHandle {
            config: dashboard,
            path: dashboards_config_directory()?.join(format!("{path}.yml")),
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

fn load_project_config() -> Result<ProjectConfig> {
    let project_yml_path = project_config_file()?;
    if !project_yml_path.exists() {
        return Err(anyhow::anyhow!(
            "project.yml not found. Please run 'featherbox new' first."
        ));
    }

    let content = fs::read_to_string(&project_yml_path)?;

    let expanded_content = expand_secrets_in_text(&content)?;

    project::parse_project_config(&expanded_content)
}

fn load_config_files_recursive<T>(
    config_dir: &Path,
    file_type: &str,
    parse_fn: fn(&str) -> Result<T>,
) -> Result<HashMap<String, T>> {
    let mut configs = HashMap::new();

    if !config_dir.exists() {
        return Ok(configs);
    }

    collect_config_files_recursive(config_dir, config_dir, file_type, parse_fn, &mut configs)?;

    Ok(configs)
}

fn collect_config_files_recursive<T>(
    dir: &Path,
    base_dir: &Path,
    file_type: &str,
    parse_fn: fn(&str) -> Result<T>,
    configs: &mut HashMap<String, T>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_config_files_recursive(&path, base_dir, file_type, parse_fn, configs)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = extract_config_name(&path, base_dir, file_type)?;
            let config = parse_config_file(&path, parse_fn)?;
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

fn parse_config_file<T>(path: &Path, parse_fn: fn(&str) -> Result<T>) -> Result<T> {
    let content = fs::read_to_string(path)?;
    let expanded_content = expand_secrets_in_text(&content)?;
    parse_fn(&expanded_content)
}

fn load_adapters() -> Result<HashMap<String, AdapterConfig>> {
    load_config_files_recursive(
        &adapters_config_directory()?,
        "adapter",
        adapter::parse_adapter_config,
    )
}

fn load_models() -> Result<HashMap<String, ModelConfig>> {
    load_config_files_recursive(
        &models_config_directory()?,
        "model",
        model::parse_model_config,
    )
}

fn load_queries() -> Result<HashMap<String, QueryConfig>> {
    load_config_files_recursive(
        &queries_config_directory()?,
        "query",
        query::parse_query_config,
    )
}

fn load_dashboards() -> Result<HashMap<String, DashboardConfig>> {
    load_config_files_recursive(
        &dashboards_config_directory()?,
        "dashboard",
        dashboard::parse_dashboard_config,
    )
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs;
//     use tempfile;
//
//     #[test]
//     fn test_load_from_directory() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_path = temp_dir.path();
//
//         fs::create_dir_all(project_path.join("adapters"))?;
//         fs::create_dir_all(project_path.join("models"))?;
//
//         let project_yml = r#"
//             storage:
//               type: local
//               path: ./storage
//             database:
//               type: sqlite
//               path: ./database.db
//             connections: {}
//             secret_key_path: secret.key"#;
//         fs::write(project_path.join("project.yml"), project_yml)?;
//
//         let adapter_yml = r#"
//             connection: test_connection
//             description: "Test adapter"
//             source:
//               type: file
//               file:
//                 path: test.csv
//                 compression: none
//                 max_batch_size: 100MB
//               format:
//                 type: csv
//                 has_header: true
//             columns: []"#;
//         fs::write(project_path.join("adapters/test_adapter.yml"), adapter_yml)?;
//
//         let model_yml = r#"
//             description: "Test model"
//             sql: "SELECT * FROM test_adapter""#;
//         fs::write(project_path.join("models/test_model.yml"), model_yml)?;
//
//         let config = Config::load_from_directory(project_path)?;
//
//         assert_eq!(config.adapters.len(), 1);
//         assert!(config.adapters.contains_key("test_adapter"));
//
//         assert_eq!(config.models.len(), 1);
//         assert!(config.models.contains_key("test_model"));
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_load_from_directory_missing_project_yml() {
//         let temp_dir = tempfile::tempdir().unwrap();
//         let project_path = temp_dir.path();
//
//         let result = Config::load_from_directory(project_path);
//         assert!(result.is_err());
//         assert!(
//             result
//                 .unwrap_err()
//                 .to_string()
//                 .contains("project.yml not found")
//         );
//     }
//
//     #[test]
//     fn test_extract_config_name_with_subdirectories() -> Result<()> {
//         use tempfile::tempdir;
//
//         let temp_dir = tempdir()?;
//         let base_dir = temp_dir.path();
//
//         let staging_users_path = base_dir.join("staging").join("users.yml");
//         let marts_orders_path = base_dir.join("marts").join("orders.yml");
//         let production_api_path = base_dir.join("production").join("api.yml");
//
//         assert_eq!(
//             extract_config_name(&staging_users_path, base_dir, "model")?,
//             "staging_users"
//         );
//         assert_eq!(
//             extract_config_name(&marts_orders_path, base_dir, "model")?,
//             "marts_orders"
//         );
//         assert_eq!(
//             extract_config_name(&production_api_path, base_dir, "adapter")?,
//             "production_api"
//         );
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_load_same_filename_models() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_path = temp_dir.path();
//
//         let project_yml = r#"
//             storage:
//               type: local
//               path: ./storage
//             database:
//               type: sqlite
//               path: ./database.db
//             connections: {}"#;
//         fs::write(project_path.join("project.yml"), project_yml)?;
//
//         fs::create_dir_all(project_path.join("models/staging"))?;
//         fs::create_dir_all(project_path.join("models/marts"))?;
//
//         let staging_users_yml = r#"
//             description: "Staging users model"
//             sql: "SELECT * FROM raw_users""#;
//         fs::write(
//             project_path.join("models/staging/users.yml"),
//             staging_users_yml,
//         )?;
//
//         let marts_users_yml = r#"
//             description: "Marts users model"
//             sql: "SELECT * FROM staging_users""#;
//         fs::write(project_path.join("models/marts/users.yml"), marts_users_yml)?;
//
//         let config = Config::load_from_directory(project_path)?;
//
//         assert_eq!(config.models.len(), 2);
//         assert!(config.models.contains_key("staging_users"));
//         assert!(config.models.contains_key("marts_users"));
//
//         assert_eq!(
//             config.models["staging_users"].description.as_ref().unwrap(),
//             "Staging users model"
//         );
//         assert_eq!(
//             config.models["marts_users"].description.as_ref().unwrap(),
//             "Marts users model"
//         );
//
//         Ok(())
//     }
// }
