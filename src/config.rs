use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use yaml_rust2::YamlLoader;

use crate::secret::expand_secrets_in_text;

pub fn load_from_directory(project_path: &Path) -> Result<Config> {
    Config::load_from_directory(project_path)
}

pub mod adapter;
pub mod model;
pub mod project;

pub use adapter::AdapterConfig;
pub use model::ModelConfig;
pub use project::ProjectConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub project: ProjectConfig,
    pub adapters: HashMap<String, AdapterConfig>,
    pub models: HashMap<String, ModelConfig>,
    pub project_root: std::path::PathBuf,
}

impl Config {
    pub fn load_from_directory(project_path: &Path) -> Result<Self> {
        let project_config = load_project_config(project_path)?;
        let adapters = load_adapters(&project_config, project_path)?;
        let models = load_models(&project_config, project_path)?;

        Ok(Config {
            project: project_config,
            adapters,
            models,
            project_root: project_path.to_path_buf(),
        })
    }
}

fn load_project_config(project_path: &Path) -> Result<ProjectConfig> {
    let project_yml_path = project_path.join("project.yml");
    if !project_yml_path.exists() {
        return Err(anyhow::anyhow!(
            "project.yml not found. Please run 'fbox init' first."
        ));
    }

    let content = fs::read_to_string(&project_yml_path)?;
    let docs = YamlLoader::load_from_str(&content)?;
    let yaml = &docs[0];

    let project_config = project::parse_project_config(yaml);

    let expanded_content = expand_secrets_in_text(&content, &project_config, project_path)?;
    let expanded_docs = YamlLoader::load_from_str(&expanded_content)?;
    let expanded_yaml = &expanded_docs[0];

    Ok(project::parse_project_config(expanded_yaml))
}

fn load_config_files<T>(
    config_dir: &Path,
    file_type: &str,
    parse_fn: fn(&yaml_rust2::Yaml) -> Result<T>,
    project_config: &ProjectConfig,
    project_path: &Path,
) -> Result<HashMap<String, T>> {
    let mut configs = HashMap::new();

    if !config_dir.exists() {
        return Ok(configs);
    }

    for entry in fs::read_dir(config_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = extract_config_name(&path, file_type)?;
            let config = parse_config_file(&path, parse_fn, project_config, project_path)?;
            configs.insert(name, config);
        }
    }

    Ok(configs)
}

fn load_config_files_recursive<T>(
    config_dir: &Path,
    file_type: &str,
    parse_fn: fn(&yaml_rust2::Yaml) -> Result<T>,
    project_config: &ProjectConfig,
    project_path: &Path,
) -> Result<HashMap<String, T>> {
    let mut configs = HashMap::new();

    if !config_dir.exists() {
        return Ok(configs);
    }

    collect_config_files_recursive(
        config_dir,
        file_type,
        parse_fn,
        project_config,
        project_path,
        &mut configs,
    )?;

    Ok(configs)
}

fn collect_config_files_recursive<T>(
    dir: &Path,
    file_type: &str,
    parse_fn: fn(&yaml_rust2::Yaml) -> Result<T>,
    project_config: &ProjectConfig,
    project_path: &Path,
    configs: &mut HashMap<String, T>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_config_files_recursive(
                &path,
                file_type,
                parse_fn,
                project_config,
                project_path,
                configs,
            )?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = extract_config_name(&path, file_type)?;
            let config = parse_config_file(&path, parse_fn, project_config, project_path)?;
            configs.insert(name, config);
        }
    }

    Ok(())
}

fn extract_config_name(path: &Path, file_type: &str) -> Result<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid {} filename: {:?}", file_type, path))
        .map(|s| s.to_string())
}

fn parse_config_file<T>(
    path: &Path,
    parse_fn: fn(&yaml_rust2::Yaml) -> Result<T>,
    project_config: &ProjectConfig,
    project_path: &Path,
) -> Result<T> {
    let content = fs::read_to_string(path)?;
    let expanded_content = expand_secrets_in_text(&content, project_config, project_path)?;
    let docs = YamlLoader::load_from_str(&expanded_content)?;
    let yaml = &docs[0];
    parse_fn(yaml)
}

fn load_adapters(
    project_config: &ProjectConfig,
    project_path: &Path,
) -> Result<HashMap<String, AdapterConfig>> {
    load_config_files(
        &project_path.join("adapters"),
        "adapter",
        adapter::parse_adapter_config,
        project_config,
        project_path,
    )
}

fn load_models(
    project_config: &ProjectConfig,
    project_path: &Path,
) -> Result<HashMap<String, ModelConfig>> {
    load_config_files_recursive(
        &project_path.join("models"),
        "model",
        model::parse_model_config,
        project_config,
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
            deployments:
              timeout: 600
            connections: {}
            secret_key_path: secret.key"#;
        fs::write(project_path.join("project.yml"), project_yml)?;

        let adapter_yml = r#"
            connection: test_connection
            description: "Test adapter"
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
}
