use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use yaml_rust2::YamlLoader;

pub mod adapter;
pub mod model;
pub mod project;

pub use adapter::AdapterConfig;
pub use model::ModelConfig;
pub use project::ProjectConfig;

#[derive(Debug)]
pub struct Config {
    pub project: ProjectConfig,
    pub adapters: HashMap<String, AdapterConfig>,
    pub models: HashMap<String, ModelConfig>,
}

impl Config {
    pub fn load_from_directory(project_path: &Path) -> Result<Self> {
        let project_config = load_project_config(project_path)?;
        let adapters = load_adapters(project_path)?;
        let models = load_models(project_path)?;

        Ok(Config {
            project: project_config,
            adapters,
            models,
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

    Ok(project::parse_project_config(yaml))
}

fn load_adapters(project_path: &Path) -> Result<HashMap<String, AdapterConfig>> {
    let adapters_dir = project_path.join("adapters");
    let mut adapters = HashMap::new();

    if !adapters_dir.exists() {
        return Ok(adapters);
    }

    for entry in fs::read_dir(&adapters_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow::anyhow!("Invalid adapter filename: {:?}", path))?
                .to_string();

            let content = fs::read_to_string(&path)?;
            let docs = YamlLoader::load_from_str(&content)?;
            let yaml = &docs[0];

            let adapter_config = adapter::parse_adapter_config(yaml)?;
            adapters.insert(name, adapter_config);
        }
    }

    Ok(adapters)
}

fn load_models(project_path: &Path) -> Result<HashMap<String, ModelConfig>> {
    let models_dir = project_path.join("models");
    let mut models = HashMap::new();

    if !models_dir.exists() {
        return Ok(models);
    }

    for entry in fs::read_dir(&models_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow::anyhow!("Invalid model filename: {:?}", path))?
                .to_string();

            let content = fs::read_to_string(&path)?;
            let docs = YamlLoader::load_from_str(&content)?;
            let yaml = &docs[0];

            let model_config = model::parse_model_config(yaml)?;
            models.insert(name, model_config);
        }
    }

    Ok(models)
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
            connections: {}"#;
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
            sql: "SELECT * FROM test_adapter"
            max_age: 3600"#;
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
