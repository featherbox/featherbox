pub mod adapter;
pub mod model;
pub mod project;

use std::{collections::HashMap, fs, path::PathBuf};
use yaml_rust2::YamlLoader;

pub use adapter::parse_adapter_config;
pub use model::parse_model_config;
pub use project::parse_project_config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub project: project::ProjectConfig,
    pub adapters: HashMap<String, adapter::AdapterConfig>,
    pub models: HashMap<String, model::ModelConfig>,
}

pub fn parse_config(workdir: PathBuf) -> Config {
    let source = fs::read_to_string(workdir.join("project.yml")).unwrap();
    let project = &YamlLoader::load_from_str(&source).unwrap()[0];
    let project = parse_project_config(project);

    let mut adapters = HashMap::new();
    for entry in fs::read_dir(workdir.join("adapters")).unwrap() {
        let entry = entry.unwrap();
        if let Some(ext) = entry.path().extension()
            && (ext == "yml" || ext == "yaml")
        {
            let source = fs::read_to_string(entry.path()).unwrap();
            let adapter = &YamlLoader::load_from_str(&source).unwrap()[0];
            let adapter_config = parse_adapter_config(adapter);
            let adapter_name = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            adapters.insert(adapter_name, adapter_config);
        }
    }

    let mut models = HashMap::new();
    for entry in fs::read_dir(workdir.join("models")).unwrap() {
        let entry = entry.unwrap();
        if let Some(ext) = entry.path().extension()
            && (ext == "yml" || ext == "yaml")
        {
            let source = fs::read_to_string(entry.path()).unwrap();
            let model = &YamlLoader::load_from_str(&source).unwrap()[0];
            let model_config = parse_model_config(model);
            let model_name = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            models.insert(model_name, model_config);
        }
    }

    Config {
        project,
        adapters,
        models,
    }
}
