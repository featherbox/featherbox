use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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
    pub project_dir: PathBuf,
}

pub struct UpsertFileHandle<'a, T: Serialize + Deserialize<'a>> {
    config: &'a T,
    path: PathBuf,
}

impl<'a, T: Serialize + Deserialize<'a>> UpsertFileHandle<'a, T> {
    pub fn save(&self) -> Result<()> {
        fs::create_dir_all(self.path.parent().unwrap())?;
        fs::write(&self.path, serde_yml::to_string(self.config)?)?;
        Ok(())
    }
}

pub struct DeleteFileHandle {
    path: PathBuf,
}

impl DeleteFileHandle {
    pub fn save(&self) -> Result<()> {
        Ok(fs::remove_file(&self.path)?)
    }
}

impl Config {
    pub fn new(project_dir: PathBuf) -> Self {
        Self {
            project: ProjectConfig::new(),
            adapters: HashMap::new(),
            models: HashMap::new(),
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir,
        }
    }

    pub fn load(&mut self) -> Result<()> {
        self.project = self.load_project_config()?;
        self.adapters = self.load_adapters()?;
        self.models = self.load_models()?;
        self.queries = self.load_queries()?;
        self.dashboards = self.load_dashboards()?;

        Ok(())
    }

    pub fn add_project_setting<'a>(
        &mut self,
        config: &'a ProjectConfig,
    ) -> Result<UpsertFileHandle<'a, ProjectConfig>> {
        self.project = config.clone();

        Ok(UpsertFileHandle {
            config,
            path: self.project_config_file()?,
        })
    }

    pub fn upsert_adapter<'a>(
        &mut self,
        path: &str,
        adapter: &'a AdapterConfig,
    ) -> Result<UpsertFileHandle<'a, AdapterConfig>> {
        self.adapters.insert(path.to_string(), adapter.clone());

        Ok(UpsertFileHandle {
            config: adapter,
            path: self
                .adapters_config_directory()?
                .join(format!("{path}.yml")),
        })
    }

    pub fn delete_adapter(&mut self, path: &str) -> Result<DeleteFileHandle> {
        self.adapters.remove(path);

        Ok(DeleteFileHandle {
            path: self
                .adapters_config_directory()?
                .join(format!("{path}.yml")),
        })
    }

    pub fn upsert_model<'a>(
        &mut self,
        path: &str,
        model: &'a ModelConfig,
    ) -> Result<UpsertFileHandle<'a, ModelConfig>> {
        self.models.insert(path.to_string(), model.clone());

        Ok(UpsertFileHandle {
            config: model,
            path: self.models_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn delete_model(&mut self, path: &str) -> Result<DeleteFileHandle> {
        self.models.remove(path);

        Ok(DeleteFileHandle {
            path: self.models_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn upsert_query<'a>(
        &mut self,
        path: &str,
        query: &'a QueryConfig,
    ) -> Result<UpsertFileHandle<'a, QueryConfig>> {
        self.queries.insert(path.to_string(), query.clone());

        Ok(UpsertFileHandle {
            config: query,
            path: self.queries_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn delete_query(&mut self, path: &str) -> Result<DeleteFileHandle> {
        self.queries.remove(path);

        Ok(DeleteFileHandle {
            path: self.queries_config_directory()?.join(format!("{path}.yml")),
        })
    }

    pub fn upsert_dashboard<'a>(
        &mut self,
        path: &str,
        dashboard: &'a DashboardConfig,
    ) -> Result<UpsertFileHandle<'a, DashboardConfig>> {
        self.dashboards.insert(path.to_string(), dashboard.clone());

        Ok(UpsertFileHandle {
            config: dashboard,
            path: self
                .dashboards_config_directory()?
                .join(format!("{path}.yml")),
        })
    }

    pub fn delete_dashboard(&mut self, path: &str) -> Result<DeleteFileHandle> {
        self.dashboards.remove(path);

        Ok(DeleteFileHandle {
            path: self
                .dashboards_config_directory()?
                .join(format!("{path}.yml")),
        })
    }

    fn project_config_file(&self) -> Result<PathBuf> {
        Ok(self.project_dir.join("project.yml"))
    }

    fn adapters_config_directory(&self) -> Result<PathBuf> {
        Ok(self.project_dir.join("adapters"))
    }

    fn models_config_directory(&self) -> Result<PathBuf> {
        Ok(self.project_dir.join("models"))
    }

    fn queries_config_directory(&self) -> Result<PathBuf> {
        Ok(self.project_dir.join("queries"))
    }

    fn dashboards_config_directory(&self) -> Result<PathBuf> {
        Ok(self.project_dir.join("dashboards"))
    }

    fn load_adapters(&self) -> Result<HashMap<String, AdapterConfig>> {
        load_config_files(
            &self.adapters_config_directory()?,
            adapter::parse_adapter_config,
        )
    }

    fn load_models(&self) -> Result<HashMap<String, ModelConfig>> {
        load_config_files(&self.models_config_directory()?, model::parse_model_config)
    }

    fn load_queries(&self) -> Result<HashMap<String, QueryConfig>> {
        load_config_files(&self.queries_config_directory()?, query::parse_query_config)
    }

    fn load_dashboards(&self) -> Result<HashMap<String, DashboardConfig>> {
        load_config_files(
            &self.dashboards_config_directory()?,
            dashboard::parse_dashboard_config,
        )
    }

    fn load_project_config(&self) -> Result<ProjectConfig> {
        let project_yml_path = self.project_config_file()?;
        if !project_yml_path.exists() {
            return Err(anyhow::anyhow!(
                "project.yml not found. Please run 'featherbox new' first."
            ));
        }

        let content = fs::read_to_string(&project_yml_path)?;

        project::parse_project_config(&content)
    }
}

fn load_config_files<T>(dir: &Path, parse_fn: fn(&str) -> Result<T>) -> Result<HashMap<String, T>> {
    let mut config = HashMap::new();

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();

        if path.is_dir() {
            todo!()
        } else if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            let content = fs::read_to_string(&path)?;
            config.insert(
                path.file_stem().unwrap().to_string_lossy().to_string(),
                parse_fn(&content)?,
            );
        }
    }

    Ok(config)
}
