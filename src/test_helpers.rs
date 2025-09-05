use crate::{
    config::{
        Config,
        adapter::AdapterConfig,
        model::ModelConfig,
        project::{ConnectionConfig, DatabaseConfig, DatabaseType, ProjectConfig, StorageConfig},
        query::QueryConfig,
    },
    dependency::graph::{Edge, Graph, Node},
};
use anyhow::Result;

#[cfg(test)]
use crate::workspace::set_project_dir_override;

#[cfg(test)]
use axum::Router;

#[cfg(test)]
use axum_test::TestServer;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

pub fn setup_test_project() -> Result<TempDir> {
    let temp_dir = tempfile::tempdir()?;
    let project_path = temp_dir.path();

    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models"))?;
    fs::write(project_path.join("project.yml"), "test")?;

    Ok(temp_dir)
}

pub fn create_project_structure(project_path: &Path) -> Result<()> {
    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models").join("staging"))?;
    fs::create_dir_all(project_path.join("models").join("marts"))?;
    Ok(())
}

pub fn create_default_project_config() -> ProjectConfig {
    ProjectConfig {
        storage: StorageConfig::LocalFile {
            path: "/tmp/foo/storage".to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some("test.db".to_string()),
            host: None,
            port: None,
            database: None,
            password: None,
            username: None,
        },
        connections: HashMap::new(),
    }
}

pub fn create_project_config_with_connections(
    connections: HashMap<String, ConnectionConfig>,
) -> ProjectConfig {
    ProjectConfig {
        storage: StorageConfig::LocalFile {
            path: "/tmp/foo/storage".to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some("test.db".to_string()),
            host: None,
            port: None,
            database: None,
            password: None,
            username: None,
        },
        connections,
    }
}

#[cfg(test)]
pub fn create_test_project() -> Result<ProjectConfig> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let project_path = temp_dir.path().to_path_buf();
    set_project_dir_override(project_path.clone());

    let config = ProjectConfig {
        storage: StorageConfig::LocalFile {
            path: project_path.join("storage").to_string_lossy().to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some(project_path.join("app.db").to_string_lossy().to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password: None,
        },
        connections: HashMap::new(),
    };

    config.create_project()?;
    std::mem::forget(temp_dir);

    Ok(config)
}

#[cfg(test)]
pub fn create_test_server<F>(routes: F) -> TestServer
where
    F: FnOnce() -> Router,
{
    let app = routes();
    TestServer::new(app).expect("Failed to create test server")
}

pub fn setup_test_db(temp_dir: &TempDir) -> Result<String> {
    let project_path = temp_dir.path();
    let db_path = project_path.join("test.db");

    create_project_structure(project_path)?;

    fs::write(project_path.join("project.yml"), "test")?;

    Ok(db_path.to_string_lossy().to_string())
}

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

pub fn create_test_model_yaml(name: &str, sql: &str) -> String {
    format!(
        r#"name: {name}
sql: {sql}
"#
    )
}

pub fn write_test_adapter(project_path: &Path, name: &str, content: &str) -> Result<()> {
    let adapter_path = project_path.join("adapters").join(format!("{name}.yml"));
    fs::write(adapter_path, content)?;
    Ok(())
}

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

pub struct TestGraphBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Default for TestGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestGraphBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(mut self, name: impl Into<String>) -> Self {
        self.nodes.push(Node { name: name.into() });
        self
    }

    pub fn add_edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
        });
        self
    }

    pub fn build(self) -> Graph {
        Graph {
            nodes: self.nodes,
            edges: self.edges,
        }
    }
}

pub struct TestProjectConfigBuilder {
    storage: StorageConfig,
    database: DatabaseConfig,
    connections: HashMap<String, ConnectionConfig>,
}

impl Default for TestProjectConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestProjectConfigBuilder {
    pub fn new() -> Self {
        Self {
            storage: StorageConfig::LocalFile {
                path: "/tmp/test_storage".to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some("test.db".to_string()),
                host: None,
                port: None,
                database: None,
                password: None,
                username: None,
            },
            connections: HashMap::new(),
        }
    }

    pub fn with_sqlite_db(mut self, path: impl Into<String>) -> Self {
        self.database = DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some(path.into()),
            host: None,
            port: None,
            database: None,
            password: None,
            username: None,
        };
        self
    }

    pub fn build(self) -> ProjectConfig {
        ProjectConfig {
            storage: self.storage,
            database: self.database,
            connections: self.connections,
        }
    }
}

#[track_caller]
pub fn assert_graph_contains_node(graph: &Graph, node_name: &str) {
    let node_names: Vec<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
    assert!(
        node_names.contains(&node_name),
        "Graph should contain node '{}', but only found: {:?}",
        node_name,
        node_names
    );
}

#[track_caller]
pub fn assert_graph_contains_edge(graph: &Graph, from: &str, to: &str) {
    let has_edge = graph.edges.iter().any(|e| e.from == from && e.to == to);
    assert!(
        has_edge,
        "Graph should have edge from '{}' to '{}', but edges are: {:?}",
        from, to, graph.edges
    );
}

#[macro_export]
macro_rules! assert_pipeline_has_levels {
    ($pipeline:expr, $expected_sizes:expr) => {
        let actual_sizes: Vec<usize> = $pipeline.levels.iter().map(|level| level.len()).collect();
        assert_eq!(
            actual_sizes, $expected_sizes,
            "Pipeline level sizes don't match. Expected: {:?}, got: {:?}",
            $expected_sizes, actual_sizes
        );
    };
}

#[macro_export]
macro_rules! assert_level_has_tables {
    ($pipeline:expr, $level_index:expr, $expected_tables:expr) => {
        let level_tables: Vec<&str> = $pipeline.levels[$level_index]
            .iter()
            .map(|action| action.table_name.as_str())
            .collect();
        for expected in $expected_tables {
            assert!(
                level_tables.contains(&expected),
                "Level {} should contain table '{}', but only found: {:?}",
                $level_index,
                expected,
                level_tables
            );
        }
    };
}

#[macro_export]
macro_rules! assert_failed_tasks_contain {
    ($failed_tasks:expr, $expected_failed:expr) => {
        for expected in $expected_failed {
            let expected_string = expected.to_string();
            assert!(
                $failed_tasks.contains(&expected_string),
                "Failed tasks should contain '{}', but only found: {:?}",
                expected,
                $failed_tasks
            );
        }
    };
}

#[macro_export]
macro_rules! assert_failed_tasks_not_contain {
    ($failed_tasks:expr, $unexpected_failed:expr) => {
        for unexpected in $unexpected_failed {
            let unexpected_string = unexpected.to_string();
            assert!(
                !$failed_tasks.contains(&unexpected_string),
                "Failed tasks should not contain '{}', but it was found in: {:?}",
                unexpected,
                $failed_tasks
            );
        }
    };
}

pub fn setup_test_project_with_dirs() -> Result<TempDir> {
    let temp_dir = tempfile::tempdir()?;
    let project_path = temp_dir.path();

    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models").join("staging"))?;
    fs::create_dir_all(project_path.join("models").join("marts"))?;
    fs::create_dir_all(project_path.join("queries"))?;
    fs::create_dir_all(project_path.join("data"))?;

    Ok(temp_dir)
}

pub fn setup_test_project_with_config(config: &ProjectConfig) -> Result<TempDir> {
    let temp_dir = setup_test_project_with_dirs()?;
    let project_path = temp_dir.path();

    let config_yaml = serde_yml::to_string(config)?;
    fs::write(project_path.join("project.yml"), config_yaml)?;

    Ok(temp_dir)
}

pub fn write_test_file(temp_dir: &TempDir, relative_path: &str, content: &str) -> Result<()> {
    let file_path = temp_dir.path().join(relative_path);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(file_path, content)?;
    Ok(())
}

#[macro_export]
macro_rules! assert_collection_len {
    ($collection:expr, $expected_len:expr) => {
        assert_eq!(
            $collection.len(),
            $expected_len,
            "Expected collection length {}, but got {}. Collection: {:?}",
            $expected_len,
            $collection.len(),
            $collection
        );
    };
}

#[macro_export]
macro_rules! assert_collection_contains_all {
    ($collection:expr, $expected_items:expr) => {
        for expected in $expected_items {
            assert!(
                $collection.contains(expected),
                "Collection should contain '{:?}', but only found: {:?}",
                expected,
                $collection
            );
        }
        assert_eq!(
            $collection.len(),
            $expected_items.len(),
            "Collection length mismatch. Expected {} items, but got {}. Collection: {:?}",
            $expected_items.len(),
            $collection.len(),
            $collection
        );
    };
}

#[macro_export]
macro_rules! assert_result_contains_error {
    ($result:expr, $expected_substring:expr) => {
        match $result {
            Ok(_) => panic!(
                "Expected an error containing '{}', but got Ok",
                $expected_substring
            ),
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains($expected_substring),
                    "Error message should contain '{}', but got: '{}'",
                    $expected_substring,
                    error_msg
                );
            }
        }
    };
}

pub struct TestConfigBuilder {
    project: ProjectConfig,
    adapters: HashMap<String, AdapterConfig>,
    models: HashMap<String, ModelConfig>,
    queries: HashMap<String, QueryConfig>,
    project_root: PathBuf,
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestConfigBuilder {
    pub fn new() -> Self {
        Self {
            project: create_default_project_config(),
            adapters: HashMap::new(),
            models: HashMap::new(),
            queries: HashMap::new(),
            project_root: PathBuf::from("/tmp"),
        }
    }

    pub fn with_project(mut self, project: ProjectConfig) -> Self {
        self.project = project;
        self
    }

    pub fn with_project_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.project_root = root.into();
        self
    }

    pub fn add_model(mut self, name: impl Into<String>, sql: impl Into<String>) -> Self {
        self.models.insert(
            name.into(),
            ModelConfig {
                description: None,
                sql: sql.into(),
            },
        );
        self
    }

    pub fn add_model_with_description(
        mut self,
        name: impl Into<String>,
        sql: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.models.insert(
            name.into(),
            ModelConfig {
                description: Some(description.into()),
                sql: sql.into(),
            },
        );
        self
    }

    pub fn add_adapter(mut self, name: impl Into<String>, adapter: AdapterConfig) -> Self {
        self.adapters.insert(name.into(), adapter);
        self
    }

    pub fn build(self) -> Config {
        Config {
            project: self.project,
            adapters: self.adapters,
            models: self.models,
            queries: self.queries,
            dashboards: HashMap::new(),
        }
    }
}
