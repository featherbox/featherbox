use crate::{
    config::{Config, project::ProjectConfig},
    dependency::graph::{Edge, Graph, Node},
};
use anyhow::Result;
use axum::{Extension, Router};
use axum_test::TestServer;
use std::{fs, path::Path, sync::Arc};
use tempfile::TempDir;
use tokio::sync::Mutex;

pub struct TestManager {
    temp_dir: TempDir,
    config: Arc<Mutex<Config>>,
}

impl TestManager {
    pub fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_dir = temp_dir.path().to_path_buf();
        let mut config = Config::new(project_dir.clone());
        config
            .add_project_setting(&ProjectConfig::default())
            .unwrap()
            .save()
            .unwrap();

        Self {
            temp_dir,
            config: Arc::new(Mutex::new(config)),
        }
    }

    pub fn directory(&self) -> &Path {
        self.temp_dir.path()
    }

    pub async fn config(&self) -> tokio::sync::MutexGuard<'_, Config> {
        self.config.lock().await
    }

    pub fn setup_project<F>(&self, routes: F) -> TestServer
    where
        F: FnOnce() -> Router,
    {
        let app = routes().layer(Extension(self.config.clone()));

        TestServer::new(app).unwrap()
    }

    pub fn create_server<F>(&self, routes: F) -> TestServer
    where
        F: FnOnce() -> Router,
    {
        let app = routes();
        TestServer::new(app).expect("Failed to create test server")
    }
}

impl Default for TestManager {
    fn default() -> Self {
        Self::new()
    }
}

pub fn create_test_server<F>(routes: F) -> TestServer
where
    F: FnOnce() -> Router,
{
    let app = routes();
    TestServer::new(app).expect("Failed to create test server")
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
