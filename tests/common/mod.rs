use anyhow::Result;
use featherbox::config::Config;
use featherbox::config::project::{
    ConnectionConfig, DatabaseConfig, DatabaseType, ProjectConfig, S3AuthMethod, S3Config,
    StorageConfig,
};
use featherbox::database::connection::connect_app_db;
use featherbox::dependency::graph::{Edge, Graph, Node};
use sea_orm::DatabaseConnection;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use typed_builder::TypedBuilder;

#[macro_export]
macro_rules! assert_pipeline_levels {
    ($pipeline:expr, $expected_level_sizes:expr) => {
        let actual_sizes: Vec<usize> = $pipeline.levels.iter().map(|level| level.len()).collect();
        assert_eq!(
            actual_sizes, $expected_level_sizes,
            "Pipeline level sizes don't match. Expected: {:?}, got: {:?}",
            $expected_level_sizes, actual_sizes
        );
    };
}

#[macro_export]
macro_rules! assert_level_contains_tables {
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
macro_rules! assert_contains_all {
    ($collection:expr, $expected_items:expr) => {
        for expected in $expected_items {
            assert!(
                $collection.contains(expected),
                "Collection should contain '{}', but only found: {:?}",
                expected,
                $collection
            );
        }
    };
}

#[macro_export]
macro_rules! assert_not_contains_any {
    ($collection:expr, $unexpected_items:expr) => {
        for unexpected in $unexpected_items {
            assert!(
                !$collection.contains(unexpected),
                "Collection should not contain '{}', but it was found in: {:?}",
                unexpected,
                $collection
            );
        }
    };
}

#[macro_export]
macro_rules! assert_error_contains {
    ($result:expr, $expected_substring:expr) => {
        match $result {
            Ok(_) => panic!("Expected an error, but got Ok"),
            Err(e) => assert!(
                e.to_string().contains($expected_substring),
                "Error message should contain '{}', but got: '{}'",
                $expected_substring,
                e.to_string()
            ),
        }
    };
}

#[allow(dead_code)]
#[track_caller]
pub fn assert_graph_has_nodes(graph: &Graph, expected_nodes: &[&str]) {
    let node_names: Vec<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
    for expected in expected_nodes {
        assert!(
            node_names.contains(expected),
            "Graph should contain node '{}', but only found: {:?}",
            expected,
            node_names
        );
    }
}

#[allow(dead_code)]
#[track_caller]
pub fn assert_graph_has_edge(graph: &Graph, from: &str, to: &str) {
    let has_edge = graph.edges.iter().any(|e| e.from == from && e.to == to);
    assert!(
        has_edge,
        "Graph should have edge from '{}' to '{}', but edges are: {:?}",
        from, to, graph.edges
    );
}

#[allow(dead_code)]
pub struct GraphBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

#[allow(dead_code)]
impl GraphBuilder {
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

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct ProjectConfigBuilder {
    #[builder(default = StorageConfig::LocalFile {
        path: "/tmp/test_storage".to_string(),
    })]
    storage: StorageConfig,
    #[builder(default = DatabaseConfig {
        ty: DatabaseType::Sqlite,
        path: Some("test.db".to_string()),
        host: None,
        port: None,
        database: None,
        password: None,
        username: None,
    })]
    database: DatabaseConfig,
    #[builder(default = HashMap::new())]
    connections: HashMap<String, ConnectionConfig>,
}

#[allow(dead_code)]
impl ProjectConfigBuilder {
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

    pub fn with_mysql_db(
        mut self,
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.database = DatabaseConfig {
            ty: DatabaseType::Mysql,
            path: None,
            host: Some(host.into()),
            port: Some(3306),
            database: Some(database.into()),
            password: Some(password.into()),
            username: Some(username.into()),
        };
        self
    }

    pub fn with_postgres_db(
        mut self,
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.database = DatabaseConfig {
            ty: DatabaseType::Postgresql,
            path: None,
            host: Some(host.into()),
            port: Some(5432),
            database: Some(database.into()),
            password: Some(password.into()),
            username: Some(username.into()),
        };
        self
    }

    pub fn with_s3_storage(mut self, bucket: impl Into<String>) -> Self {
        self.storage = StorageConfig::S3(S3Config {
            bucket: bucket.into(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9010".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "user".to_string(),
            secret_access_key: "password".to_string(),
            session_token: None,
            path_style_access: true,
        });
        self
    }

    pub fn add_connection(mut self, name: impl Into<String>, config: ConnectionConfig) -> Self {
        self.connections.insert(name.into(), config);
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

#[allow(dead_code)]
pub fn setup_temp_project() -> Result<TempDir> {
    let temp_dir = tempfile::tempdir()?;
    let project_path = temp_dir.path();

    fs::create_dir_all(project_path.join("adapters"))?;
    fs::create_dir_all(project_path.join("models").join("staging"))?;
    fs::create_dir_all(project_path.join("models").join("marts"))?;
    fs::write(project_path.join("project.yml"), "test")?;

    Ok(temp_dir)
}

#[allow(dead_code)]
pub async fn setup_test_db_connection_with_config(
    config: ProjectConfig,
) -> Result<DatabaseConnection> {
    connect_app_db(&config).await
}

#[allow(dead_code)]
pub async fn setup_test_config_with_db(temp_dir: &TempDir) -> Result<(DatabaseConnection, Config)> {
    let db_path = temp_dir.path().join("test.db");

    let project_config = ProjectConfig {
        storage: StorageConfig::LocalFile {
            path: temp_dir.path().to_string_lossy().to_string(),
        },
        database: DatabaseConfig {
            ty: DatabaseType::Sqlite,
            path: Some(db_path.to_string_lossy().to_string()),
            host: None,
            port: None,
            database: None,
            password: None,
            username: None,
        },
        connections: HashMap::new(),
    };

    let config = Config {
        project: project_config.clone(),
        adapters: HashMap::new(),
        models: HashMap::new(),
        queries: HashMap::new(),
        dashboards: HashMap::new(),
        project_root: temp_dir.path().to_path_buf(),
    };

    let db = connect_app_db(&project_config).await?;
    Ok((db, config))
}

#[allow(dead_code)]
pub fn write_adapter_config(project_path: &Path, name: &str, content: &str) -> Result<()> {
    let adapter_path = project_path.join("adapters").join(format!("{}.yml", name));
    fs::write(adapter_path, content)?;
    Ok(())
}

#[allow(dead_code)]
pub fn write_model_config(
    project_path: &Path,
    subdir: &str,
    name: &str,
    content: &str,
) -> Result<()> {
    let model_path = project_path
        .join("models")
        .join(subdir)
        .join(format!("{}.yml", name));
    fs::write(model_path, content)?;
    Ok(())
}

#[allow(dead_code)]
pub struct GraphFixtures;

#[allow(dead_code)]
impl GraphFixtures {
    pub fn linear_dependency() -> Graph {
        GraphBuilder::new()
            .add_node("A")
            .add_node("B")
            .add_node("C")
            .add_edge("A", "B")
            .add_edge("B", "C")
            .build()
    }

    pub fn parallel_branches() -> Graph {
        GraphBuilder::new()
            .add_node("adapter_a")
            .add_node("adapter_b")
            .add_node("model_c")
            .add_node("model_d")
            .add_edge("adapter_a", "model_c")
            .add_edge("adapter_b", "model_d")
            .build()
    }

    pub fn diamond_dependency() -> Graph {
        GraphBuilder::new()
            .add_node("A")
            .add_node("B")
            .add_node("C")
            .add_node("D")
            .add_edge("A", "B")
            .add_edge("A", "C")
            .add_edge("B", "D")
            .add_edge("C", "D")
            .build()
    }

    pub fn complex_multi_level() -> Graph {
        GraphBuilder::new()
            .add_node("A")
            .add_node("B")
            .add_node("C")
            .add_node("D")
            .add_node("E")
            .add_node("F")
            .add_edge("A", "C")
            .add_edge("B", "D")
            .add_edge("C", "E")
            .add_edge("D", "F")
            .build()
    }
}
