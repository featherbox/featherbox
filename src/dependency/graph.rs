use crate::config::Config;
use sqlparser::{
    ast::{Statement, TableFactor},
    dialect::DuckDbDialect,
    parser::Parser,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
};

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, PartialEq)]
pub enum GraphError {
    CircularDependency {
        nodes: Vec<String>,
    },
    NonExistentTableReference {
        model_name: String,
        table_name: String,
    },
    SqlParseError {
        model_name: String,
        error: String,
    },
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::CircularDependency { nodes } => {
                write!(
                    f,
                    "Circular dependency detected involving nodes: {}",
                    nodes.join(" -> ")
                )
            }
            GraphError::NonExistentTableReference {
                model_name,
                table_name,
            } => {
                write!(
                    f,
                    "Model '{model_name}' references non-existent table '{table_name}'"
                )
            }
            GraphError::SqlParseError { model_name, error } => {
                write!(f, "SQL parse error in model '{model_name}': {error}")
            }
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphChanges {
    pub added_nodes: Vec<String>,
    pub removed_nodes: Vec<String>,
    pub added_edges: Vec<(String, String)>,
    pub removed_edges: Vec<(String, String)>,
    pub config_changed_nodes: Vec<String>,
}

impl GraphChanges {
    pub fn has_changes(&self) -> bool {
        !self.added_nodes.is_empty()
            || !self.removed_nodes.is_empty()
            || !self.added_edges.is_empty()
            || !self.removed_edges.is_empty()
            || !self.config_changed_nodes.is_empty()
    }

    pub fn get_all_affected_nodes(&self) -> Vec<String> {
        let mut nodes = Vec::new();
        nodes.extend(self.added_nodes.clone());
        nodes.extend(self.removed_nodes.clone());
        nodes.extend(self.config_changed_nodes.clone());

        for (_, to) in &self.added_edges {
            if !nodes.contains(to) {
                nodes.push(to.clone());
            }
        }
        for (_, to) in &self.removed_edges {
            if !nodes.contains(to) {
                nodes.push(to.clone());
            }
        }

        nodes
    }
}

pub fn calculate_affected_nodes(graph: &Graph, changes: &GraphChanges) -> Vec<String> {
    let mut affected_nodes = HashSet::new();

    affected_nodes.extend(changes.added_nodes.iter().cloned());
    affected_nodes.extend(changes.removed_nodes.iter().cloned());
    affected_nodes.extend(changes.config_changed_nodes.iter().cloned());

    for (_, to) in &changes.added_edges {
        affected_nodes.insert(to.clone());
    }
    for (_, to) in &changes.removed_edges {
        affected_nodes.insert(to.clone());
    }

    let adjacency_map = build_adjacency_map(graph);

    let mut to_visit = VecDeque::new();
    for node in &affected_nodes {
        to_visit.push_back(node.clone());
    }

    while let Some(current_node) = to_visit.pop_front() {
        if let Some(downstream_nodes) = adjacency_map.get(&current_node) {
            for downstream_node in downstream_nodes {
                if !affected_nodes.contains(downstream_node) {
                    affected_nodes.insert(downstream_node.clone());
                    to_visit.push_back(downstream_node.clone());
                }
            }
        }
    }

    affected_nodes.into_iter().collect()
}

pub fn build_adjacency_map(graph: &Graph) -> HashMap<String, Vec<String>> {
    let mut adjacency_map: HashMap<String, Vec<String>> = HashMap::new();

    for node in &graph.nodes {
        adjacency_map.insert(node.name.clone(), Vec::new());
    }

    for edge in &graph.edges {
        adjacency_map
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }

    adjacency_map
}

impl Graph {
    fn validate_circular_dependencies(&self) -> Result<(), GraphError> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adjacency_map: HashMap<String, Vec<String>> = HashMap::new();

        for node in &self.nodes {
            in_degree.insert(node.name.clone(), 0);
            adjacency_map.insert(node.name.clone(), Vec::new());
        }

        for edge in &self.edges {
            *in_degree.entry(edge.to.clone()).or_insert(0) += 1;
            adjacency_map
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }

        let mut queue: VecDeque<String> = VecDeque::new();
        for (node, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(node.clone());
            }
        }

        let mut sorted_count = 0;
        while let Some(current) = queue.pop_front() {
            sorted_count += 1;

            if let Some(neighbors) = adjacency_map.get(&current) {
                for neighbor in neighbors {
                    if let Some(degree) = in_degree.get_mut(neighbor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
            }
        }

        if sorted_count != self.nodes.len() {
            let remaining_nodes: Vec<String> = in_degree
                .into_iter()
                .filter(|(_, degree)| *degree > 0)
                .map(|(node, _)| node)
                .collect();

            return Err(GraphError::CircularDependency {
                nodes: remaining_nodes,
            });
        }

        Ok(())
    }

    fn validate_table_references(&self, config: &Config) -> Result<(), GraphError> {
        let mut all_table_names = HashSet::new();

        for adapter_name in config.adapters.keys() {
            all_table_names.insert(adapter_name.clone());
        }

        for model_name in config.models.keys() {
            all_table_names.insert(model_name.clone());
        }

        for (model_name, model_config) in &config.models {
            let dependent_tables =
                dependent_tables(&model_config.sql).map_err(|e| GraphError::SqlParseError {
                    model_name: model_name.clone(),
                    error: e,
                })?;

            for table in dependent_tables {
                if !all_table_names.contains(&table) {
                    return Err(GraphError::NonExistentTableReference {
                        model_name: model_name.clone(),
                        table_name: table,
                    });
                }
            }
        }

        Ok(())
    }

    pub fn from_config(config: &Config) -> Result<Self, GraphError> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for adapter_name in config.adapters.keys() {
            nodes.push(Node {
                name: adapter_name.to_string(),
            });
        }

        for (model_name, model_config) in &config.models {
            nodes.push(Node {
                name: model_name.to_string(),
            });

            let dependent_tables =
                dependent_tables(&model_config.sql).map_err(|e| GraphError::SqlParseError {
                    model_name: model_name.clone(),
                    error: e,
                })?;
            for table in dependent_tables {
                edges.push(Edge {
                    from: table,
                    to: model_name.to_string(),
                });
            }
        }

        let graph = Self { nodes, edges };

        graph.validate_table_references(config)?;
        graph.validate_circular_dependencies()?;

        Ok(graph)
    }
}

pub fn dependent_tables(sql: &str) -> Result<Vec<String>, String> {
    let dialect = DuckDbDialect;
    let ast = match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => ast,
        Err(e) => return Err(e.to_string()),
    };

    if let Some(Statement::Query(query)) = ast.first()
        && let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
    {
        let mut tables = Vec::new();

        for table in &select.from {
            collect_table_names(&table.relation, &mut tables);

            for join in &table.joins {
                collect_table_names(&join.relation, &mut tables);
            }
        }

        return Ok(tables);
    }

    Ok(vec![])
}

fn collect_table_names(table_factor: &TableFactor, tables: &mut Vec<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_names(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_table_names(&join.relation, tables);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        model::ModelConfig,
        project::{ProjectConfig, StorageConfig},
    };
    use std::collections::HashMap;

    #[test]
    fn test_from_config() {
        let mut adapters = HashMap::new();
        adapters.insert(
            "raw_users".to_string(),
            crate::config::adapter::AdapterConfig {
                connection: "default".to_string(),
                description: None,
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "/tmp/raw_users.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        delimiter: Some(",".to_string()),
                        null_value: None,
                        has_header: Some(true),
                    },
                },
                columns: vec![],
            },
        );

        adapters.insert(
            "order_items".to_string(),
            crate::config::adapter::AdapterConfig {
                connection: "default".to_string(),
                description: None,
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "/tmp/order_items.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        delimiter: Some(",".to_string()),
                        null_value: None,
                        has_header: Some(true),
                    },
                },
                columns: vec![],
            },
        );

        let mut models = HashMap::new();

        models.insert(
            "users".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM raw_users WHERE active = true".to_string(),
            },
        );

        models.insert(
            "orders".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT o.*, u.name FROM order_items o JOIN users u ON o.user_id = u.id"
                    .to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: StorageConfig::LocalFile {
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters,
            models,
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir: todo!(),
        };

        let graph = Graph::from_config(&config).unwrap();

        assert_eq!(graph.nodes.len(), 4);
        let node_names: Vec<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains(&"raw_users"));
        assert!(node_names.contains(&"order_items"));
        assert!(node_names.contains(&"users"));
        assert!(node_names.contains(&"orders"));

        assert_eq!(graph.edges.len(), 3);
        let edge_exists =
            |from: &str, to: &str| graph.edges.iter().any(|e| e.from == from && e.to == to);
        assert!(edge_exists("raw_users", "users"));
        assert!(edge_exists("order_items", "orders"));
        assert!(edge_exists("users", "orders"));
    }

    #[test]
    fn test_circular_dependency() {
        let adapters = HashMap::new();
        let mut models = HashMap::new();

        models.insert(
            "model_a".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM model_b".to_string(),
            },
        );

        models.insert(
            "model_b".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM model_c".to_string(),
            },
        );

        models.insert(
            "model_c".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM model_a".to_string(),
            },
        );

        models.insert(
            "self_reference_model".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM self_reference_model WHERE id > 10".to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: StorageConfig::LocalFile {
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters,
            models,
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir: todo!(),
        };

        let result = Graph::from_config(&config);
        assert!(result.is_err());
        if let Err(GraphError::CircularDependency { nodes }) = result {
            assert_eq!(nodes.len(), 4);
            assert!(nodes.contains(&"model_a".to_string()));
            assert!(nodes.contains(&"model_b".to_string()));
            assert!(nodes.contains(&"model_c".to_string()));
            assert!(nodes.contains(&"self_reference_model".to_string()));
        } else {
            panic!("Expected CircularDependency error");
        }
    }

    #[test]
    fn test_non_existent_model_reference() {
        let adapters = HashMap::new();
        let mut models = HashMap::new();

        models.insert(
            "model_a".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM non_existent_model".to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: StorageConfig::LocalFile {
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters,
            models,
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir: todo!(),
        };

        let result = Graph::from_config(&config);
        assert_eq!(
            result,
            Err(GraphError::NonExistentTableReference {
                model_name: "model_a".to_string(),
                table_name: "non_existent_model".to_string(),
            })
        );
    }

    #[test]
    fn test_non_existent_adapter_reference() {
        let mut adapters = HashMap::new();
        adapters.insert(
            "existing_adapter".to_string(),
            crate::config::adapter::AdapterConfig {
                connection: "default".to_string(),
                description: None,
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "/tmp/data.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        delimiter: Some(",".to_string()),
                        null_value: None,
                        has_header: Some(true),
                    },
                },
                columns: vec![],
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "model_a".to_string(),
            ModelConfig {
                description: None,
                sql: "SELECT * FROM non_existent_adapter".to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: StorageConfig::LocalFile {
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters,
            models,
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir: todo!(),
        };

        let result = Graph::from_config(&config);
        assert_eq!(
            result,
            Err(GraphError::NonExistentTableReference {
                model_name: "model_a".to_string(),
                table_name: "non_existent_adapter".to_string(),
            })
        );
    }

    #[test]
    fn test_sql_parse_error_handling() {
        let adapters = HashMap::new();
        let mut models = HashMap::new();

        models.insert(
            "invalid_model".to_string(),
            ModelConfig {
                description: None,
                sql: "INVALID SQL SYNTAX HERE".to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: StorageConfig::LocalFile {
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters,
            models,
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_dir: todo!(),
        };

        let result = Graph::from_config(&config);
        assert!(result.is_err());
        match result {
            Err(GraphError::SqlParseError { model_name, error }) => {
                assert_eq!(model_name, "invalid_model");
                assert!(!error.is_empty());
            }
            _ => panic!("Expected SqlParseError"),
        }
    }

    #[test]
    fn test_calculate_affected_nodes_simple() {
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

        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![Edge {
                from: "users".to_string(),
                to: "user_stats".to_string(),
            }],
        };

        let changes = GraphChanges {
            added_nodes: vec!["users".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);

        assert_collection_contains_all!(
            affected_nodes,
            &["users".to_string(), "user_stats".to_string()]
        );
    }

    #[test]
    fn test_calculate_affected_nodes_complex() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
                Node {
                    name: "order_stats".to_string(),
                },
                Node {
                    name: "dashboard".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "order_stats".to_string(),
                },
                Edge {
                    from: "user_stats".to_string(),
                    to: "dashboard".to_string(),
                },
                Edge {
                    from: "order_stats".to_string(),
                    to: "dashboard".to_string(),
                },
            ],
        };

        let changes = GraphChanges {
            added_nodes: vec!["users".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);

        assert!(affected_nodes.contains(&"users".to_string()));
        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert!(affected_nodes.contains(&"dashboard".to_string()));
        assert!(!affected_nodes.contains(&"orders".to_string()));
        assert!(!affected_nodes.contains(&"order_stats".to_string()));
        assert_eq!(affected_nodes.len(), 3);
    }

    #[test]
    fn test_calculate_affected_nodes_edge_changes() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
                Node {
                    name: "dashboard".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "user_stats".to_string(),
                    to: "dashboard".to_string(),
                },
            ],
        };

        let changes = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![("users".to_string(), "user_stats".to_string())],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);

        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert!(affected_nodes.contains(&"dashboard".to_string()));
        assert!(!affected_nodes.contains(&"users".to_string()));
        assert_eq!(affected_nodes.len(), 2);
    }

    #[test]
    fn test_calculate_affected_nodes_no_changes() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![Edge {
                from: "users".to_string(),
                to: "user_stats".to_string(),
            }],
        };

        let changes = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);
        assert!(affected_nodes.is_empty());
    }

    #[test]
    fn test_build_adjacency_map() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "b".to_string(),
                },
                Node {
                    name: "c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
            ],
        };

        let adjacency_map = build_adjacency_map(&graph);

        assert_eq!(adjacency_map.get("a").unwrap(), &vec!["b"]);
        assert_eq!(adjacency_map.get("b").unwrap(), &vec!["c"]);
        assert_eq!(adjacency_map.get("c").unwrap(), &Vec::<String>::new());
    }

    #[test]
    fn test_calculate_affected_nodes_removed_nodes() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![Edge {
                from: "users".to_string(),
                to: "user_stats".to_string(),
            }],
        };

        let changes = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec!["users".to_string()],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);

        assert!(affected_nodes.contains(&"users".to_string()));
        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert_eq!(affected_nodes.len(), 2);
    }

    #[test]
    fn test_graph_changes_has_changes() {
        let changes_empty = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };
        assert!(!changes_empty.has_changes());

        let changes_with_added_node = GraphChanges {
            added_nodes: vec!["users".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };
        assert!(changes_with_added_node.has_changes());

        let changes_with_edge = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![("users".to_string(), "stats".to_string())],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };
        assert!(changes_with_edge.has_changes());
    }

    #[test]
    fn test_graph_changes_get_all_affected_nodes() {
        let changes = GraphChanges {
            added_nodes: vec!["new_table".to_string()],
            removed_nodes: vec!["old_table".to_string()],
            added_edges: vec![("users".to_string(), "user_stats".to_string())],
            removed_edges: vec![("orders".to_string(), "old_stats".to_string())],
            config_changed_nodes: vec![],
        };

        let affected_nodes = changes.get_all_affected_nodes();
        assert!(affected_nodes.contains(&"new_table".to_string()));
        assert!(affected_nodes.contains(&"old_table".to_string()));
        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert!(affected_nodes.contains(&"old_stats".to_string()));
    }
}
