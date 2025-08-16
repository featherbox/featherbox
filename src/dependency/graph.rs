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

#[derive(Clone)]
pub struct Node {
    pub name: String,
}

#[derive(Clone)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

#[derive(Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Debug)]
pub struct GraphError {
    pub message: String,
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for GraphError {}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphChanges {
    pub added_nodes: Vec<String>,
    pub removed_nodes: Vec<String>,
    pub added_edges: Vec<(String, String)>,
    pub removed_edges: Vec<(String, String)>,
}

impl GraphChanges {
    pub fn has_changes(&self) -> bool {
        !self.added_nodes.is_empty()
            || !self.removed_nodes.is_empty()
            || !self.added_edges.is_empty()
            || !self.removed_edges.is_empty()
    }

    pub fn get_all_affected_nodes(&self) -> Vec<String> {
        let mut nodes = Vec::new();
        nodes.extend(self.added_nodes.clone());
        nodes.extend(self.removed_nodes.clone());

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

            let dependent_tables = from_table(&model_config.sql);
            for table in dependent_tables {
                if table == *model_name {
                    return Err(GraphError {
                        message: format!("Model '{model_name}' has a self-reference"),
                    });
                }

                edges.push(Edge {
                    from: table,
                    to: model_name.to_string(),
                });
            }
        }

        Ok(Self { nodes, edges })
    }
}

pub fn from_table(sql: &str) -> Vec<String> {
    let dialect = DuckDbDialect;
    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    if let Some(Statement::Query(query)) = ast.first() {
        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
            let mut tables = Vec::new();

            for table in &select.from {
                collect_table_names(&table.relation, &mut tables);

                for join in &table.joins {
                    collect_table_names(&join.relation, &mut tables);
                }
            }

            return tables;
        }
    }

    vec![]
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
    use crate::config::{model::ModelConfig, project::ProjectConfig};
    use std::collections::HashMap;

    #[test]
    fn test_from_config() {
        let adapters = HashMap::new();
        let mut models = HashMap::new();

        models.insert(
            "users".to_string(),
            ModelConfig {
                description: None,
                max_age: None,
                sql: "SELECT * FROM raw_users WHERE active = true".to_string(),
            },
        );

        models.insert(
            "orders".to_string(),
            ModelConfig {
                description: None,
                max_age: None,
                sql: "SELECT o.*, u.name FROM order_items o JOIN users u ON o.user_id = u.id"
                    .to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: crate::config::project::StorageConfig {
                    ty: crate::config::project::StorageType::Local,
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    username: None,
                    password: None,
                },
                deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
                connections: HashMap::new(),
            },
            adapters,
            models,
            project_root: std::path::PathBuf::from("/tmp"),
        };

        let graph = Graph::from_config(&config).unwrap();

        assert_eq!(graph.nodes.len(), 2);
        let node_names: Vec<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
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
    fn test_self_reference_error() {
        let adapters = HashMap::new();
        let mut models = HashMap::new();

        models.insert(
            "recursive_model".to_string(),
            ModelConfig {
                description: None,
                max_age: None,
                sql: "SELECT * FROM recursive_model WHERE id > 10".to_string(),
            },
        );

        let config = Config {
            project: ProjectConfig {
                storage: crate::config::project::StorageConfig {
                    ty: crate::config::project::StorageType::Local,
                    path: "/tmp".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("/tmp/test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    username: None,
                    password: None,
                },
                deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
                connections: HashMap::new(),
            },
            adapters,
            models,
            project_root: std::path::PathBuf::from("/tmp"),
        };

        let result = Graph::from_config(&config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.message.contains("self-reference"));
            assert!(e.message.contains("recursive_model"));
        }
    }

    #[test]
    fn test_calculate_affected_nodes_simple() {
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
        };

        let affected_nodes = calculate_affected_nodes(&graph, &changes);

        assert!(affected_nodes.contains(&"users".to_string()));
        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert_eq!(affected_nodes.len(), 2);
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
        };
        assert!(!changes_empty.has_changes());

        let changes_with_added_node = GraphChanges {
            added_nodes: vec!["users".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
        };
        assert!(changes_with_added_node.has_changes());

        let changes_with_edge = GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![("users".to_string(), "stats".to_string())],
            removed_edges: vec![],
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
        };

        let affected_nodes = changes.get_all_affected_nodes();
        assert!(affected_nodes.contains(&"new_table".to_string()));
        assert!(affected_nodes.contains(&"old_table".to_string()));
        assert!(affected_nodes.contains(&"user_stats".to_string()));
        assert!(affected_nodes.contains(&"old_stats".to_string()));
    }
}
