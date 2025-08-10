use crate::config::Config;
use sqlparser::{
    ast::{Statement, TableFactor},
    dialect::DuckDbDialect,
    parser::Parser,
};
use std::fmt;

pub struct Node {
    pub name: String,
}

pub struct Edge {
    pub from: String,
    pub to: String,
}

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
                    path: "/tmp/test.db".to_string(),
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
                    path: "/tmp/test.db".to_string(),
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
}
