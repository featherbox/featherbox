use anyhow::Result;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, NotSet, QueryFilter,
    QueryOrder, Set,
};
use std::collections::HashSet;

use crate::entities::{edges, graphs, nodes, pipeline_actions, pipelines};
use crate::graph::Graph;
use crate::pipeline::Pipeline;

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

pub async fn detect_changes(
    db: &DatabaseConnection,
    current_graph: &Graph,
) -> Result<Option<GraphChanges>> {
    let last_graph = graphs::Entity::find()
        .order_by_desc(graphs::Column::Id)
        .one(db)
        .await?;

    let Some(last_graph) = last_graph else {
        return Ok(Some(GraphChanges {
            added_nodes: current_graph.nodes.iter().map(|n| n.name.clone()).collect(),
            removed_nodes: vec![],
            added_edges: current_graph
                .edges
                .iter()
                .map(|e| (e.from.clone(), e.to.clone()))
                .collect(),
            removed_edges: vec![],
        }));
    };

    let last_nodes: HashSet<String> = nodes::Entity::find()
        .filter(nodes::Column::GraphId.eq(last_graph.id))
        .all(db)
        .await?
        .into_iter()
        .map(|n| n.name)
        .collect();

    let last_edges: HashSet<(String, String)> = edges::Entity::find()
        .filter(edges::Column::GraphId.eq(last_graph.id))
        .all(db)
        .await?
        .into_iter()
        .map(|e| (e.from_node, e.to_node))
        .collect();

    let current_nodes: HashSet<String> =
        current_graph.nodes.iter().map(|n| n.name.clone()).collect();
    let current_edges: HashSet<(String, String)> = current_graph
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();

    let added_nodes: Vec<String> = current_nodes.difference(&last_nodes).cloned().collect();
    let removed_nodes: Vec<String> = last_nodes.difference(&current_nodes).cloned().collect();
    let added_edges: Vec<(String, String)> =
        current_edges.difference(&last_edges).cloned().collect();
    let removed_edges: Vec<(String, String)> =
        last_edges.difference(&current_edges).cloned().collect();

    let changes = GraphChanges {
        added_nodes,
        removed_nodes,
        added_edges,
        removed_edges,
    };

    if changes.has_changes() {
        Ok(Some(changes))
    } else {
        Ok(None)
    }
}

pub async fn save_execution_history(
    db: &DatabaseConnection,
    graph: &Graph,
    pipeline: &Pipeline,
) -> Result<()> {
    let graph_model = graphs::ActiveModel {
        id: NotSet,
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_graph = graph_model.insert(db).await?;

    for node in &graph.nodes {
        let node_model = nodes::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            name: Set(node.name.clone()),
        };
        node_model.insert(db).await?;
    }

    for edge in &graph.edges {
        let edge_model = edges::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            from_node: Set(edge.from.clone()),
            to_node: Set(edge.to.clone()),
        };
        edge_model.insert(db).await?;
    }

    let pipeline_model = pipelines::ActiveModel {
        id: NotSet,
        graph_id: Set(saved_graph.id),
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_pipeline = pipeline_model.insert(db).await?;

    for (order, action) in pipeline.actions.iter().enumerate() {
        let action_model = pipeline_actions::ActiveModel {
            id: NotSet,
            pipeline_id: Set(saved_pipeline.id),
            table_name: Set(action.table_name.clone()),
            execution_order: Set(order as i32),
        };
        action_model.insert(db).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::{DatabaseConfig, DatabaseType, ProjectConfig};
    use crate::database::connect_app_db;
    use crate::graph::Node;
    use crate::migration::Migrator;
    use crate::pipeline::Action;
    use sea_orm_migration::MigratorTrait;
    use tempfile;

    async fn setup_test_db() -> Result<DatabaseConnection> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: crate::config::project::StorageConfig {
                ty: crate::config::project::StorageType::Local,
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: db_path.to_string_lossy().to_string(),
            },
            deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        Migrator::up(&db, None).await?;

        std::mem::forget(temp_dir);
        Ok(db)
    }

    #[tokio::test]
    async fn test_detect_changes_first_run() -> Result<()> {
        let db = setup_test_db().await?;

        let graph = Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let changes = detect_changes(&db, &graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert_eq!(changes.added_nodes, vec!["users"]);
        assert!(changes.removed_nodes.is_empty());
        assert!(changes.added_edges.is_empty());
        assert!(changes.removed_edges.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_no_changes() -> Result<()> {
        let db = setup_test_db().await?;

        let graph = Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            actions: vec![Action {
                table_name: "users".to_string(),
            }],
        };

        save_execution_history(&db, &graph, &pipeline).await?;

        let changes = detect_changes(&db, &graph).await?;
        assert!(changes.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_with_changes() -> Result<()> {
        let db = setup_test_db().await?;

        let old_graph = Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            actions: vec![Action {
                table_name: "users".to_string(),
            }],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
            ],
            edges: vec![],
        };

        let changes = detect_changes(&db, &new_graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert_eq!(changes.added_nodes, vec!["orders"]);
        assert!(changes.removed_nodes.is_empty());
        assert!(changes.added_edges.is_empty());
        assert!(changes.removed_edges.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_removed_nodes() -> Result<()> {
        let db = setup_test_db().await?;

        let old_graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
            ],
            edges: vec![],
        };

        let pipeline = Pipeline {
            actions: vec![
                Action {
                    table_name: "users".to_string(),
                },
                Action {
                    table_name: "orders".to_string(),
                },
            ],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let changes = detect_changes(&db, &new_graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.is_empty());
        assert_eq!(changes.removed_nodes, vec!["orders"]);
        assert!(changes.added_edges.is_empty());
        assert!(changes.removed_edges.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_added_edges() -> Result<()> {
        use crate::graph::Edge;

        let db = setup_test_db().await?;

        let old_graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![],
        };

        let pipeline = Pipeline {
            actions: vec![
                Action {
                    table_name: "users".to_string(),
                },
                Action {
                    table_name: "user_stats".to_string(),
                },
            ],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = Graph {
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

        let changes = detect_changes(&db, &new_graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.is_empty());
        assert!(changes.removed_nodes.is_empty());
        assert_eq!(
            changes.added_edges,
            vec![("users".to_string(), "user_stats".to_string())]
        );
        assert!(changes.removed_edges.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_removed_edges() -> Result<()> {
        use crate::graph::Edge;

        let db = setup_test_db().await?;

        let old_graph = Graph {
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

        let pipeline = Pipeline {
            actions: vec![
                Action {
                    table_name: "users".to_string(),
                },
                Action {
                    table_name: "user_stats".to_string(),
                },
            ],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![],
        };

        let changes = detect_changes(&db, &new_graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.is_empty());
        assert!(changes.removed_nodes.is_empty());
        assert!(changes.added_edges.is_empty());
        assert_eq!(
            changes.removed_edges,
            vec![("users".to_string(), "user_stats".to_string())]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_multiple_changes() -> Result<()> {
        use crate::graph::Edge;

        let db = setup_test_db().await?;

        let old_graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "old_table".to_string(),
                },
            ],
            edges: vec![Edge {
                from: "users".to_string(),
                to: "old_table".to_string(),
            }],
        };

        let pipeline = Pipeline {
            actions: vec![
                Action {
                    table_name: "users".to_string(),
                },
                Action {
                    table_name: "old_table".to_string(),
                },
            ],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = Graph {
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
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "user_stats".to_string(),
                },
            ],
        };

        let changes = detect_changes(&db, &new_graph).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert_eq!(changes.added_nodes.len(), 2);
        assert!(changes.added_nodes.contains(&"orders".to_string()));
        assert!(changes.added_nodes.contains(&"user_stats".to_string()));
        assert_eq!(changes.removed_nodes, vec!["old_table"]);
        assert_eq!(changes.added_edges.len(), 2);
        assert!(
            changes
                .added_edges
                .contains(&("users".to_string(), "user_stats".to_string()))
        );
        assert!(
            changes
                .added_edges
                .contains(&("orders".to_string(), "user_stats".to_string()))
        );
        assert_eq!(
            changes.removed_edges,
            vec![("users".to_string(), "old_table".to_string())]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_changes_has_changes() -> Result<()> {
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

        Ok(())
    }

    #[tokio::test]
    async fn test_graph_changes_get_all_affected_nodes() -> Result<()> {
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

        Ok(())
    }
}
