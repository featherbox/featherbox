pub mod graph;

pub use graph::*;

use anyhow::Result;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, NotSet, QueryFilter,
    QueryOrder, QuerySelect, RelationTrait, Set,
};
use std::collections::HashSet;

use crate::database::entities::{edges, graphs, nodes, pipeline_actions, pipelines};
use crate::pipeline::build::{Pipeline, TimeRange};

pub async fn detect_changes(
    db: &DatabaseConnection,
    current_graph: &graph::Graph,
) -> Result<Option<graph::GraphChanges>> {
    let last_graph = graphs::Entity::find()
        .order_by_desc(graphs::Column::Id)
        .one(db)
        .await?;

    let Some(last_graph) = last_graph else {
        return Ok(Some(graph::GraphChanges {
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

    let changes = graph::GraphChanges {
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
    graph: &graph::Graph,
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

    let all_actions = pipeline.all_actions();
    for (order, action) in all_actions.iter().enumerate() {
        let action_model = pipeline_actions::ActiveModel {
            id: NotSet,
            pipeline_id: Set(saved_pipeline.id),
            table_name: Set(action.table_name.clone()),
            execution_order: Set(order as i32),
            since: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.since)
                .map(|dt| dt.naive_utc())),
            until: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.until)
                .map(|dt| dt.naive_utc())),
        };
        action_model.insert(db).await?;
    }

    Ok(())
}

pub async fn save_graph_if_changed(
    db: &DatabaseConnection,
    current_graph: &graph::Graph,
) -> Result<i32> {
    let graph_model = graphs::ActiveModel {
        id: NotSet,
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_graph = graph_model.insert(db).await?;

    for node in &current_graph.nodes {
        let node_model = nodes::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            name: Set(node.name.clone()),
        };
        node_model.insert(db).await?;
    }

    for edge in &current_graph.edges {
        let edge_model = edges::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            from_node: Set(edge.from.clone()),
            to_node: Set(edge.to.clone()),
        };
        edge_model.insert(db).await?;
    }

    Ok(saved_graph.id)
}

pub async fn latest_graph_id(db: &DatabaseConnection) -> Result<Option<i32>> {
    let latest_graph = graphs::Entity::find()
        .order_by_desc(graphs::Column::CreatedAt)
        .one(db)
        .await?;

    Ok(latest_graph.map(|graph| graph.id))
}

pub async fn save_pipeline_execution(
    db: &DatabaseConnection,
    graph_id: i32,
    pipeline: &Pipeline,
) -> Result<()> {
    let pipeline_model = pipelines::ActiveModel {
        id: NotSet,
        graph_id: Set(graph_id),
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_pipeline = pipeline_model.insert(db).await?;

    let all_actions = pipeline.all_actions();
    for (order, action) in all_actions.iter().enumerate() {
        let action_model = pipeline_actions::ActiveModel {
            id: NotSet,
            pipeline_id: Set(saved_pipeline.id),
            table_name: Set(action.table_name.clone()),
            execution_order: Set(order as i32),
            since: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.since)
                .map(|dt| dt.naive_utc())),
            until: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.until)
                .map(|dt| dt.naive_utc())),
        };
        action_model.insert(db).await?;
    }

    Ok(())
}

pub async fn get_executed_ranges_for_graph(
    db: &DatabaseConnection,
    graph_id: i32,
    table_name: &str,
) -> Result<Vec<TimeRange>> {
    use sea_orm::JoinType;

    let pipeline_actions = pipeline_actions::Entity::find()
        .filter(pipeline_actions::Column::TableName.eq(table_name))
        .join(
            JoinType::InnerJoin,
            pipeline_actions::Relation::Pipeline.def(),
        )
        .filter(pipelines::Column::GraphId.eq(graph_id))
        .all(db)
        .await?;

    let mut ranges = Vec::new();
    for action in pipeline_actions {
        let since = action
            .since
            .map(|dt| chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));
        let until = action
            .until
            .map(|dt| chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));
        ranges.push(TimeRange { since, until });
    }

    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::Node;
    use crate::pipeline::build::Action;
    use tempfile;

    async fn setup_test_db_connection() -> Result<sea_orm::DatabaseConnection> {
        use crate::config::project::{
            DatabaseConfig, DatabaseType, DeploymentsConfig, ProjectConfig, StorageConfig,
            StorageType,
        };
        use crate::database::connection::connect_app_db;

        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: StorageConfig {
                ty: StorageType::Local,
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some(db_path.to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            deployments: DeploymentsConfig { timeout: 600 },
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        std::mem::forget(temp_dir);
        Ok(db)
    }

    #[tokio::test]
    async fn test_detect_changes_first_run() -> Result<()> {
        let db = setup_test_db_connection().await?;

        let graph = graph::Graph {
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
        let db = setup_test_db_connection().await?;

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
                time_range: Some(TimeRange {
                    since: None,
                    until: None,
                }),
            }]],
        };

        save_execution_history(&db, &graph, &pipeline).await?;

        let changes = detect_changes(&db, &graph).await?;
        assert!(changes.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_with_changes() -> Result<()> {
        let db = setup_test_db_connection().await?;

        let old_graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
                time_range: Some(TimeRange {
                    since: None,
                    until: None,
                }),
            }]],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = graph::Graph {
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
        let db = setup_test_db_connection().await?;

        let old_graph = graph::Graph {
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
            levels: vec![vec![
                Action {
                    table_name: "users".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
                Action {
                    table_name: "orders".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
            ]],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = graph::Graph {
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
        use crate::dependency::graph::Edge;

        let db = setup_test_db_connection().await?;

        let old_graph = graph::Graph {
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
            levels: vec![vec![
                Action {
                    table_name: "users".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
                Action {
                    table_name: "user_stats".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
            ]],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = graph::Graph {
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
        use crate::dependency::graph::Edge;

        let db = setup_test_db_connection().await?;

        let old_graph = graph::Graph {
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
            levels: vec![vec![
                Action {
                    table_name: "users".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
                Action {
                    table_name: "user_stats".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
            ]],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = graph::Graph {
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
        use crate::dependency::graph::Edge;

        let db = setup_test_db_connection().await?;

        let old_graph = graph::Graph {
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
            levels: vec![vec![
                Action {
                    table_name: "users".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
                Action {
                    table_name: "old_table".to_string(),
                    time_range: Some(TimeRange {
                        since: None,
                        until: None,
                    }),
                },
            ]],
        };

        save_execution_history(&db, &old_graph, &pipeline).await?;

        let new_graph = graph::Graph {
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
}
