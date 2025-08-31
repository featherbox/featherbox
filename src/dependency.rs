pub mod graph;

pub use graph::*;

use anyhow::Result;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, NotSet, QueryFilter,
    QueryOrder, Set,
};
use std::collections::HashSet;

use crate::config::{AdapterConfig, Config, ModelConfig};
use crate::database::entities::{edges, graphs, nodes, pipeline_actions, pipelines};
use crate::pipeline::build::Pipeline;

pub async fn detect_changes(
    db: &DatabaseConnection,
    current_graph: &graph::Graph,
    config: &Config,
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
            config_changed_nodes: vec![],
        }));
    };

    let last_nodes_with_config: Vec<(String, Option<String>)> = nodes::Entity::find()
        .filter(nodes::Column::GraphId.eq(last_graph.id))
        .all(db)
        .await?
        .into_iter()
        .map(|n| (n.name, n.config_json))
        .collect();

    let last_nodes: HashSet<String> = last_nodes_with_config
        .iter()
        .map(|(name, _)| name.clone())
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

    let mut config_changed_nodes = Vec::new();

    for (node_name, last_config_json) in last_nodes_with_config {
        if current_nodes.contains(&node_name) {
            let current_config_json = if let Some(adapter_config) = config.adapters.get(&node_name)
            {
                Some(serde_json::to_string(adapter_config)?)
            } else if let Some(model_config) = config.models.get(&node_name) {
                Some(serde_json::to_string(model_config)?)
            } else {
                None
            };

            match (last_config_json.as_ref(), current_config_json.as_ref()) {
                (Some(last_json), Some(current_json)) => {
                    if let (Ok(last_adapter), Ok(current_adapter)) = (
                        serde_json::from_str::<AdapterConfig>(last_json),
                        serde_json::from_str::<AdapterConfig>(current_json),
                    ) {
                        if last_adapter.has_changed(&current_adapter) {
                            config_changed_nodes.push(node_name.clone());
                        }
                    } else if let (Ok(last_model), Ok(current_model)) = (
                        serde_json::from_str::<ModelConfig>(last_json),
                        serde_json::from_str::<ModelConfig>(current_json),
                    ) {
                        if last_model.has_changed(&current_model) {
                            config_changed_nodes.push(node_name.clone());
                        }
                    }
                }
                (None, Some(_)) | (Some(_), None) => {
                    config_changed_nodes.push(node_name.clone());
                }
                (None, None) => {}
            }
        }
    }

    let changes = graph::GraphChanges {
        added_nodes,
        removed_nodes,
        added_edges,
        removed_edges,
        config_changed_nodes,
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
    config: &Config,
) -> Result<()> {
    let graph_model = graphs::ActiveModel {
        id: NotSet,
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_graph = graph_model.insert(db).await?;

    for node in &graph.nodes {
        let config_json = if let Some(adapter_config) = config.adapters.get(&node.name) {
            Some(serde_json::to_string(adapter_config)?)
        } else if let Some(model_config) = config.models.get(&node.name) {
            Some(serde_json::to_string(model_config)?)
        } else {
            None
        };

        let node_model = nodes::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            name: Set(node.name.clone()),
            config_json: Set(config_json),
            last_updated_at: Set(None),
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
        };
        action_model.insert(db).await?;
    }

    Ok(())
}

pub async fn save_graph_if_changed(
    db: &DatabaseConnection,
    current_graph: &graph::Graph,
    config: &Config,
) -> Result<i32> {
    save_graph_with_changes(db, current_graph, config, None).await
}

pub async fn save_graph_with_changes(
    db: &DatabaseConnection,
    current_graph: &graph::Graph,
    config: &Config,
    changes: Option<&graph::GraphChanges>,
) -> Result<i32> {
    let previous_timestamps = get_previous_node_timestamps(db).await?;

    let graph_model = graphs::ActiveModel {
        id: NotSet,
        created_at: Set(chrono::Utc::now().naive_utc()),
    };
    let saved_graph = graph_model.insert(db).await?;

    for node in &current_graph.nodes {
        let config_json = if let Some(adapter_config) = config.adapters.get(&node.name) {
            Some(serde_json::to_string(adapter_config)?)
        } else if let Some(model_config) = config.models.get(&node.name) {
            Some(serde_json::to_string(model_config)?)
        } else {
            None
        };

        let last_updated_at = determine_node_timestamp(&node.name, changes, &previous_timestamps);

        let node_model = nodes::ActiveModel {
            id: NotSet,
            graph_id: Set(saved_graph.id),
            name: Set(node.name.clone()),
            config_json: Set(config_json),
            last_updated_at: Set(last_updated_at),
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

async fn get_previous_node_timestamps(
    db: &DatabaseConnection,
) -> Result<std::collections::HashMap<String, Option<chrono::NaiveDateTime>>> {
    let last_graph = graphs::Entity::find()
        .order_by_desc(graphs::Column::CreatedAt)
        .one(db)
        .await?;

    let mut timestamps = std::collections::HashMap::new();

    if let Some(last_graph) = last_graph {
        let previous_nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(last_graph.id))
            .all(db)
            .await?;

        for node in previous_nodes {
            timestamps.insert(node.name, node.last_updated_at);
        }
    }

    Ok(timestamps)
}

fn determine_node_timestamp(
    node_name: &str,
    changes: Option<&graph::GraphChanges>,
    previous_timestamps: &std::collections::HashMap<String, Option<chrono::NaiveDateTime>>,
) -> Option<chrono::NaiveDateTime> {
    if let Some(changes) = changes {
        if changes.added_nodes.contains(&node_name.to_string()) {
            return None;
        }

        if changes
            .config_changed_nodes
            .contains(&node_name.to_string())
        {
            return None;
        }
    }

    previous_timestamps
        .get(node_name)
        .and_then(|timestamp| *timestamp)
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
        };
        action_model.insert(db).await?;
    }

    Ok(())
}

pub async fn update_node_timestamp(
    db: &DatabaseConnection,
    node_name: &str,
    timestamp: chrono::NaiveDateTime,
) -> Result<()> {
    let latest_graph_id = latest_graph_id(db).await?;

    if let Some(graph_id) = latest_graph_id {
        let node = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(graph_id))
            .filter(nodes::Column::Name.eq(node_name))
            .one(db)
            .await?;

        if let Some(node) = node {
            let mut active_node: nodes::ActiveModel = node.into();
            active_node.last_updated_at = Set(Some(timestamp));
            active_node.update(db).await?;
        }
    }

    Ok(())
}

pub async fn get_oldest_dependency_timestamp(
    db: &DatabaseConnection,
    node_name: &str,
    graph: &Graph,
) -> Result<Option<chrono::NaiveDateTime>> {
    let latest_graph_id = latest_graph_id(db).await?;

    if let Some(graph_id) = latest_graph_id {
        let mut dependency_timestamps = Vec::new();

        for edge in &graph.edges {
            if edge.to == node_name {
                let dependency_node = nodes::Entity::find()
                    .filter(nodes::Column::GraphId.eq(graph_id))
                    .filter(nodes::Column::Name.eq(&edge.from))
                    .one(db)
                    .await?;

                if let Some(node) = dependency_node {
                    if let Some(timestamp) = node.last_updated_at {
                        dependency_timestamps.push(timestamp);
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        if dependency_timestamps.is_empty() {
            return Ok(None);
        }

        let oldest_timestamp = dependency_timestamps.into_iter().min();
        return Ok(oldest_timestamp);
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::Node;
    use crate::pipeline::build::Action;
    use std::collections::HashMap;

    fn create_test_config() -> Config {
        Config {
            project: crate::config::project::ProjectConfig {
                storage: crate::config::project::StorageConfig::LocalFile {
                    path: "./storage".to_string(),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: Some("./test.db".to_string()),
                    host: None,
                    port: None,
                    database: None,
                    password: None,
                    username: None,
                },
                connections: HashMap::new(),
            },
            adapters: HashMap::new(),
            models: HashMap::new(),
            project_root: std::path::PathBuf::from("."),
        }
    }

    async fn setup_test_db() -> Result<sea_orm::DatabaseConnection> {
        use crate::config::project::{DatabaseConfig, DatabaseType, ProjectConfig, StorageConfig};
        use crate::database::connection::connect_app_db;
        use tempfile;

        let temp_dir = tempfile::tempdir()?;
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
            connections: std::collections::HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;
        std::mem::forget(temp_dir);
        Ok(db)
    }

    #[tokio::test]
    async fn test_detect_changes_first_run() -> Result<()> {
        let db = setup_test_db().await?;

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let config = create_test_config();
        let changes = detect_changes(&db, &graph, &config).await?;
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

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
            }]],
        };

        let config = create_test_config();
        save_execution_history(&db, &graph, &pipeline, &config).await?;

        let changes = detect_changes(&db, &graph, &config).await?;
        assert!(changes.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_changes_with_changes() -> Result<()> {
        let db = setup_test_db().await?;

        let old_graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
            }]],
        };

        let config = create_test_config();
        save_execution_history(&db, &old_graph, &pipeline, &config).await?;

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

        let changes = detect_changes(&db, &new_graph, &config).await?;
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
                },
                Action {
                    table_name: "orders".to_string(),
                },
            ]],
        };

        let config = create_test_config();
        save_execution_history(&db, &old_graph, &pipeline, &config).await?;

        let new_graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let changes = detect_changes(&db, &new_graph, &config).await?;
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

        let db = setup_test_db().await?;

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
                },
                Action {
                    table_name: "user_stats".to_string(),
                },
            ]],
        };

        let config = create_test_config();
        save_execution_history(&db, &old_graph, &pipeline, &config).await?;

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

        let changes = detect_changes(&db, &new_graph, &config).await?;
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

        let db = setup_test_db().await?;

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
                },
                Action {
                    table_name: "user_stats".to_string(),
                },
            ]],
        };

        let config = create_test_config();
        save_execution_history(&db, &old_graph, &pipeline, &config).await?;

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

        let changes = detect_changes(&db, &new_graph, &config).await?;
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

        let db = setup_test_db().await?;

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
                },
                Action {
                    table_name: "old_table".to_string(),
                },
            ]],
        };

        let config = create_test_config();
        save_execution_history(&db, &old_graph, &pipeline, &config).await?;

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

        let changes = detect_changes(&db, &new_graph, &config).await?;
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
    async fn test_detect_config_changes_adapter() -> Result<()> {
        use crate::config::adapter::{
            AdapterConfig, AdapterSource, ColumnConfig, FileConfig, FormatConfig,
        };

        let db = setup_test_db().await?;

        let old_graph = graph::Graph {
            nodes: vec![Node {
                name: "users_adapter".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users_adapter".to_string(),
            }]],
        };

        let mut old_config = create_test_config();
        old_config.adapters.insert(
            "users_adapter".to_string(),
            AdapterConfig {
                connection: "test_db".to_string(),
                description: Some("Test adapter".to_string()),
                source: AdapterSource::File {
                    file: FileConfig {
                        path: "/data/users.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: FormatConfig {
                        ty: "csv".to_string(),
                        delimiter: None,
                        null_value: None,
                        has_header: Some(true),
                    },
                },
                columns: vec![ColumnConfig {
                    name: "id".to_string(),
                    ty: "INTEGER".to_string(),
                    description: None,
                }],
            },
        );

        save_execution_history(&db, &old_graph, &pipeline, &old_config).await?;

        let mut new_config = old_config.clone();
        if let Some(adapter_config) = new_config.adapters.get_mut("users_adapter") {
            adapter_config.connection = "new_db".to_string();
        }

        let changes = detect_changes(&db, &old_graph, &new_config).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.is_empty());
        assert!(changes.removed_nodes.is_empty());
        assert!(changes.added_edges.is_empty());
        assert!(changes.removed_edges.is_empty());
        assert_eq!(changes.config_changed_nodes, vec!["users_adapter"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_detect_config_changes_model() -> Result<()> {
        use crate::config::ModelConfig;

        let db = setup_test_db().await?;

        let old_graph = graph::Graph {
            nodes: vec![Node {
                name: "user_stats".to_string(),
            }],
            edges: vec![],
        };

        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "user_stats".to_string(),
            }]],
        };

        let mut old_config = create_test_config();
        old_config.models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: Some("User statistics".to_string()),
                sql: "SELECT COUNT(*) FROM users".to_string(),
            },
        );

        save_execution_history(&db, &old_graph, &pipeline, &old_config).await?;

        let mut new_config = old_config.clone();
        if let Some(model_config) = new_config.models.get_mut("user_stats") {
            model_config.sql = "SELECT COUNT(*), AVG(age) FROM users".to_string();
        }

        let changes = detect_changes(&db, &old_graph, &new_config).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.is_empty());
        assert!(changes.removed_nodes.is_empty());
        assert!(changes.added_edges.is_empty());
        assert!(changes.removed_edges.is_empty());
        assert_eq!(changes.config_changed_nodes, vec!["user_stats"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_config_changes_ignore_description() -> Result<()> {
        use crate::config::adapter::{AdapterConfig, AdapterSource};
        use crate::config::model::ModelConfig;

        let db = setup_test_db().await?;

        let old_graph = graph::Graph {
            nodes: vec![
                Node {
                    name: "users_adapter".to_string(),
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
                    table_name: "users_adapter".to_string(),
                },
                Action {
                    table_name: "user_stats".to_string(),
                },
            ]],
        };

        let mut old_config = create_test_config();
        old_config.adapters.insert(
            "users_adapter".to_string(),
            AdapterConfig {
                connection: "test_db".to_string(),
                description: Some("Original description".to_string()),
                source: AdapterSource::Database {
                    table_name: "users".to_string(),
                },
                columns: vec![],
            },
        );
        old_config.models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: Some("Original model description".to_string()),
                sql: "SELECT COUNT(*) FROM users".to_string(),
            },
        );

        save_execution_history(&db, &old_graph, &pipeline, &old_config).await?;

        let mut new_config = old_config.clone();
        if let Some(adapter_config) = new_config.adapters.get_mut("users_adapter") {
            adapter_config.description = Some("Modified description".to_string());
        }
        if let Some(model_config) = new_config.models.get_mut("user_stats") {
            model_config.description = Some("Modified model description".to_string());
        }

        let changes = detect_changes(&db, &old_graph, &new_config).await?;
        assert!(changes.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_calculate_affected_nodes_with_config_changes() -> Result<()> {
        use crate::dependency::graph::Edge;

        let graph = graph::Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
                Node {
                    name: "reports".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "user_stats".to_string(),
                    to: "reports".to_string(),
                },
            ],
        };

        let changes = graph::GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec!["users".to_string()],
        };

        let affected = calculate_affected_nodes(&graph, &changes);

        assert_eq!(affected.len(), 3);
        assert!(affected.contains(&"users".to_string()));
        assert!(affected.contains(&"user_stats".to_string()));
        assert!(affected.contains(&"reports".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_management_new_nodes() -> Result<()> {
        let db = setup_test_db().await?;

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let changes = graph::GraphChanges {
            added_nodes: vec!["users".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let config = create_test_config();
        let graph_id = save_graph_with_changes(&db, &graph, &config, Some(&changes)).await?;

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(graph_id))
            .all(&db)
            .await?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].name, "users");
        assert!(nodes[0].last_updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_management_config_changed_nodes() -> Result<()> {
        let db = setup_test_db().await?;

        let initial_graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let config = create_test_config();
        let initial_pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
            }]],
        };

        save_execution_history(&db, &initial_graph, &initial_pipeline, &config).await?;

        let test_time =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        update_node_timestamp(&db, "users", test_time).await?;

        let changes = graph::GraphChanges {
            added_nodes: vec![],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec!["users".to_string()],
        };

        let new_graph_id =
            save_graph_with_changes(&db, &initial_graph, &config, Some(&changes)).await?;

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(new_graph_id))
            .all(&db)
            .await?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].name, "users");
        assert!(nodes[0].last_updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_management_preserved_nodes() -> Result<()> {
        let db = setup_test_db().await?;

        let initial_graph = graph::Graph {
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

        let config = create_test_config();
        let initial_pipeline = Pipeline {
            levels: vec![vec![
                Action {
                    table_name: "users".to_string(),
                },
                Action {
                    table_name: "orders".to_string(),
                },
            ]],
        };

        save_execution_history(&db, &initial_graph, &initial_pipeline, &config).await?;

        let test_time =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        update_node_timestamp(&db, "users", test_time).await?;

        let changes = graph::GraphChanges {
            added_nodes: vec!["new_table".to_string()],
            removed_nodes: vec![],
            added_edges: vec![],
            removed_edges: vec![],
            config_changed_nodes: vec![],
        };

        let new_graph = graph::Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
                Node {
                    name: "new_table".to_string(),
                },
            ],
            edges: vec![],
        };

        let new_graph_id =
            save_graph_with_changes(&db, &new_graph, &config, Some(&changes)).await?;

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(new_graph_id))
            .all(&db)
            .await?;

        assert_eq!(nodes.len(), 3);

        let users_node = nodes.iter().find(|n| n.name == "users").unwrap();
        assert_eq!(users_node.last_updated_at, Some(test_time));

        let orders_node = nodes.iter().find(|n| n.name == "orders").unwrap();
        assert!(orders_node.last_updated_at.is_none());

        let new_table_node = nodes.iter().find(|n| n.name == "new_table").unwrap();
        assert!(new_table_node.last_updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_node_timestamp() -> Result<()> {
        let db = setup_test_db().await?;

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let config = create_test_config();
        let pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
            }]],
        };

        save_execution_history(&db, &graph, &pipeline, &config).await?;

        let test_time =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 12:30:45", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        update_node_timestamp(&db, "users", test_time).await?;

        let latest_graph_id = latest_graph_id(&db).await?.unwrap();
        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(latest_graph_id))
            .filter(nodes::Column::Name.eq("users"))
            .one(&db)
            .await?;

        let node = nodes.unwrap();
        assert_eq!(node.last_updated_at, Some(test_time));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_oldest_dependency_timestamp() -> Result<()> {
        use crate::dependency::graph::Edge;

        let db = setup_test_db().await?;

        let graph = graph::Graph {
            nodes: vec![
                Node {
                    name: "adapter_a".to_string(),
                },
                Node {
                    name: "adapter_b".to_string(),
                },
                Node {
                    name: "model_c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "adapter_a".to_string(),
                    to: "model_c".to_string(),
                },
                Edge {
                    from: "adapter_b".to_string(),
                    to: "model_c".to_string(),
                },
            ],
        };

        let config = create_test_config();
        let pipeline = Pipeline {
            levels: vec![vec![
                Action {
                    table_name: "adapter_a".to_string(),
                },
                Action {
                    table_name: "adapter_b".to_string(),
                },
                Action {
                    table_name: "model_c".to_string(),
                },
            ]],
        };

        save_execution_history(&db, &graph, &pipeline, &config).await?;

        let time_a =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        let time_b =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 11:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();

        update_node_timestamp(&db, "adapter_a", time_a).await?;
        update_node_timestamp(&db, "adapter_b", time_b).await?;

        let oldest_timestamp = get_oldest_dependency_timestamp(&db, "model_c", &graph).await?;

        assert_eq!(oldest_timestamp, Some(time_a));

        Ok(())
    }
}
