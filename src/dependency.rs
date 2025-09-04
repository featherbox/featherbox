pub mod graph;

pub use graph::*;

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::config::{AdapterConfig, Config, ModelConfig};
use crate::metadata::Metadata;

pub async fn detect_changes(
    project_dir: &Path,
    current_graph: &graph::Graph,
    config: &Config,
) -> Result<Option<graph::GraphChanges>> {
    let metadata = Metadata::load(project_dir).await?;

    if metadata.nodes.is_empty() {
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
    }

    let last_nodes_with_config: Vec<(String, Option<String>)> = metadata
        .nodes
        .keys()
        .map(|name| {
            let config_json = if let Some(adapter) = config.adapters.get(name) {
                serde_json::to_string(adapter).ok()
            } else if let Some(model) = config.models.get(name) {
                serde_json::to_string(model).ok()
            } else {
                None
            };
            (name.clone(), config_json)
        })
        .collect();

    let last_nodes: HashSet<String> = metadata.nodes.keys().cloned().collect();

    let mut last_edges: HashSet<(String, String)> = HashSet::new();
    for (from_node, node_data) in &metadata.nodes {
        for to_node in &node_data.referenced {
            last_edges.insert((from_node.clone(), to_node.clone()));
        }
    }

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
                    ) && last_model.has_changed(&current_model)
                    {
                        config_changed_nodes.push(node_name.clone());
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

pub async fn save_graph(project_dir: &Path, current_graph: &graph::Graph) -> Result<()> {
    let mut metadata = Metadata::load(project_dir).await.unwrap_or_default();

    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();
    for node in &current_graph.nodes {
        dependencies.entry(node.name.clone()).or_default();
    }
    for edge in &current_graph.edges {
        dependencies
            .entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }

    metadata.set_dependencies(dependencies);
    metadata.save(project_dir).await?;

    Ok(())
}

pub async fn update_node_timestamp(
    project_dir: &Path,
    table_name: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut metadata = Metadata::load(project_dir).await?;
    metadata.update_node_timestamp(table_name, timestamp);
    metadata.save(project_dir).await?;
    Ok(())
}

pub async fn get_oldest_dependency_timestamp(
    project_dir: &Path,
    table_name: &str,
    graph: &Graph,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let metadata = Metadata::load(project_dir).await?;

    let dependencies: Vec<String> = graph
        .edges
        .iter()
        .filter(|edge| edge.to == table_name)
        .map(|edge| edge.from.clone())
        .collect();

    let oldest = metadata.get_oldest_dependency_timestamp(table_name, &dependencies);
    Ok(oldest)
}

pub fn calculate_affected_nodes(
    graph: &graph::Graph,
    changes: &graph::GraphChanges,
) -> Vec<String> {
    let mut affected = HashSet::new();

    affected.extend(changes.added_nodes.iter().cloned());
    affected.extend(changes.config_changed_nodes.iter().cloned());

    for (_, to) in &changes.added_edges {
        affected.insert(to.clone());
    }
    for (_, to) in &changes.removed_edges {
        affected.insert(to.clone());
    }

    let mut queue: Vec<String> = affected.iter().cloned().collect();

    while let Some(node) = queue.pop() {
        for edge in &graph.edges {
            if edge.from == node && !affected.contains(&edge.to) {
                affected.insert(edge.to.clone());
                queue.push(edge.to.clone());
            }
        }
    }

    affected.into_iter().collect()
}

pub async fn save_graph_with_changes(
    project_dir: &std::path::Path,
    current_graph: &graph::Graph,
    _config: &Config,
    _changes: Option<&graph::GraphChanges>,
) -> Result<i32> {
    save_graph(project_dir, current_graph).await?;
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::{Edge, Node};
    use std::collections::HashMap;
    use tempfile;

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
            queries: HashMap::new(),
            dashboards: HashMap::new(),
            project_root: std::path::PathBuf::from("."),
        }
    }

    #[tokio::test]
    async fn test_detect_changes_first_run() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_dir = temp_dir.path();

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let config = create_test_config();
        let changes = detect_changes(project_dir, &graph, &config).await?;
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
        let temp_dir = tempfile::tempdir()?;
        let project_dir = temp_dir.path();

        let graph = graph::Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let config = create_test_config();
        save_graph(project_dir, &graph).await?;

        let changes = detect_changes(project_dir, &graph, &config).await?;
        assert!(changes.is_none());

        Ok(())
    }
}
