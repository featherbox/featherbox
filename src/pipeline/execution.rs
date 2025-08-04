use crate::{
    config::Config,
    dependency::graph::{Edge, Graph, Node},
    pipeline::ducklake::DuckLake,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub table_name: String,
}

#[derive(Debug)]
pub struct Pipeline {
    pub actions: Vec<Action>,
}

impl Pipeline {
    pub fn from_graph(graph: &Graph) -> Self {
        let sorted_nodes = topological_sort(graph);
        let actions = sorted_nodes
            .into_iter()
            .map(|node_name| Action {
                table_name: node_name,
            })
            .collect();

        Pipeline { actions }
    }

    pub fn create_partial_pipeline(graph: &Graph, affected_nodes: &[String]) -> Self {
        let subgraph = create_subgraph(graph, affected_nodes);
        Self::from_graph(&subgraph)
    }

    pub async fn execute(&self, config: &Config, ducklake: &DuckLake) -> Result<()> {
        for action in &self.actions {
            println!("Executing action for table: {}", action.table_name);

            if let Some(adapter) = config.adapters.get(&action.table_name) {
                println!("  Loading adapter: {}", action.table_name);
                ducklake
                    .extract_and_load(adapter, &action.table_name)
                    .await?;
            } else if let Some(model) = config.models.get(&action.table_name) {
                println!("  Executing model: {}", action.table_name);
                if let Err(e) = ducklake.transform(model, &action.table_name).await {
                    eprintln!("    Transform failed: {e}");
                    return Err(e);
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
    }
}

fn topological_sort(graph: &Graph) -> Vec<String> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut adjacency_list: HashMap<String, Vec<String>> = HashMap::new();

    for node in &graph.nodes {
        in_degree.insert(node.name.clone(), 0);
        adjacency_list.insert(node.name.clone(), Vec::new());
    }

    for edge in &graph.edges {
        adjacency_list
            .get_mut(&edge.from)
            .unwrap()
            .push(edge.to.clone());
        *in_degree.get_mut(&edge.to).unwrap() += 1;
    }

    let mut queue: VecDeque<String> = VecDeque::new();
    for (node, &degree) in &in_degree {
        if degree == 0 {
            queue.push_back(node.clone());
        }
    }

    let mut sorted = Vec::new();

    while let Some(node) = queue.pop_front() {
        sorted.push(node.clone());

        if let Some(neighbors) = adjacency_list.get(&node) {
            for neighbor in neighbors {
                let degree = in_degree.get_mut(neighbor).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    sorted
}

fn create_subgraph(graph: &Graph, affected_nodes: &[String]) -> Graph {
    let affected_set: HashSet<String> = affected_nodes.iter().cloned().collect();

    let nodes: Vec<Node> = graph
        .nodes
        .iter()
        .filter(|node| affected_set.contains(&node.name))
        .map(|node| Node {
            name: node.name.clone(),
        })
        .collect();

    let edges: Vec<Edge> = graph
        .edges
        .iter()
        .filter(|edge| affected_set.contains(&edge.from) && affected_set.contains(&edge.to))
        .map(|edge| Edge {
            from: edge.from.clone(),
            to: edge.to.clone(),
        })
        .collect();

    Graph { nodes, edges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            adapter::{AdapterConfig, FileConfig, FormatConfig},
            model::ModelConfig,
        },
        dependency::graph::{Edge, Node},
        pipeline::ducklake::{CatalogConfig, DuckLake, StorageConfig},
    };
    use std::collections::HashMap;

    #[test]
    fn test_topological_sort_simple() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "b".to_string(),
                },
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
            ],
        };

        let sorted = topological_sort(&graph);
        assert_eq!(sorted, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_topological_sort_multiple_roots() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "c".to_string(),
                },
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "d".to_string(),
                },
                Node {
                    name: "b".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "c".to_string(),
                    to: "d".to_string(),
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
            ],
        };

        let sorted = topological_sort(&graph);
        assert!(
            sorted.iter().position(|x| x == "a").unwrap()
                < sorted.iter().position(|x| x == "c").unwrap()
        );
        assert!(
            sorted.iter().position(|x| x == "b").unwrap()
                < sorted.iter().position(|x| x == "c").unwrap()
        );
        assert!(
            sorted.iter().position(|x| x == "c").unwrap()
                < sorted.iter().position(|x| x == "d").unwrap()
        );
    }

    #[test]
    fn test_pipeline_from_graph() {
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

        let pipeline = Pipeline::from_graph(&graph);

        assert_eq!(pipeline.actions.len(), 3);

        let users_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "users")
            .unwrap();
        let orders_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "orders")
            .unwrap();
        let user_stats_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "user_stats")
            .unwrap();

        assert!(users_pos < user_stats_pos);
        assert!(orders_pos < user_stats_pos);
    }

    #[tokio::test]
    async fn test_pipeline_execute() {
        use std::fs;

        let test_dir = "/tmp/pipeline_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();
        fs::create_dir_all(format!("{test_dir}/data")).unwrap();

        let users_csv = format!("{test_dir}/data/users.csv");
        fs::write(&users_csv, "id,name\n1,Alice\n2,Bob").unwrap();

        let orders_csv = format!("{test_dir}/data/orders.csv");
        fs::write(&orders_csv, "id,user_id,amount\n1,1,100\n2,2,200").unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/storage"),
        };
        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut adapters = HashMap::new();
        adapters.insert(
            "users".to_string(),
            AdapterConfig {
                connection: "users".to_string(),
                description: None,
                file: FileConfig {
                    path: users_csv.clone(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
                columns: vec![],
            },
        );
        adapters.insert(
            "orders".to_string(),
            AdapterConfig {
                connection: "orders".to_string(),
                description: None,
                file: FileConfig {
                    path: orders_csv.clone(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
                columns: vec![],
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: None,
                max_age: None,
                sql: r#"
                    SELECT
                        u.id,
                        u.name,
                        COUNT(o.id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM
                        users u
                    LEFT JOIN
                        orders o ON u.id = o.user_id
                    GROUP BY
                        u.id, u.name
                "#
                .to_string(),
            },
        );

        let config = Config {
            project: crate::config::project::ProjectConfig {
                storage: crate::config::project::StorageConfig {
                    ty: crate::config::project::StorageType::Local,
                    path: format!("{test_dir}/storage"),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: format!("{test_dir}/test.sqlite"),
                },
                deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
                connections: HashMap::new(),
            },
            adapters,
            models,
        };

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

        let pipeline = Pipeline::from_graph(&graph);
        let result = pipeline.execute(&config, &ducklake).await;

        assert!(result.is_ok());

        let users_result = ducklake
            .query("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        assert_eq!(users_result.len(), 2);
        assert_eq!(users_result[0], vec!["1", "Alice"]);
        assert_eq!(users_result[1], vec!["2", "Bob"]);

        let orders_result = ducklake.query("SELECT * FROM orders ORDER BY id").unwrap();
        assert_eq!(orders_result.len(), 2);
        assert_eq!(orders_result[0], vec!["1", "1", "100"]);
        assert_eq!(orders_result[1], vec!["2", "2", "200"]);

        let user_stats_result = ducklake
            .query("SELECT * FROM user_stats ORDER BY id")
            .unwrap();
        assert_eq!(user_stats_result.len(), 2);
        assert_eq!(user_stats_result[0], vec!["1", "Alice", "1", "100"]);
        assert_eq!(user_stats_result[1], vec!["2", "Bob", "1", "200"]);
    }

    #[test]
    fn test_create_partial_pipeline() {
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
                    name: "order_summary".to_string(),
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
                Edge {
                    from: "orders".to_string(),
                    to: "order_summary".to_string(),
                },
            ],
        };

        let affected_nodes = vec!["orders".to_string(), "user_stats".to_string()];
        let partial_pipeline = Pipeline::create_partial_pipeline(&graph, &affected_nodes);

        assert_eq!(partial_pipeline.actions.len(), 2);

        let action_names: Vec<&str> = partial_pipeline
            .actions
            .iter()
            .map(|a| a.table_name.as_str())
            .collect();

        assert!(action_names.contains(&"orders"));
        assert!(action_names.contains(&"user_stats"));
        assert!(!action_names.contains(&"users"));
        assert!(!action_names.contains(&"order_summary"));

        let orders_pos = partial_pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "orders")
            .unwrap();
        let user_stats_pos = partial_pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "user_stats")
            .unwrap();

        assert!(orders_pos < user_stats_pos);
    }

    #[test]
    fn test_create_subgraph() {
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
                Node {
                    name: "d".to_string(),
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
                Edge {
                    from: "c".to_string(),
                    to: "d".to_string(),
                },
            ],
        };

        let affected_nodes = vec!["b".to_string(), "c".to_string()];
        let subgraph = create_subgraph(&graph, &affected_nodes);

        assert_eq!(subgraph.nodes.len(), 2);
        let node_names: Vec<&str> = subgraph.nodes.iter().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains(&"b"));
        assert!(node_names.contains(&"c"));

        assert_eq!(subgraph.edges.len(), 1);
        assert_eq!(subgraph.edges[0].from, "b");
        assert_eq!(subgraph.edges[0].to, "c");
    }
}
