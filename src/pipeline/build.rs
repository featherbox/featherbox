use crate::{
    config::{Config, adapter::RangeConfig},
    dependency::{get_executed_ranges_for_graph, graph::Graph},
};
use anyhow::Result;
use sea_orm::DatabaseConnection;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub table_name: String,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeRange {
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    pub until: Option<chrono::DateTime<chrono::Utc>>,
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
                time_range: Some(TimeRange {
                    since: None,
                    until: None,
                }),
            })
            .collect();

        Pipeline { actions }
    }

    pub async fn from_graph_with_ranges(
        graph: &Graph,
        config: &Config,
        app_db: &DatabaseConnection,
        graph_id: i32,
    ) -> Result<Self> {
        let sorted_nodes = topological_sort(graph);
        let mut actions = Vec::new();

        for node_name in sorted_nodes {
            let time_range = if let Some(adapter) = config.adapters.get(&node_name) {
                if let Some(strategy) = &adapter.update_strategy {
                    let executed_ranges_raw =
                        get_executed_ranges_for_graph(app_db, graph_id, &node_name).await?;

                    let executed_ranges: Vec<ExecutedRange> = executed_ranges_raw
                        .into_iter()
                        .map(|range| ExecutedRange {
                            since: range.since.expect("Expected since to be present"),
                            until: range.until.expect("Expected until to be present"),
                        })
                        .collect();

                    let config_range = &strategy.range;

                    calculate_remaining_range(config_range, &executed_ranges)?
                } else {
                    None
                }
            } else {
                None
            };

            actions.push(Action {
                table_name: node_name,
                time_range,
            });
        }

        Ok(Pipeline { actions })
    }

}

pub fn topological_sort(graph: &Graph) -> Vec<String> {
    let mut in_degree = HashMap::new();
    let mut adjacency = HashMap::<String, Vec<String>>::new();

    for node in &graph.nodes {
        in_degree.insert(node.name.clone(), 0);
        adjacency.insert(node.name.clone(), Vec::new());
    }

    for edge in &graph.edges {
        *in_degree.get_mut(&edge.to).unwrap() += 1;
        adjacency.get_mut(&edge.from).unwrap().push(edge.to.clone());
    }

    let mut queue: VecDeque<String> = VecDeque::new();
    for (node, degree) in &in_degree {
        if *degree == 0 {
            queue.push_back(node.clone());
        }
    }

    let mut sorted_nodes = Vec::new();
    while let Some(current_node) = queue.pop_front() {
        sorted_nodes.push(current_node.clone());

        if let Some(neighbors) = adjacency.get(&current_node) {
            for neighbor in neighbors {
                let degree = in_degree.get_mut(neighbor).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    if sorted_nodes.len() != graph.nodes.len() {
        panic!(
            "Circular dependency detected in graph. Expected {} nodes, got {}",
            graph.nodes.len(),
            sorted_nodes.len()
        );
    }

    sorted_nodes
}

pub fn create_subgraph(graph: &Graph, affected_nodes: &[String]) -> Graph {
    let _affected_set: HashSet<String> = affected_nodes.iter().cloned().collect();

    let mut downstream_nodes = HashSet::new();
    let mut queue: VecDeque<String> = affected_nodes.iter().cloned().collect();

    while let Some(current_node) = queue.pop_front() {
        if downstream_nodes.contains(&current_node) {
            continue;
        }
        downstream_nodes.insert(current_node.clone());

        for edge in &graph.edges {
            if edge.from == current_node && !downstream_nodes.contains(&edge.to) {
                queue.push_back(edge.to.clone());
            }
        }
    }

    let filtered_nodes = graph
        .nodes
        .iter()
        .filter(|node| downstream_nodes.contains(&node.name))
        .cloned()
        .collect();

    let filtered_edges = graph
        .edges
        .iter()
        .filter(|edge| downstream_nodes.contains(&edge.from) && downstream_nodes.contains(&edge.to))
        .cloned()
        .collect();

    Graph {
        nodes: filtered_nodes,
        edges: filtered_edges,
    }
}

#[derive(Debug, Clone)]
pub struct ExecutedRange {
    pub since: chrono::DateTime<chrono::Utc>,
    pub until: chrono::DateTime<chrono::Utc>,
}

pub fn calculate_remaining_range(
    config_range: &RangeConfig,
    executed_ranges: &[ExecutedRange],
) -> Result<Option<TimeRange>> {
    let Some(config_since) = config_range.since else {
        return Ok(None);
    };
    let Some(config_until) = config_range.until else {
        return Ok(None);
    };

    let config_since_utc = config_since
        .and_utc()
        .checked_add_signed(chrono::Duration::zero())
        .ok_or_else(|| anyhow::anyhow!("Failed to convert config_since to UTC"))?;
    let config_until_utc = config_until
        .and_utc()
        .checked_add_signed(chrono::Duration::zero())
        .ok_or_else(|| anyhow::anyhow!("Failed to convert config_until to UTC"))?;

    if executed_ranges.is_empty() {
        return Ok(Some(TimeRange {
            since: Some(config_since_utc),
            until: Some(config_until_utc),
        }));
    }

    let mut sorted_ranges = executed_ranges.to_vec();
    sorted_ranges.sort_by(|a, b| a.since.cmp(&b.since));

    let mut merged_ranges = Vec::new();
    let mut current_start = sorted_ranges[0].since;
    let mut current_end = sorted_ranges[0].until;

    for range in sorted_ranges.iter().skip(1) {
        if range.since <= current_end {
            current_end = current_end.max(range.until);
        } else {
            merged_ranges.push(ExecutedRange {
                since: current_start,
                until: current_end,
            });
            current_start = range.since;
            current_end = range.until;
        }
    }
    merged_ranges.push(ExecutedRange {
        since: current_start,
        until: current_end,
    });

    let last_executed = merged_ranges.last().unwrap();

    if last_executed.until >= config_until_utc {
        return Ok(None);
    }

    let start_time = if last_executed.until >= config_since_utc {
        last_executed.until
    } else {
        config_since_utc
    };

    Ok(Some(TimeRange {
        since: Some(start_time),
        until: Some(config_until_utc),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::{Edge, Node};

    #[test]
    fn test_topological_sort_simple() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "B".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "C".to_string(),
                },
            ],
        };

        let result = topological_sort(&graph);
        assert_eq!(result, vec!["A", "B", "C"]);
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
                    name: "analytics".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "analytics".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "analytics".to_string(),
                },
            ],
        };

        let pipeline = Pipeline::from_graph(&graph);

        assert_eq!(pipeline.actions.len(), 3);

        let analytics_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "analytics")
            .unwrap();
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

        assert!(users_pos < analytics_pos);
        assert!(orders_pos < analytics_pos);
    }

    #[test]
    fn test_create_subgraph() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
                Node {
                    name: "D".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "B".to_string(),
                },
                Edge {
                    from: "A".to_string(),
                    to: "C".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "D".to_string(),
                },
            ],
        };

        let affected_nodes = vec!["A".to_string()];
        let subgraph = create_subgraph(&graph, &affected_nodes);

        assert_eq!(subgraph.nodes.len(), 4);

        let node_names: HashSet<String> = subgraph.nodes.iter().map(|n| n.name.clone()).collect();
        assert!(node_names.contains("A"));
        assert!(node_names.contains("B"));
        assert!(node_names.contains("C"));
        assert!(node_names.contains("D"));

        assert_eq!(subgraph.edges.len(), 3);
    }

    #[test]
    fn test_calculate_remaining_range_empty_executed() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };

        let executed_ranges = vec![];

        let result = calculate_remaining_range(&config_range, &executed_ranges).unwrap();

        assert!(result.is_some());
        let time_range = result.unwrap();
        assert!(time_range.since.is_some());
        assert!(time_range.until.is_some());
        assert_eq!(
            time_range.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(
            time_range.until.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }
}
