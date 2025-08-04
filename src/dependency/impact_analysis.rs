use std::collections::{HashMap, HashSet, VecDeque};

use crate::dependency::graph::Graph;
use crate::dependency::metadata::GraphChanges;

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

fn build_adjacency_map(graph: &Graph) -> HashMap<String, Vec<String>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::{Edge, Graph, Node};

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
}
