use crate::{
    config::Config,
    dependency::Graph,
    pipeline::{build::Pipeline, ducklake::DuckLake},
    workspace::find_project_root,
};
use anyhow::Result;
use axum::{Router, extract::Path as AxumPath, http::StatusCode, response::Json, routing::post};
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Serialize, Deserialize)]
pub struct RunRequest {
    pub project_path: String,
}

#[derive(Serialize, Deserialize)]
pub struct RunResponse {
    pub success: bool,
    pub message: String,
    pub pipeline_id: Option<i32>,
}

pub fn execute_with_path(_project_path: &str) -> Result<i32> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { execute_run_internal(None).await })
}

pub fn execute_with_target_node(_project_path: &str, target_node: &str) -> Result<i32> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { execute_run_internal(Some(target_node.to_string())).await })
}

async fn execute_run_internal(target_node: Option<String>) -> Result<i32> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        return Err(anyhow::anyhow!("No adapters or models found"));
    }

    let current_graph = Graph::from_config(&config)?;
    let ducklake = DuckLake::from_config(&config).await?;

    let pipeline = if let Some(target) = target_node {
        let execution_graph = create_execution_subgraph(&current_graph, &target)?;
        Pipeline::from_graph(&execution_graph)
    } else {
        Pipeline::from_graph(&current_graph)
    };

    crate::dependency::save_graph(&project_root, &current_graph).await?;

    pipeline
        .execute(&current_graph, &config, &ducklake, &project_root)
        .await?;

    Ok(1)
}

fn create_execution_subgraph(graph: &Graph, target_node: &str) -> Result<Graph> {
    use std::collections::{HashSet, VecDeque};

    if !graph.nodes.iter().any(|n| n.name == target_node) {
        return Err(anyhow::anyhow!(
            "Target node '{}' not found in graph",
            target_node
        ));
    }

    let mut included_nodes = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(target_node);

    while let Some(current_node) = queue.pop_front() {
        if included_nodes.insert(current_node.to_string()) {
            for edge in &graph.edges {
                if edge.to == current_node && !included_nodes.contains(&edge.from) {
                    queue.push_back(&edge.from);
                }
            }
        }
    }

    let filtered_nodes = graph
        .nodes
        .iter()
        .filter(|node| included_nodes.contains(&node.name))
        .cloned()
        .collect();

    let filtered_edges = graph
        .edges
        .iter()
        .filter(|edge| included_nodes.contains(&edge.from) && included_nodes.contains(&edge.to))
        .cloned()
        .collect();

    Ok(Graph {
        nodes: filtered_nodes,
        edges: filtered_edges,
    })
}

async fn handle_run() -> Result<Json<RunResponse>, StatusCode> {
    match execute_run_internal(None).await {
        Ok(pipeline_id) => Ok(Json(RunResponse {
            success: true,
            message: "Pipeline execution completed successfully".to_string(),
            pipeline_id: Some(pipeline_id),
        })),
        Err(e) => {
            error!("Pipeline execution failed: {}", e);
            Ok(Json(RunResponse {
                success: false,
                message: format!("Pipeline execution failed: {}", e),
                pipeline_id: None,
            }))
        }
    }
}

async fn handle_run_target(
    AxumPath(target_node): AxumPath<String>,
) -> Result<Json<RunResponse>, StatusCode> {
    match execute_run_internal(Some(target_node.clone())).await {
        Ok(pipeline_id) => Ok(Json(RunResponse {
            success: true,
            message: format!(
                "Pipeline execution for target '{}' completed successfully",
                target_node
            ),
            pipeline_id: Some(pipeline_id),
        })),
        Err(e) => {
            error!(
                "Pipeline execution for target '{}' failed: {}",
                target_node, e
            );
            Ok(Json(RunResponse {
                success: false,
                message: format!(
                    "Pipeline execution for target '{}' failed: {}",
                    target_node, e
                ),
                pipeline_id: None,
            }))
        }
    }
}

pub fn routes() -> Router {
    Router::new()
        .route("/run", post(handle_run))
        .route("/run/{target_node}", post(handle_run_target))
}
