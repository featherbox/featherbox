use super::{migrate, run};
use crate::{
    config::Config,
    dependency::Graph,
    metadata::Metadata,
    status::{PipelineStatusInfo, Status},
};
use anyhow::Result;
use axum::{Router, http::StatusCode, response::Json, routing::get};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PipelineStatusResponse {
    pub pipeline: PipelineStatusInfo,
}

#[derive(Serialize, Deserialize)]
pub struct GraphResponse {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Serialize, Deserialize)]
pub struct GraphNode {
    pub name: String,
    pub status: Option<String>,
    pub last_updated_at: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
}

pub fn routes() -> Router {
    Router::new()
        .route("/pipeline/status", get(handle_get_latest_status))
        .route("/graph", get(handle_get_graph))
        .nest("/pipeline", migrate::routes())
        .nest("/pipeline", run::routes())
}

async fn handle_get_latest_status() -> Result<Json<PipelineStatusResponse>, StatusCode> {
    let project_root =
        crate::workspace::find_project_root().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match Status::get_latest(&project_root).await {
        Ok(Some((_, status))) => {
            let pipeline_info = status.to_pipeline_info();
            Ok(Json(PipelineStatusResponse {
                pipeline: pipeline_info,
            }))
        }
        Ok(None) => {
            let empty_pipeline = PipelineStatusInfo {
                status: "idle".to_string(),
                started_at: None,
                completed_at: None,
                tasks: vec![],
            };
            Ok(Json(PipelineStatusResponse {
                pipeline: empty_pipeline,
            }))
        }
        Err(e) => {
            eprintln!("Failed to get pipeline status: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_get_graph() -> Result<Json<GraphResponse>, StatusCode> {
    let project_root =
        crate::workspace::find_project_root().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = Config::load_from_directory(&project_root)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let graph = Graph::from_config(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let metadata = Metadata::load(&project_root).await.unwrap_or_default();

    let status = Status::get_latest(&project_root)
        .await
        .unwrap_or(None)
        .map(|(_, s)| s);

    let nodes: Vec<GraphNode> = graph
        .nodes
        .iter()
        .map(|node| {
            let status_str = if let Some(ref status) = status {
                status.states.get(&node.name).map(|s| {
                    match s.phase {
                        crate::status::Phase::Running => "running",
                        crate::status::Phase::Completed => "completed",
                        crate::status::Phase::Failed => "failed",
                    }
                    .to_string()
                })
            } else {
                None
            };

            let last_updated = metadata
                .get_node(&node.name)
                .and_then(|n| n.last_updated_at)
                .map(|dt| dt.to_rfc3339());

            GraphNode {
                name: node.name.clone(),
                status: status_str,
                last_updated_at: last_updated,
            }
        })
        .collect();

    let edges: Vec<GraphEdge> = graph
        .edges
        .iter()
        .map(|edge| GraphEdge {
            from: edge.from.clone(),
            to: edge.to.clone(),
        })
        .collect();

    Ok(Json(GraphResponse { nodes, edges }))
}
