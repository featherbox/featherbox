use super::{migrate, run};
use crate::{
    config::Config,
    pipeline::state_manager::{StateManager, TaskStatusInfo},
};
use anyhow::Result;
use axum::{Router, extract::Path, http::StatusCode, response::Json, routing::get};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PipelineStatusResponse {
    pub pipeline: PipelineStatusInfo,
}

#[derive(Serialize, Deserialize)]
pub struct PipelineStatusInfo {
    pub id: i32,
    pub graph_id: i32,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub tasks: Vec<TaskStatusInfo>,
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
        .route("/pipeline/{id}/status", get(handle_get_status))
        .route("/graph", get(handle_get_graph))
        .nest("/pipeline", migrate::routes())
        .nest("/pipeline", run::routes())
}

async fn handle_get_status(
    Path(pipeline_id): Path<i32>,
) -> Result<Json<PipelineStatusResponse>, StatusCode> {
    let project_root =
        crate::workspace::find_project_root().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = Config::load_from_directory(&project_root)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let app_db = crate::database::connect_app_db(&config.project)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let state_manager = StateManager::new(app_db);

    match state_manager.get_pipeline_info(pipeline_id).await {
        Ok(pipeline_info) => {
            let pipeline = PipelineStatusInfo {
                id: pipeline_info.id,
                graph_id: pipeline_info.graph_id,
                status: pipeline_info.status.to_string(),
                created_at: pipeline_info
                    .created_at
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
                started_at: pipeline_info
                    .started_at
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
                completed_at: pipeline_info
                    .completed_at
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
                tasks: pipeline_info.tasks,
            };

            Ok(Json(PipelineStatusResponse { pipeline }))
        }
        Err(e) => {
            eprintln!("Failed to get pipeline status: {}", e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

async fn handle_get_graph() -> Result<Json<GraphResponse>, StatusCode> {
    let project_root =
        crate::workspace::find_project_root().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = Config::load_from_directory(&project_root)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let app_db = crate::database::connect_app_db(&config.project)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match get_current_graph(&app_db).await {
        Ok(graph_response) => Ok(Json(graph_response)),
        Err(e) => {
            eprintln!("Failed to get graph: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_current_graph(db: &DatabaseConnection) -> Result<GraphResponse> {
    use crate::database::entities::{edges, graphs, nodes};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

    let latest_graph = match graphs::Entity::find()
        .order_by_desc(graphs::Column::CreatedAt)
        .one(db)
        .await?
    {
        Some(graph) => graph,
        None => {
            return Ok(GraphResponse {
                nodes: vec![],
                edges: vec![],
            });
        }
    };

    let nodes_data = nodes::Entity::find()
        .filter(nodes::Column::GraphId.eq(latest_graph.id))
        .all(db)
        .await?;

    let edges_data = edges::Entity::find()
        .filter(edges::Column::GraphId.eq(latest_graph.id))
        .all(db)
        .await?;

    let graph_nodes = nodes_data
        .into_iter()
        .map(|node| GraphNode {
            name: node.name,
            status: None,
            last_updated_at: node
                .last_updated_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        })
        .collect();

    let graph_edges = edges_data
        .into_iter()
        .map(|edge| GraphEdge {
            from: edge.from_node,
            to: edge.to_node,
        })
        .collect();

    Ok(GraphResponse {
        nodes: graph_nodes,
        edges: graph_edges,
    })
}
