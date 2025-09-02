use anyhow::Result;
use axum::{
    Router,
    extract::Path,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::task;

use crate::{
    commands::{migrate, run},
    config::Config,
    pipeline::state_manager::{StateManager, TaskStatusInfo},
};
use sea_orm::DatabaseConnection;

#[derive(Serialize, Deserialize)]
pub struct MigrateResponse {
    pub success: bool,
    pub message: String,
    pub graph_id: Option<i32>,
}

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
        .route("/pipeline/migrate", post(handle_migrate))
        .route("/pipeline/run", post(handle_run))
        .route("/pipeline/run-nodes/{node_name}", post(handle_run_node))
        .route("/pipeline/{id}/status", get(handle_get_status))
        .route("/graph", get(handle_get_graph))
}

async fn handle_migrate() -> Result<Json<MigrateResponse>, StatusCode> {
    match task::spawn_blocking(migrate::execute).await {
        Ok(result) => match result {
            Ok(graph_id) => Ok(Json(MigrateResponse {
                success: true,
                message: if graph_id.is_some() {
                    "Migration completed successfully".to_string()
                } else {
                    "No changes detected".to_string()
                },
                graph_id,
            })),
            Err(e) => {
                eprintln!("Migration failed: {}", e);
                Ok(Json(MigrateResponse {
                    success: false,
                    message: format!("Migration failed: {}", e),
                    graph_id: None,
                }))
            }
        },
        Err(e) => {
            eprintln!("Migration task failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_run(Json(request): Json<RunRequest>) -> Result<Json<RunResponse>, StatusCode> {
    let project_path = request.project_path;

    match task::spawn_blocking(move || run::execute_with_path(&project_path)).await {
        Ok(result) => match result {
            Ok(pipeline_id) => Ok(Json(RunResponse {
                success: true,
                message: "Pipeline execution started successfully".to_string(),
                pipeline_id: Some(pipeline_id),
            })),
            Err(e) => {
                eprintln!("Pipeline execution failed: {}", e);
                Ok(Json(RunResponse {
                    success: false,
                    message: format!("Pipeline execution failed: {}", e),
                    pipeline_id: None,
                }))
            }
        },
        Err(e) => {
            eprintln!("Pipeline execution task failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_run_node(
    Path(node_name): Path<String>,
    Json(request): Json<RunRequest>,
) -> Result<Json<RunResponse>, StatusCode> {
    let project_path = request.project_path;
    let node_name_clone = node_name.clone();

    match task::spawn_blocking(move || {
        run::execute_with_target_node(&project_path, &node_name_clone)
    })
    .await
    {
        Ok(result) => match result {
            Ok(pipeline_id) => Ok(Json(RunResponse {
                success: true,
                message: format!(
                    "Node '{}' and its dependencies execution started successfully",
                    node_name
                ),
                pipeline_id: Some(pipeline_id),
            })),
            Err(e) => {
                eprintln!("Node execution failed: {}", e);
                Ok(Json(RunResponse {
                    success: false,
                    message: format!("Node execution failed: {}", e),
                    pipeline_id: None,
                }))
            }
        },
        Err(e) => {
            eprintln!("Node execution task failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_get_status(
    Path(pipeline_id): Path<i32>,
) -> Result<Json<PipelineStatusResponse>, StatusCode> {
    let current_dir = std::env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config =
        Config::load_from_directory(&current_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
    let current_dir = std::env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config =
        Config::load_from_directory(&current_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
