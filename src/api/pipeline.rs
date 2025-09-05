use std::collections::HashMap;

use super::{migrate, run};
use crate::{
    api::AppError,
    metadata::{Metadata, Node},
    status::{PipelineStatus, StatusManager},
};
use anyhow::Result;
use axum::{Router, response::Json, routing::get};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct PipelineStatusResponse {
    pub pipeline: Option<PipelineStatus>,
}

#[derive(Serialize, Deserialize)]
pub struct GraphResponse {
    pub nodes: HashMap<String, Node>,
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

async fn handle_get_latest_status() -> Result<Json<PipelineStatusResponse>, AppError> {
    let project_root = crate::workspace::find_project_root()?;

    let status = StatusManager::find_latest_status(&project_root).await?;

    Ok(Json(PipelineStatusResponse { pipeline: status }))
}

async fn handle_get_graph() -> Result<Json<GraphResponse>, AppError> {
    let project_root = crate::workspace::find_project_root()?;
    let metadata = Metadata::load(&project_root).await.unwrap_or_default();

    Ok(Json(GraphResponse {
        nodes: metadata.nodes,
    }))
}
