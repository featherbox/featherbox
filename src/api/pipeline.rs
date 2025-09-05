use super::run;
use crate::{
    api::AppError,
    config::Config,
    metadata::{Metadata, Node},
    status::{PipelineStatus, StatusManager},
};
use anyhow::Result;
use axum::{Extension, Router, response::Json, routing::get};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

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
        .nest("/pipeline", run::routes())
}

async fn handle_get_latest_status(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<PipelineStatusResponse>, AppError> {
    let config = config.lock().await;
    let project_root = config.project_dir.clone();

    let status = StatusManager::find_latest_status(&project_root).await?;

    Ok(Json(PipelineStatusResponse { pipeline: status }))
}

async fn handle_get_graph(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<GraphResponse>, AppError> {
    let config = config.lock().await;
    let project_root = config.project_dir.clone();
    let metadata = Metadata::load(&project_root).await.unwrap_or_default();

    Ok(Json(GraphResponse {
        nodes: metadata.nodes,
    }))
}
