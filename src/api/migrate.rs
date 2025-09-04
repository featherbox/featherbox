use crate::{
    config::Config,
    dependency::{Graph, detect_changes, save_graph_with_changes},
    workspace::find_project_root,
};
use anyhow::Result;
use axum::{Router, http::StatusCode, response::Json, routing::post};
use serde::{Deserialize, Serialize};
use tokio::task;
use tracing::error;

#[derive(Serialize, Deserialize)]
pub struct MigrateResponse {
    pub success: bool,
    pub message: String,
    pub graph_id: Option<i32>,
}

pub fn execute() -> Result<Option<i32>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let project_root = find_project_root()?;
        let config = Config::load_from_directory(&project_root)?;

        if config.adapters.is_empty() && config.models.is_empty() {
            return Ok(None);
        }

        migrate_from_config(&config, &project_root).await
    })
}

pub async fn execute_async() -> Result<Option<i32>> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        return Ok(None);
    }

    migrate_from_config(&config, &project_root).await
}

pub async fn validate_migration() -> anyhow::Result<()> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        return Ok(());
    }

    let current_graph = Graph::from_config(&config)?;
    detect_changes(&project_root, &current_graph, &config).await?;
    
    Ok(())
}

pub async fn migrate_from_config(
    config: &Config,
    project_dir: &std::path::Path,
) -> Result<Option<i32>> {
    let current_graph = Graph::from_config(config)?;

    let changes = detect_changes(project_dir, &current_graph, config).await?;

    if changes.is_none() {
        return Ok(None);
    }

    Ok(Some(
        save_graph_with_changes(project_dir, &current_graph, config, changes.as_ref()).await?,
    ))
}

pub fn routes() -> Router {
    Router::new().route("/migrate", post(handle_migrate))
}

async fn handle_migrate() -> Result<Json<MigrateResponse>, StatusCode> {
    match task::spawn_blocking(execute).await {
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
                error!(error = %e, "Migration failed");
                Ok(Json(MigrateResponse {
                    success: false,
                    message: format!("Migration failed: {}", e),
                    graph_id: None,
                }))
            }
        },
        Err(e) => {
            error!(error = %e, "Migration task failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
