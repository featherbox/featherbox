use crate::{
    config::Config,
    dependency::{Graph, detect_changes, save_graph_with_changes},
    workspace::find_project_root,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MigrateResponse {
    pub success: bool,
    pub message: String,
    pub graph_id: Option<i32>,
}

pub async fn execute() -> Result<()> {
    let project_root = find_project_root()?;
    let config = Config::load()?;

    migrate_from_config(&config, &project_root).await
}

pub async fn validate_migration() -> anyhow::Result<()> {
    let project_root = find_project_root()?;
    let config = Config::load()?;

    if config.adapters.is_empty() && config.models.is_empty() {
        return Ok(());
    }

    let current_graph = Graph::from_config(&config)?;
    detect_changes(&project_root, &current_graph, &config).await?;

    Ok(())
}

pub async fn migrate_from_config(config: &Config, project_dir: &std::path::Path) -> Result<()> {
    let current_graph = Graph::from_config(config)?;

    save_graph_with_changes(project_dir, &current_graph).await
}

// pub fn routes() -> Router {
//     Router::new().route("/migrate", post(handle_migrate))
// }

// async fn handle_migrate() -> Result<Json<MigrateResponse>, AppError> {
//     match task::spawn_blocking(execute).await {
//         Ok(result) => match result {
//             Ok(graph_id) => Ok(Json(MigrateResponse {
//                 success: true,
//                 message: if graph_id.is_some() {
//                     "Migration completed successfully".to_string()
//                 } else {
//                     "No changes detected".to_string()
//                 },
//                 graph_id,
//             })),
//             Err(e) => {
//                 error!(error = %e, "Migration failed");
//                 Ok(Json(MigrateResponse {
//                     success: false,
//                     message: format!("Migration failed: {}", e),
//                     graph_id: None,
//                 }))
//             }
//         },
//         Err(e) => {
//             error!(error = %e, "Migration task failed");
//             app_error(StatusCode::INTERNAL_SERVER_ERROR)
//         }
//     }
// }
