use anyhow::Result;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::fs;

use crate::commands::workspace::find_project_root;
use crate::config::adapter::{AdapterConfig, parse_adapter_config};

#[derive(Serialize, Deserialize)]
pub struct AdapterSummary {
    pub name: String,
    pub description: Option<String>,
    pub connection: String,
    pub source_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct AdapterDetails {
    pub name: String,
    pub config: AdapterConfig,
}

#[derive(Deserialize)]
pub struct CreateAdapterRequest {
    pub name: String,
    pub config: AdapterConfig,
}

#[derive(Deserialize)]
pub struct UpdateAdapterRequest {
    pub config: AdapterConfig,
}

pub fn routes() -> Router {
    Router::new()
        .route("/adapters", get(list_adapters).post(create_adapter))
        .route(
            "/adapters/{name}",
            get(get_adapter).put(update_adapter).delete(delete_adapter),
        )
}

async fn list_adapters() -> Result<Json<Vec<AdapterSummary>>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let adapters_dir = project_root.join("adapters");

    if !adapters_dir.exists() {
        return Ok(Json(vec![]));
    }

    let mut adapters = Vec::new();

    let entries = fs::read_dir(&adapters_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entries {
        let entry = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("yml") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                let adapter_name = stem.to_string();

                if let Ok(content) = fs::read_to_string(&path) {
                    if let Ok(config) = parse_adapter_config(&content) {
                        let source_type = match &config.source {
                            crate::config::adapter::AdapterSource::File { .. } => {
                                "file".to_string()
                            }
                            crate::config::adapter::AdapterSource::Database { .. } => {
                                "database".to_string()
                            }
                        };

                        adapters.push(AdapterSummary {
                            name: adapter_name,
                            description: config.description,
                            connection: config.connection,
                            source_type,
                        });
                    }
                }
            }
        }
    }

    adapters.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(adapters))
}

async fn get_adapter(Path(name): Path<String>) -> Result<Json<AdapterDetails>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content =
        fs::read_to_string(&adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = parse_adapter_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AdapterDetails { name, config }))
}

async fn create_adapter(
    Json(req): Json<CreateAdapterRequest>,
) -> Result<Json<AdapterDetails>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let adapters_dir = project_root.join("adapters");
    let adapter_file = adapters_dir.join(format!("{}.yml", req.name));

    if adapter_file.exists() {
        return Err(StatusCode::CONFLICT);
    }

    if !adapters_dir.exists() {
        fs::create_dir_all(&adapters_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&adapter_file, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AdapterDetails {
        name: req.name,
        config: req.config,
    }))
}

async fn update_adapter(
    Path(name): Path<String>,
    Json(req): Json<UpdateAdapterRequest>,
) -> Result<Json<AdapterDetails>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&adapter_file, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AdapterDetails {
        name,
        config: req.config,
    }))
}

async fn delete_adapter(Path(name): Path<String>) -> Result<StatusCode, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    fs::remove_file(&adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}
