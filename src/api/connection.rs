use crate::commands::workspace::find_project_root;
use crate::config::project::{ConnectionConfig, parse_project_config};
use anyhow::Result;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub name: String,
    pub connection_type: String,
    pub details: String,
}

#[derive(Deserialize)]
pub struct CreateConnectionRequest {
    pub name: String,
    pub config: ConnectionConfig,
}

#[derive(Deserialize)]
pub struct UpdateConnectionRequest {
    pub config: ConnectionConfig,
}

pub fn routes() -> Router {
    Router::new()
        .route(
            "/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/connections/{name}",
            get(get_connection)
                .put(update_connection)
                .delete(delete_connection),
        )
}

async fn list_connections() -> Result<Json<Vec<ConnectionSummary>>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Ok(Json(vec![]));
    }

    let content =
        fs::read_to_string(&project_yml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = parse_project_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut connections = Vec::new();
    for (name, conn_config) in &config.connections {
        let (connection_type, details) = match conn_config {
            ConnectionConfig::LocalFile { base_path } => {
                ("localfile".to_string(), base_path.clone())
            }
            ConnectionConfig::Sqlite { path } => ("sqlite".to_string(), path.clone()),
            ConnectionConfig::MySql { host, database, .. } => {
                ("mysql".to_string(), format!("{database}@{host}"))
            }
            ConnectionConfig::PostgreSql { host, database, .. } => {
                ("postgresql".to_string(), format!("{database}@{host}"))
            }
            ConnectionConfig::S3(s3) => ("s3".to_string(), s3.bucket.clone()),
        };

        connections.push(ConnectionSummary {
            name: name.clone(),
            connection_type,
            details,
        });
    }

    connections.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(connections))
}

async fn get_connection(Path(name): Path<String>) -> Result<Json<ConnectionConfig>, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content =
        fs::read_to_string(&project_yml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = parse_project_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match config.connections.get(&name) {
        Some(conn_config) => Ok(Json(conn_config.clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn create_connection(
    Json(req): Json<CreateConnectionRequest>,
) -> Result<StatusCode, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content =
        fs::read_to_string(&project_yml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut config =
        parse_project_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if config.connections.contains_key(&req.name) {
        return Err(StatusCode::CONFLICT);
    }

    config.connections.insert(req.name, req.config);

    let yaml_content =
        serde_yml::to_string(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&project_yml, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}

async fn update_connection(
    Path(name): Path<String>,
    Json(req): Json<UpdateConnectionRequest>,
) -> Result<StatusCode, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content =
        fs::read_to_string(&project_yml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut config =
        parse_project_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !config.connections.contains_key(&name) {
        return Err(StatusCode::NOT_FOUND);
    }

    config.connections.insert(name, req.config);

    let yaml_content =
        serde_yml::to_string(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&project_yml, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

async fn delete_connection(Path(name): Path<String>) -> Result<StatusCode, StatusCode> {
    let project_root = find_project_root(None).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content =
        fs::read_to_string(&project_yml).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut config =
        parse_project_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !config.connections.contains_key(&name) {
        return Err(StatusCode::NOT_FOUND);
    }

    config.connections.remove(&name);

    let yaml_content =
        serde_yml::to_string(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&project_yml, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}
