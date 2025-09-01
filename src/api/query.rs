use crate::commands::{run::connect_ducklake, workspace::ensure_project_directory};
use crate::config::{Config, QueryConfig};
use anyhow::Result;
use axum::{
    Router,
    extract::Path as AxumPath,
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env};

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Deserialize)]
pub struct SaveQueryRequest {
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateQueryRequest {
    pub sql: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub results: Vec<Vec<String>>,
    pub column_count: usize,
}

#[derive(Serialize)]
pub struct QueryListResponse {
    pub queries: HashMap<String, QueryConfig>,
}

#[derive(Serialize)]
pub struct SaveQueryResponse {
    pub message: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn execute_query_handler(
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match execute_query_internal(&payload.sql).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

async fn execute_query_internal(sql: &str) -> Result<(Vec<Vec<String>>, usize)> {
    let current_dir = env::current_dir()?;
    let project_root = ensure_project_directory(Some(&current_dir))?;
    let config = Config::load_from_directory(&project_root)?;

    let ducklake = connect_ducklake(&config).await?;
    let results = ducklake.query(sql)?;

    let column_count = if results.is_empty() {
        0
    } else {
        results[0].len()
    };

    Ok((results, column_count))
}

pub async fn list_queries_handler()
-> Result<Json<QueryListResponse>, (StatusCode, Json<ErrorResponse>)> {
    match list_queries_internal().await {
        Ok(queries) => Ok(Json(QueryListResponse { queries })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn save_query_handler(
    Json(payload): Json<SaveQueryRequest>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match save_query_internal(&payload.name, &payload.sql, payload.description).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' saved successfully", payload.name),
        })),
        Err(e) => {
            let status = if e.to_string().contains("already exists") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

pub async fn get_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryConfig>, (StatusCode, Json<ErrorResponse>)> {
    match get_query_internal(&name).await {
        Ok(query) => Ok(Json(query)),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn update_query_handler(
    AxumPath(name): AxumPath<String>,
    Json(payload): Json<UpdateQueryRequest>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match update_query_internal(&name, payload.sql, payload.description).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' updated successfully", name),
        })),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn delete_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match delete_query_internal(&name).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' deleted successfully", name),
        })),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn run_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match run_query_internal(&name).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

async fn list_queries_internal() -> Result<HashMap<String, QueryConfig>> {
    let current_dir = env::current_dir()?;
    let project_root = ensure_project_directory(Some(&current_dir))?;
    let config = Config::load_from_directory(&project_root)?;
    Ok(config.queries)
}

async fn save_query_internal(name: &str, sql: &str, description: Option<String>) -> Result<()> {
    let current_dir = env::current_dir()?;
    crate::commands::query::save_query(name, sql, description, &current_dir)
}

async fn get_query_internal(name: &str) -> Result<QueryConfig> {
    let current_dir = env::current_dir()?;
    let project_root = ensure_project_directory(Some(&current_dir))?;
    let config = Config::load_from_directory(&project_root)?;

    config
        .queries
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found", name))
}

async fn update_query_internal(
    name: &str,
    sql: Option<String>,
    description: Option<String>,
) -> Result<()> {
    let current_dir = env::current_dir()?;
    crate::commands::query::update_query(name, sql, description, &current_dir)
}

async fn delete_query_internal(name: &str) -> Result<()> {
    let current_dir = env::current_dir()?;
    crate::commands::query::delete_query(name, &current_dir)
}

async fn run_query_internal(name: &str) -> Result<(Vec<Vec<String>>, usize)> {
    let current_dir = env::current_dir()?;
    let sql = crate::commands::query::load_query(name, &current_dir)?;
    execute_query_internal(&sql).await
}

pub fn routes() -> Router {
    Router::new()
        .route("/query", post(execute_query_handler))
        .route("/queries", get(list_queries_handler))
        .route("/queries", post(save_query_handler))
        .route("/queries/{name}", get(get_query_handler))
        .route("/queries/{name}", put(update_query_handler))
        .route("/queries/{name}", delete(delete_query_handler))
        .route("/queries/{name}/run", post(run_query_handler))
}
