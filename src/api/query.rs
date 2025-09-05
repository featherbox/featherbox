use crate::api::{AppError, app_error};
use crate::config::{Config, QueryConfig};
use crate::pipeline::ducklake::DuckLake;
use anyhow::Result;
use axum::{
    Extension, Router,
    extract::Path as AxumPath,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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

pub fn routes() -> Router {
    Router::new()
        .route("/query", post(execute_query_handler))
        .route(
            "/queries",
            get(list_queries_handler).post(save_query_handler),
        )
        .route(
            "/queries/{name}",
            get(get_query_handler)
                .put(update_query_handler)
                .delete(delete_query_handler),
        )
        .route("/queries/{name}/run", post(run_query_handler))
}

async fn execute_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    let config = config.lock().await;
    match execute_query_internal(&config, &payload.sql).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(_e) => app_error(StatusCode::BAD_REQUEST),
    }
}

async fn execute_query_internal(config: &Config, sql: &str) -> Result<(Vec<Vec<String>>, usize)> {
    let ducklake = DuckLake::from_config(config).await?;
    let results = ducklake.query(sql)?;

    let column_count = if results.is_empty() {
        0
    } else {
        results[0].len()
    };

    Ok((results, column_count))
}

async fn list_queries_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<QueryListResponse>, AppError> {
    let config = config.lock().await;
    Ok(Json(QueryListResponse {
        queries: config.queries.clone(),
    }))
}

async fn save_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(payload): Json<SaveQueryRequest>,
) -> Result<Json<SaveQueryResponse>, AppError> {
    let mut config = config.lock().await;
    if config.queries.contains_key(&payload.name) {
        return app_error(StatusCode::CONFLICT);
    }

    let query_config = QueryConfig {
        name: payload.name.clone(),
        description: payload.description,
        sql: payload.sql.clone(),
    };

    let query_file = config.upsert_query(&payload.name, &query_config)?;
    query_file.save()?;

    Ok(Json(SaveQueryResponse {
        message: format!("Query '{}' saved successfully", payload.name),
    }))
}

async fn get_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryConfig>, AppError> {
    let config = config.lock().await;
    match config.queries.get(&name) {
        Some(query) => Ok(Json(query.clone())),
        None => app_error(StatusCode::NOT_FOUND),
    }
}

async fn update_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
    Json(payload): Json<UpdateQueryRequest>,
) -> Result<Json<SaveQueryResponse>, AppError> {
    let mut config = config.lock().await;
    let mut query_config = match config.queries.get_mut(&name) {
        Some(q) => q.clone(),
        None => return app_error(StatusCode::NOT_FOUND),
    };

    if let Some(sql) = payload.sql {
        query_config.sql = sql;
    }
    if let Some(description) = payload.description {
        query_config.description = Some(description);
    }

    let query_file = config.upsert_query(&name, &query_config)?;
    query_file.save()?;

    Ok(Json(SaveQueryResponse {
        message: format!("Query '{}' updated successfully", name),
    }))
}

async fn delete_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<SaveQueryResponse>, AppError> {
    let mut config = config.lock().await;
    if !config.queries.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    }

    let query_file = config.delete_query(&name)?;
    query_file.save()?;

    Ok(Json(SaveQueryResponse {
        message: format!("Query '{}' deleted successfully", name),
    }))
}

async fn run_query_handler(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryResponse>, AppError> {
    let config_guard = config.lock().await;
    let sql = match config_guard.queries.get(&name) {
        Some(query) => query.sql.clone(),
        None => return app_error(StatusCode::NOT_FOUND),
    };

    match execute_query_internal(&config_guard, &sql).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(_e) => app_error(StatusCode::BAD_REQUEST),
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::ProjectConfig;
//     use crate::test_helpers::create_test_server;
//     use serde_json::{Value, json};
//     use tempfile;
//
//     fn setup_test_project() -> (ProjectConfig, tempfile::TempDir) {
//         let temp_dir = tempfile::tempdir().unwrap();
//         let project_path = temp_dir.path().to_path_buf();
//
//         std::fs::create_dir_all(&project_path).unwrap();
//         std::fs::create_dir_all(project_path.join("queries")).unwrap();
//         std::fs::create_dir_all(project_path.join("storage")).unwrap();
//
//         let db_path = project_path.join("test.db");
//         let storage_path = project_path.join("storage");
//
//         let project_config = ProjectConfig {
//             storage: crate::config::project::StorageConfig::LocalFile {
//                 path: storage_path.to_string_lossy().to_string(),
//             },
//             database: crate::config::project::DatabaseConfig {
//                 ty: crate::config::project::DatabaseType::Sqlite,
//                 path: Some(db_path.to_string_lossy().to_string()),
//                 host: None,
//                 port: None,
//                 database: None,
//                 username: None,
//                 password: None,
//             },
//             connections: std::collections::HashMap::new(),
//         };
//
//         let yaml_content = serde_yml::to_string(&project_config).unwrap();
//         std::fs::write(project_path.join("project.yml"), yaml_content).unwrap();
//         crate::workspace::set_project_dir(project_path.clone());
//
//         (project_config, temp_dir)
//     }
//
//     #[tokio::test]
//     async fn test_execute_query_simple() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let request = json!({
//             "sql": "SELECT 1 as test_column"
//         });
//
//         let response = server.post("/query").json(&request).await;
//
//         response.assert_status_ok();
//         let query_response: Value = response.json();
//         assert_eq!(query_response["column_count"], 1);
//         assert_eq!(query_response["results"][0][0], "1");
//     }
//
//     #[tokio::test]
//     async fn test_execute_query_invalid_sql() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let request = json!({
//             "sql": "INVALID SQL QUERY"
//         });
//
//         let response = server.post("/query").json(&request).await;
//         response.assert_status(StatusCode::BAD_REQUEST);
//     }
//
//     #[tokio::test]
//     async fn test_list_queries_empty() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.get("/queries").await;
//
//         response.assert_status_ok();
//         let queries_response: Value = response.json();
//         assert!(queries_response["queries"].as_object().unwrap().is_empty());
//     }
//
//     #[tokio::test]
//     async fn test_save_query() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let request = json!({
//             "name": "test_query",
//             "sql": "SELECT 1 as result",
//             "description": "Test query description"
//         });
//
//         let response = server.post("/queries").json(&request).await;
//
//         response.assert_status_ok();
//         let save_response: Value = response.json();
//         assert!(
//             save_response["message"]
//                 .as_str()
//                 .unwrap()
//                 .contains("saved successfully")
//         );
//     }
//
//     #[tokio::test]
//     async fn test_save_query_conflict() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let request = json!({
//             "name": "conflict_query",
//             "sql": "SELECT 1 as result",
//             "description": "First query"
//         });
//
//         server
//             .post("/queries")
//             .json(&request)
//             .await
//             .assert_status_ok();
//
//         let response = server.post("/queries").json(&request).await;
//         response.assert_status(StatusCode::CONFLICT);
//     }
//
//     #[tokio::test]
//     async fn test_get_query() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let query_content = "
// name: test_query
// description: Test query
// sql: SELECT 42 as answer
//         ";
//         std::fs::write(
//             temp_dir.path().join("queries/test_query.yml"),
//             query_content,
//         )
//         .unwrap();
//
//         let response = server.get("/queries/test_query").await;
//
//         response.assert_status_ok();
//         let query: Value = response.json();
//         assert_eq!(query["description"], "Test query");
//         assert_eq!(query["sql"], "SELECT 42 as answer");
//     }
//
//     #[tokio::test]
//     async fn test_get_query_not_found() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.get("/queries/nonexistent").await;
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_update_query() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let query_content = "
// name: update_query
// description: Original query
// sql: SELECT 1 as original
//         ";
//         std::fs::write(
//             temp_dir.path().join("queries/update_query.yml"),
//             query_content,
//         )
//         .unwrap();
//
//         let request = json!({
//             "sql": "SELECT 2 as updated",
//             "description": "Updated query"
//         });
//
//         let response = server.put("/queries/update_query").json(&request).await;
//
//         response.assert_status_ok();
//         let update_response: Value = response.json();
//         assert!(
//             update_response["message"]
//                 .as_str()
//                 .unwrap()
//                 .contains("updated successfully")
//         );
//     }
//
//     #[tokio::test]
//     async fn test_delete_query() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let query_content = "
// description: Query to delete
// sql: SELECT 'delete me' as message
//         ";
//         std::fs::write(
//             temp_dir.path().join("queries/delete_query.yml"),
//             query_content,
//         )
//         .unwrap();
//
//         let response = server.delete("/queries/delete_query").await;
//         response.assert_status_ok();
//
//         let response = server.get("/queries/delete_query").await;
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_run_saved_query() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let query_content = "
// name: run_query
// description: Test query to run
// sql: SELECT 'Hello World' as greeting, 42 as number
//         ";
//         std::fs::write(temp_dir.path().join("queries/run_query.yml"), query_content).unwrap();
//
//         let response = server.post("/queries/run_query/run").await;
//
//         response.assert_status_ok();
//         let query_response: Value = response.json();
//         assert_eq!(query_response["column_count"], 2);
//         assert_eq!(query_response["results"][0][0], "Hello World");
//         assert_eq!(query_response["results"][0][1], "42");
//     }
//
//     #[tokio::test]
//     async fn test_run_nonexistent_query() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.post("/queries/nonexistent/run").await;
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
// }
