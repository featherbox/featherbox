use crate::api::{AppError, app_error};
use crate::config::Config;
use crate::config::project::ConnectionConfig;
use anyhow::Result;
use axum::extract::Path;
use axum::response::Json;
use axum::{Extension, Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

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

async fn list_connections(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<ConnectionSummary>>, AppError> {
    let config = config.lock().await;
    let mut connections = Vec::new();
    for (name, conn_config) in &config.project.connections {
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

async fn get_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionConfig>, AppError> {
    let config = config.lock().await;
    match config.project.connections.get(&name) {
        Some(conn_config) => Ok(Json(conn_config.clone())),
        None => app_error(StatusCode::NOT_FOUND),
    }
}

async fn create_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(req): Json<CreateConnectionRequest>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if project_config.connections.contains_key(&req.name) {
        return app_error(StatusCode::CONFLICT);
    }

    project_config
        .connections
        .insert(req.name.clone(), req.config.clone());
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    Ok(StatusCode::CREATED)
}

async fn update_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(req): Json<UpdateConnectionRequest>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if !project_config.connections.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    }

    project_config.connections.insert(name, req.config.clone());
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    Ok(StatusCode::OK)
}

async fn delete_connection(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;
    let mut project_config = config.project.clone();

    if !project_config.connections.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    }

    project_config.connections.remove(&name);
    let project_file = config.add_project_setting(&project_config)?;
    project_file.save()?;

    Ok(StatusCode::NO_CONTENT)
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::project::ConnectionConfig;
//     use crate::test_helpers::create_test_project;
//     use crate::{config::ProjectConfig, test_helpers::create_test_server};
//     use serde_json::{Value, json};
//     use std::collections::HashMap;
//
//     fn setup_test_project() -> ProjectConfig {
//         let mut config = create_test_project().unwrap();
//
//         let mut connections = HashMap::new();
//         connections.insert(
//             "test_sqlite".to_string(),
//             ConnectionConfig::Sqlite {
//                 path: "test.db".to_string(),
//             },
//         );
//         connections.insert(
//             "test_mysql".to_string(),
//             ConnectionConfig::MySql {
//                 host: "localhost".to_string(),
//                 port: 3306,
//                 database: "testdb".to_string(),
//                 username: "user".to_string(),
//                 password: "password".to_string(),
//             },
//         );
//         config.connections = connections;
//
//         config.create_project().unwrap();
//         config
//     }
//
//     #[tokio::test]
//     async fn test_list_connections_success() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.get("/connections").await;
//
//         response.assert_status_ok();
//         let connections: Value = response.json();
//
//         assert!(connections.is_array());
//         let connections_array = connections.as_array().unwrap();
//         assert_eq!(connections_array.len(), 2);
//
//         let connection_names: Vec<String> = connections_array
//             .iter()
//             .map(|conn| conn["name"].as_str().unwrap().to_string())
//             .collect();
//         assert!(connection_names.contains(&"test_sqlite".to_string()));
//         assert!(connection_names.contains(&"test_mysql".to_string()));
//     }
//
//     #[tokio::test]
//     async fn test_get_connection_success() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.get("/connections/test_sqlite").await;
//
//         response.assert_status_ok();
//         response.assert_json(&json!({
//             "type": "sqlite",
//             "path": "test.db"
//         }));
//     }
//
//     #[tokio::test]
//     async fn test_get_connection_not_found() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.get("/connections/nonexistent").await;
//
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_create_connection_success() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let new_connection = json!({
//             "name": "new_connection",
//             "config": {
//                 "type": "postgresql",
//                 "host": "localhost",
//                 "port": 5432,
//                 "database": "newdb",
//                 "username": "newuser",
//                 "password": "newpass"
//             }
//         });
//
//         let response = server.post("/connections").json(&new_connection).await;
//
//         response.assert_status(StatusCode::CREATED);
//
//         let get_response = server.get("/connections/new_connection").await;
//         get_response.assert_status_ok();
//         get_response.assert_json(&json!({
//             "type": "postgresql",
//             "host": "localhost",
//             "port": 5432,
//             "database": "newdb",
//             "username": "newuser",
//             "password": "newpass"
//         }));
//     }
//
//     #[tokio::test]
//     async fn test_create_connection_conflict() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let existing_connection = json!({
//             "name": "test_sqlite",
//             "config": {
//                 "type": "sqlite",
//                 "path": "another.db"
//             }
//         });
//
//         let response = server.post("/connections").json(&existing_connection).await;
//
//         response.assert_status(StatusCode::CONFLICT);
//     }
//
//     #[tokio::test]
//     async fn test_update_connection_success() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let updated_config = json!({
//             "config": {
//                 "type": "sqlite",
//                 "path": "updated.db"
//             }
//         });
//
//         let response = server
//             .put("/connections/test_sqlite")
//             .json(&updated_config)
//             .await;
//
//         response.assert_status_ok();
//
//         let get_response = server.get("/connections/test_sqlite").await;
//         get_response.assert_status_ok();
//         get_response.assert_json(&json!({
//             "type": "sqlite",
//             "path": "updated.db"
//         }));
//     }
//
//     #[tokio::test]
//     async fn test_update_connection_not_found() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let updated_config = json!({
//             "config": {
//                 "type": "sqlite",
//                 "path": "updated.db"
//             }
//         });
//
//         let response = server
//             .put("/connections/nonexistent")
//             .json(&updated_config)
//             .await;
//
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_delete_connection_success() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.delete("/connections/test_sqlite").await;
//
//         response.assert_status(StatusCode::NO_CONTENT);
//
//         let get_response = server.get("/connections/test_sqlite").await;
//         get_response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_delete_connection_not_found() {
//         setup_test_project();
//         let server = create_test_server(routes);
//
//         let response = server.delete("/connections/nonexistent").await;
//
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
// }
