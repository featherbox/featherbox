use crate::config::ProjectConfig;
use crate::config::project::ConnectionConfig;
use crate::secret::SecretManager;
use anyhow::Result;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::error;

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
    let config = project_config()?;

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
    let config = project_config()?;

    match config.connections.get(&name) {
        Some(conn_config) => Ok(Json(conn_config.clone())),
        None => Err(StatusCode::NOT_FOUND),
    }
}

fn project_config() -> Result<ProjectConfig, StatusCode> {
    match ProjectConfig::from_project() {
        Ok(config) => Ok(config),
        Err(err) => {
            error!(error = %err, "Failed to load project configuration");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn export_config(config: &ProjectConfig) -> Result<(), StatusCode> {
    match config.export_project() {
        Ok(()) => Ok(()),
        Err(err) => {
            error!(error = %err, "Failed to export project configuration");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn create_connection(
    Json(req): Json<CreateConnectionRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut config = project_config()?;

    if config.connections.contains_key(&req.name) {
        return Err(StatusCode::CONFLICT);
    }

    config.connections.insert(req.name, req.config);
    export_config(&config)?;

    Ok(StatusCode::CREATED)
}

async fn update_connection(
    Path(name): Path<String>,
    Json(req): Json<UpdateConnectionRequest>,
) -> Result<StatusCode, StatusCode> {
    let mut config = project_config()?;

    if !config.connections.contains_key(&name) {
        return Err(StatusCode::NOT_FOUND);
    }

    config.connections.insert(name, req.config);
    export_config(&config)?;

    Ok(StatusCode::OK)
}

async fn delete_connection(Path(name): Path<String>) -> Result<StatusCode, StatusCode> {
    let mut config = project_config()?;

    if !config.connections.contains_key(&name) {
        return Err(StatusCode::NOT_FOUND);
    }

    let connection_config = config.connections.get(&name).unwrap();
    let secret_keys = extract_secret_keys_from_connection(connection_config);

    config.connections.remove(&name);
    export_config(&config)?;

    let manager = SecretManager::new().map_err(|err| {
        error!(error = ?err, "Failed to initialize SecretManager");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    for secret_key in &secret_keys {
        match manager.delete_secret(secret_key) {
            Ok(true) => {}
            Ok(false) => {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            Err(err) => {
                error!(error = %err, secret_key = %secret_key, "Failed to delete secret");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

fn extract_secret_keys_from_connection(config: &ConnectionConfig) -> Vec<String> {
    let mut secret_keys = Vec::new();
    let secret_regex = Regex::new(r"\$\{SECRET_([a-zA-Z][a-zA-Z0-9_]*)\}").unwrap();

    match config {
        ConnectionConfig::MySql { password, .. } => {
            if let Some(key) = extract_secret_key_from_value(password, &secret_regex) {
                secret_keys.push(key);
            }
        }
        ConnectionConfig::PostgreSql { password, .. } => {
            if let Some(key) = extract_secret_key_from_value(password, &secret_regex) {
                secret_keys.push(key);
            }
        }
        ConnectionConfig::S3(s3_config) => {
            if let Some(key) =
                extract_secret_key_from_value(&s3_config.secret_access_key, &secret_regex)
            {
                secret_keys.push(key);
            }
            if let Some(session_token) = &s3_config.session_token
                && let Some(key) = extract_secret_key_from_value(session_token, &secret_regex)
            {
                secret_keys.push(key);
            }
        }
        _ => {}
    }

    secret_keys
}

fn extract_secret_key_from_value(value: &str, regex: &Regex) -> Option<String> {
    regex
        .captures(value)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::ConnectionConfig;
    use crate::test_helpers::create_test_project;
    use crate::{config::ProjectConfig, test_helpers::create_test_server};
    use serde_json::{Value, json};
    use std::collections::HashMap;

    fn setup_test_project() -> ProjectConfig {
        let mut config = create_test_project().unwrap();

        let mut connections = HashMap::new();
        connections.insert(
            "test_sqlite".to_string(),
            ConnectionConfig::Sqlite {
                path: "test.db".to_string(),
            },
        );
        connections.insert(
            "test_mysql".to_string(),
            ConnectionConfig::MySql {
                host: "localhost".to_string(),
                port: 3306,
                database: "testdb".to_string(),
                username: "user".to_string(),
                password: "password".to_string(),
            },
        );
        config.connections = connections;

        config.create_project().unwrap();
        config
    }

    #[tokio::test]
    async fn test_list_connections_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/connections").await;

        response.assert_status_ok();
        let connections: Value = response.json();

        assert!(connections.is_array());
        let connections_array = connections.as_array().unwrap();
        assert_eq!(connections_array.len(), 2);

        let connection_names: Vec<String> = connections_array
            .iter()
            .map(|conn| conn["name"].as_str().unwrap().to_string())
            .collect();
        assert!(connection_names.contains(&"test_sqlite".to_string()));
        assert!(connection_names.contains(&"test_mysql".to_string()));
    }

    #[tokio::test]
    async fn test_get_connection_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/connections/test_sqlite").await;

        response.assert_status_ok();
        response.assert_json(&json!({
            "type": "sqlite",
            "path": "test.db"
        }));
    }

    #[tokio::test]
    async fn test_get_connection_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/connections/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_connection_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let new_connection = json!({
            "name": "new_connection",
            "config": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "newdb",
                "username": "newuser",
                "password": "newpass"
            }
        });

        let response = server.post("/connections").json(&new_connection).await;

        response.assert_status(StatusCode::CREATED);

        let get_response = server.get("/connections/new_connection").await;
        get_response.assert_status_ok();
        get_response.assert_json(&json!({
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "newdb",
            "username": "newuser",
            "password": "newpass"
        }));
    }

    #[tokio::test]
    async fn test_create_connection_conflict() {
        setup_test_project();
        let server = create_test_server(routes);

        let existing_connection = json!({
            "name": "test_sqlite",
            "config": {
                "type": "sqlite",
                "path": "another.db"
            }
        });

        let response = server.post("/connections").json(&existing_connection).await;

        response.assert_status(StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_connection_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let updated_config = json!({
            "config": {
                "type": "sqlite",
                "path": "updated.db"
            }
        });

        let response = server
            .put("/connections/test_sqlite")
            .json(&updated_config)
            .await;

        response.assert_status_ok();

        let get_response = server.get("/connections/test_sqlite").await;
        get_response.assert_status_ok();
        get_response.assert_json(&json!({
            "type": "sqlite",
            "path": "updated.db"
        }));
    }

    #[tokio::test]
    async fn test_update_connection_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let updated_config = json!({
            "config": {
                "type": "sqlite",
                "path": "updated.db"
            }
        });

        let response = server
            .put("/connections/nonexistent")
            .json(&updated_config)
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connection_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.delete("/connections/test_sqlite").await;

        response.assert_status(StatusCode::NO_CONTENT);

        let get_response = server.get("/connections/test_sqlite").await;
        get_response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_connection_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.delete("/connections/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }
}
