use crate::api::migrate;
use crate::config::adapter::{AdapterConfig, parse_adapter_config};
use crate::workspace::find_project_root;
use anyhow::Result;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::error;

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


fn adapter_dir() -> Result<PathBuf, StatusCode> {
    let project_root = find_project_root().map_err(|err| {
        error!(error = ?err, "Failed to find project root");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(project_root.join("adapters"))
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
    let adapters_dir = adapter_dir()?;

    if !adapters_dir.exists() {
        return Ok(Json(vec![]));
    }

    let mut adapters = Vec::new();

    let entries = fs::read_dir(&adapters_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entries {
        let entry = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("yml")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
        {
            let adapter_name = stem.to_string();

            if let Ok(content) = fs::read_to_string(&path)
                && let Ok(config) = parse_adapter_config(&content)
            {
                let source_type = match &config.source {
                    crate::config::adapter::AdapterSource::File { .. } => "file".to_string(),
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

    adapters.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(adapters))
}

async fn get_adapter(Path(name): Path<String>) -> Result<Json<AdapterDetails>, StatusCode> {
    let adapters_dir = adapter_dir()?;
    let adapter_file = adapters_dir.join(format!("{name}.yml"));

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
    let adapters_dir = adapter_dir()?;
    let adapter_file = adapters_dir.join(format!("{}.yml", req.name));

    if adapter_file.exists() {
        return Err(StatusCode::CONFLICT);
    }

    if !adapters_dir.exists() {
        fs::create_dir_all(&adapters_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let temp_file = adapters_dir.join(format!("{}.tmp", req.name));
    fs::write(&temp_file, &yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Err(e) = migrate::validate_migration().await {
        let _ = fs::remove_file(&temp_file);
        error!(error = %e, "Migration validation failed");
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    fs::rename(&temp_file, &adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    if let Err(e) = migrate::execute_async().await {
        error!(error = %e, "Migration failed after adapter creation");
    }
    
    Ok(Json(AdapterDetails {
        name: req.name,
        config: req.config,
    }))
}

async fn update_adapter(
    Path(name): Path<String>,
    Json(req): Json<UpdateAdapterRequest>,
) -> Result<Json<AdapterDetails>, StatusCode> {
    let adapters_dir = adapter_dir()?;
    let adapter_file = adapters_dir.join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let original_content =
        fs::read_to_string(&adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let temp_file = adapters_dir.join(format!("{name}.tmp"));
    fs::write(&temp_file, &yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Err(e) = migrate::validate_migration().await {
        let _ = fs::remove_file(&temp_file);
        fs::write(&adapter_file, original_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        error!(error = %e, "Migration validation failed");
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    fs::rename(&temp_file, &adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    if let Err(e) = migrate::execute_async().await {
        error!(error = %e, "Migration failed after adapter update");
    }
    
    Ok(Json(AdapterDetails {
        name,
        config: req.config,
    }))
}

async fn delete_adapter(Path(name): Path<String>) -> Result<StatusCode, StatusCode> {
    let adapters_dir = adapter_dir()?;
    let adapter_file = adapters_dir.join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let backup_file = adapters_dir.join(format!("{name}.bak"));
    fs::copy(&adapter_file, &backup_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::remove_file(&adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Err(e) = migrate::validate_migration().await {
        fs::rename(&backup_file, &adapter_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        error!(error = %e, "Migration validation failed");
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }

    let _ = fs::remove_file(&backup_file);
    
    if let Err(e) = migrate::execute_async().await {
        error!(error = %e, "Migration failed after adapter deletion");
    }
    
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::create_test_project;
    use crate::test_helpers::create_test_server;
    use serde_json::{Value, json};

    fn setup_test_project() {
        create_test_project().unwrap();
    }

    #[tokio::test]
    async fn test_list_adapters_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/adapters").await;

        response.assert_status_ok();
        let adapters: Value = response.json();
        assert!(adapters.is_array());
    }

    #[tokio::test]
    async fn test_get_adapter_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/adapters/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_adapter_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let new_adapter = json!({
            "name": "test_csv_adapter",
            "config": {
                "description": "Test CSV adapter",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "test.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server.post("/adapters").json(&new_adapter).await;

        response.assert_status_ok();
        let adapter: Value = response.json();
        assert_eq!(adapter["name"], "test_csv_adapter");
    }

    #[tokio::test]
    async fn test_create_adapter_conflict() {
        setup_test_project();
        let server = create_test_server(routes);

        let adapter_config = json!({
            "name": "duplicate_adapter",
            "config": {
                "description": "Test adapter",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "test.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let _first_response = server.post("/adapters").json(&adapter_config).await;

        let second_response = server.post("/adapters").json(&adapter_config).await;

        second_response.assert_status(StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_adapter_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let create_adapter = json!({
            "name": "update_test_adapter",
            "config": {
                "description": "Original description",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "original.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        server.post("/adapters").json(&create_adapter).await;

        let update_config = json!({
            "config": {
                "description": "Updated description",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "updated.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server
            .put("/adapters/update_test_adapter")
            .json(&update_config)
            .await;

        response.assert_status_ok();
        let adapter: Value = response.json();
        assert_eq!(adapter["config"]["description"], "Updated description");
    }

    #[tokio::test]
    async fn test_update_adapter_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let update_config = json!({
            "config": {
                "description": "Updated description",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "updated.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server
            .put("/adapters/nonexistent")
            .json(&update_config)
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_adapter_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let create_adapter = json!({
            "name": "delete_test_adapter",
            "config": {
                "description": "To be deleted",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "delete.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        server.post("/adapters").json(&create_adapter).await;

        let response = server.delete("/adapters/delete_test_adapter").await;

        response.assert_status(StatusCode::NO_CONTENT);

        let get_response = server.get("/adapters/delete_test_adapter").await;
        get_response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_adapter_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.delete("/adapters/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_adapter_with_auto_migration() {
        setup_test_project();
        let server = create_test_server(routes);

        let new_adapter = json!({
            "name": "auto_migration_test",
            "config": {
                "description": "Test adapter with auto migration",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "test_data.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server.post("/adapters").json(&new_adapter).await;

        response.assert_status_ok();
        let adapter: Value = response.json();
        assert_eq!(adapter["name"], "auto_migration_test");
    }

    #[tokio::test]
    async fn test_update_adapter_with_auto_migration() {
        setup_test_project();
        let server = create_test_server(routes);

        let create_adapter = json!({
            "name": "update_migration_test",
            "config": {
                "description": "Original for migration test",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "original.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        server.post("/adapters").json(&create_adapter).await;

        let update_config = json!({
            "config": {
                "description": "Updated for migration test",
                "connection": "test_connection",
                "source": {
                    "type": "file",
                    "file": {
                        "path": "updated.csv"
                    },
                    "format": {
                        "type": "csv",
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "columns": []
            }
        });

        let response = server
            .put("/adapters/update_migration_test")
            .json(&update_config)
            .await;

        response.assert_status_ok();
        let adapter: Value = response.json();
        assert_eq!(
            adapter["config"]["description"],
            "Updated for migration test"
        );
    }
}
