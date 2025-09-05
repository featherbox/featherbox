use crate::api::{AppError, app_error};
use crate::config::Config;
use crate::config::adapter::AdapterConfig;
use crate::core::graph::Graph;
use anyhow::Result;
use axum::Extension;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
pub struct AdapterSummary {
    pub name: String,
    pub description: Option<String>,
    pub connection: String,
    pub source_type: String,
}

#[derive(Deserialize)]
pub struct CreateAdapterRequest {
    pub name: String,
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

async fn list_adapters(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<AdapterSummary>>, AppError> {
    let mut adapters: Vec<AdapterSummary> = config
        .lock()
        .await
        .adapters
        .clone()
        .into_iter()
        .map(|(name, config)| {
            let source_type = match &config.source {
                crate::config::adapter::AdapterSource::File { .. } => "file".to_string(),
                crate::config::adapter::AdapterSource::Database { .. } => "database".to_string(),
            };
            AdapterSummary {
                name,
                description: config.description,
                connection: config.connection,
                source_type,
            }
        })
        .collect();

    adapters.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(adapters))
}

async fn get_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<AdapterConfig>, AppError> {
    if let Some(adapter_config) = config.lock().await.adapters.get(&name) {
        Ok(Json(adapter_config.clone()))
    } else {
        app_error(StatusCode::NOT_FOUND)
    }
}

async fn create_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(adapter): Json<CreateAdapterRequest>,
) -> Result<(), AppError> {
    let mut config = config.lock().await;

    if config.adapters.contains_key(&adapter.name) {
        return app_error(StatusCode::CONFLICT);
    }

    let mut graph = Graph::load(&config.project_dir).await?;
    graph.create_node(&adapter.name, &[]);
    graph.save(&config.project_dir).await?;

    let adapter_file = config.upsert_adapter(&adapter.name, &adapter.config)?;
    adapter_file.save()?;

    Ok(())
}

async fn update_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(adapter): Json<AdapterConfig>,
) -> Result<(), AppError> {
    let mut config = config.lock().await;

    if !config.adapters.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    };

    let mut graph = Graph::load(&config.project_dir).await?;
    graph.update_node(&name);
    graph.save(&config.project_dir).await?;

    let adapter_file = config.upsert_adapter(&name, &adapter)?;
    adapter_file.save()?;

    Ok(())
}

async fn delete_adapter(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;

    if !config.adapters.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    };

    let mut graph = Graph::load(&config.project_dir).await?;
    graph.delete_node(&name);
    graph.save(&config.project_dir).await?;

    let adapter_file = config.delete_adapter(&name)?;
    adapter_file.save()?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::TestManager;
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        // Create request
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

        let graph = Graph::load(test.directory()).await?;
        assert!(graph.has_node("test_csv_adapter"));

        // Get request
        let response = server.get("/adapters/test_csv_adapter").await;
        response.assert_status_ok();

        let adapter_config: AdapterConfig = response.json();
        assert_eq!(
            adapter_config.description,
            Some("Test CSV adapter".to_string())
        );
        assert_eq!(adapter_config.connection, "test_connection");
        match &adapter_config.source {
            crate::config::adapter::AdapterSource::File { file, format } => {
                assert_eq!(file.path, "test.csv");
                assert_eq!(format.ty, "csv");
                assert_eq!(format.has_header, Some(true));
                assert_eq!(format.delimiter, Some(",".to_string()));
            }
            _ => panic!("Expected File source"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        let original_adapter = AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Original adapter".to_string()),
            source: crate::config::adapter::AdapterSource::File {
                file: crate::config::adapter::FileConfig {
                    path: "original.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: crate::config::adapter::FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: Some(",".to_string()),
                    null_value: None,
                    has_header: Some(true),
                },
            },
            columns: vec![],
        };

        // Create adapter directly
        {
            let mut config = test.config().await;
            let adapter_file = config.upsert_adapter("test_adapter", &original_adapter)?;
            adapter_file.save()?;
        }

        // Create node in graph
        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("test_adapter", &[]);
        graph.set_current_time("test_adapter");
        graph.save(test.directory()).await?;

        // Update request
        let updated_config = json!({
            "description": "Updated adapter",
            "connection": "test_connection",
            "source": {
                "type": "file",
                "file": {
                    "path": "updated.csv"
                },
                "format": {
                    "type": "csv",
                    "has_header": false,
                    "delimiter": ";"
                }
            },
            "columns": []
        });

        let response = server
            .put("/adapters/test_adapter")
            .json(&updated_config)
            .await;
        response.assert_status_ok();

        // GET request to verify update
        let get_response = server.get("/adapters/test_adapter").await;
        get_response.assert_status_ok();

        let adapter_config: AdapterConfig = get_response.json();
        assert_eq!(
            adapter_config.description,
            Some("Updated adapter".to_string())
        );
        match &adapter_config.source {
            crate::config::adapter::AdapterSource::File { file, format } => {
                assert_eq!(file.path, "updated.csv");
                assert_eq!(format.has_header, Some(false));
                assert_eq!(format.delimiter, Some(";".to_string()));
            }
            _ => panic!("Expected File source"),
        }

        // Verify graph node was updated (timestamp reset)
        let graph = Graph::load(test.directory()).await?;
        assert!(graph.has_node("test_adapter"));
        assert!(graph.get_node("test_adapter").unwrap().updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_adapter() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        let adapter_config = AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Adapter to delete".to_string()),
            source: crate::config::adapter::AdapterSource::Database {
                table_name: "test_table".to_string(),
            },
            columns: vec![],
        };

        {
            let mut config = test.config().await;
            let adapter_file = config.upsert_adapter("adapter_to_delete", &adapter_config)?;
            adapter_file.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("adapter_to_delete", &[]);
        graph.save(test.directory()).await?;

        let get_response = server.get("/adapters/adapter_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/adapters/adapter_to_delete").await;
        delete_response.assert_status(StatusCode::NO_CONTENT);

        let get_response_after = server.get("/adapters/adapter_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        let graph = Graph::load(test.directory()).await?;
        assert!(!graph.has_node("adapter_to_delete"));

        Ok(())
    }
}
