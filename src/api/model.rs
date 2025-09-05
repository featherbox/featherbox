use crate::api::{AppError, app_error};
use crate::config::Config;
use crate::config::model::ModelConfig;
use crate::core::graph::{Graph, dependent_tables};
use anyhow::Result;
use axum::Extension;
use axum::extract::Path;
use axum::response::Json;
use axum::{Router, http::StatusCode, routing::get};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize)]
pub struct ModelSummary {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ModelDetails {
    pub name: String,
    pub config: ModelConfig,
}

#[derive(Deserialize)]
pub struct CreateModelRequest {
    pub name: String,
    pub config: ModelConfig,
}

pub fn routes() -> Router {
    Router::new()
        .route("/models", get(list_models).post(create_model))
        .route(
            "/models/{*path}",
            get(get_model).put(update_model).delete(delete_model),
        )
}

async fn list_models(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<ModelSummary>>, AppError> {
    let config = config.lock().await;

    let models: Vec<ModelSummary> = config
        .models
        .clone()
        .into_iter()
        .map(|(name, config)| ModelSummary {
            name,
            description: config.description,
        })
        .collect();

    Ok(Json(models))
}

async fn get_model(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<ModelConfig>, AppError> {
    let config = config.lock().await;
    if let Some(model_config) = config.models.get(&name) {
        Ok(Json(model_config.clone()))
    } else {
        app_error(StatusCode::NOT_FOUND)
    }
}

async fn create_model(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(model): Json<CreateModelRequest>,
) -> Result<(), AppError> {
    let mut config = config.lock().await;

    if config.models.contains_key(&model.name) {
        return app_error(StatusCode::CONFLICT);
    }

    let dependencies = dependent_tables(&model.config.sql)
        .map_err(|_| AppError::from(anyhow::anyhow!("Failed to parse SQL")))?;

    let mut graph = Graph::load(&config.project_dir).await?;
    let deps: Vec<&str> = dependencies.iter().map(|s| s.as_str()).collect();
    graph.create_node(&model.name, &deps);
    graph.save(&config.project_dir).await?;

    let model_file = config.upsert_model(&model.name, &model.config)?;
    model_file.save()?;

    Ok(())
}

async fn update_model(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(model): Json<ModelConfig>,
) -> Result<(), AppError> {
    let mut config = config.lock().await;

    if !config.models.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    };

    let dependencies = dependent_tables(&model.sql)
        .map_err(|_| AppError::from(anyhow::anyhow!("Failed to parse SQL")))?;

    let mut graph = Graph::load(&config.project_dir).await?;
    let deps: Vec<&str> = dependencies.iter().map(|s| s.as_str()).collect();
    graph.update_dependencies(&name, &deps);
    graph.update_node(&name);
    graph.save(&config.project_dir).await?;

    let model_file = config.upsert_model(&name, &model)?;
    model_file.save()?;

    Ok(())
}

async fn delete_model(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;

    if !config.models.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    };

    let mut graph = Graph::load(&config.project_dir).await?;
    graph.delete_node(&name);
    graph.save(&config.project_dir).await?;

    let model_file = config.delete_model(&name)?;
    model_file.save()?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::TestManager;
    use anyhow::Result;
    use serde_json::json;

    #[tokio::test]
    async fn test_create_model() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        let users_adapter = crate::config::adapter::AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Users table".to_string()),
            source: crate::config::adapter::AdapterSource::Database {
                table_name: "users".to_string(),
            },
            columns: vec![],
        };

        {
            let mut config = test.config().await;
            let adapter_file = config.upsert_adapter("users", &users_adapter)?;
            adapter_file.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("users", &[]);
        graph.save(test.directory()).await?;

        let new_model = json!({
            "name": "active_users",
            "config": {
                "description": "Active users model",
                "sql": "SELECT * FROM users WHERE active = true"
            }
        });

        let response = server.post("/models").json(&new_model).await;
        response.assert_status_ok();

        let graph = Graph::load(test.directory()).await?;
        assert!(graph.has_node("active_users"));

        let upstream = graph.upstream("active_users");
        assert_eq!(upstream, vec!["users"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_model() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        let users_adapter = crate::config::adapter::AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Users table".to_string()),
            source: crate::config::adapter::AdapterSource::Database {
                table_name: "users".to_string(),
            },
            columns: vec![],
        };

        let orders_adapter = crate::config::adapter::AdapterConfig {
            connection: "test_connection".to_string(),
            description: Some("Orders table".to_string()),
            source: crate::config::adapter::AdapterSource::Database {
                table_name: "orders".to_string(),
            },
            columns: vec![],
        };

        {
            let mut config = test.config().await;
            config.upsert_adapter("users", &users_adapter)?.save()?;
            config.upsert_adapter("orders", &orders_adapter)?.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("users", &[]);
        graph.create_node("orders", &[]);
        graph.save(test.directory()).await?;

        let original_model = ModelConfig {
            description: Some("Original model".to_string()),
            sql: "SELECT * FROM users".to_string(),
        };

        {
            let mut config = test.config().await;
            let model_file = config.upsert_model("test_model", &original_model)?;
            model_file.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("test_model", &["users"]);
        graph.set_current_time("test_model");
        graph.save(test.directory()).await?;

        let updated_config = json!({
            "description": "Updated model",
            "sql": "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        });

        let response = server.put("/models/test_model").json(&updated_config).await;
        response.assert_status_ok();

        let get_response = server.get("/models/test_model").await;
        get_response.assert_status_ok();

        let model_config: ModelConfig = get_response.json();
        assert_eq!(model_config.description, Some("Updated model".to_string()));
        assert!(model_config.sql.contains("JOIN orders"));

        let graph = Graph::load(test.directory()).await?;
        assert!(graph.has_node("test_model"));

        let mut upstream = graph.upstream("test_model");
        upstream.sort();
        assert_eq!(upstream, vec!["orders", "users"]);

        assert!(graph.get_node("test_model").unwrap().updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_model() -> Result<()> {
        let test = TestManager::new();
        let server = test.setup_project(routes);

        let model_config = ModelConfig {
            description: Some("Model to delete".to_string()),
            sql: "SELECT * FROM test_table".to_string(),
        };

        {
            let mut config = test.config().await;
            let model_file = config.upsert_model("model_to_delete", &model_config)?;
            model_file.save()?;
        }

        let mut graph = Graph::load(test.directory()).await?;
        graph.create_node("model_to_delete", &[]);
        graph.save(test.directory()).await?;

        let get_response = server.get("/models/model_to_delete").await;
        get_response.assert_status_ok();

        let delete_response = server.delete("/models/model_to_delete").await;
        delete_response.assert_status(StatusCode::NO_CONTENT);

        let get_response_after = server.get("/models/model_to_delete").await;
        get_response_after.assert_status(StatusCode::NOT_FOUND);

        let graph = Graph::load(test.directory()).await?;
        assert!(!graph.has_node("model_to_delete"));

        Ok(())
    }
}
