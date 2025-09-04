use crate::config::model::{ModelConfig, parse_model_config};
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
pub struct ModelSummary {
    pub name: String,
    pub path: String,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ModelDetails {
    pub name: String,
    pub path: String,
    pub config: ModelConfig,
}

#[derive(Deserialize)]
pub struct CreateModelRequest {
    pub name: String,
    pub path: String,
    pub config: ModelConfig,
}

#[derive(Deserialize)]
pub struct UpdateModelRequest {
    pub config: ModelConfig,
}

fn models_dir() -> Result<PathBuf, StatusCode> {
    let project_root = find_project_root().map_err(|err| {
        error!(error = ?err, "Failed to find project root");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(project_root.join("models"))
}

pub fn routes() -> Router {
    Router::new()
        .route("/models", get(list_models).post(create_model))
        .route(
            "/models/{*path}",
            get(get_model).put(update_model).delete(delete_model),
        )
}

async fn list_models() -> Result<Json<Vec<ModelSummary>>, StatusCode> {
    let models_dir = models_dir()?;

    if !models_dir.exists() {
        return Ok(Json(vec![]));
    }

    let mut models = Vec::new();
    collect_models(&models_dir, &models_dir, &mut models)?;

    models.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Json(models))
}

fn collect_models(
    base_dir: &PathBuf,
    current_dir: &PathBuf,
    models: &mut Vec<ModelSummary>,
) -> Result<(), StatusCode> {
    let entries = fs::read_dir(current_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for entry in entries {
        let entry = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let path = entry.path();

        if path.is_dir() {
            collect_models(base_dir, &path, models)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("yml")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
        {
            let relative_path = path
                .strip_prefix(base_dir)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let model_path = relative_path.to_string_lossy().replace(".yml", "");

            if let Ok(content) = fs::read_to_string(&path)
                && let Ok(config) = parse_model_config(&content)
            {
                models.push(ModelSummary {
                    name: stem.to_string(),
                    path: model_path,
                    description: config.description,
                });
            }
        }
    }

    Ok(())
}

async fn get_model(Path(model_path): Path<String>) -> Result<Json<ModelDetails>, StatusCode> {
    let models_dir = models_dir()?;
    let model_file = models_dir.join(format!("{model_path}.yml"));

    if !model_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let content = fs::read_to_string(&model_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let config = parse_model_config(&content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let name = PathBuf::from(&model_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&model_path)
        .to_string();

    Ok(Json(ModelDetails {
        name,
        path: model_path,
        config,
    }))
}

async fn create_model(
    Json(req): Json<CreateModelRequest>,
) -> Result<Json<ModelDetails>, StatusCode> {
    let models_dir = models_dir()?;
    let model_file = models_dir.join(format!("{}.yml", req.path));

    if model_file.exists() {
        return Err(StatusCode::CONFLICT);
    }

    if let Some(parent) = model_file.parent() {
        fs::create_dir_all(parent).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&model_file, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(ModelDetails {
        name: req.name,
        path: req.path,
        config: req.config,
    }))
}

async fn update_model(
    Path(model_path): Path<String>,
    Json(req): Json<UpdateModelRequest>,
) -> Result<Json<ModelDetails>, StatusCode> {
    let models_dir = models_dir()?;
    let model_file = models_dir.join(format!("{model_path}.yml"));

    if !model_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let yaml_content =
        serde_yml::to_string(&req.config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    fs::write(&model_file, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let name = PathBuf::from(&model_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&model_path)
        .to_string();

    Ok(Json(ModelDetails {
        name,
        path: model_path,
        config: req.config,
    }))
}

async fn delete_model(Path(model_path): Path<String>) -> Result<StatusCode, StatusCode> {
    let models_dir = models_dir()?;
    let model_file = models_dir.join(format!("{model_path}.yml"));

    if !model_file.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    fs::remove_file(&model_file).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    async fn test_list_models_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/models").await;

        response.assert_status_ok();
        let models: Value = response.json();
        assert!(models.is_array());
    }

    #[tokio::test]
    async fn test_get_model_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/models/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_model_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let new_model = json!({
            "name": "test_model",
            "path": "staging/test_model",
            "config": {
                "description": "Test SQL model",
                "sql": "SELECT * FROM test_table"
            }
        });

        let response = server.post("/models").json(&new_model).await;

        response.assert_status_ok();
        let model: Value = response.json();
        assert_eq!(model["name"], "test_model");
        assert_eq!(model["path"], "staging/test_model");
    }

    #[tokio::test]
    async fn test_create_model_conflict() {
        setup_test_project();
        let server = create_test_server(routes);

        let model_config = json!({
            "name": "duplicate_model",
            "path": "staging/duplicate_model",
            "config": {
                "description": "Duplicate model",
                "sql": "SELECT * FROM test_table"
            }
        });

        let _first_response = server.post("/models").json(&model_config).await;

        let second_response = server.post("/models").json(&model_config).await;

        second_response.assert_status(StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_model_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let create_model = json!({
            "name": "update_test_model",
            "path": "staging/update_test_model",
            "config": {
                "description": "Original description",
                "sql": "SELECT * FROM original_table"
            }
        });

        server.post("/models").json(&create_model).await;

        let update_config = json!({
            "config": {
                "description": "Updated description",
                "sql": "SELECT * FROM updated_table"
            }
        });

        let response = server
            .put("/models/staging/update_test_model")
            .json(&update_config)
            .await;

        response.assert_status_ok();
        let model: Value = response.json();
        assert_eq!(model["config"]["description"], "Updated description");
    }

    #[tokio::test]
    async fn test_update_model_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let update_config = json!({
            "config": {
                "description": "Updated description",
                "sql": "SELECT * FROM updated_table"
            }
        });

        let response = server
            .put("/models/staging/nonexistent")
            .json(&update_config)
            .await;

        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_model_success() {
        setup_test_project();
        let server = create_test_server(routes);

        let create_model = json!({
            "name": "delete_test_model",
            "path": "staging/delete_test_model",
            "config": {
                "description": "To be deleted",
                "sql": "SELECT * FROM delete_table"
            }
        });

        server.post("/models").json(&create_model).await;

        let response = server.delete("/models/staging/delete_test_model").await;

        response.assert_status(StatusCode::NO_CONTENT);

        let get_response = server.get("/models/staging/delete_test_model").await;
        get_response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_model_not_found() {
        setup_test_project();
        let server = create_test_server(routes);

        let response = server.delete("/models/staging/nonexistent").await;

        response.assert_status(StatusCode::NOT_FOUND);
    }
}
