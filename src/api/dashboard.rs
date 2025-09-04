use crate::api::{AppError, app_error};
use crate::config::Config;
use crate::config::dashboard::{DashboardConfig, parse_dashboard_config};
use crate::pipeline::ducklake::DuckLake;
use crate::workspace::find_project_root;
use axum::{
    Router,
    extract::Path,
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::error;

pub fn router() -> Router<()> {
    Router::new()
        .route("/dashboards", get(list_dashboards))
        .route("/dashboards", post(create_dashboard))
        .route("/dashboards/{name}", get(get_dashboard))
        .route("/dashboards/{name}", put(update_dashboard))
        .route("/dashboards/{name}", delete(delete_dashboard))
        .route("/dashboards/{name}/data", get(get_dashboard_data))
}

fn dashboards_dir() -> Result<PathBuf, AppError> {
    let project_root = find_project_root().map_err(|err| {
        error!(error = ?err, "Failed to find project root");
        AppError::StatusCode(StatusCode::INTERNAL_SERVER_ERROR)
    })?;
    Ok(project_root.join("dashboards"))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardListItem {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    pub chart_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardRequest {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    pub chart: ChartRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChartRequest {
    #[serde(rename = "type")]
    pub chart_type: String,
    pub x_column: String,
    pub y_column: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DashboardDataResponse {
    pub labels: Vec<serde_json::Value>,
    pub values: Vec<serde_json::Value>,
}

async fn list_dashboards() -> Result<Json<Vec<DashboardListItem>>, AppError> {
    let dashboards_dir = dashboards_dir()?;
    let mut dashboards = Vec::new();

    if dashboards_dir.exists() {
        let entries = fs::read_dir(&dashboards_dir)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("yml")
                && let Some(name) = path.file_stem().and_then(|s| s.to_str())
            {
                match load_dashboard_config(&path) {
                    Ok(config) => {
                        dashboards.push(DashboardListItem {
                            name: name.to_string(),
                            description: config.description,
                            query: config.query,
                            chart_type: match config.chart.chart_type {
                                crate::config::dashboard::ChartType::Line => "line".to_string(),
                                crate::config::dashboard::ChartType::Bar => "bar".to_string(),
                            },
                        });
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    Ok(Json(dashboards))
}

async fn get_dashboard(Path(name): Path<String>) -> Result<Json<DashboardConfig>, AppError> {
    let dashboards_dir = dashboards_dir()?;
    let dashboard_path = dashboards_dir.join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return app_error(StatusCode::NOT_FOUND);
    }

    let config = load_dashboard_config(&dashboard_path)?;

    Ok(Json(config))
}

async fn create_dashboard(
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, AppError> {
    let dashboards_dir = dashboards_dir()?;
    fs::create_dir_all(&dashboards_dir)?;

    let dashboard_path = dashboards_dir.join(format!("{}.yml", request.name));

    if dashboard_path.exists() {
        return app_error(StatusCode::CONFLICT);
    }

    let config = DashboardConfig {
        name: request.name,
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => crate::config::dashboard::ChartType::Line,
                "bar" => crate::config::dashboard::ChartType::Bar,
                _ => return app_error(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let yaml_content = serde_yml::to_string(&config)?;

    fs::write(&dashboard_path, yaml_content)?;

    Ok(Json(config))
}

async fn update_dashboard(
    Path(name): Path<String>,
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, AppError> {
    let dashboards_dir = dashboards_dir()?;
    let dashboard_path = dashboards_dir.join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return app_error(StatusCode::NOT_FOUND);
    }

    let config = DashboardConfig {
        name: request.name,
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => crate::config::dashboard::ChartType::Line,
                "bar" => crate::config::dashboard::ChartType::Bar,
                _ => return app_error(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let yaml_content = serde_yml::to_string(&config)?;

    fs::write(&dashboard_path, yaml_content)?;

    Ok(Json(config))
}

async fn delete_dashboard(Path(name): Path<String>) -> Result<StatusCode, AppError> {
    let dashboards_dir = dashboards_dir()?;
    let dashboard_path = dashboards_dir.join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return app_error(StatusCode::NOT_FOUND);
    }

    fs::remove_file(&dashboard_path)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_dashboard_data(
    Path(name): Path<String>,
) -> Result<Json<DashboardDataResponse>, AppError> {
    let project_root = find_project_root()?;
    let dashboards_dir = project_root.join("dashboards");
    let dashboard_path = dashboards_dir.join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return app_error(StatusCode::NOT_FOUND);
    }

    let dashboard_config = load_dashboard_config(&dashboard_path)?;

    let queries_dir = project_root.join("queries");
    let query_path = queries_dir.join(format!("{}.yml", dashboard_config.query));

    if !query_path.exists() {
        return app_error(StatusCode::NOT_FOUND);
    }

    let query_content = fs::read_to_string(&query_path)?;

    let query_config: crate::config::QueryConfig = serde_yml::from_str(&query_content)?;

    let config = Config::load_from_directory(&project_root)?;

    let ducklake = DuckLake::from_config(&config).await?;

    let describe_sql = format!("DESCRIBE ({})", query_config.sql);
    let describe_results = ducklake.query(&describe_sql)?;

    let mut x_column_index = None;
    let mut y_column_index = None;

    for (idx, describe_row) in describe_results.iter().enumerate() {
        if !describe_row.is_empty() {
            let column_name = &describe_row[0];
            if column_name == &dashboard_config.chart.x_column {
                x_column_index = Some(idx);
            }
            if column_name == &dashboard_config.chart.y_column {
                y_column_index = Some(idx);
            }
        }
    }

    if x_column_index.is_none() || y_column_index.is_none() {
        return app_error(StatusCode::BAD_REQUEST);
    }

    let x_idx = x_column_index.unwrap();
    let y_idx = y_column_index.unwrap();

    let query_results = ducklake.query(&query_config.sql)?;

    let mut labels = Vec::new();
    let mut values = Vec::new();

    for row in query_results {
        if row.len() > x_idx && row.len() > y_idx {
            labels.push(serde_json::Value::String(row[x_idx].clone()));
            if let Ok(num) = row[y_idx].parse::<f64>() {
                values.push(serde_json::Value::Number(
                    serde_json::Number::from_f64(num)
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                ));
            } else {
                values.push(serde_json::Value::String(row[y_idx].clone()));
            }
        }
    }

    Ok(Json(DashboardDataResponse { labels, values }))
}

fn load_dashboard_config(path: &PathBuf) -> anyhow::Result<DashboardConfig> {
    let content = fs::read_to_string(path)?;
    parse_dashboard_config(&content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProjectConfig;
    use crate::test_helpers::create_test_server;
    use serde_json::{Value, json};
    use tempfile;

    fn setup_test_project() -> (ProjectConfig, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path().to_path_buf();

        // プロジェクト構造を作成
        std::fs::create_dir_all(&project_path).unwrap();
        std::fs::create_dir_all(project_path.join("dashboards")).unwrap();
        std::fs::create_dir_all(project_path.join("queries")).unwrap();
        std::fs::write(
            project_path.join("project.yml"),
            "
storage:
  type: local
  path: ./storage
database:
  type: sqlite
  path: ./test.db
connections: {}
        ",
        )
        .unwrap();

        // thread_localのPROJECT_DIR_OVERRIDEを設定（スレッド安全）
        crate::workspace::set_project_dir_override(project_path.clone());

        let config = ProjectConfig {
            storage: crate::config::project::StorageConfig::LocalFile {
                path: project_path.join("storage").to_string_lossy().to_string(),
            },
            database: crate::config::project::DatabaseConfig {
                ty: crate::config::project::DatabaseType::Sqlite,
                path: Some(project_path.join("test.db").to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            connections: std::collections::HashMap::new(),
        };

        (config, temp_dir)
    }

    #[tokio::test]
    async fn test_list_dashboards_empty() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(router);

        let response = server.get("/dashboards").await;

        response.assert_status_ok();
        let dashboards: Value = response.json();
        assert!(dashboards.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_create_dashboard() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(router);

        let request = json!({
            "name": "test_dashboard",
            "description": "Test dashboard",
            "query": "test_query",
            "chart": {
                "type": "line",
                "x_column": "date",
                "y_column": "value"
            }
        });

        let response = server.post("/dashboards").json(&request).await;

        response.assert_status_ok();
        let dashboard: Value = response.json();
        assert_eq!(dashboard["name"], "test_dashboard");
        assert_eq!(dashboard["description"], "Test dashboard");
    }

    #[tokio::test]
    async fn test_create_dashboard_conflict() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(router);

        let request = json!({
            "name": "conflict_dashboard",
            "description": "Test dashboard",
            "query": "test_query",
            "chart": {
                "type": "line",
                "x_column": "date",
                "y_column": "value"
            }
        });

        // 最初の作成
        server
            .post("/dashboards")
            .json(&request)
            .await
            .assert_status_ok();

        // 同じ名前で再度作成（競合）
        let response = server.post("/dashboards").json(&request).await;
        response.assert_status(StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_dashboard() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(router);

        // テスト用ダッシュボードファイルを作成
        let dashboard_content = "
name: get_dashboard
description: Test dashboard
query: test_query
chart:
  type: line
  x_column: date
  y_column: value
        ";
        std::fs::write(
            temp_dir.path().join("dashboards/get_dashboard.yml"),
            dashboard_content,
        )
        .unwrap();

        let response = server.get("/dashboards/get_dashboard").await;

        response.assert_status_ok();
        let dashboard: Value = response.json();
        assert_eq!(dashboard["name"], "get_dashboard");
        assert_eq!(dashboard["description"], "Test dashboard");
    }

    #[tokio::test]
    async fn test_get_dashboard_not_found() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(router);

        let response = server.get("/dashboards/nonexistent").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_dashboard() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(router);

        // テスト用ダッシュボードファイルを作成
        let dashboard_content = "
name: update_dashboard
description: Test dashboard
query: test_query
chart:
  type: line
  x_column: date
  y_column: value
        ";
        std::fs::write(
            temp_dir.path().join("dashboards/update_dashboard.yml"),
            dashboard_content,
        )
        .unwrap();

        let request = json!({
            "name": "update_dashboard",
            "description": "Updated dashboard",
            "query": "updated_query",
            "chart": {
                "type": "bar",
                "x_column": "category",
                "y_column": "count"
            }
        });

        let response = server
            .put("/dashboards/update_dashboard")
            .json(&request)
            .await;

        response.assert_status_ok();
        let dashboard: Value = response.json();
        assert_eq!(dashboard["description"], "Updated dashboard");
        assert_eq!(dashboard["query"], "updated_query");
    }

    #[tokio::test]
    async fn test_delete_dashboard() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(router);

        // テスト用ダッシュボードファイルを作成
        let dashboard_content = "
name: delete_dashboard
description: Test dashboard
query: test_query
chart:
  type: line
  x_column: date
  y_column: value
        ";
        std::fs::write(
            temp_dir.path().join("dashboards/delete_dashboard.yml"),
            dashboard_content,
        )
        .unwrap();

        let response = server.delete("/dashboards/delete_dashboard").await;
        response.assert_status(StatusCode::NO_CONTENT);

        // 削除後に取得を試行すると404になることを確認
        let response = server.get("/dashboards/delete_dashboard").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_invalid_chart_type() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(router);

        let request = json!({
            "name": "test_dashboard",
            "description": "Test dashboard",
            "query": "test_query",
            "chart": {
                "type": "invalid_type",
                "x_column": "date",
                "y_column": "value"
            }
        });

        let response = server.post("/dashboards").json(&request).await;
        response.assert_status(StatusCode::BAD_REQUEST);
    }
}
