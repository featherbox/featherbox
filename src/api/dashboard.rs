use crate::api::{AppError, app_error};
use crate::config::Config;
use crate::config::dashboard::{ChartType, DashboardConfig};
use crate::pipeline::ducklake::DuckLake;
use axum::{Extension, Router, extract::Path, http::StatusCode, response::Json, routing::get};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn router() -> Router {
    Router::new()
        .route("/dashboards", get(list_dashboards).post(create_dashboard))
        .route(
            "/dashboards/{name}",
            get(get_dashboard)
                .put(update_dashboard)
                .delete(delete_dashboard),
        )
        .route("/dashboards/{name}/data", get(get_dashboard_data))
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

async fn list_dashboards(
    Extension(config): Extension<Arc<Mutex<Config>>>,
) -> Result<Json<Vec<DashboardListItem>>, AppError> {
    let config = config.lock().await;
    let dashboards: Vec<DashboardListItem> = config
        .dashboards
        .iter()
        .map(|(name, dashboard_config)| DashboardListItem {
            name: name.clone(),
            description: dashboard_config.description.clone(),
            query: dashboard_config.query.clone(),
            chart_type: match dashboard_config.chart.chart_type {
                ChartType::Line => "line".to_string(),
                ChartType::Bar => "bar".to_string(),
            },
        })
        .collect();
    Ok(Json(dashboards))
}

async fn get_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<DashboardConfig>, AppError> {
    let config = config.lock().await;
    if let Some(dashboard_config) = config.dashboards.get(&name) {
        Ok(Json(dashboard_config.clone()))
    } else {
        app_error(StatusCode::NOT_FOUND)
    }
}

async fn create_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, AppError> {
    let mut config = config.lock().await;
    if config.dashboards.contains_key(&request.name) {
        return app_error(StatusCode::CONFLICT);
    }

    let new_dashboard = DashboardConfig {
        name: request.name.clone(),
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => ChartType::Line,
                "bar" => ChartType::Bar,
                _ => return app_error(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let dashboard_file = config.upsert_dashboard(&request.name, &new_dashboard)?;
    dashboard_file.save()?;

    Ok(Json(new_dashboard))
}

async fn update_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, AppError> {
    let mut config = config.lock().await;
    if !config.dashboards.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    }

    let updated_dashboard = DashboardConfig {
        name: request.name,
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => ChartType::Line,
                "bar" => ChartType::Bar,
                _ => return app_error(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let dashboard_file = config.upsert_dashboard(&name, &updated_dashboard)?;
    dashboard_file.save()?;

    Ok(Json(updated_dashboard))
}

async fn delete_dashboard(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let mut config = config.lock().await;
    if !config.dashboards.contains_key(&name) {
        return app_error(StatusCode::NOT_FOUND);
    }

    let dashboard_file = config.delete_dashboard(&name)?;
    dashboard_file.save()?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_dashboard_data(
    Extension(config): Extension<Arc<Mutex<Config>>>,
    Path(name): Path<String>,
) -> Result<Json<DashboardDataResponse>, AppError> {
    let config = config.lock().await;
    let dashboard_config = match config.dashboards.get(&name) {
        Some(c) => c,
        None => return app_error(StatusCode::NOT_FOUND),
    };

    let query_config = match config.queries.get(&dashboard_config.query) {
        Some(q) => q,
        None => return app_error(StatusCode::NOT_FOUND),
    };

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

    let x_idx = x_column_index.ok_or(AppError::StatusCode(StatusCode::BAD_REQUEST))?;
    let y_idx = y_column_index.ok_or(AppError::StatusCode(StatusCode::BAD_REQUEST))?;

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
//         std::fs::create_dir_all(project_path.join("dashboards")).unwrap();
//         std::fs::create_dir_all(project_path.join("queries")).unwrap();
//         std::fs::write(
//             project_path.join("project.yml"),
//             "
// storage:
//   type: local
//   path: ./storage
// database:
//   type: sqlite
//   path: ./test.db
// connections: {}
//         ",
//         )
//         .unwrap();
//
//         crate::workspace::set_project_dir(project_path.clone());
//
//         let config = ProjectConfig {
//             storage: crate::config::project::StorageConfig::LocalFile {
//                 path: project_path.join("storage").to_string_lossy().to_string(),
//             },
//             database: crate::config::project::DatabaseConfig {
//                 ty: crate::config::project::DatabaseType::Sqlite,
//                 path: Some(project_path.join("test.db").to_string_lossy().to_string()),
//                 host: None,
//                 port: None,
//                 database: None,
//                 username: None,
//                 password: None,
//             },
//             connections: std::collections::HashMap::new(),
//         };
//
//         (config, temp_dir)
//     }
//
//     #[tokio::test]
//     async fn test_list_dashboards_empty() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         let response = server.get("/dashboards").await;
//
//         response.assert_status_ok();
//         let dashboards: Value = response.json();
//         assert!(dashboards.as_array().unwrap().is_empty());
//     }
//
//     #[tokio::test]
//     async fn test_create_dashboard() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         let request = json!({
//             "name": "test_dashboard",
//             "description": "Test dashboard",
//             "query": "test_query",
//             "chart": {
//                 "type": "line",
//                 "x_column": "date",
//                 "y_column": "value"
//             }
//         });
//
//         let response = server.post("/dashboards").json(&request).await;
//
//         response.assert_status_ok();
//         let dashboard: Value = response.json();
//         assert_eq!(dashboard["name"], "test_dashboard");
//         assert_eq!(dashboard["description"], "Test dashboard");
//     }
//
//     #[tokio::test]
//     async fn test_create_dashboard_conflict() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         let request = json!({
//             "name": "conflict_dashboard",
//             "description": "Test dashboard",
//             "query": "test_query",
//             "chart": {
//                 "type": "line",
//                 "x_column": "date",
//                 "y_column": "value"
//             }
//         });
//
//         // 最初の作成
//         server
//             .post("/dashboards")
//             .json(&request)
//             .await
//             .assert_status_ok();
//
//         // 同じ名前で再度作成（競合）
//         let response = server.post("/dashboards").json(&request).await;
//         response.assert_status(StatusCode::CONFLICT);
//     }
//
//     #[tokio::test]
//     async fn test_get_dashboard() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         // テスト用ダッシュボードファイルを作成
//         let dashboard_content = "
// name: get_dashboard
// description: Test dashboard
// query: test_query
// chart:
//   type: line
//   x_column: date
//   y_column: value
//         ";
//         std::fs::write(
//             temp_dir.path().join("dashboards/get_dashboard.yml"),
//             dashboard_content,
//         )
//         .unwrap();
//
//         let response = server.get("/dashboards/get_dashboard").await;
//
//         response.assert_status_ok();
//         let dashboard: Value = response.json();
//         assert_eq!(dashboard["name"], "get_dashboard");
//         assert_eq!(dashboard["description"], "Test dashboard");
//     }
//
//     #[tokio::test]
//     async fn test_get_dashboard_not_found() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         let response = server.get("/dashboards/nonexistent").await;
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_update_dashboard() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         // テスト用ダッシュボードファイルを作成
//         let dashboard_content = "
// name: update_dashboard
// description: Test dashboard
// query: test_query
// chart:
//   type: line
//   x_column: date
//   y_column: value
//         ";
//         std::fs::write(
//             temp_dir.path().join("dashboards/update_dashboard.yml"),
//             dashboard_content,
//         )
//         .unwrap();
//
//         let request = json!({
//             "name": "update_dashboard",
//             "description": "Updated dashboard",
//             "query": "updated_query",
//             "chart": {
//                 "type": "bar",
//                 "x_column": "category",
//                 "y_column": "count"
//             }
//         });
//
//         let response = server
//             .put("/dashboards/update_dashboard")
//             .json(&request)
//             .await;
//
//         response.assert_status_ok();
//         let dashboard: Value = response.json();
//         assert_eq!(dashboard["description"], "Updated dashboard");
//         assert_eq!(dashboard["query"], "updated_query");
//     }
//
//     #[tokio::test]
//     async fn test_delete_dashboard() {
//         let (_config, temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         // テスト用ダッシュボードファイルを作成
//         let dashboard_content = "
// name: delete_dashboard
// description: Test dashboard
// query: test_query
// chart:
//   type: line
//   x_column: date
//   y_column: value
//         ";
//         std::fs::write(
//             temp_dir.path().join("dashboards/delete_dashboard.yml"),
//             dashboard_content,
//         )
//         .unwrap();
//
//         let response = server.delete("/dashboards/delete_dashboard").await;
//         response.assert_status(StatusCode::NO_CONTENT);
//
//         // 削除後に取得を試行すると404になることを確認
//         let response = server.get("/dashboards/delete_dashboard").await;
//         response.assert_status(StatusCode::NOT_FOUND);
//     }
//
//     #[tokio::test]
//     async fn test_invalid_chart_type() {
//         let (_config, _temp_dir) = setup_test_project();
//         let server = create_test_server(router);
//
//         let request = json!({
//             "name": "test_dashboard",
//             "description": "Test dashboard",
//             "query": "test_query",
//             "chart": {
//                 "type": "invalid_type",
//                 "x_column": "date",
//                 "y_column": "value"
//             }
//         });
//
//         let response = server.post("/dashboards").json(&request).await;
//         response.assert_status(StatusCode::BAD_REQUEST);
//     }
// }
