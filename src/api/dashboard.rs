use axum::{
    Router,
    extract::Path,
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::commands::run::connect_ducklake;
use crate::commands::workspace::ensure_project_directory;
use crate::config::Config;
use crate::config::dashboard::{DashboardConfig, parse_dashboard_config};

pub fn router() -> Router<()> {
    Router::new()
        .route("/dashboards", get(list_dashboards))
        .route("/dashboards", post(create_dashboard))
        .route("/dashboards/{name}", get(get_dashboard))
        .route("/dashboards/{name}", put(update_dashboard))
        .route("/dashboards/{name}", delete(delete_dashboard))
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

async fn list_dashboards() -> Result<Json<Vec<DashboardListItem>>, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboards_dir = project_root.join("dashboards");
    let mut dashboards = Vec::new();

    if dashboards_dir.exists() {
        let entries =
            fs::read_dir(&dashboards_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        for entry in entries {
            let entry = entry.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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

async fn get_dashboard(Path(name): Path<String>) -> Result<Json<DashboardConfig>, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboard_path = project_root
        .join("dashboards")
        .join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let config =
        load_dashboard_config(&dashboard_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(config))
}

async fn create_dashboard(
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboards_dir = project_root.join("dashboards");
    fs::create_dir_all(&dashboards_dir).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let dashboard_path = dashboards_dir.join(format!("{}.yml", request.name));

    if dashboard_path.exists() {
        return Err(StatusCode::CONFLICT);
    }

    let config = DashboardConfig {
        name: request.name,
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => crate::config::dashboard::ChartType::Line,
                "bar" => crate::config::dashboard::ChartType::Bar,
                _ => return Err(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let yaml_content =
        serde_yml::to_string(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    fs::write(&dashboard_path, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(config))
}

async fn update_dashboard(
    Path(name): Path<String>,
    Json(request): Json<DashboardRequest>,
) -> Result<Json<DashboardConfig>, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboard_path = project_root
        .join("dashboards")
        .join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let config = DashboardConfig {
        name: request.name,
        description: request.description,
        query: request.query,
        chart: crate::config::dashboard::ChartConfig {
            chart_type: match request.chart.chart_type.as_str() {
                "line" => crate::config::dashboard::ChartType::Line,
                "bar" => crate::config::dashboard::ChartType::Bar,
                _ => return Err(StatusCode::BAD_REQUEST),
            },
            x_column: request.chart.x_column,
            y_column: request.chart.y_column,
        },
    };

    let yaml_content =
        serde_yml::to_string(&config).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    fs::write(&dashboard_path, yaml_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(config))
}

async fn delete_dashboard(Path(name): Path<String>) -> Result<StatusCode, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboard_path = project_root
        .join("dashboards")
        .join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    fs::remove_file(&dashboard_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_dashboard_data(
    Path(name): Path<String>,
) -> Result<Json<DashboardDataResponse>, StatusCode> {
    let current_dir = env::current_dir().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let project_root =
        ensure_project_directory(Some(&current_dir)).map_err(|_| StatusCode::NOT_FOUND)?;

    let dashboard_path = project_root
        .join("dashboards")
        .join(format!("{}.yml", name));

    if !dashboard_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let dashboard_config =
        load_dashboard_config(&dashboard_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let queries_dir = project_root.join("queries");
    let query_path = queries_dir.join(format!("{}.yml", dashboard_config.query));

    if !query_path.exists() {
        return Err(StatusCode::NOT_FOUND);
    }

    let query_content =
        fs::read_to_string(&query_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let query_config: crate::config::QueryConfig =
        serde_yml::from_str(&query_content).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 既存のクエリ実行機能を使用
    let config = Config::load_from_directory(&project_root)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let ducklake = connect_ducklake(&config)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let describe_sql = format!("DESCRIBE ({})", query_config.sql);
    let describe_results = ducklake
        .query(&describe_sql)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
        return Err(StatusCode::BAD_REQUEST);
    }

    let x_idx = x_column_index.unwrap();
    let y_idx = y_column_index.unwrap();

    let query_results = ducklake
        .query(&query_config.sql)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
