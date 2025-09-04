use crate::config::{Config, QueryConfig};
use crate::pipeline::ducklake::DuckLake;
use crate::workspace::find_project_root;
use anyhow::Result;
use axum::{
    Router,
    extract::Path as AxumPath,
    http::StatusCode,
    response::Json,
    routing::{delete, get, post, put},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn execute_query_handler(
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match execute_query_internal(&payload.sql).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

async fn execute_query_internal(sql: &str) -> Result<(Vec<Vec<String>>, usize)> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    let ducklake = DuckLake::from_config(&config).await?;
    let results = ducklake.query(sql)?;

    let column_count = if results.is_empty() {
        0
    } else {
        results[0].len()
    };

    Ok((results, column_count))
}

pub async fn list_queries_handler()
-> Result<Json<QueryListResponse>, (StatusCode, Json<ErrorResponse>)> {
    match list_queries_internal().await {
        Ok(queries) => Ok(Json(QueryListResponse { queries })),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn save_query_handler(
    Json(payload): Json<SaveQueryRequest>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match save_query_internal(&payload.name, &payload.sql, payload.description).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' saved successfully", payload.name),
        })),
        Err(e) => {
            let status = if e.to_string().contains("already exists") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

pub async fn get_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryConfig>, (StatusCode, Json<ErrorResponse>)> {
    match get_query_internal(&name).await {
        Ok(query) => Ok(Json(query)),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn update_query_handler(
    AxumPath(name): AxumPath<String>,
    Json(payload): Json<UpdateQueryRequest>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match update_query_internal(&name, payload.sql, payload.description).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' updated successfully", name),
        })),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn delete_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<SaveQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match delete_query_internal(&name).await {
        Ok(_) => Ok(Json(SaveQueryResponse {
            message: format!("Query '{}' deleted successfully", name),
        })),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

pub async fn run_query_handler(
    AxumPath(name): AxumPath<String>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    match run_query_internal(&name).await {
        Ok((results, column_count)) => Ok(Json(QueryResponse {
            results,
            column_count,
        })),
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

async fn list_queries_internal() -> Result<HashMap<String, QueryConfig>> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;
    Ok(config.queries)
}

async fn save_query_internal(name: &str, sql: &str, description: Option<String>) -> Result<()> {
    let project_root = find_project_root()?;
    let queries_dir = project_root.join("queries");

    if !queries_dir.exists() {
        std::fs::create_dir_all(&queries_dir)?;
    }

    let query_config = QueryConfig {
        name: name.to_string(),
        description,
        sql: sql.to_string(),
    };

    let yaml_content = serde_yml::to_string(&query_config)?;
    let query_file = queries_dir.join(format!("{}.yml", name));

    if query_file.exists() {
        return Err(anyhow::anyhow!(
            "Query '{}' already exists. Use update command to modify it.",
            name
        ));
    }

    std::fs::write(&query_file, yaml_content)?;
    Ok(())
}

async fn get_query_internal(name: &str) -> Result<QueryConfig> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    config
        .queries
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found", name))
}

async fn update_query_internal(
    name: &str,
    sql: Option<String>,
    description: Option<String>,
) -> Result<()> {
    let project_root = find_project_root()?;
    let queries_dir = project_root.join("queries");
    let query_file = queries_dir.join(format!("{}.yml", name));

    if !query_file.exists() {
        return Err(anyhow::anyhow!("Query '{}' not found.", name));
    }

    let config = Config::load_from_directory(&project_root)?;
    let mut query_config = config
        .queries
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found.", name))?
        .clone();

    if let Some(new_sql) = sql {
        query_config.sql = new_sql;
    }

    if let Some(new_description) = description {
        query_config.description = Some(new_description);
    }

    let yaml_content = serde_yml::to_string(&query_config)?;
    std::fs::write(&query_file, yaml_content)?;
    Ok(())
}

async fn delete_query_internal(name: &str) -> Result<()> {
    let project_root = find_project_root()?;
    let queries_dir = project_root.join("queries");
    let query_file = queries_dir.join(format!("{}.yml", name));

    if !query_file.exists() {
        return Err(anyhow::anyhow!("Query '{}' not found.", name));
    }

    std::fs::remove_file(&query_file)?;
    Ok(())
}

async fn run_query_internal(name: &str) -> Result<(Vec<Vec<String>>, usize)> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;
    let sql = config
        .queries
        .get(name)
        .map(|query_config| query_config.sql.clone())
        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found.", name))?;
    execute_query_internal(&sql).await
}

pub fn routes() -> Router {
    Router::new()
        .route("/query", post(execute_query_handler))
        .route("/queries", get(list_queries_handler))
        .route("/queries", post(save_query_handler))
        .route("/queries/{name}", get(get_query_handler))
        .route("/queries/{name}", put(update_query_handler))
        .route("/queries/{name}", delete(delete_query_handler))
        .route("/queries/{name}/run", post(run_query_handler))
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

        std::fs::create_dir_all(&project_path).unwrap();
        std::fs::create_dir_all(project_path.join("queries")).unwrap();
        std::fs::create_dir_all(project_path.join("storage")).unwrap();

        let db_path = project_path.join("test.db");
        let storage_path = project_path.join("storage");

        let project_config = ProjectConfig {
            storage: crate::config::project::StorageConfig::LocalFile {
                path: storage_path.to_string_lossy().to_string(),
            },
            database: crate::config::project::DatabaseConfig {
                ty: crate::config::project::DatabaseType::Sqlite,
                path: Some(db_path.to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            connections: std::collections::HashMap::new(),
        };

        let yaml_content = serde_yml::to_string(&project_config).unwrap();
        std::fs::write(project_path.join("project.yml"), yaml_content).unwrap();
        crate::workspace::set_project_dir_override(project_path.clone());

        (project_config, temp_dir)
    }

    #[tokio::test]
    async fn test_execute_query_simple() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let request = json!({
            "sql": "SELECT 1 as test_column"
        });

        let response = server.post("/query").json(&request).await;

        response.assert_status_ok();
        let query_response: Value = response.json();
        assert_eq!(query_response["column_count"], 1);
        assert_eq!(query_response["results"][0][0], "1");
    }

    #[tokio::test]
    async fn test_execute_query_invalid_sql() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let request = json!({
            "sql": "INVALID SQL QUERY"
        });

        let response = server.post("/query").json(&request).await;

        response.assert_status(StatusCode::BAD_REQUEST);
        let error_response: Value = response.json();
        assert!(error_response["error"].is_string());
    }

    #[tokio::test]
    async fn test_list_queries_empty() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/queries").await;

        response.assert_status_ok();
        let queries_response: Value = response.json();
        assert!(queries_response["queries"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_save_query() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let request = json!({
            "name": "test_query",
            "sql": "SELECT 1 as result",
            "description": "Test query description"
        });

        let response = server.post("/queries").json(&request).await;

        response.assert_status_ok();
        let save_response: Value = response.json();
        assert!(
            save_response["message"]
                .as_str()
                .unwrap()
                .contains("saved successfully")
        );
    }

    #[tokio::test]
    async fn test_save_query_conflict() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let request = json!({
            "name": "conflict_query",
            "sql": "SELECT 1 as result",
            "description": "First query"
        });

        // 最初の保存
        server
            .post("/queries")
            .json(&request)
            .await
            .assert_status_ok();

        // 同じ名前で再度保存（競合）
        let response = server.post("/queries").json(&request).await;
        response.assert_status(StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_query() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        // テスト用クエリファイルを作成
        let query_content = "
name: test_query
description: Test query
sql: SELECT 42 as answer
        ";
        std::fs::write(
            temp_dir.path().join("queries/test_query.yml"),
            query_content,
        )
        .unwrap();

        let response = server.get("/queries/test_query").await;

        response.assert_status_ok();
        let query: Value = response.json();
        assert_eq!(query["description"], "Test query");
        assert_eq!(query["sql"], "SELECT 42 as answer");
    }

    #[tokio::test]
    async fn test_get_query_not_found() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let response = server.get("/queries/nonexistent").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_query() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        // テスト用クエリファイルを作成
        let query_content = "
name: update_query
description: Original query
sql: SELECT 1 as original
        ";
        std::fs::write(
            temp_dir.path().join("queries/update_query.yml"),
            query_content,
        )
        .unwrap();

        let request = json!({
            "sql": "SELECT 2 as updated",
            "description": "Updated query"
        });

        let response = server.put("/queries/update_query").json(&request).await;

        response.assert_status_ok();
        let update_response: Value = response.json();
        assert!(
            update_response["message"]
                .as_str()
                .unwrap()
                .contains("updated successfully")
        );
    }

    #[tokio::test]
    async fn test_delete_query() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        // テスト用クエリファイルを作成
        let query_content = "
description: Query to delete
sql: SELECT 'delete me' as message
        ";
        std::fs::write(
            temp_dir.path().join("queries/delete_query.yml"),
            query_content,
        )
        .unwrap();

        let response = server.delete("/queries/delete_query").await;
        response.assert_status_ok();

        // 削除後に取得を試行すると404になることを確認
        let response = server.get("/queries/delete_query").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_run_saved_query() {
        let (_config, temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        // テスト用クエリファイルを作成
        let query_content = "
name: run_query
description: Test query to run
sql: SELECT 'Hello World' as greeting, 42 as number
        ";
        std::fs::write(temp_dir.path().join("queries/run_query.yml"), query_content).unwrap();

        let response = server.post("/queries/run_query/run").await;

        response.assert_status_ok();
        let query_response: Value = response.json();
        assert_eq!(query_response["column_count"], 2);
        assert_eq!(query_response["results"][0][0], "Hello World");
        assert_eq!(query_response["results"][0][1], "42");
    }

    #[tokio::test]
    async fn test_run_nonexistent_query() {
        let (_config, _temp_dir) = setup_test_project();
        let server = create_test_server(routes);

        let response = server.post("/queries/nonexistent/run").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }
}
