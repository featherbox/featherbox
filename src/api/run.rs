use crate::{
    config::Config,
    database::{
        connect_app_db,
        entities::{pipeline_actions, pipelines},
    },
    dependency::Graph,
    pipeline::{build::Pipeline, ducklake::DuckLake},
    workspace::find_project_root,
};
use anyhow::Result;
use axum::{Router, extract::Path as AxumPath, http::StatusCode, response::Json, routing::post};
use sea_orm::DatabaseConnection;
use sea_orm::{ActiveModelTrait, NotSet, Set};
use serde::{Deserialize, Serialize};
use tokio::task;
use tracing::error;

#[derive(Serialize, Deserialize)]
pub struct RunRequest {
    pub project_path: String,
}

#[derive(Serialize, Deserialize)]
pub struct RunResponse {
    pub success: bool,
    pub message: String,
    pub pipeline_id: Option<i32>,
}

async fn save_pipeline(
    app_db: &DatabaseConnection,
    graph_id: i32,
    pipeline: &Pipeline,
) -> Result<()> {
    let pipeline_model = pipelines::ActiveModel {
        id: NotSet,
        graph_id: Set(graph_id),
        created_at: Set(chrono::Utc::now().naive_utc()),
        status: Set("PENDING".to_string()),
        started_at: NotSet,
        completed_at: NotSet,
    };
    let saved_pipeline = pipeline_model.insert(app_db).await?;
    let pipeline_id = saved_pipeline.id;

    let mut action_ids = Vec::new();
    let all_actions = pipeline.all_actions();
    for (execution_order, action) in all_actions.iter().enumerate() {
        let action_model = pipeline_actions::ActiveModel {
            id: NotSet,
            pipeline_id: Set(pipeline_id),
            table_name: Set(action.table_name.clone()),
            execution_order: Set(execution_order as i32),
            status: Set("PENDING".to_string()),
            started_at: NotSet,
            completed_at: NotSet,
            error_message: NotSet,
        };
        let saved_action = action_model.insert(app_db).await?;
        action_ids.push(saved_action.id);
    }

    Ok(())
}

pub fn execute_with_path(_project_path: &str) -> Result<i32> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { execute_run_internal(None).await })
}

pub fn execute_with_target_node(_project_path: &str, target_node: &str) -> Result<i32> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { execute_run_internal(Some(target_node.to_string())).await })
}

async fn execute_run_internal(target_node: Option<String>) -> Result<i32> {
    let project_root = find_project_root()?;
    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        return Err(anyhow::anyhow!("No adapters or models found"));
    }

    let app_db = connect_app_db(&config.project).await?;
    let graph_id = crate::dependency::latest_graph_id(&app_db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No graph found. Run migrate first"))?;

    let current_graph = Graph::from_config(&config)?;
    let ducklake = DuckLake::from_config(&config).await?;

    let pipeline = if let Some(target) = target_node {
        let execution_graph = create_execution_subgraph(&current_graph, &target)?;
        Pipeline::from_graph(&execution_graph)
    } else {
        Pipeline::from_graph(&current_graph)
    };

    save_pipeline(&app_db, graph_id, &pipeline).await?;

    let pipeline_id = get_latest_pipeline_id(&app_db).await?;

    pipeline
        .execute(&current_graph, &config, &ducklake, &app_db)
        .await?;

    Ok(pipeline_id)
}

fn create_execution_subgraph(graph: &Graph, target_node: &str) -> Result<Graph> {
    use std::collections::{HashSet, VecDeque};

    if !graph.nodes.iter().any(|n| n.name == target_node) {
        return Err(anyhow::anyhow!(
            "Target node '{}' not found in graph",
            target_node
        ));
    }

    let mut upstream_nodes = HashSet::new();
    let mut queue: VecDeque<String> = vec![target_node.to_string()].into();

    while let Some(current_node) = queue.pop_front() {
        if upstream_nodes.contains(&current_node) {
            continue;
        }
        upstream_nodes.insert(current_node.clone());

        for edge in &graph.edges {
            if edge.to == current_node && !upstream_nodes.contains(&edge.from) {
                queue.push_back(edge.from.clone());
            }
        }
    }

    let filtered_nodes = graph
        .nodes
        .iter()
        .filter(|node| upstream_nodes.contains(&node.name))
        .cloned()
        .collect();

    let filtered_edges = graph
        .edges
        .iter()
        .filter(|edge| upstream_nodes.contains(&edge.from) && upstream_nodes.contains(&edge.to))
        .cloned()
        .collect();

    Ok(Graph {
        nodes: filtered_nodes,
        edges: filtered_edges,
    })
}

async fn get_latest_pipeline_id(app_db: &DatabaseConnection) -> Result<i32> {
    use crate::database::entities::pipelines;
    use sea_orm::{EntityTrait, QueryOrder};

    let latest_pipeline = pipelines::Entity::find()
        .order_by_desc(pipelines::Column::CreatedAt)
        .one(app_db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No pipeline found"))?;

    Ok(latest_pipeline.id)
}

pub fn routes() -> Router {
    Router::new()
        .route("/run", post(handle_run))
        .route("/run-nodes/{node_name}", post(handle_run_node))
}

async fn handle_run(Json(request): Json<RunRequest>) -> Result<Json<RunResponse>, StatusCode> {
    let project_path = request.project_path;

    match task::spawn_blocking(move || execute_with_path(&project_path)).await {
        Ok(result) => match result {
            Ok(pipeline_id) => Ok(Json(RunResponse {
                success: true,
                message: "Pipeline execution started successfully".to_string(),
                pipeline_id: Some(pipeline_id),
            })),
            Err(e) => {
                error!(error = %e, "Pipeline execution failed");
                Ok(Json(RunResponse {
                    success: false,
                    message: format!("Pipeline execution failed: {}", e),
                    pipeline_id: None,
                }))
            }
        },
        Err(e) => {
            error!(error = %e, "Pipeline execution task failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn handle_run_node(
    AxumPath(node_name): AxumPath<String>,
    Json(request): Json<RunRequest>,
) -> Result<Json<RunResponse>, StatusCode> {
    let project_path = request.project_path;
    let node_name_clone = node_name.clone();

    match task::spawn_blocking(move || execute_with_target_node(&project_path, &node_name_clone))
        .await
    {
        Ok(result) => match result {
            Ok(pipeline_id) => Ok(Json(RunResponse {
                success: true,
                message: format!(
                    "Node '{}' and its dependencies execution started successfully",
                    node_name
                ),
                pipeline_id: Some(pipeline_id),
            })),
            Err(e) => {
                error!(error = %e, node = %node_name, "Node execution failed");
                Ok(Json(RunResponse {
                    success: false,
                    message: format!("Node execution failed: {}", e),
                    pipeline_id: None,
                }))
            }
        },
        Err(e) => {
            error!(error = %e, node = %node_name, "Node execution task failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProjectConfig;
    use crate::test_helpers::create_test_server;
    use serde_json::{Value, json};
    use tempfile;

    fn setup_test_project_for_api() -> (ProjectConfig, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path().to_path_buf();

        // プロジェクト構造を作成
        std::fs::create_dir_all(&project_path).unwrap();
        std::fs::create_dir_all(project_path.join("adapters")).unwrap();
        std::fs::create_dir_all(project_path.join("models")).unwrap();
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

        // 環境変数を設定（spawn_blockingの別スレッドでも有効）
        unsafe {
            std::env::set_var(
                "FEATHERBOX_PROJECT_DIRECTORY",
                project_path.to_str().unwrap(),
            );
        }

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
    async fn test_run_api_with_invalid_project_path() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let request = json!({
            "project_path": "/nonexistent/path"
        });

        let response = server.post("/run").json(&request).await;

        response.assert_status_ok();
        let run_response: Value = response.json();
        assert_eq!(run_response["success"], false);
        assert!(run_response["message"].as_str().unwrap().contains("failed"));
        assert!(run_response["pipeline_id"].is_null());
    }

    #[tokio::test]
    async fn test_run_node_api_with_invalid_project_path() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let request = json!({
            "project_path": "/nonexistent/path"
        });

        let response = server.post("/run-nodes/test_node").json(&request).await;

        response.assert_status_ok();
        let run_response: Value = response.json();
        assert_eq!(run_response["success"], false);
        assert!(run_response["message"].as_str().unwrap().contains("failed"));
        assert!(run_response["pipeline_id"].is_null());
    }

    #[tokio::test]
    async fn test_run_api_response_structure() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let temp_dir = tempfile::tempdir().unwrap();
        let request = json!({
            "project_path": temp_dir.path().to_string_lossy()
        });

        let response = server.post("/run").json(&request).await;

        response.assert_status_ok();
        let run_response: Value = response.json();
        assert!(run_response.get("success").is_some());
        assert!(run_response.get("message").is_some());
        assert!(run_response.get("pipeline_id").is_some());
    }

    #[tokio::test]
    async fn test_run_node_api_response_structure() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let temp_dir = tempfile::tempdir().unwrap();
        let request = json!({
            "project_path": temp_dir.path().to_string_lossy()
        });

        let response = server.post("/run-nodes/test_node").json(&request).await;

        response.assert_status_ok();
        let run_response: Value = response.json();
        assert!(run_response.get("success").is_some());
        assert!(run_response.get("message").is_some());
        assert!(run_response.get("pipeline_id").is_some());
    }
}
