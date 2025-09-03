use crate::{
    config::Config,
    dependency::{Graph, detect_changes, save_graph_with_changes},
};
use anyhow::Result;
use axum::{Router, http::StatusCode, response::Json, routing::post};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use tokio::task;
use tracing::error;

#[derive(Serialize, Deserialize)]
pub struct MigrateResponse {
    pub success: bool,
    pub message: String,
    pub graph_id: Option<i32>,
}

pub fn execute() -> Result<Option<i32>> {
    println!("execute start");
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let project_root = crate::workspace::find_project_root()?;
        let config = Config::load_from_directory(&project_root)?;

        println!("execute point");
        if config.adapters.is_empty() && config.models.is_empty() {
            return Ok(None);
        }

        let app_db = crate::database::connect_app_db(&config.project).await?;
        migrate_from_config(&config, &app_db).await
    })
}

pub async fn migrate_from_config(
    config: &Config,
    app_db: &DatabaseConnection,
) -> Result<Option<i32>> {
    let current_graph = Graph::from_config(config)?;

    let changes = detect_changes(app_db, &current_graph, config).await?;

    if changes.is_none() {
        return Ok(None);
    }

    Ok(Some(
        save_graph_with_changes(app_db, &current_graph, config, changes.as_ref()).await?,
    ))
}

pub fn routes() -> Router {
    Router::new().route("/migrate", post(handle_migrate))
}

async fn handle_migrate() -> Result<Json<MigrateResponse>, StatusCode> {
    match task::spawn_blocking(execute).await {
        Ok(result) => match result {
            Ok(graph_id) => Ok(Json(MigrateResponse {
                success: true,
                message: if graph_id.is_some() {
                    "Migration completed successfully".to_string()
                } else {
                    "No changes detected".to_string()
                },
                graph_id,
            })),
            Err(e) => {
                error!(error = %e, "Migration failed");
                Ok(Json(MigrateResponse {
                    success: false,
                    message: format!("Migration failed: {}", e),
                    graph_id: None,
                }))
            }
        },
        Err(e) => {
            error!(error = %e, "Migration task failed");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProjectConfig, adapter::AdapterConfig, model::ModelConfig};
    use crate::database::entities::{edges, nodes};
    use crate::test_helpers::{create_test_server, setup_test_db_with_config};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use serde_json::Value;

    #[tokio::test]
    async fn test_execute_migrate_from_config() -> Result<()> {
        let (app_db, mut config) = setup_test_db_with_config().await?;

        config.adapters.insert(
            "test".to_string(),
            AdapterConfig {
                connection: "test".to_string(),
                description: Some("Test adapter".to_string()),
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "test.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        has_header: Some(true),
                        delimiter: None,
                        null_value: None,
                    },
                },
                columns: vec![],
            },
        );

        let result1 = migrate_from_config(&config, &app_db).await?;
        let Some(first_graph_id) = result1 else {
            panic!("Expected a new graph ID")
        };

        let first_nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(first_graph_id))
            .all(&app_db)
            .await?;
        assert_eq!(first_nodes.len(), 1);
        assert_eq!(first_nodes[0].name, "test");

        let first_edges = edges::Entity::find()
            .filter(edges::Column::GraphId.eq(first_graph_id))
            .all(&app_db)
            .await?;
        assert_eq!(first_edges.len(), 0);

        let result2 = migrate_from_config(&config, &app_db).await?;
        assert!(result2.is_none());

        config.models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: Some("User statistics".to_string()),
                sql: "SELECT * FROM test".to_string(),
            },
        );

        let result3 = migrate_from_config(&config, &app_db).await?;
        let Some(third_graph_id) = result3 else {
            panic!("Expected a new graph ID")
        };

        let third_nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(third_graph_id))
            .all(&app_db)
            .await?;
        assert_eq!(third_nodes.len(), 2);
        let node_names: Vec<_> = third_nodes.iter().map(|n| &n.name).collect();
        assert!(node_names.contains(&&"test".to_string()));
        assert!(node_names.contains(&&"user_stats".to_string()));

        let third_edges = edges::Entity::find()
            .filter(edges::Column::GraphId.eq(third_graph_id))
            .all(&app_db)
            .await?;
        assert_eq!(third_edges.len(), 1);
        assert_eq!(third_edges[0].from_node, "test");
        assert_eq!(third_edges[0].to_node, "user_stats");

        assert_ne!(first_graph_id, third_graph_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_new_adapter_creates_null_timestamp() -> Result<()> {
        let (app_db, mut config) = setup_test_db_with_config().await?;

        config.adapters.insert(
            "users_adapter".to_string(),
            AdapterConfig {
                connection: "test".to_string(),
                description: Some("Users adapter".to_string()),
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "users.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        has_header: Some(true),
                        delimiter: None,
                        null_value: None,
                    },
                },
                columns: vec![],
            },
        );

        let result = migrate_from_config(&config, &app_db).await?;
        let graph_id = result.unwrap();

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(graph_id))
            .all(&app_db)
            .await?;

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].name, "users_adapter");
        assert!(nodes[0].last_updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_preserves_existing_node_timestamp() -> Result<()> {
        let (app_db, mut config) = setup_test_db_with_config().await?;

        config.adapters.insert(
            "users_adapter".to_string(),
            AdapterConfig {
                connection: "test".to_string(),
                description: Some("Users adapter".to_string()),
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "users.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        has_header: Some(true),
                        delimiter: None,
                        null_value: None,
                    },
                },
                columns: vec![],
            },
        );

        let first_result = migrate_from_config(&config, &app_db).await?;
        first_result.unwrap();

        let test_time =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        crate::dependency::update_node_timestamp(&app_db, "users_adapter", test_time).await?;

        config.models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: Some("User statistics".to_string()),
                sql: "SELECT COUNT(*) FROM users_adapter".to_string(),
            },
        );

        let second_result = migrate_from_config(&config, &app_db).await?;
        let graph_id = second_result.unwrap();

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(graph_id))
            .all(&app_db)
            .await?;

        assert_eq!(nodes.len(), 2);

        let users_adapter_node = nodes.iter().find(|n| n.name == "users_adapter").unwrap();
        assert_eq!(users_adapter_node.last_updated_at, Some(test_time));

        let user_stats_node = nodes.iter().find(|n| n.name == "user_stats").unwrap();
        assert!(user_stats_node.last_updated_at.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_config_change_resets_timestamp() -> Result<()> {
        let (app_db, mut config) = setup_test_db_with_config().await?;

        config.adapters.insert(
            "users_adapter".to_string(),
            AdapterConfig {
                connection: "test".to_string(),
                description: Some("Users adapter".to_string()),
                source: crate::config::adapter::AdapterSource::File {
                    file: crate::config::adapter::FileConfig {
                        path: "users.csv".to_string(),
                        compression: None,
                        max_batch_size: None,
                    },
                    format: crate::config::adapter::FormatConfig {
                        ty: "csv".to_string(),
                        has_header: Some(true),
                        delimiter: None,
                        null_value: None,
                    },
                },
                columns: vec![],
            },
        );

        let first_result = migrate_from_config(&config, &app_db).await?;
        first_result.unwrap();

        let test_time =
            chrono::NaiveDateTime::parse_from_str("2024-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap();
        crate::dependency::update_node_timestamp(&app_db, "users_adapter", test_time).await?;

        config.adapters.get_mut("users_adapter").unwrap().connection =
            "updated_connection".to_string();

        let second_result = migrate_from_config(&config, &app_db).await?;
        let graph_id = second_result.unwrap();

        let nodes = nodes::Entity::find()
            .filter(nodes::Column::GraphId.eq(graph_id))
            .all(&app_db)
            .await?;

        let modified_adapter = nodes.iter().find(|n| n.name == "users_adapter").unwrap();
        assert!(modified_adapter.last_updated_at.is_none());

        Ok(())
    }

    fn setup_test_project_for_api() -> (ProjectConfig, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path().to_path_buf();

        // プロジェクト構造を作成
        std::fs::create_dir_all(&project_path).unwrap();
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
    async fn test_migrate_api_success() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let response = server.post("/migrate").await;

        response.assert_status_ok();
        let migrate_response: Value = response.json();
        println!("Migrate response: {}", migrate_response);

        assert!(migrate_response["success"].as_bool().unwrap_or(false));
        assert!(migrate_response["message"].is_string());
    }

    #[tokio::test]
    async fn test_migrate_api_response_structure() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let response = server.post("/migrate").await;

        response.assert_status_ok();
        let migrate_response: Value = response.json();
        assert!(migrate_response.get("success").is_some());
        assert!(migrate_response.get("message").is_some());
        assert!(migrate_response.get("graph_id").is_some());
    }

    #[tokio::test]
    async fn test_migrate_api_no_changes() {
        let (_config, _temp_dir) = setup_test_project_for_api();
        let server = create_test_server(routes);

        let response = server.post("/migrate").await;
        response.assert_status_ok();
        let migrate_response: Value = response.json();

        assert!(migrate_response["success"].as_bool().unwrap_or(false));
        assert_eq!(migrate_response["message"], "No changes detected");
        assert!(migrate_response["graph_id"].is_null());
    }
}
