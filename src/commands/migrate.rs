use anyhow::Result;
use std::path::Path;

use crate::{
    commands::workspace::ensure_project_directory,
    config::Config,
    database::connect_app_db,
    dependency::{Graph, detect_changes, save_graph_with_changes},
};
use sea_orm::DatabaseConnection;

pub async fn migrate(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;

    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        println!(
            "No adapters or models found. Create some with 'fbox adapter new' or 'fbox model new'"
        );
        return Ok(());
    }

    let app_db = connect_app_db(&config.project).await?;

    if let Some(graph_id) = migrate_from_config(&config, &app_db).await? {
        println!("Graph migrated successfully! Graph ID: {graph_id}");
    } else {
        println!("No changes detected. Graph is up to date.");
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{adapter::AdapterConfig, model::ModelConfig};
    use crate::database::entities::{edges, nodes};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use tempfile;
    async fn setup_test_db_with_config()
    -> Result<(sea_orm::DatabaseConnection, crate::config::Config)> {
        use crate::config::project::{DatabaseConfig, DatabaseType, StorageConfig};
        use crate::database::connection::connect_app_db;

        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = crate::config::project::ProjectConfig {
            storage: StorageConfig::LocalFile {
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some(db_path.to_string_lossy().to_string()),
                host: None,
                port: None,
                database: None,
                password: None,
                username: None,
            },
            connections: std::collections::HashMap::new(),
        };

        let config = crate::config::Config {
            project: project_config.clone(),
            adapters: std::collections::HashMap::new(),
            models: std::collections::HashMap::new(),
            queries: std::collections::HashMap::new(),
            project_root: temp_dir.path().to_path_buf(),
        };

        let db = connect_app_db(&project_config).await?;
        std::mem::forget(temp_dir);
        Ok((db, config))
    }

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
}
