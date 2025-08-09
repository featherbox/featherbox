use anyhow::Result;
use std::path::Path;

use crate::{
    commands::workspace::ensure_project_directory,
    config::Config,
    database::connect_app_db,
    dependency::{Graph, detect_changes, save_graph_if_changed},
};
use sea_orm::DatabaseConnection;

pub async fn execute_migrate(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;

    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        println!(
            "No adapters or models found. Create some with 'fbox adapter new' or 'fbox model new'"
        );
        return Ok(());
    }

    let app_db = connect_app_db(&config.project).await?;

    if let Some(graph_id) = execute_migrate_from_config(&config, &app_db).await? {
        println!("Graph migrated successfully! Graph ID: {graph_id}");
    } else {
        println!("No changes detected. Graph is up to date.");
    }

    Ok(())
}

pub async fn execute_migrate_from_config(
    config: &Config,
    app_db: &DatabaseConnection,
) -> Result<Option<i32>> {
    let current_graph = Graph::from_config(config)?;

    let changes = detect_changes(app_db, &current_graph).await?;

    if changes.is_none() {
        return Ok(None);
    }

    Ok(Some(save_graph_if_changed(app_db, &current_graph).await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config,
        adapter::AdapterConfig,
        model::ModelConfig,
        project::{
            DatabaseConfig, DatabaseType, DeploymentsConfig, ProjectConfig, StorageConfig,
            StorageType,
        },
    };
    use crate::database::connect_app_db;
    use crate::database::entities::{edges, nodes};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use std::collections::HashMap;
    use tempfile;

    async fn setup_test_db() -> Result<(DatabaseConnection, Config)> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test.db");

        let project_config = ProjectConfig {
            storage: StorageConfig {
                ty: StorageType::Local,
                path: temp_dir.path().to_string_lossy().to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: db_path.to_string_lossy().to_string(),
            },
            deployments: DeploymentsConfig { timeout: 600 },
            connections: HashMap::new(),
        };

        let config = Config {
            project: project_config.clone(),
            adapters: HashMap::new(),
            models: HashMap::new(),
        };

        let db = connect_app_db(&project_config).await?;

        std::mem::forget(temp_dir);
        Ok((db, config))
    }

    #[tokio::test]
    async fn test_execute_migrate_from_config() -> Result<()> {
        let (app_db, mut config) = setup_test_db().await?;

        config.adapters.insert(
            "test".to_string(),
            AdapterConfig {
                connection: "test".to_string(),
                description: Some("Test adapter".to_string()),
                file: crate::config::adapter::FileConfig {
                    path: "test.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: crate::config::adapter::FormatConfig {
                    ty: "csv".to_string(),
                    has_header: Some(true),
                    delimiter: None,
                    null_value: None,
                },
                columns: vec![],
                limits: None,
            },
        );

        // First migration should create the initial graph
        let result1 = execute_migrate_from_config(&config, &app_db).await?;
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

        // Second migration with no changes should return None
        let result2 = execute_migrate_from_config(&config, &app_db).await?;
        assert!(result2.is_none());

        // Add a new model and expect a new graph ID
        config.models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: Some("User statistics".to_string()),
                sql: "SELECT * FROM test".to_string(),
                max_age: Some(3600),
            },
        );

        // This should create a new graph with the model
        let result3 = execute_migrate_from_config(&config, &app_db).await?;
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
}
