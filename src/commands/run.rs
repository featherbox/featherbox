use crate::{
    commands::workspace::ensure_project_directory,
    config::{
        Config,
        project::{DatabaseType, RemoteDatabaseConfig},
    },
    database::{
        connect_app_db,
        entities::{pipeline_actions, pipelines},
    },
    dependency::{Graph, latest_graph_id},
    pipeline::{
        build::Pipeline,
        ducklake::{CatalogConfig, DuckLake},
    },
};
use anyhow::Result;
use sea_orm::DatabaseConnection;
use sea_orm::{ActiveModelTrait, NotSet, Set};
use std::path::Path;

#[cfg(test)]
use crate::dependency::{calculate_affected_nodes, detect_changes, save_execution_history};

pub async fn run(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;

    let config = Config::load_from_directory(&project_root)?;

    if config.adapters.is_empty() && config.models.is_empty() {
        println!(
            "No adapters or models found. Create some with 'fbox adapter new' or 'fbox model new'"
        );
        return Ok(());
    }

    let app_db = connect_app_db(&config.project).await?;

    let graph_id = latest_graph_id(&app_db).await?;

    let Some(graph_id) = graph_id else {
        println!("No graph found. Run 'fbox migrate' first to create the initial graph.");
        return Ok(());
    };

    let current_graph = Graph::from_config(&config)?;

    let ducklake = connect_ducklake(&config).await?;

    let pipeline = Pipeline::from_graph(&current_graph);
    save_pipeline(&app_db, graph_id, &pipeline).await?;

    pipeline
        .execute(&current_graph, &config, &ducklake, &app_db)
        .await?;

    Ok(())
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
        };
        let saved_action = action_model.insert(app_db).await?;
        action_ids.push(saved_action.id);
    }

    Ok(())
}

pub async fn connect_ducklake(config: &Config) -> Result<DuckLake> {
    let catalog_config = match &config.project.database.ty {
        crate::config::project::DatabaseType::Sqlite => CatalogConfig::Sqlite {
            path: config
                .project
                .database
                .path
                .as_ref()
                .expect("SQLite database path is required")
                .clone(),
        },
        crate::config::project::DatabaseType::Mysql => {
            let remote_config = RemoteDatabaseConfig {
                host: config
                    .project
                    .database
                    .host
                    .as_ref()
                    .expect("MySQL host is required")
                    .clone(),
                port: config.project.database.port.unwrap_or(3306),
                database: config
                    .project
                    .database
                    .database
                    .as_ref()
                    .expect("MySQL database name is required")
                    .clone(),
                username: config
                    .project
                    .database
                    .username
                    .as_ref()
                    .expect("MySQL username is required")
                    .clone(),
                password: config
                    .project
                    .database
                    .password
                    .as_ref()
                    .expect("MySQL password is required")
                    .clone(),
            };
            CatalogConfig::RemoteDatabase {
                db_type: DatabaseType::Mysql,
                config: remote_config,
            }
        }
        crate::config::project::DatabaseType::Postgresql => {
            let remote_config = RemoteDatabaseConfig {
                host: config
                    .project
                    .database
                    .host
                    .as_ref()
                    .expect("PostgreSQL host is required")
                    .clone(),
                port: config.project.database.port.unwrap_or(5432),
                database: config
                    .project
                    .database
                    .database
                    .as_ref()
                    .expect("PostgreSQL database name is required")
                    .clone(),
                username: config
                    .project
                    .database
                    .username
                    .as_ref()
                    .expect("PostgreSQL username is required")
                    .clone(),
                password: config
                    .project
                    .database
                    .password
                    .as_ref()
                    .expect("PostgreSQL password is required")
                    .clone(),
            };
            CatalogConfig::RemoteDatabase {
                db_type: DatabaseType::Postgresql,
                config: remote_config,
            }
        }
    };

    DuckLake::new(catalog_config, config.project.storage.clone()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::Node;
    use crate::pipeline::build::Action;
    fn create_project_structure(project_path: &std::path::Path) -> Result<()> {
        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
        Ok(())
    }
    use std::fs;
    use tempfile;

    #[tokio::test]
    async fn test_execute_run_empty_project() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        create_project_structure(project_path)?;

        let project_yml = format!(
            r#"
            storage:
              type: local
              path: {}/storage
            database:
              type: sqlite
              path: {}/database.db
            deployments:
              timeout: 600
            connections: {{}}"#,
            project_path.display(),
            project_path.display()
        );
        fs::write(project_path.join("project.yml"), project_yml)?;

        let result = run(project_path).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_with_changes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        create_project_structure(project_path)?;

        let project_yml = format!(
            r#"
            storage:
              type: local
              path: {}/storage
            database:
              type: sqlite
              path: {}/database.db
            deployments:
              timeout: 600
            connections: {{}}"#,
            project_path.display(),
            project_path.display()
        );
        fs::write(project_path.join("project.yml"), project_yml)?;

        let config = Config::load_from_directory(project_path)?;
        connect_app_db(&config.project).await?;

        let result = run(project_path).await;
        assert!(result.is_ok());

        fs::create_dir_all(project_path.join("data"))?;
        let users_csv = project_path.join("data/users.csv");
        fs::write(&users_csv, "id,name\n1,Alice\n2,Bob")?;

        let adapter_yml = format!(
            r#"
            connection: test_connection
            description: "Test adapter"
            file:
              path: {}
              compression: none
            format:
              type: csv
              has_header: true
            columns: []"#,
            users_csv.to_string_lossy()
        );
        fs::write(project_path.join("adapters/users.yml"), adapter_yml)?;

        let result = run(project_path).await;
        assert!(result.is_ok());

        let result = run(project_path).await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_missing_project_yml() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let result = run(project_path).await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        println!("Error message: {error_msg}");
        assert!(
            error_msg.contains("project.yml not found")
                || error_msg.contains("This command must be run inside a FeatherBox project")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_with_impact_analysis() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        create_project_structure(project_path)?;
        fs::create_dir_all(project_path.join("data"))?;

        let project_yml = format!(
            r#"
            storage:
              type: local
              path: {}/storage
            database:
              type: sqlite
              path: {}/database.db
            deployments:
              timeout: 600
            connections: {{}}"#,
            project_path.display(),
            project_path.display()
        );
        fs::write(project_path.join("project.yml"), project_yml)?;

        let users_csv = project_path.join("data/users.csv");
        fs::write(
            &users_csv,
            "id,name,active\n1,Alice,true\n2,Bob,false\n3,Charlie,true",
        )?;

        let adapter_yml = format!(
            r#"
            connection: test_connection
            description: "Test adapter"
            file:
              path: {}
              compression: none
            format:
              type: csv
              has_header: true
            columns: []"#,
            users_csv.to_string_lossy()
        );
        fs::write(project_path.join("adapters/users.yml"), adapter_yml)?;

        let config = Config::load_from_directory(project_path)?;
        let app_db = connect_app_db(&config.project).await?;

        let initial_graph = Graph {
            nodes: vec![Node {
                name: "users".to_string(),
            }],
            edges: vec![],
        };

        let initial_pipeline = Pipeline {
            levels: vec![vec![Action {
                table_name: "users".to_string(),
            }]],
        };

        let config = Config::load_from_directory(project_path)?;
        save_execution_history(&app_db, &initial_graph, &initial_pipeline, &config).await?;

        let model_yml = r#"
            description: "User statistics model"
            sql: "SELECT id, name FROM users WHERE active = true""#;
        fs::write(project_path.join("models/user_stats.yml"), model_yml)?;

        let new_config = Config::load_from_directory(project_path)?;
        let changes =
            detect_changes(&app_db, &Graph::from_config(&new_config)?, &new_config).await?;
        assert!(changes.is_some());

        let changes = changes.unwrap();
        assert!(changes.added_nodes.contains(&"user_stats".to_string()));
        assert!(
            changes
                .added_edges
                .contains(&("users".to_string(), "user_stats".to_string()))
        );

        let graph = Graph::from_config(&Config::load_from_directory(project_path)?)?;
        let affected_nodes = calculate_affected_nodes(&graph, &changes);
        assert!(affected_nodes.contains(&"user_stats".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_ducklake_with_s3_connections() -> Result<()> {
        use std::env;
        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let project_path = temp_dir.path();

        create_project_structure(project_path)?;

        unsafe {
            env::set_var("TEST_DUCKLAKE_S3_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_DUCKLAKE_S3_SECRET_KEY", "test_secret_key");
        }

        let project_yml = format!(
            r#"
            storage:
              type: local
              path: {}/storage
            database:
              type: sqlite
              path: {}/database.db
            deployments:
              timeout: 600
            connections:
              s3_data:
                type: s3
                bucket: test-bucket
                region: us-west-2
                access_key_id: ${{TEST_DUCKLAKE_S3_ACCESS_KEY}}
                secret_access_key: ${{TEST_DUCKLAKE_S3_SECRET_KEY}}"#,
            project_path.display(),
            project_path.display()
        );
        fs::write(project_path.join("project.yml"), project_yml)?;

        let config = Config::load_from_directory(project_path)?;
        let ducklake = connect_ducklake(&config).await?;

        if let Some(s3_connection) = config.project.connections.get("s3_data") {
            ducklake.configure_s3_connection(s3_connection).await?;
            let result = ducklake.query("SELECT current_setting('s3_region')");
            assert!(result.is_ok(), "Failed to query s3_region setting");
            let results = result.unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0][0], "us-west-2");
        } else {
            panic!("S3 connection not found in config");
        }

        unsafe {
            env::remove_var("TEST_DUCKLAKE_S3_ACCESS_KEY");
            env::remove_var("TEST_DUCKLAKE_S3_SECRET_KEY");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_ducklake_without_connections() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        create_project_structure(project_path)?;

        let project_yml = format!(
            r#"
            storage:
              type: local
              path: {}/storage
            database:
              type: sqlite
              path: {}/database.db
            deployments:
              timeout: 600
            connections: {{}}"#,
            project_path.display(),
            project_path.display()
        );
        fs::write(project_path.join("project.yml"), project_yml)?;

        let config = Config::load_from_directory(project_path)?;
        let ducklake = connect_ducklake(&config).await?;

        let result = ducklake.query("SELECT 1 as test_query");
        assert!(result.is_ok(), "Basic query should work");

        let result = ducklake.query("SELECT current_setting('s3_region')");
        if let Ok(results) = result {
            if !results.is_empty() && !results[0][0].is_empty() && results[0][0] != "NULL" {
                println!("S3 region returned default value: {:?}", results[0][0]);
                assert_eq!(
                    results[0][0], "us-east-1",
                    "Without S3 connections, should return DuckDB default region"
                );
            }
        } else {
            println!("S3 region query failed as expected");
        }

        Ok(())
    }
}
