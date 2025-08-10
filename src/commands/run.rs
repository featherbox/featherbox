use crate::{
    commands::workspace::ensure_project_directory,
    config::Config,
    database::{
        connect_app_db,
        entities::{pipeline_actions, pipelines},
    },
    dependency::{Graph, latest_graph_id},
    pipeline::{
        build::Pipeline,
        ducklake::{CatalogConfig, DuckLake, StorageConfig},
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

    let pipeline =
        Pipeline::from_graph_with_ranges(&current_graph, &config, &app_db, graph_id).await?;
    save_pipeline(&app_db, graph_id, &pipeline).await?;

    if let Err(e) = pipeline
        .execute_with_delta(&config, &ducklake, &app_db)
        .await
    {
        eprintln!("Pipeline execution failed: {e}");
        return Err(e);
    }

    println!("Pipeline execution completed successfully!");

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
    for (execution_order, action) in pipeline.actions.iter().enumerate() {
        let action_model = pipeline_actions::ActiveModel {
            id: NotSet,
            pipeline_id: Set(pipeline_id),
            table_name: Set(action.table_name.clone()),
            execution_order: Set(execution_order as i32),
            since: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.since)
                .map(|dt| dt.naive_utc())),
            until: Set(action
                .time_range
                .as_ref()
                .and_then(|tr| tr.until)
                .map(|dt| dt.naive_utc())),
        };
        let saved_action = action_model.insert(app_db).await?;
        action_ids.push(saved_action.id);
    }

    Ok(())
}

pub async fn connect_ducklake(config: &Config) -> Result<DuckLake> {
    let catalog_config = match &config.project.database.ty {
        crate::config::project::DatabaseType::Sqlite => CatalogConfig::Sqlite {
            path: config.project.database.path.clone(),
        },
    };

    let storage_config = match &config.project.storage.ty {
        crate::config::project::StorageType::Local => StorageConfig::LocalFile {
            path: config.project.storage.path.clone(),
        },
    };

    DuckLake::new(catalog_config, storage_config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency::graph::Node;
    use crate::pipeline::build::Action;
    use std::fs;
    use tempfile;

    #[tokio::test]
    async fn test_execute_run_empty_project() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;

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
    async fn test_execute_run_with_simple_pipeline() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
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

        let model_yml = r#"
            description: "Test model"
            sql: "SELECT id, name FROM users WHERE id > 0"
            max_age: 3600"#;
        fs::write(project_path.join("models/active_users.yml"), model_yml)?;

        let config = Config::load_from_directory(project_path)?;
        let graph = Graph::from_config(&config)?;
        let pipeline = Pipeline::from_graph(&graph);

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{}/test.db", project_path.to_string_lossy()),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{}/storage", project_path.to_string_lossy()),
        };
        let ducklake = DuckLake::new(catalog_config, storage_config).await?;

        let result = pipeline.execute(&config, &ducklake).await;
        assert!(result.is_ok());

        let users_result = ducklake.query("SELECT * FROM users ORDER BY id")?;
        assert_eq!(users_result.len(), 2);
        assert_eq!(users_result[0], vec!["1", "Alice"]);
        assert_eq!(users_result[1], vec!["2", "Bob"]);

        let active_users_result = ducklake.query("SELECT * FROM active_users ORDER BY id")?;
        assert_eq!(active_users_result.len(), 2);
        assert_eq!(active_users_result[0], vec!["1", "Alice"]);
        assert_eq!(active_users_result[1], vec!["2", "Bob"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_run_with_changes() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;

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
        let _app_db = connect_app_db(&config.project).await?;

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

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
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
            actions: vec![Action {
                table_name: "users".to_string(),
                time_range: Some(crate::pipeline::build::TimeRange {
                    since: None,
                    until: None,
                }),
            }],
        };

        save_execution_history(&app_db, &initial_graph, &initial_pipeline).await?;

        let model_yml = r#"
            description: "User statistics model"
            sql: "SELECT id, name FROM users WHERE active = true"
            max_age: 3600"#;
        fs::write(project_path.join("models/user_stats.yml"), model_yml)?;

        let changes = detect_changes(
            &app_db,
            &Graph::from_config(&Config::load_from_directory(project_path)?)?,
        )
        .await?;
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
    async fn test_execute_run_partial_execution() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
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
        fs::write(&users_csv, "id,name\n1,Alice\n2,Bob")?;

        let orders_csv = project_path.join("data/orders.csv");
        fs::write(&orders_csv, "id,user_id,amount\n1,1,100\n2,2,200")?;

        let users_adapter_yml = format!(
            r#"
            connection: test_connection
            description: "Users adapter"
            file:
              path: {}
              compression: none
            format:
              type: csv
              has_header: true
            columns: []"#,
            users_csv.to_string_lossy()
        );
        fs::write(project_path.join("adapters/users.yml"), users_adapter_yml)?;

        let orders_adapter_yml = format!(
            r#"
            connection: test_connection
            description: "Orders adapter"
            file:
              path: {}
              compression: none
            format:
              type: csv
              has_header: true
            columns: []"#,
            orders_csv.to_string_lossy()
        );
        fs::write(project_path.join("adapters/orders.yml"), orders_adapter_yml)?;

        let user_stats_yml = r#"
            description: "User statistics model"
            sql: "SELECT id, name FROM users WHERE id > 0"
            max_age: 3600"#;
        fs::write(project_path.join("models/user_stats.yml"), user_stats_yml)?;

        let order_summary_yml = r#"
            description: "Order summary model"
            sql: "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
            max_age: 3600"#;
        fs::write(
            project_path.join("models/order_summary.yml"),
            order_summary_yml,
        )?;

        let config = Config::load_from_directory(project_path)?;
        let app_db = connect_app_db(&config.project).await?;

        let initial_graph = Graph::from_config(&config)?;
        let initial_pipeline = Pipeline::from_graph(&initial_graph);
        save_execution_history(&app_db, &initial_graph, &initial_pipeline).await?;

        let combined_model_yml = r#"
            description: "Combined statistics model"
            sql: "SELECT u.id, u.name, o.order_count FROM user_stats u LEFT JOIN order_summary o ON u.id = o.user_id"
            max_age: 3600"#;
        fs::write(
            project_path.join("models/combined_stats.yml"),
            combined_model_yml,
        )?;

        let updated_config = Config::load_from_directory(project_path)?;
        let updated_graph = Graph::from_config(&updated_config)?;
        let changes = detect_changes(&app_db, &updated_graph).await?;

        assert!(changes.is_some());
        let changes = changes.unwrap();
        assert!(changes.added_nodes.contains(&"combined_stats".to_string()));

        let affected_nodes = calculate_affected_nodes(&updated_graph, &changes);
        assert!(!affected_nodes.is_empty());
        assert!(affected_nodes.contains(&"combined_stats".to_string()));

        let partial_pipeline = Pipeline::create_partial_pipeline(&updated_graph, &affected_nodes);

        assert!(partial_pipeline.actions.len() < updated_graph.nodes.len());
        assert!(
            partial_pipeline
                .actions
                .iter()
                .any(|a| a.table_name == "combined_stats")
        );

        Ok(())
    }
}
