use crate::{
    config::Config,
    dependency::Graph,
    pipeline::{
        adapter::Adapter,
        build::{Action, Pipeline},
        ducklake::DuckLake,
        logger::Logger,
        model::Model,
        state_manager::StateManager,
        status::{PipelineStatus, TaskStatus},
    },
};
use anyhow::{Context, Result};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::task::JoinHandle;

struct ExecutionContext {
    pipeline_id: i32,
    graph: Arc<Graph>,
    config: Arc<Config>,
    logger: Arc<Logger>,
    adapters: Arc<HashMap<String, Adapter>>,
    models: Arc<HashMap<String, Model>>,
    app_db: Arc<sea_orm::DatabaseConnection>,
    state_manager: Arc<StateManager>,
}

enum TaskResult {
    Success {
        table_name: String,
        execution_time_ms: u64,
        execution_start_time: chrono::NaiveDateTime,
    },
    Failed {
        table_name: String,
        error: anyhow::Error,
        execution_time_ms: u64,
    },
}

impl Pipeline {
    async fn get_latest_pipeline_id(&self, app_db: &sea_orm::DatabaseConnection) -> Result<i32> {
        use crate::database::entities::pipelines;
        use sea_orm::{EntityTrait, QueryOrder};

        let latest_pipeline = pipelines::Entity::find()
            .order_by_desc(pipelines::Column::CreatedAt)
            .one(app_db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No pipeline found in database"))?;

        Ok(latest_pipeline.id)
    }

    pub async fn execute(
        &self,
        graph: &Graph,
        config: &Config,
        ducklake: &DuckLake,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<()> {
        let shared_ducklake = Arc::new(ducklake.clone());
        let shared_logger = Arc::new(Logger::new(Arc::clone(&shared_ducklake)).await?);

        let mut adapters = HashMap::new();
        for (table_name, adapter_config) in &config.adapters {
            let adapter = Adapter::new(adapter_config.clone(), Arc::clone(&shared_ducklake));
            adapters.insert(table_name.clone(), adapter);
        }
        let shared_adapters = Arc::new(adapters);

        let mut models = HashMap::new();
        for (table_name, model_config) in &config.models {
            let model = Model::new(model_config.clone(), Arc::clone(&shared_ducklake));
            models.insert(table_name.clone(), model);
        }
        let shared_models = Arc::new(models);

        let pipeline_id = self.get_latest_pipeline_id(app_db).await?;
        let state_manager = Arc::new(StateManager::new(app_db.clone()));

        if let Err(e) = state_manager
            .update_pipeline_status(pipeline_id, PipelineStatus::Running)
            .await
        {
            anyhow::bail!("Failed to update pipeline status to running: {}", e);
        }

        let context = ExecutionContext {
            pipeline_id,
            graph: Arc::new(graph.clone()),
            config: Arc::new(config.clone()),
            logger: shared_logger.clone(),
            adapters: shared_adapters,
            models: shared_models,
            app_db: Arc::new(app_db.clone()),
            state_manager: state_manager.clone(),
        };

        context
            .logger
            .log_pipeline_event(
                context.pipeline_id,
                "PIPELINE_START",
                &format!(
                    "Pipeline execution started with {} levels",
                    self.levels.len()
                ),
                None,
            )
            .context("Failed to log pipeline start")?;

        let mut failed_tasks = HashSet::new();

        for level in &self.levels {
            let results = self
                .execute_level(level, &context, &mut failed_tasks)
                .await?;

            for result in results {
                match result {
                    TaskResult::Success {
                        table_name,
                        execution_time_ms,
                        execution_start_time,
                    } => {
                        println!(
                            "Task completed successfully: {table_name} ({execution_time_ms}ms)"
                        );

                        context
                            .state_manager
                            .update_task_status(
                                context.pipeline_id,
                                &table_name,
                                TaskStatus::Completed,
                                None,
                            )
                            .await
                            .context("Failed to update task status to completed")?;

                        context
                            .logger
                            .log_task_execution(
                                context.pipeline_id,
                                &table_name,
                                "SUCCESS",
                                None,
                                execution_time_ms,
                            )
                            .context("Failed to log successful task execution")?;

                        if let Err(e) = crate::dependency::update_node_timestamp(
                            &context.app_db,
                            &table_name,
                            execution_start_time,
                        )
                        .await
                        {
                            eprintln!("Failed to update timestamp for {table_name}: {e}");
                        }
                    }
                    TaskResult::Failed {
                        table_name,
                        error,
                        execution_time_ms,
                    } => {
                        eprintln!(
                            "Task failed for table '{table_name}': {error} ({execution_time_ms}ms)"
                        );

                        context
                            .state_manager
                            .update_task_status(
                                context.pipeline_id,
                                &table_name,
                                TaskStatus::Failed,
                                Some(&error.to_string()),
                            )
                            .await
                            .context("Failed to update task status to failed")?;

                        context
                            .logger
                            .log_task_execution(
                                context.pipeline_id,
                                &table_name,
                                "FAILED",
                                Some(&error.to_string()),
                                execution_time_ms,
                            )
                            .context("Failed to log failed task execution")?;

                        failed_tasks.insert(table_name.clone());
                        self.mark_downstream_as_failed(
                            &table_name,
                            &context.graph,
                            &mut failed_tasks,
                        );
                    }
                }
            }
        }

        if !failed_tasks.is_empty() {
            let error_message =
                format!("Pipeline execution had {} failed tasks", failed_tasks.len());
            context
                .logger
                .log_pipeline_event(
                    context.pipeline_id,
                    "PIPELINE_FAILED",
                    &error_message,
                    Some(&format!("Failed tasks: {failed_tasks:?}")),
                )
                .context("Failed to log pipeline failure")?;

            context
                .state_manager
                .update_pipeline_status(context.pipeline_id, PipelineStatus::Failed)
                .await
                .context("Failed to update pipeline status to failed")?;

            self.print_pipeline_summary(context.pipeline_id, &context.logger)?;

            return Err(anyhow::anyhow!(error_message));
        }

        context
            .logger
            .log_pipeline_event(
                context.pipeline_id,
                "PIPELINE_SUCCESS",
                "Pipeline execution completed successfully",
                None,
            )
            .context("Failed to log pipeline success")?;

        context
            .state_manager
            .update_pipeline_status(context.pipeline_id, PipelineStatus::Completed)
            .await
            .context("Failed to update pipeline status to completed")?;

        self.print_pipeline_summary(context.pipeline_id, &context.logger)?;

        Ok(())
    }

    pub fn print_pipeline_summary(&self, pipeline_id: i32, logger: &Logger) -> Result<()> {
        let task_summary_query = format!(
            "SELECT status, COUNT(*) as count FROM db.__fbox_task_logs WHERE pipeline_id = {pipeline_id} GROUP BY status"
        );

        let task_results = logger.query_logs(&task_summary_query)?;

        let mut success_count = 0;
        let mut failed_count = 0;

        for row in &task_results {
            if row.len() >= 2 {
                let count: i32 = row[1].parse().unwrap_or(0);
                match row[0].as_str() {
                    "SUCCESS" => success_count = count,
                    "FAILED" => failed_count = count,
                    _ => {}
                }
            }
        }

        let failed_tasks_query = format!(
            "SELECT table_name, error_message FROM db.__fbox_task_logs WHERE pipeline_id = {pipeline_id} AND status = 'FAILED'"
        );

        let failed_tasks = logger.query_logs(&failed_tasks_query)?;

        println!("\n=== Pipeline Execution Summary ===");
        println!("✅ Successful tasks: {success_count}");
        println!("❌ Failed tasks: {failed_count}");

        if !failed_tasks.is_empty() {
            println!("\nFailed tasks:");
            for task in &failed_tasks {
                if task.len() >= 2 {
                    println!("  - {}: {}", task[0], task[1]);
                }
            }
        }

        println!("==================================\n");

        Ok(())
    }

    async fn execute_level(
        &self,
        actions: &[Action],
        context: &ExecutionContext,
        failed_tasks: &mut HashSet<String>,
    ) -> Result<Vec<TaskResult>> {
        let mut task_handles = Vec::new();

        for action in actions {
            if failed_tasks.contains(&action.table_name) {
                continue;
            }

            let should_skip =
                self.should_skip_task(&action.table_name, &context.graph, failed_tasks);
            if should_skip {
                failed_tasks.insert(action.table_name.clone());
                context
                    .state_manager
                    .update_task_status(
                        context.pipeline_id,
                        &action.table_name,
                        TaskStatus::Failed,
                        Some("Skipped due to upstream failure"),
                    )
                    .await
                    .ok();
                continue;
            }

            context
                .state_manager
                .update_task_status(
                    context.pipeline_id,
                    &action.table_name,
                    TaskStatus::Running,
                    None,
                )
                .await
                .ok();

            let handle = self.spawn_task(action, context)?;
            task_handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in task_handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(join_error) => {
                    let error_message = format!("Task join error: {join_error}");
                    eprintln!("{error_message}");
                    context
                        .logger
                        .log_pipeline_event(
                            context.pipeline_id,
                            "TASK_JOIN_ERROR",
                            &error_message,
                            Some(&join_error.to_string()),
                        )
                        .ok();
                }
            }
        }

        Ok(results)
    }

    fn should_skip_task(
        &self,
        table_name: &str,
        graph: &Graph,
        failed_tasks: &HashSet<String>,
    ) -> bool {
        for edge in &graph.edges {
            if edge.to == table_name && failed_tasks.contains(&edge.from) {
                return true;
            }
        }
        false
    }

    fn spawn_task(
        &self,
        action: &Action,
        context: &ExecutionContext,
    ) -> Result<JoinHandle<TaskResult>> {
        let table_name = action.table_name.clone();
        let connections = context.config.project.connections.clone();

        if let Some(adapter) = context.adapters.get(&action.table_name).cloned() {
            Ok(tokio::spawn(async move {
                let start_time = std::time::Instant::now();
                let execution_start_time = chrono::Utc::now().naive_utc();
                match adapter
                    .execute_import(&table_name, Some(&connections))
                    .await
                {
                    Ok(_) => TaskResult::Success {
                        table_name,
                        execution_time_ms: start_time.elapsed().as_millis() as u64,
                        execution_start_time,
                    },
                    Err(error) => TaskResult::Failed {
                        table_name,
                        error,
                        execution_time_ms: start_time.elapsed().as_millis() as u64,
                    },
                }
            }))
        } else if let Some(model) = context.models.get(&action.table_name).cloned() {
            let app_db = Arc::clone(&context.app_db);
            let graph = Arc::clone(&context.graph);
            let table_name_for_deps = table_name.clone();

            Ok(tokio::spawn(async move {
                let start_time = std::time::Instant::now();

                let dependency_timestamp = crate::dependency::get_oldest_dependency_timestamp(
                    &app_db,
                    &table_name_for_deps,
                    &graph,
                )
                .await
                .unwrap_or(None);

                match model.execute_transform(&table_name).await {
                    Ok(_) => TaskResult::Success {
                        table_name,
                        execution_time_ms: start_time.elapsed().as_millis() as u64,
                        execution_start_time: dependency_timestamp
                            .unwrap_or(chrono::Utc::now().naive_utc()),
                    },
                    Err(error) => TaskResult::Failed {
                        table_name,
                        error,
                        execution_time_ms: start_time.elapsed().as_millis() as u64,
                    },
                }
            }))
        } else {
            Err(anyhow::anyhow!(
                "Table '{}' not found in adapters or models",
                action.table_name
            ))
        }
    }

    fn mark_downstream_as_failed(
        &self,
        failed_table: &str,
        graph: &Graph,
        failed_tasks: &mut HashSet<String>,
    ) {
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(failed_table.to_string());

        while let Some(current_table) = queue.pop_front() {
            for edge in &graph.edges {
                if edge.from == current_table && !failed_tasks.contains(&edge.to) {
                    failed_tasks.insert(edge.to.clone());
                    queue.push_back(edge.to.clone());
                    println!("Marking downstream task as failed: {}", edge.to);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dependency::graph::{Edge, Node},
        pipeline::build::Pipeline,
    };

    #[tokio::test]
    async fn test_parallel_execution_basic() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "adapter_a".to_string(),
                },
                Node {
                    name: "adapter_b".to_string(),
                },
                Node {
                    name: "model_c".to_string(),
                },
                Node {
                    name: "model_d".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "adapter_a".to_string(),
                    to: "model_c".to_string(),
                },
                Edge {
                    from: "adapter_b".to_string(),
                    to: "model_d".to_string(),
                },
            ],
        };

        let pipeline = Pipeline::from_graph(&graph);

        assert_eq!(pipeline.levels.len(), 2);
        assert_eq!(pipeline.levels[0].len(), 2);

        let level_0_tables: Vec<&str> = pipeline.levels[0]
            .iter()
            .map(|a| a.table_name.as_str())
            .collect();
        assert!(level_0_tables.contains(&"adapter_a"));
        assert!(level_0_tables.contains(&"adapter_b"));

        assert_eq!(pipeline.levels[1].len(), 2);

        let level_1_tables: Vec<&str> = pipeline.levels[1]
            .iter()
            .map(|a| a.table_name.as_str())
            .collect();
        assert!(level_1_tables.contains(&"model_c"));
        assert!(level_1_tables.contains(&"model_d"));

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_downstream_as_failed() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
                Node {
                    name: "D".to_string(),
                },
                Node {
                    name: "E".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "B".to_string(),
                },
                Edge {
                    from: "A".to_string(),
                    to: "C".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "D".to_string(),
                },
                Edge {
                    from: "C".to_string(),
                    to: "E".to_string(),
                },
            ],
        };

        let mut failed_tasks = HashSet::new();
        let pipeline = Pipeline { levels: vec![] };

        pipeline.mark_downstream_as_failed("A", &graph, &mut failed_tasks);

        assert!(failed_tasks.contains("B"));
        assert!(failed_tasks.contains("C"));
        assert!(failed_tasks.contains("D"));
        assert!(failed_tasks.contains("E"));
        assert!(!failed_tasks.contains("A"));

        Ok(())
    }

    #[tokio::test]
    async fn test_should_skip_task() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "B".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "C".to_string(),
                },
            ],
        };

        let mut failed_tasks = HashSet::new();
        failed_tasks.insert("A".to_string());

        let pipeline = Pipeline { levels: vec![] };

        assert!(!pipeline.should_skip_task("A", &graph, &failed_tasks));
        assert!(pipeline.should_skip_task("B", &graph, &failed_tasks));
        assert!(!pipeline.should_skip_task("C", &graph, &failed_tasks));

        failed_tasks.insert("B".to_string());
        assert!(pipeline.should_skip_task("C", &graph, &failed_tasks));

        Ok(())
    }

    #[tokio::test]
    async fn test_parallel_execution_with_independent_failure() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "adapter_a".to_string(),
                },
                Node {
                    name: "adapter_b".to_string(),
                },
                Node {
                    name: "model_c".to_string(),
                },
                Node {
                    name: "model_d".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "adapter_a".to_string(),
                    to: "model_c".to_string(),
                },
                Edge {
                    from: "adapter_b".to_string(),
                    to: "model_d".to_string(),
                },
            ],
        };

        let pipeline = Pipeline::from_graph(&graph);

        assert_eq!(pipeline.levels.len(), 2);
        assert_eq!(pipeline.levels[0].len(), 2);
        assert_eq!(pipeline.levels[1].len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_dependency_failure_propagation() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
                Node {
                    name: "D".to_string(),
                },
                Node {
                    name: "E".to_string(),
                },
                Node {
                    name: "F".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "C".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "D".to_string(),
                },
                Edge {
                    from: "C".to_string(),
                    to: "E".to_string(),
                },
                Edge {
                    from: "D".to_string(),
                    to: "F".to_string(),
                },
            ],
        };

        let mut failed_tasks = HashSet::new();
        let pipeline = Pipeline { levels: vec![] };

        pipeline.mark_downstream_as_failed("A", &graph, &mut failed_tasks);

        assert!(failed_tasks.contains("C"));
        assert!(failed_tasks.contains("E"));
        assert!(!failed_tasks.contains("A"));
        assert!(!failed_tasks.contains("B"));
        assert!(!failed_tasks.contains("D"));
        assert!(!failed_tasks.contains("F"));

        pipeline.mark_downstream_as_failed("B", &graph, &mut failed_tasks);

        assert!(failed_tasks.contains("C"));
        assert!(failed_tasks.contains("E"));
        assert!(failed_tasks.contains("D"));
        assert!(failed_tasks.contains("F"));
        assert!(!failed_tasks.contains("A"));
        assert!(!failed_tasks.contains("B"));

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_task_with_multiple_dependencies() -> Result<()> {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "A".to_string(),
                },
                Node {
                    name: "B".to_string(),
                },
                Node {
                    name: "C".to_string(),
                },
                Node {
                    name: "D".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "A".to_string(),
                    to: "C".to_string(),
                },
                Edge {
                    from: "B".to_string(),
                    to: "C".to_string(),
                },
                Edge {
                    from: "C".to_string(),
                    to: "D".to_string(),
                },
            ],
        };

        let mut failed_tasks = HashSet::new();
        let pipeline = Pipeline { levels: vec![] };

        assert!(!pipeline.should_skip_task("A", &graph, &failed_tasks));
        assert!(!pipeline.should_skip_task("B", &graph, &failed_tasks));
        assert!(!pipeline.should_skip_task("C", &graph, &failed_tasks));
        assert!(!pipeline.should_skip_task("D", &graph, &failed_tasks));

        failed_tasks.insert("A".to_string());
        assert!(pipeline.should_skip_task("C", &graph, &failed_tasks));
        assert!(!pipeline.should_skip_task("D", &graph, &failed_tasks));

        failed_tasks.insert("B".to_string());
        assert!(pipeline.should_skip_task("C", &graph, &failed_tasks));

        failed_tasks.remove("A");
        assert!(pipeline.should_skip_task("C", &graph, &failed_tasks));

        Ok(())
    }
}
