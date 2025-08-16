use crate::{
    config::Config,
    dependency::Graph,
    pipeline::{
        adapter::Adapter,
        build::{Action, Pipeline},
        delta::DeltaManager,
        ducklake::DuckLake,
        model::Model,
    },
};
use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::task::JoinSet;

impl Pipeline {
    async fn get_latest_pipeline_action_ids(
        &self,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<Vec<i32>> {
        use crate::database::entities::{pipeline_actions, pipelines};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder};

        let latest_pipeline = pipelines::Entity::find()
            .order_by_desc(pipelines::Column::CreatedAt)
            .one(app_db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No pipeline found in database"))?;

        let mut action_ids = Vec::new();

        for action in self.all_actions() {
            let pipeline_action = pipeline_actions::Entity::find()
                .filter(pipeline_actions::Column::PipelineId.eq(latest_pipeline.id))
                .filter(pipeline_actions::Column::TableName.eq(&action.table_name))
                .one(app_db)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Action for table '{}' not found in latest pipeline",
                        action.table_name
                    )
                })?;

            action_ids.push(pipeline_action.id);
        }

        Ok(action_ids)
    }

    pub async fn execute(
        &self,
        graph: &Graph,
        config: &Config,
        ducklake: &DuckLake,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<()> {
        let shared_ducklake = Arc::new(ducklake.clone());
        let shared_delta_manager = Arc::new(DeltaManager::new(
            &config.project_root,
            Arc::clone(&shared_ducklake),
        )?);

        let mut adapters = HashMap::new();
        for (table_name, adapter_config) in &config.adapters {
            let adapter = Adapter::new(adapter_config.clone(), Arc::clone(&shared_delta_manager));
            adapters.insert(table_name.clone(), adapter);
        }
        let shared_adapters = Arc::new(adapters);

        let mut models = HashMap::new();
        for (table_name, model_config) in &config.models {
            let model = Model::new(
                model_config.clone(),
                Arc::clone(&shared_ducklake),
                Arc::clone(&shared_delta_manager),
            );
            models.insert(table_name.clone(), model);
        }
        let shared_models = Arc::new(models);

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;
        let mut action_id_map = HashMap::new();
        let all_actions = self.all_actions();
        for (idx, action) in all_actions.iter().enumerate() {
            action_id_map.insert(action.table_name.clone(), action_ids[idx]);
        }

        let mut failed_tasks = HashSet::new();

        for level in &self.levels {
            self.execute_level(
                level,
                graph,
                &mut failed_tasks,
                app_db,
                &shared_adapters,
                &shared_models,
                config,
                &action_id_map,
            )
            .await?;
        }

        if !failed_tasks.is_empty() {
            eprintln!("Pipeline completed with failures. Failed tasks: {failed_tasks:?}");
            return Err(anyhow::anyhow!(
                "Pipeline execution had {} failed tasks",
                failed_tasks.len()
            ));
        }

        Ok(())
    }

    async fn execute_level(
        &self,
        actions: &[Action],
        graph: &Graph,
        failed_tasks: &mut HashSet<String>,
        app_db: &sea_orm::DatabaseConnection,
        shared_adapters: &Arc<HashMap<String, Adapter>>,
        shared_models: &Arc<HashMap<String, Model>>,
        config: &Config,
        action_id_map: &HashMap<String, i32>,
    ) -> Result<()> {
        let mut task_set = JoinSet::new();

        for action in actions {
            if failed_tasks.contains(&action.table_name) {
                continue;
            }

            let should_skip = self.should_skip_task(&action.table_name, graph, failed_tasks);
            if should_skip {
                failed_tasks.insert(action.table_name.clone());
                continue;
            }

            self.spawn_task(
                action,
                &mut task_set,
                app_db,
                shared_adapters,
                shared_models,
                config,
                action_id_map,
            )?;
        }

        self.collect_task_results(&mut task_set, graph, failed_tasks)
            .await
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
        task_set: &mut JoinSet<Result<String, (String, anyhow::Error)>>,
        app_db: &sea_orm::DatabaseConnection,
        shared_adapters: &Arc<HashMap<String, Adapter>>,
        shared_models: &Arc<HashMap<String, Model>>,
        config: &Config,
        action_id_map: &HashMap<String, i32>,
    ) -> Result<()> {
        let action_id = *action_id_map.get(&action.table_name).unwrap();
        let table_name = action.table_name.clone();
        let time_range = action.time_range.clone();
        let app_db_clone = app_db.clone();
        let connections_clone = config.project.connections.clone();
        let config_clone = Config {
            project: config.project.clone(),
            adapters: config.adapters.clone(),
            models: config.models.clone(),
            project_root: config.project_root.clone(),
        };

        if let Some(adapter) = shared_adapters.get(&action.table_name).cloned() {
            task_set.spawn(async move {
                match adapter
                    .execute_import(
                        &table_name,
                        time_range,
                        &app_db_clone,
                        action_id,
                        Some(&connections_clone),
                    )
                    .await
                {
                    Ok(_) => Ok(table_name.clone()),
                    Err(e) => Err((table_name.clone(), e)),
                }
            });
        } else if let Some(model) = shared_models.get(&action.table_name).cloned() {
            task_set.spawn(async move {
                match model
                    .execute_transform(&table_name, &app_db_clone, action_id, &config_clone)
                    .await
                {
                    Ok(_) => Ok(table_name.clone()),
                    Err(e) => Err((table_name.clone(), e)),
                }
            });
        } else {
            return Err(anyhow::anyhow!(
                "Table '{}' not found in adapters or models",
                action.table_name
            ));
        }

        Ok(())
    }

    async fn collect_task_results(
        &self,
        task_set: &mut JoinSet<Result<String, (String, anyhow::Error)>>,
        graph: &Graph,
        failed_tasks: &mut HashSet<String>,
    ) -> Result<()> {
        while let Some(result) = task_set.join_next().await {
            match result {
                Ok(Ok(table_name)) => {
                    println!("Task completed successfully: {table_name}");
                }
                Ok(Err((table_name, e))) => {
                    eprintln!("Task failed for table '{table_name}': {e}");
                    failed_tasks.insert(table_name.clone());
                    self.mark_downstream_as_failed(&table_name, graph, failed_tasks);
                }
                Err(join_error) => {
                    eprintln!("Task join error: {join_error}");
                }
            }
        }
        Ok(())
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
