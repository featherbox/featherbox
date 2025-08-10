use crate::{
    config::{Config, adapter::RangeConfig},
    dependency::{
        graph::{Edge, Graph, Node},
        metadata::get_executed_ranges_for_graph,
    },
    pipeline::{
        delta::{DeltaManager, DeltaMetadata},
        ducklake::DuckLake,
    },
};
use anyhow::{Context, Result};
use sea_orm::DatabaseConnection;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub struct Action {
    pub table_name: String,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimeRange {
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    pub until: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug)]
pub struct Pipeline {
    pub actions: Vec<Action>,
}

impl Pipeline {
    pub fn from_graph(graph: &Graph) -> Self {
        let sorted_nodes = topological_sort(graph);
        let actions = sorted_nodes
            .into_iter()
            .map(|node_name| Action {
                table_name: node_name,
                time_range: Some(TimeRange {
                    since: None,
                    until: None,
                }),
            })
            .collect();

        Pipeline { actions }
    }

    pub async fn from_graph_with_ranges(
        graph: &Graph,
        config: &Config,
        db: &sea_orm::DatabaseConnection,
        graph_id: i32,
    ) -> Result<Self> {
        let sorted_nodes = topological_sort(graph);
        let mut actions = Vec::new();

        for node_name in sorted_nodes {
            let remaining_range = Self::remaining_range(config, &node_name, db, graph_id).await?;

            actions.push(Action {
                table_name: node_name,
                time_range: remaining_range,
            });
        }

        Ok(Pipeline { actions })
    }

    async fn remaining_range(
        config: &Config,
        table_name: &str,
        db: &DatabaseConnection,
        graph_id: i32,
    ) -> Result<Option<TimeRange>> {
        let Some(adapter) = config.adapters.get(table_name) else {
            return Ok(None);
        };

        let Some(strategy) = &adapter.update_strategy else {
            return Ok(None);
        };

        let executed_ranges = get_executed_ranges_for_graph(db, graph_id, table_name).await?;
        Ok(calculate_remaining_range(&strategy.range, &executed_ranges))
    }

    pub fn create_partial_pipeline(graph: &Graph, affected_nodes: &[String]) -> Self {
        let subgraph = create_subgraph(graph, affected_nodes);
        Self::from_graph(&subgraph)
    }

    pub async fn execute(&self, config: &Config, ducklake: &DuckLake) -> Result<()> {
        for action in &self.actions {
            if let Some(adapter) = config.adapters.get(&action.table_name) {
                let file_paths =
                    crate::pipeline::file_pattern::FilePatternProcessor::process_pattern(
                        &adapter.file.path,
                        adapter,
                    )?;

                if !file_paths.is_empty() {
                    let sql = ducklake.build_create_and_load_sql_multiple(
                        &action.table_name,
                        adapter,
                        &file_paths,
                    )?;
                    ducklake.execute_batch(&sql).with_context(|| {
                        format!(
                            "Failed to execute adapter SQL for table '{}'",
                            action.table_name
                        )
                    })?;
                }
            } else if let Some(model) = config.models.get(&action.table_name) {
                ducklake.transform(model, &action.table_name).await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
    }

    pub async fn execute_with_delta(
        &self,
        config: &Config,
        ducklake: &DuckLake,
        app_db: &sea_orm::DatabaseConnection,
    ) -> Result<()> {
        let delta_manager = DeltaManager::new(&config.project_root)?;

        let action_ids = self.get_latest_pipeline_action_ids(app_db).await?;

        for (idx, action) in self.actions.iter().enumerate() {
            let action_id = action_ids[idx];

            if let Some(adapter) = config.adapters.get(&action.table_name) {
                let file_paths =
                    ducklake.files_for_processing(adapter, action.time_range.clone())?;

                if !file_paths.is_empty() {
                    ducklake
                        .process_delta(
                            adapter,
                            &action.table_name,
                            &file_paths,
                            &delta_manager,
                            app_db,
                            action_id,
                        )
                        .await?;
                }
            } else if let Some(model) = config.models.get(&action.table_name) {
                let dependency_deltas = self
                    .collect_dependency_deltas(&action.table_name, &delta_manager, app_db, config)
                    .await?;

                ducklake
                    .transform_with_delta(
                        model,
                        &action.table_name,
                        &delta_manager,
                        app_db,
                        action_id,
                        &dependency_deltas,
                    )
                    .await?;
            } else {
                return Err(anyhow::anyhow!(
                    "Table '{}' not found in adapters or models",
                    action.table_name
                ));
            }
        }

        Ok(())
    }

    async fn collect_dependency_deltas(
        &self,
        model_table_name: &str,
        delta_manager: &DeltaManager,
        app_db: &sea_orm::DatabaseConnection,
        config: &Config,
    ) -> Result<HashMap<String, DeltaMetadata>> {
        use crate::dependency::graph::from_table;

        let model = config
            .models
            .get(model_table_name)
            .ok_or_else(|| anyhow::anyhow!("Model {} not found", model_table_name))?;

        let dependencies = from_table(&model.sql);
        let mut dependency_deltas = HashMap::new();

        for dep_table in dependencies {
            if config.adapters.contains_key(&dep_table) {
                if let Some(delta_metadata) = delta_manager
                    .latest_delta_metadata(app_db, &dep_table)
                    .await?
                {
                    dependency_deltas.insert(dep_table, delta_metadata);
                }
            }
        }

        Ok(dependency_deltas)
    }

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

        for action in &self.actions {
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
}

fn topological_sort(graph: &Graph) -> Vec<String> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut adjacency_list: HashMap<String, Vec<String>> = HashMap::new();

    for node in &graph.nodes {
        in_degree.insert(node.name.clone(), 0);
        adjacency_list.insert(node.name.clone(), Vec::new());
    }

    for edge in &graph.edges {
        adjacency_list
            .get_mut(&edge.from)
            .unwrap()
            .push(edge.to.clone());
        *in_degree.get_mut(&edge.to).unwrap() += 1;
    }

    let mut queue: VecDeque<String> = VecDeque::new();
    for (node, &degree) in &in_degree {
        if degree == 0 {
            queue.push_back(node.clone());
        }
    }

    let mut sorted = Vec::new();

    while let Some(node) = queue.pop_front() {
        sorted.push(node.clone());

        if let Some(neighbors) = adjacency_list.get(&node) {
            for neighbor in neighbors {
                let degree = in_degree.get_mut(neighbor).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(neighbor.clone());
                }
            }
        }
    }

    sorted
}

fn create_subgraph(graph: &Graph, affected_nodes: &[String]) -> Graph {
    let affected_set: HashSet<String> = affected_nodes.iter().cloned().collect();

    let nodes: Vec<Node> = graph
        .nodes
        .iter()
        .filter(|node| affected_set.contains(&node.name))
        .map(|node| Node {
            name: node.name.clone(),
        })
        .collect();

    let edges: Vec<Edge> = graph
        .edges
        .iter()
        .filter(|edge| affected_set.contains(&edge.from) && affected_set.contains(&edge.to))
        .map(|edge| Edge {
            from: edge.from.clone(),
            to: edge.to.clone(),
        })
        .collect();

    Graph { nodes, edges }
}

fn calculate_remaining_range(
    config_range: &RangeConfig,
    executed_ranges: &[TimeRange],
) -> Option<TimeRange> {
    let config_since = config_range
        .since
        .map(|dt| chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));
    let config_until = config_range
        .until
        .map(|dt| chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc));

    if executed_ranges.is_empty() {
        return Some(TimeRange {
            since: config_since,
            until: config_until,
        });
    }

    let mut latest_until: Option<chrono::DateTime<chrono::Utc>> = None;

    for executed_range in executed_ranges {
        if let Some(until) = executed_range.until {
            if latest_until.is_none() || Some(until) > latest_until {
                latest_until = Some(until);
            }
        }
    }

    match (config_since, config_until, latest_until) {
        (Some(config_since), Some(config_until), Some(latest_until)) => {
            if latest_until >= config_until {
                None // すべて処理済み
            } else if latest_until >= config_since {
                Some(TimeRange {
                    since: Some(latest_until),
                    until: Some(config_until),
                })
            } else {
                Some(TimeRange {
                    since: Some(config_since),
                    until: Some(config_until),
                })
            }
        }
        (Some(config_since), None, Some(latest_until)) => {
            if latest_until >= config_since {
                Some(TimeRange {
                    since: Some(latest_until),
                    until: None,
                })
            } else {
                Some(TimeRange {
                    since: Some(config_since),
                    until: None,
                })
            }
        }
        _ => Some(TimeRange {
            since: config_since,
            until: config_until,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{
            adapter::{AdapterConfig, FileConfig, FormatConfig},
            model::ModelConfig,
        },
        dependency::graph::{Edge, Node},
        pipeline::ducklake::{CatalogConfig, DuckLake},
    };
    use std::collections::HashMap;

    #[test]
    fn test_topological_sort_simple() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "b".to_string(),
                },
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "c".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
            ],
        };

        let sorted = topological_sort(&graph);
        assert_eq!(sorted, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_topological_sort_multiple_roots() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "c".to_string(),
                },
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "d".to_string(),
                },
                Node {
                    name: "b".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "c".to_string(),
                    to: "d".to_string(),
                },
                Edge {
                    from: "a".to_string(),
                    to: "c".to_string(),
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
            ],
        };

        let sorted = topological_sort(&graph);
        assert!(
            sorted.iter().position(|x| x == "a").unwrap()
                < sorted.iter().position(|x| x == "c").unwrap()
        );
        assert!(
            sorted.iter().position(|x| x == "b").unwrap()
                < sorted.iter().position(|x| x == "c").unwrap()
        );
        assert!(
            sorted.iter().position(|x| x == "c").unwrap()
                < sorted.iter().position(|x| x == "d").unwrap()
        );
    }

    #[test]
    fn test_pipeline_from_graph() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "user_stats".to_string(),
                },
            ],
        };

        let pipeline = Pipeline::from_graph(&graph);

        assert_eq!(pipeline.actions.len(), 3);

        let users_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "users")
            .unwrap();
        let orders_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "orders")
            .unwrap();
        let user_stats_pos = pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "user_stats")
            .unwrap();

        assert!(users_pos < user_stats_pos);
        assert!(orders_pos < user_stats_pos);
    }

    #[tokio::test]
    async fn test_pipeline_execute() {
        use std::fs;

        let test_dir = "/tmp/pipeline_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();
        fs::create_dir_all(format!("{test_dir}/data")).unwrap();

        let users_csv = format!("{test_dir}/data/users.csv");
        fs::write(&users_csv, "id,name\n1,Alice\n2,Bob").unwrap();

        let orders_csv = format!("{test_dir}/data/orders.csv");
        fs::write(&orders_csv, "id,user_id,amount\n1,1,100\n2,2,200").unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test.sqlite"),
        };
        let storage_config = crate::pipeline::ducklake::StorageConfig::LocalFile {
            path: format!("{test_dir}/storage"),
        };
        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut adapters = HashMap::new();
        adapters.insert(
            "users".to_string(),
            AdapterConfig {
                connection: "users".to_string(),
                description: None,
                file: FileConfig {
                    path: users_csv.clone(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
                columns: vec![],
                limits: None,
            },
        );
        adapters.insert(
            "orders".to_string(),
            AdapterConfig {
                connection: "orders".to_string(),
                description: None,
                file: FileConfig {
                    path: orders_csv.clone(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
                columns: vec![],
                limits: None,
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "user_stats".to_string(),
            ModelConfig {
                description: None,
                max_age: None,
                sql: r#"
                    SELECT
                        u.id,
                        u.name,
                        COUNT(o.id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM
                        users u
                    LEFT JOIN
                        orders o ON u.id = o.user_id
                    GROUP BY
                        u.id, u.name
                "#
                .to_string(),
            },
        );

        let config = Config {
            project: crate::config::project::ProjectConfig {
                storage: crate::config::project::StorageConfig {
                    ty: crate::config::project::StorageType::Local,
                    path: format!("{test_dir}/storage"),
                },
                database: crate::config::project::DatabaseConfig {
                    ty: crate::config::project::DatabaseType::Sqlite,
                    path: format!("{test_dir}/test.sqlite"),
                },
                deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
                connections: HashMap::new(),
            },
            adapters,
            models,
            project_root: std::path::PathBuf::from(test_dir),
        };

        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "user_stats".to_string(),
                },
            ],
        };

        let pipeline = Pipeline::from_graph(&graph);
        let result = pipeline.execute(&config, &ducklake).await;

        assert!(result.is_ok());

        let users_result = ducklake
            .query("SELECT id, name FROM users ORDER BY id")
            .unwrap();
        assert_eq!(users_result.len(), 2);
        assert_eq!(users_result[0], vec!["1", "Alice"]);
        assert_eq!(users_result[1], vec!["2", "Bob"]);

        let orders_result = ducklake.query("SELECT * FROM orders ORDER BY id").unwrap();
        assert_eq!(orders_result.len(), 2);
        assert_eq!(orders_result[0], vec!["1", "1", "100"]);
        assert_eq!(orders_result[1], vec!["2", "2", "200"]);

        let user_stats_result = ducklake
            .query("SELECT * FROM user_stats ORDER BY id")
            .unwrap();
        assert_eq!(user_stats_result.len(), 2);
        assert_eq!(user_stats_result[0], vec!["1", "Alice", "1", "100"]);
        assert_eq!(user_stats_result[1], vec!["2", "Bob", "1", "200"]);
    }

    #[test]
    fn test_create_partial_pipeline() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "users".to_string(),
                },
                Node {
                    name: "orders".to_string(),
                },
                Node {
                    name: "user_stats".to_string(),
                },
                Node {
                    name: "order_summary".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "users".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "user_stats".to_string(),
                },
                Edge {
                    from: "orders".to_string(),
                    to: "order_summary".to_string(),
                },
            ],
        };

        let affected_nodes = vec!["orders".to_string(), "user_stats".to_string()];
        let partial_pipeline = Pipeline::create_partial_pipeline(&graph, &affected_nodes);

        assert_eq!(partial_pipeline.actions.len(), 2);

        let action_names: Vec<&str> = partial_pipeline
            .actions
            .iter()
            .map(|a| a.table_name.as_str())
            .collect();

        assert!(action_names.contains(&"orders"));
        assert!(action_names.contains(&"user_stats"));
        assert!(!action_names.contains(&"users"));
        assert!(!action_names.contains(&"order_summary"));

        let orders_pos = partial_pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "orders")
            .unwrap();
        let user_stats_pos = partial_pipeline
            .actions
            .iter()
            .position(|a| a.table_name == "user_stats")
            .unwrap();

        assert!(orders_pos < user_stats_pos);
    }

    #[test]
    fn test_create_subgraph() {
        let graph = Graph {
            nodes: vec![
                Node {
                    name: "a".to_string(),
                },
                Node {
                    name: "b".to_string(),
                },
                Node {
                    name: "c".to_string(),
                },
                Node {
                    name: "d".to_string(),
                },
            ],
            edges: vec![
                Edge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
                Edge {
                    from: "b".to_string(),
                    to: "c".to_string(),
                },
                Edge {
                    from: "c".to_string(),
                    to: "d".to_string(),
                },
            ],
        };

        let affected_nodes = vec!["b".to_string(), "c".to_string()];
        let subgraph = create_subgraph(&graph, &affected_nodes);

        assert_eq!(subgraph.nodes.len(), 2);
        let node_names: Vec<&str> = subgraph.nodes.iter().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains(&"b"));
        assert!(node_names.contains(&"c"));

        assert_eq!(subgraph.edges.len(), 1);
        assert_eq!(subgraph.edges[0].from, "b");
        assert_eq!(subgraph.edges[0].to, "c");
    }

    #[test]
    fn test_calculate_remaining_range_empty_executed() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };
        let executed_ranges = vec![];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        let result = result.unwrap();
        assert!(result.since.is_some());
        assert!(result.until.is_some());
        assert_eq!(
            result.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert_eq!(
            result.until.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }

    #[test]
    fn test_calculate_remaining_range_partial_executed() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };
        let executed_ranges = vec![TimeRange {
            since: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                chrono::Utc,
            )),
            until: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 6, 30)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
                chrono::Utc,
            )),
        }];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        let result = result.unwrap();
        assert!(result.since.is_some());
        assert!(result.until.is_some());
        assert_eq!(
            result.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
        );
        assert_eq!(
            result.until.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }

    #[test]
    fn test_calculate_remaining_range_fully_executed() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };
        let executed_ranges = vec![TimeRange {
            since: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                chrono::Utc,
            )),
            until: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
                chrono::Utc,
            )),
        }];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        assert!(result.is_none()); // すべて処理済み
    }

    #[test]
    fn test_calculate_remaining_range_multiple_executed_ranges() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };
        let executed_ranges = vec![
            TimeRange {
                since: Some(chrono::DateTime::from_naive_utc_and_offset(
                    chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                    chrono::Utc,
                )),
                until: Some(chrono::DateTime::from_naive_utc_and_offset(
                    chrono::NaiveDate::from_ymd_opt(2024, 3, 31)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                    chrono::Utc,
                )),
            },
            TimeRange {
                since: Some(chrono::DateTime::from_naive_utc_and_offset(
                    chrono::NaiveDate::from_ymd_opt(2024, 4, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                    chrono::Utc,
                )),
                until: Some(chrono::DateTime::from_naive_utc_and_offset(
                    chrono::NaiveDate::from_ymd_opt(2024, 8, 31)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                    chrono::Utc,
                )),
            },
        ];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        let result = result.unwrap();
        assert!(result.since.is_some());
        assert!(result.until.is_some());
        // Should return from the latest executed until date
        assert_eq!(
            result.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 8, 31).unwrap()
        );
        assert_eq!(
            result.until.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }

    #[test]
    fn test_calculate_remaining_range_open_ended() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: None,
        };
        let executed_ranges = vec![TimeRange {
            since: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                chrono::Utc,
            )),
            until: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 6, 30)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
                chrono::Utc,
            )),
        }];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        let result = result.unwrap();
        assert!(result.since.is_some());
        assert!(result.until.is_none());
        assert_eq!(
            result.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 30).unwrap()
        );
    }

    #[test]
    fn test_calculate_remaining_range_executed_before_config_start() {
        let config_range = RangeConfig {
            since: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 6, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };
        let executed_ranges = vec![TimeRange {
            since: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                chrono::Utc,
            )),
            until: Some(chrono::DateTime::from_naive_utc_and_offset(
                chrono::NaiveDate::from_ymd_opt(2024, 3, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
                chrono::Utc,
            )),
        }];

        let result = calculate_remaining_range(&config_range, &executed_ranges);

        let result = result.unwrap();
        assert!(result.since.is_some());
        assert!(result.until.is_some());
        assert_eq!(
            result.since.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()
        );
        assert_eq!(
            result.until.unwrap().date_naive(),
            chrono::NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }
}
