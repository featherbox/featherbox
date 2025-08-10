use crate::config::{Config, model::ModelConfig};
use crate::pipeline::{
    delta::{DeltaManager, DeltaMetadata, DeltaProcessor},
    ducklake::DuckLake,
};
use anyhow::{Context, Result};
use sea_orm::DatabaseConnection;
use std::collections::HashMap;

pub struct Transformer<'a> {
    ducklake: &'a DuckLake,
    delta_processor: DeltaProcessor<'a>,
}

impl<'a> Transformer<'a> {
    pub fn new(ducklake: &'a DuckLake) -> Self {
        let delta_processor = DeltaProcessor::new(ducklake);
        Self {
            ducklake,
            delta_processor,
        }
    }

    pub fn transform_model(&self, model: &ModelConfig, model_name: &str) -> Result<()> {
        let create_table_sql =
            format!("CREATE OR REPLACE TABLE {} AS ({});", model_name, model.sql);

        self.ducklake
            .execute_batch(&create_table_sql)
            .with_context(|| {
                format!("Failed to execute model transformation. SQL: {create_table_sql}")
            })?;

        Ok(())
    }

    pub async fn transform_model_with_delta(
        &self,
        model: &ModelConfig,
        model_name: &str,
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
        action_id: i32,
        dependency_deltas: &HashMap<String, DeltaMetadata>,
    ) -> Result<Option<DeltaMetadata>> {
        if dependency_deltas.is_empty() {
            self.transform_model(model, model_name)?;
            return Ok(None);
        }

        let modified_sql = self.rewrite_sql_for_deltas(&model.sql, dependency_deltas)?;

        let delta_files = delta_manager.create_delta_files(model_name)?;

        self.delta_processor
            .create_model_delta(&delta_files, &modified_sql)?;

        let delta_metadata = delta_manager
            .save_delta_metadata(app_db, action_id, &delta_files)
            .await?;

        self.delta_processor
            .apply_model_delta_to_table(model_name, &delta_files)?;

        Ok(Some(delta_metadata))
    }

    pub fn rewrite_sql_for_deltas(
        &self,
        sql: &str,
        dependency_deltas: &HashMap<String, DeltaMetadata>,
    ) -> Result<String> {
        use sqlparser::{dialect::DuckDbDialect, parser::Parser};

        let dialect = DuckDbDialect {};
        let parsed = Parser::parse_sql(&dialect, sql)
            .with_context(|| format!("Failed to parse SQL: {sql}"))?;

        if parsed.is_empty() {
            return Err(anyhow::anyhow!("Empty SQL statement"));
        }

        let mut modified_sql = sql.to_string();

        for (table_name, delta_metadata) in dependency_deltas {
            let delta_function_call =
                format!("read_parquet('{}')", delta_metadata.insert_delta_path);

            let patterns = vec![
                (
                    format!(r"(?i)\bFROM\s+{}\b", regex::escape(table_name)),
                    format!("FROM {delta_function_call}"),
                ),
                (
                    format!(r"(?i)\bJOIN\s+{}\b", regex::escape(table_name)),
                    format!("JOIN {delta_function_call}"),
                ),
                (
                    format!(r"(?i)\b{}\s+AS\b", regex::escape(table_name)),
                    format!("{delta_function_call} AS"),
                ),
                (
                    format!(r",\s*{}\b", regex::escape(table_name)),
                    format!(", {delta_function_call}"),
                ),
            ];

            for (pattern_str, replacement) in patterns {
                let re = regex::Regex::new(&pattern_str).with_context(|| {
                    format!("Failed to create regex for pattern: {pattern_str}")
                })?;

                modified_sql = re
                    .replace_all(&modified_sql, replacement.as_str())
                    .to_string();
            }
        }

        let _validated = Parser::parse_sql(&dialect, &modified_sql)
            .with_context(|| format!("Modified SQL is not valid: {modified_sql}"))?;

        Ok(modified_sql)
    }

    pub async fn collect_dependency_deltas(
        &self,
        model_table_name: &str,
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ducklake::{CatalogConfig, StorageConfig};
    use chrono::Utc;

    #[tokio::test]
    async fn test_transform_model() {
        let test_dir = "/tmp/transform_processor_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();
        let processor = Transformer::new(&ducklake);

        ducklake
            .execute_batch(
                "CREATE TABLE source_table (id INTEGER, name VARCHAR, value INTEGER);
                 INSERT INTO source_table VALUES (1, 'Alice', 100), (2, 'Bob', 200);",
            )
            .unwrap();

        let model = ModelConfig {
            description: Some("Test model".to_string()),
            sql: "SELECT id, name, value * 2 as doubled_value FROM source_table WHERE value > 150"
                .to_string(),
            max_age: None,
        };

        processor.transform_model(&model, "result_table").unwrap();

        let verify_sql = "SELECT * FROM result_table ORDER BY id";
        let results = ducklake.query(verify_sql).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec!["2".to_string(), "Bob".to_string(), "400".to_string()]
        );
    }

    #[tokio::test]
    async fn test_rewrite_sql_for_deltas() {
        let test_dir = "/tmp/transform_processor_rewrite_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();
        let processor = Transformer::new(&ducklake);

        let mut dependency_deltas = HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/path/to/delta_users_insert.parquet".to_string(),
                created_at: Utc::now(),
            },
        );

        let original_sql = "SELECT id, name FROM users WHERE active = true";
        let rewritten_sql = processor
            .rewrite_sql_for_deltas(original_sql, &dependency_deltas)
            .unwrap();

        let expected = "SELECT id, name FROM read_parquet('/path/to/delta_users_insert.parquet') WHERE active = true";
        assert_eq!(rewritten_sql, expected);

        let multi_table_sql =
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id";
        dependency_deltas.insert(
            "posts".to_string(),
            DeltaMetadata {
                action_id: 2,
                insert_delta_path: "/path/to/delta_posts_insert.parquet".to_string(),
                created_at: Utc::now(),
            },
        );

        let rewritten_multi = processor
            .rewrite_sql_for_deltas(multi_table_sql, &dependency_deltas)
            .unwrap();
        assert!(rewritten_multi.contains("read_parquet('/path/to/delta_users_insert.parquet')"));
        assert!(rewritten_multi.contains("read_parquet('/path/to/delta_posts_insert.parquet')"));
    }

    #[tokio::test]
    async fn test_rewrite_sql_edge_cases() {
        let test_dir = "/tmp/transform_processor_edge_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();
        let processor = Transformer::new(&ducklake);

        let mut dependency_deltas = HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/path/to/delta_users_insert.parquet".to_string(),
                created_at: Utc::now(),
            },
        );
        dependency_deltas.insert(
            "user".to_string(),
            DeltaMetadata {
                action_id: 2,
                insert_delta_path: "/path/to/delta_user_insert.parquet".to_string(),
                created_at: Utc::now(),
            },
        );

        let sql_with_column = "SELECT user.name, users.email FROM users WHERE user.active = true";
        let rewritten = processor
            .rewrite_sql_for_deltas(sql_with_column, &dependency_deltas)
            .unwrap();

        assert!(
            rewritten.contains("user.name"),
            "Column 'user.name' was incorrectly replaced"
        );
        assert!(
            rewritten.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Table 'users' was not replaced with delta"
        );

        let invalid_sql = "INVALID SQL SYNTAX HERE";
        let result = processor.rewrite_sql_for_deltas(invalid_sql, &dependency_deltas);
        assert!(result.is_err(), "Invalid SQL should return an error");

        let empty_sql = "";
        let empty_result = processor.rewrite_sql_for_deltas(empty_sql, &dependency_deltas);
        assert!(empty_result.is_err(), "Empty SQL should return an error");
    }
}
