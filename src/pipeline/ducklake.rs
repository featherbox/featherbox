use crate::config::{adapter::AdapterConfig, model::ModelConfig};
use crate::pipeline::file_pattern::FilePatternProcessor;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Sqlite { path: String },
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    LocalFile { path: String },
}

pub struct DuckLake {
    catalog_config: CatalogConfig,
    storage_config: StorageConfig,
    connection: Connection,
}

impl DuckLake {
    pub async fn new(catalog_config: CatalogConfig, storage_config: StorageConfig) -> Result<Self> {
        let connection =
            Connection::open_in_memory().context("Failed to create DuckDB connection")?;

        let instance = Self {
            catalog_config,
            storage_config,
            connection,
        };

        instance.initialize().await?;
        Ok(instance)
    }

    async fn initialize(&self) -> Result<()> {
        self.connection
            .execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("Failed to install and load extensions")?;

        match &self.catalog_config {
            CatalogConfig::Sqlite { path } => {
                let catalog_path = Path::new(path);
                if let Some(parent) = catalog_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context("Failed to create catalog directory")?;
                }

                let data_path = match &self.storage_config {
                    StorageConfig::LocalFile { path } => path,
                };

                self.connection
                    .execute_batch("INSTALL sqlite; LOAD sqlite;")
                    .context("Failed to install and load SQLite extension")?;

                let attach_sql = format!(
                    "ATTACH 'ducklake:sqlite:{path}' AS db (DATA_PATH '{data_path}'); USE db;"
                );
                self.connection
                    .execute_batch(&attach_sql)
                    .context("Failed to attach DuckLake catalog")?;
            }
        }

        match &self.storage_config {
            StorageConfig::LocalFile { path } => {
                std::fs::create_dir_all(path).context("Failed to create storage directory")?;
            }
        }

        Ok(())
    }

    pub async fn extract_and_load(&self, adapter: &AdapterConfig, table_name: &str) -> Result<()> {
        let file_paths = FilePatternProcessor::process_pattern(&adapter.file.path, adapter)?;

        let create_and_load_sql =
            self.build_create_and_load_sql_multiple(table_name, adapter, &file_paths)?;

        self.connection
            .execute_batch(&create_and_load_sql)
            .context("Failed to create and load data")?;

        Ok(())
    }

    pub async fn transform(&self, model: &ModelConfig, model_name: &str) -> Result<()> {
        let create_table_sql =
            format!("CREATE OR REPLACE TABLE {} AS ({});", model_name, model.sql);

        println!("    Executing SQL: {create_table_sql}");

        self.connection
            .execute_batch(&create_table_sql)
            .with_context(|| {
                format!("Failed to execute model transformation. SQL: {create_table_sql}")
            })?;

        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let mut stmt = self.connection.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let column_count = rows.as_ref().unwrap().column_count();

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                use duckdb::types::Value;
                let value: Result<Value, _> = row.get(i);
                let string_value = match value {
                    Ok(Value::Null) => "NULL".to_string(),
                    Ok(Value::Boolean(b)) => b.to_string(),
                    Ok(Value::TinyInt(i)) => i.to_string(),
                    Ok(Value::SmallInt(i)) => i.to_string(),
                    Ok(Value::Int(i)) => i.to_string(),
                    Ok(Value::BigInt(i)) => i.to_string(),
                    Ok(Value::HugeInt(i)) => i.to_string(),
                    Ok(Value::UTinyInt(i)) => i.to_string(),
                    Ok(Value::USmallInt(i)) => i.to_string(),
                    Ok(Value::UInt(i)) => i.to_string(),
                    Ok(Value::UBigInt(i)) => i.to_string(),
                    Ok(Value::Float(f)) => f.to_string(),
                    Ok(Value::Double(f)) => f.to_string(),
                    Ok(Value::Decimal(d)) => d.to_string(),
                    Ok(Value::Text(s)) => s,
                    Ok(Value::Blob(b)) => format!("{b:?}"),
                    Ok(Value::Date32(d)) => d.to_string(),
                    Ok(Value::Time64(_, t)) => t.to_string(),
                    Ok(Value::Timestamp(_, t)) => t.to_string(),
                    Ok(Value::Interval {
                        months,
                        days,
                        nanos,
                    }) => {
                        format!("Interval({months} months, {days} days, {nanos} nanos)")
                    }
                    Ok(_) => "UNKNOWN".to_string(),
                    Err(_) => "ERROR".to_string(),
                };
                row_data.push(string_value);
            }
            results.push(row_data);
        }

        Ok(results)
    }

    fn build_create_and_load_sql_multiple(
        &self,
        table_name: &str,
        adapter: &AdapterConfig,
        file_paths: &[String],
    ) -> Result<String> {
        if file_paths.is_empty() {
            return Err(anyhow::anyhow!("No files to load"));
        }

        if file_paths.len() == 1 {
            let file_path = &file_paths[0];
            return self.build_create_and_load_sql_single(table_name, adapter, file_path);
        }

        let file_paths_str = file_paths
            .iter()
            .map(|p| format!("'{p}'"))
            .collect::<Vec<_>>()
            .join(", ");

        match adapter.format.ty.as_str() {
            "csv" => {
                let has_header = adapter.format.has_header.unwrap_or(true);
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto([{file_paths_str}], header={has_header});"
                );
                Ok(sql)
            }
            "parquet" => {
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet([{file_paths_str}]);"
                );
                Ok(sql)
            }
            "json" => {
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto([{file_paths_str}]);"
                );
                Ok(sql)
            }
            _ => Err(anyhow::anyhow!("Unsupported format: {}", adapter.format.ty)),
        }
    }

    fn build_create_and_load_sql_single(
        &self,
        table_name: &str,
        adapter: &AdapterConfig,
        file_path: &str,
    ) -> Result<String> {
        match adapter.format.ty.as_str() {
            "csv" => {
                let has_header = adapter.format.has_header.unwrap_or(true);
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}', header={has_header});"
                );
                Ok(sql)
            }
            "parquet" => {
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}');"
                );
                Ok(sql)
            }
            "json" => {
                let sql = format!(
                    "CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{file_path}');"
                );
                Ok(sql)
            }
            _ => Err(anyhow::anyhow!("Unsupported format: {}", adapter.format.ty)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig, UpdateStrategyConfig};
    use sqlparser::{dialect::DuckDbDialect, parser::Parser};

    #[tokio::test]
    async fn test_ducklake_new() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        if let Err(e) = &ducklake {
            println!("Error: {e}");
        }
        assert!(ducklake.is_ok());
    }

    #[tokio::test]
    async fn test_build_create_and_load_sql() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test3";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let adapter = AdapterConfig {
            connection: "test_table".to_string(),
            description: None,
            file: FileConfig {
                path: "test.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: Some(UpdateStrategyConfig {
                detection: "full".to_string(),
                timestamp_from: None,
                range: None,
            }),
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
            columns: vec![],
            limits: None,
        };

        let sql = ducklake
            .build_create_and_load_sql_single("test_table", &adapter, "test.csv")
            .unwrap();

        let dialect = DuckDbDialect {};
        let parsed = Parser::parse_sql(&dialect, &sql).unwrap();

        let expected_sql = "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto('test.csv', header=true);";
        let expected_parsed = Parser::parse_sql(&dialect, expected_sql).unwrap();

        assert_eq!(parsed, expected_parsed);
    }

    #[tokio::test]
    async fn test_build_create_and_load_sql_with_wildcards() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_wildcards";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let test_cases = vec![
            (
                "*.csv",
                "csv",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto('*.csv', header=true);",
            ),
            (
                "data/*.csv",
                "csv",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto('data/*.csv', header=true);",
            ),
            (
                "logs/*/*.csv",
                "csv",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto('logs/*/*.csv', header=true);",
            ),
            (
                "**/*.csv",
                "csv",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto('**/*.csv', header=true);",
            ),
            (
                "data/*.json",
                "json",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_json_auto('data/*.json');",
            ),
            (
                "logs/*/*.parquet",
                "parquet",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_parquet('logs/*/*.parquet');",
            ),
            (
                "2024/*/sales_*.parquet",
                "parquet",
                "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_parquet('2024/*/sales_*.parquet');",
            ),
        ];

        for (path, format_type, expected_sql) in test_cases {
            let adapter = AdapterConfig {
                connection: "test_table".to_string(),
                description: None,
                file: FileConfig {
                    path: path.to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                update_strategy: None,
                format: FormatConfig {
                    ty: format_type.to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: None,
                },
                columns: vec![],
                limits: None,
            };

            let sql = ducklake
                .build_create_and_load_sql_single("test_table", &adapter, path)
                .unwrap();

            assert_eq!(sql, expected_sql, "Failed for path: {path}");
        }
    }

    #[tokio::test]
    async fn test_build_create_and_load_sql_multiple() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_multiple";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let adapter = AdapterConfig {
            connection: "test_table".to_string(),
            description: None,
            file: FileConfig {
                path: "data/logs.csv".to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
            columns: vec![],
            limits: None,
        };

        let file_paths = vec![
            "data/logs_2024-01-01.csv".to_string(),
            "data/logs_2024-01-02.csv".to_string(),
            "data/logs_2024-01-03.csv".to_string(),
        ];

        let sql = ducklake
            .build_create_and_load_sql_multiple("test_table", &adapter, &file_paths)
            .unwrap();

        let expected_sql = "CREATE OR REPLACE TABLE test_table AS SELECT * FROM read_csv_auto(['data/logs_2024-01-01.csv', 'data/logs_2024-01-02.csv', 'data/logs_2024-01-03.csv'], header=true);";
        assert_eq!(sql, expected_sql);
    }
}
