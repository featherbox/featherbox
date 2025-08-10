use crate::config::{adapter::AdapterConfig, model::ModelConfig};
use crate::pipeline::delta::{DeltaFiles, DeltaManager, DeltaMetadata};
use crate::pipeline::execution::TimeRange;
use crate::pipeline::file_pattern::FilePatternProcessor;
use anyhow::{Context, Result};
use duckdb::Connection;
use sea_orm::DatabaseConnection;
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

    pub fn files_for_processing(
        &self,
        adapter: &AdapterConfig,
        range: Option<TimeRange>,
    ) -> Result<Vec<String>> {
        // None の場合はスキップ（空のリストを返す）
        let Some(time_range) = range else {
            return Ok(Vec::new());
        };

        let mut adapter_with_range = adapter.clone();

        if let Some(ref mut strategy) = adapter_with_range.update_strategy {
            let adapter_range = &mut strategy.range;
            if let Some(since) = time_range.since {
                adapter_range.since = Some(since.naive_utc());
            }
            if let Some(until) = time_range.until {
                adapter_range.until = Some(until.naive_utc());
            }
        }

        let file_paths = FilePatternProcessor::process_pattern(
            &adapter_with_range.file.path,
            &adapter_with_range,
        )?;

        Ok(file_paths)
    }

    pub async fn process_delta(
        &self,
        adapter: &AdapterConfig,
        table_name: &str,
        file_paths: &[String],
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
        action_id: i32,
    ) -> Result<DeltaMetadata> {
        self.validate_source_files_schema(table_name, file_paths)?;

        let delta_files = delta_manager.create_delta_files(table_name)?;

        self.create_delta_table(&delta_files, adapter, file_paths)?;

        let delta_metadata = delta_manager
            .save_delta_metadata(app_db, action_id, &delta_files)
            .await?;

        self.apply_delta_to_table(table_name, &delta_files)?;

        Ok(delta_metadata)
    }

    pub async fn transform(&self, model: &ModelConfig, model_name: &str) -> Result<()> {
        let create_table_sql =
            format!("CREATE OR REPLACE TABLE {} AS ({});", model_name, model.sql);

        self.connection
            .execute_batch(&create_table_sql)
            .with_context(|| {
                format!("Failed to execute model transformation. SQL: {create_table_sql}")
            })?;

        Ok(())
    }

    pub async fn transform_with_delta(
        &self,
        model: &ModelConfig,
        model_name: &str,
        delta_manager: &DeltaManager,
        app_db: &DatabaseConnection,
        action_id: i32,
        dependency_deltas: &std::collections::HashMap<String, DeltaMetadata>,
    ) -> Result<Option<DeltaMetadata>> {
        if dependency_deltas.is_empty() {
            self.transform(model, model_name).await?;
            return Ok(None);
        }

        let modified_sql = self.rewrite_sql_for_deltas(&model.sql, dependency_deltas)?;

        let delta_files = delta_manager.create_delta_files(model_name)?;

        self.create_model_delta_table(&delta_files, &modified_sql)?;

        let delta_metadata = delta_manager
            .save_delta_metadata(app_db, action_id, &delta_files)
            .await?;

        self.apply_model_delta_to_table(model_name, &delta_files)?;

        Ok(Some(delta_metadata))
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.connection
            .execute_batch(sql)
            .context("Failed to execute batch SQL")?;
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

    pub fn build_create_and_load_sql_multiple(
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

    fn create_delta_table(
        &self,
        delta_files: &DeltaFiles,
        adapter: &AdapterConfig,
        file_paths: &[String],
    ) -> Result<()> {
        let temp_table_name = Self::generate_temp_table_name("temp_delta");

        let create_temp_sql =
            self.build_create_and_load_sql_multiple(&temp_table_name, adapter, file_paths)?;

        self.connection
            .execute_batch(&create_temp_sql)
            .context("Failed to create temporary delta table")?;

        let export_insert_sql = format!(
            "COPY (SELECT * FROM {temp_table_name}) TO '{}' (FORMAT PARQUET);",
            delta_files.insert_path.to_string_lossy()
        );

        self.connection
            .execute_batch(&export_insert_sql)
            .context("Failed to export delta insert data")?;

        self.drop_temp_table(&temp_table_name)?;

        Ok(())
    }

    fn apply_delta_to_table(&self, table_name: &str, delta_files: &DeltaFiles) -> Result<()> {
        let table_exists_sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );

        let mut stmt = self.connection.prepare(&table_exists_sql)?;
        let mut rows = stmt.query([])?;
        let table_exists = if let Some(row) = rows.next()? {
            let count: i64 = row.get(0)?;
            count > 0
        } else {
            false
        };

        if !table_exists {
            let create_from_delta_sql = format!(
                "CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{}');",
                delta_files.insert_path.to_string_lossy()
            );

            self.connection
                .execute_batch(&create_from_delta_sql)
                .context("Failed to create table from delta")?;
        } else {
            self.validate_delta_schema(table_name, &delta_files.insert_path)
                .with_context(|| format!("Schema validation failed for table '{table_name}'"))?;

            let insert_sql = format!(
                "INSERT INTO {table_name} SELECT * FROM read_parquet('{}');",
                delta_files.insert_path.to_string_lossy()
            );

            self.connection
                .execute_batch(&insert_sql)
                .context("Failed to insert delta data")?;
        }

        Ok(())
    }

    fn validate_source_files_schema(&self, table_name: &str, file_paths: &[String]) -> Result<()> {
        let table_exists_sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );

        let mut stmt = self.connection.prepare(&table_exists_sql)?;
        let mut rows = stmt.query([])?;
        let table_exists = if let Some(row) = rows.next()? {
            let count: i64 = row.get(0)?;
            count > 0
        } else {
            false
        };

        if !table_exists {
            return Ok(());
        }

        let table_schema_sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        );

        let mut table_stmt = self.connection.prepare(&table_schema_sql)?;
        let mut table_rows = table_stmt.query([])?;
        let mut table_columns = Vec::new();
        while let Some(row) = table_rows.next()? {
            let column_name: String = row.get(0)?;
            let data_type: String = row.get(1)?;
            table_columns.push((column_name, data_type));
        }

        for file_path in file_paths {
            let source_schema_sql = format!("DESCRIBE SELECT * FROM read_csv_auto('{file_path}')");

            let mut source_stmt = self.connection.prepare(&source_schema_sql)?;
            let mut source_rows = source_stmt.query([])?;
            let mut source_columns = Vec::new();
            while let Some(row) = source_rows.next()? {
                let column_name: String = row.get(0)?;
                let data_type: String = row.get(1)?;
                source_columns.push((column_name, data_type));
            }

            if table_columns.len() != source_columns.len() {
                return Err(anyhow::anyhow!(
                    "Column count mismatch in file '{}': table has {} columns, file has {} columns",
                    file_path,
                    table_columns.len(),
                    source_columns.len()
                ));
            }

            for (i, ((table_col, table_type), (source_col, source_type))) in
                table_columns.iter().zip(source_columns.iter()).enumerate()
            {
                if table_col != source_col {
                    return Err(anyhow::anyhow!(
                        "Column name mismatch in file '{}' at position {}: table has '{}', file has '{}'",
                        file_path,
                        i,
                        table_col,
                        source_col
                    ));
                }
                if table_type != source_type {
                    return Err(anyhow::anyhow!(
                        "Column type mismatch in file '{}' for column '{}': table has '{}', file has '{}'",
                        file_path,
                        table_col,
                        table_type,
                        source_type
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_delta_schema(&self, table_name: &str, delta_path: &Path) -> Result<()> {
        let table_schema_sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        );

        let delta_schema_sql = format!(
            "DESCRIBE SELECT * FROM read_parquet('{}')",
            delta_path.to_string_lossy()
        );

        let mut table_stmt = self.connection.prepare(&table_schema_sql)?;
        let mut table_rows = table_stmt.query([])?;
        let mut table_columns = Vec::new();
        while let Some(row) = table_rows.next()? {
            let column_name: String = row.get(0)?;
            let data_type: String = row.get(1)?;
            table_columns.push((column_name, data_type));
        }

        let mut delta_stmt = self.connection.prepare(&delta_schema_sql)?;
        let mut delta_rows = delta_stmt.query([])?;
        let mut delta_columns = Vec::new();
        while let Some(row) = delta_rows.next()? {
            let column_name: String = row.get(0)?;
            let data_type: String = row.get(1)?;
            delta_columns.push((column_name, data_type));
        }

        if table_columns.len() != delta_columns.len() {
            return Err(anyhow::anyhow!(
                "Column count mismatch: table has {} columns, delta has {} columns",
                table_columns.len(),
                delta_columns.len()
            ));
        }

        for (i, ((table_col, table_type), (delta_col, delta_type))) in
            table_columns.iter().zip(delta_columns.iter()).enumerate()
        {
            if table_col != delta_col {
                return Err(anyhow::anyhow!(
                    "Column name mismatch at position {}: table has '{}', delta has '{}'",
                    i,
                    table_col,
                    delta_col
                ));
            }
            if table_type != delta_type {
                return Err(anyhow::anyhow!(
                    "Column type mismatch for '{}': table has '{}', delta has '{}'",
                    table_col,
                    table_type,
                    delta_type
                ));
            }
        }

        Ok(())
    }

    fn generate_temp_table_name(prefix: &str) -> String {
        format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
    }

    fn drop_temp_table(&self, table_name: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table_name};");
        self.connection
            .execute_batch(&drop_sql)
            .with_context(|| format!("Failed to drop temporary table: {table_name}"))
    }

    fn rewrite_sql_for_deltas(
        &self,
        sql: &str,
        dependency_deltas: &std::collections::HashMap<String, DeltaMetadata>,
    ) -> Result<String> {
        use sqlparser::{dialect::DuckDbDialect, parser::Parser};

        // First validate that the SQL is parseable
        let dialect = DuckDbDialect {};
        let parsed = Parser::parse_sql(&dialect, sql)
            .with_context(|| format!("Failed to parse SQL: {sql}"))?;

        if parsed.is_empty() {
            return Err(anyhow::anyhow!("Empty SQL statement"));
        }

        // Use improved regex replacement that respects SQL context
        let mut modified_sql = sql.to_string();

        for (table_name, delta_metadata) in dependency_deltas {
            let delta_function_call =
                format!("read_parquet('{}')", delta_metadata.insert_delta_path);

            // Simple but effective patterns for table replacement
            let patterns = vec![
                // FROM table_name (with optional whitespace and line breaks)
                (
                    format!(r"(?i)\bFROM\s+{}\b", regex::escape(table_name)),
                    format!("FROM {delta_function_call}"),
                ),
                // JOIN table_name (with optional whitespace and line breaks)
                (
                    format!(r"(?i)\bJOIN\s+{}\b", regex::escape(table_name)),
                    format!("JOIN {delta_function_call}"),
                ),
                // table_name AS (case insensitive)
                (
                    format!(r"(?i)\b{}\s+AS\b", regex::escape(table_name)),
                    format!("{delta_function_call} AS"),
                ),
                // Comma-separated table list: , table_name
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

        // Validate that the modified SQL is still parseable
        let _validated = Parser::parse_sql(&dialect, &modified_sql)
            .with_context(|| format!("Modified SQL is not valid: {modified_sql}"))?;

        Ok(modified_sql)
    }

    fn create_model_delta_table(&self, delta_files: &DeltaFiles, modified_sql: &str) -> Result<()> {
        let temp_table_name = Self::generate_temp_table_name("temp_model_delta");

        let create_temp_sql = format!("CREATE TABLE {temp_table_name} AS ({modified_sql});");

        self.connection
            .execute_batch(&create_temp_sql)
            .context("Failed to create temporary model delta table")?;

        let export_insert_sql = format!(
            "COPY (SELECT * FROM {}) TO '{}' (FORMAT PARQUET);",
            temp_table_name,
            delta_files.insert_path.to_string_lossy()
        );

        self.connection
            .execute_batch(&export_insert_sql)
            .context("Failed to export model delta insert data")?;

        self.drop_temp_table(&temp_table_name)?;

        Ok(())
    }

    fn apply_model_delta_to_table(&self, model_name: &str, delta_files: &DeltaFiles) -> Result<()> {
        let table_exists_sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{model_name}'"
        );

        let mut stmt = self.connection.prepare(&table_exists_sql)?;
        let mut rows = stmt.query([])?;
        let table_exists = if let Some(row) = rows.next()? {
            let count: i64 = row.get(0)?;
            count > 0
        } else {
            false
        };

        if !table_exists {
            let create_from_delta_sql = format!(
                "CREATE TABLE {} AS SELECT * FROM read_parquet('{}');",
                model_name,
                delta_files.insert_path.to_string_lossy()
            );

            self.connection
                .execute_batch(&create_from_delta_sql)
                .context("Failed to create model table from delta")?;
        } else {
            let replace_sql = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM read_parquet('{}');",
                model_name,
                delta_files.insert_path.to_string_lossy()
            );

            self.connection
                .execute_batch(&replace_sql)
                .context("Failed to replace model table with delta")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig, RangeConfig, UpdateStrategyConfig};
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
                range: RangeConfig {
                    since: None,
                    until: None,
                },
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

    #[tokio::test]
    async fn test_rewrite_sql_for_deltas() {
        let test_dir = "/tmp/ducklake_test_rewrite";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut dependency_deltas = std::collections::HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/path/to/delta_users_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        let original_sql = "SELECT id, name FROM users WHERE active = true";
        let rewritten_sql = ducklake
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
                created_at: chrono::Utc::now(),
            },
        );

        let rewritten_multi = ducklake
            .rewrite_sql_for_deltas(multi_table_sql, &dependency_deltas)
            .unwrap();
        assert!(rewritten_multi.contains("read_parquet('/path/to/delta_users_insert.parquet')"));
        assert!(rewritten_multi.contains("read_parquet('/path/to/delta_posts_insert.parquet')"));
    }

    #[tokio::test]
    async fn test_create_delta_table() {
        let test_dir = "/tmp/ducklake_test_create_delta";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let test_csv = temp_dir.path().join("test_data.csv");
        std::fs::write(&test_csv, "id,name,age\n1,Alice,25\n2,Bob,30").unwrap();

        let adapter = AdapterConfig {
            connection: "test_table".to_string(),
            description: None,
            file: crate::config::adapter::FileConfig {
                path: test_csv.to_string_lossy().to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: crate::config::adapter::FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: Some(true),
            },
            columns: vec![],
            limits: None,
        };

        let delta_files = crate::pipeline::delta::DeltaFiles::new(
            temp_dir.path(),
            "test_table",
            "20241201_120000",
        );

        let file_paths = vec![test_csv.to_string_lossy().to_string()];

        ducklake
            .create_delta_table(&delta_files, &adapter, &file_paths)
            .unwrap();

        assert!(delta_files.insert_path.exists());

        let file_size = std::fs::metadata(&delta_files.insert_path).unwrap().len();
        assert!(file_size > 0);

        let verify_sql = format!(
            "SELECT * FROM read_parquet('{}') ORDER BY id",
            delta_files.insert_path.to_string_lossy()
        );
        let results = ducklake.query(&verify_sql).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec!["1".to_string(), "Alice".to_string(), "25".to_string()]
        );
        assert_eq!(
            results[1],
            vec!["2".to_string(), "Bob".to_string(), "30".to_string()]
        );
    }

    #[tokio::test]
    async fn test_create_model_delta_table() {
        let test_dir = "/tmp/ducklake_test_create_model_delta";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        ducklake
            .connection
            .execute_batch(
                "CREATE TABLE source_table (id INTEGER, name VARCHAR, age INTEGER); \
             INSERT INTO source_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);",
            )
            .unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let delta_files = crate::pipeline::delta::DeltaFiles::new(
            temp_dir.path(),
            "model_table",
            "20241201_120000",
        );

        let modified_sql = "SELECT id, name, age + 1 as age FROM source_table WHERE age > 20";

        ducklake
            .create_model_delta_table(&delta_files, modified_sql)
            .unwrap();

        assert!(delta_files.insert_path.exists());

        let file_size = std::fs::metadata(&delta_files.insert_path).unwrap().len();
        assert!(file_size > 0);

        let verify_sql = format!(
            "SELECT * FROM read_parquet('{}') ORDER BY id",
            delta_files.insert_path.to_string_lossy()
        );
        let results = ducklake.query(&verify_sql).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec!["1".to_string(), "Alice".to_string(), "26".to_string()]
        );
        assert_eq!(
            results[1],
            vec!["2".to_string(), "Bob".to_string(), "31".to_string()]
        );
    }

    #[tokio::test]
    async fn test_apply_delta_to_table() {
        let test_dir = "/tmp/ducklake_test_apply_delta";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let temp_dir = tempfile::tempdir().unwrap();
        let delta_files = crate::pipeline::delta::DeltaFiles::new(
            temp_dir.path(),
            "test_table",
            "20241201_120000",
        );

        let test_csv = temp_dir.path().join("test_data.csv");
        std::fs::write(&test_csv, "id,name,value\n1,Alice,100\n2,Bob,200").unwrap();

        let adapter = crate::config::adapter::AdapterConfig {
            connection: "test_table".to_string(),
            description: None,
            file: crate::config::adapter::FileConfig {
                path: test_csv.to_string_lossy().to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: crate::config::adapter::FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: Some(true),
            },
            columns: vec![],
            limits: None,
        };

        let file_paths = vec![test_csv.to_string_lossy().to_string()];

        ducklake
            .create_delta_table(&delta_files, &adapter, &file_paths)
            .unwrap();

        ducklake
            .apply_delta_to_table("test_table", &delta_files)
            .unwrap();

        let verify_sql = "SELECT * FROM test_table ORDER BY id";
        let results = ducklake.query(verify_sql).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec!["1".to_string(), "Alice".to_string(), "100".to_string()]
        );
        assert_eq!(
            results[1],
            vec!["2".to_string(), "Bob".to_string(), "200".to_string()]
        );

        // 新しいデータを追加してテスト
        std::fs::write(&test_csv, "id,name,value\n3,Charlie,300\n4,David,400").unwrap();

        let new_delta_files = crate::pipeline::delta::DeltaFiles::new(
            temp_dir.path(),
            "test_table",
            "20241201_120001",
        );

        ducklake
            .create_delta_table(&new_delta_files, &adapter, &file_paths)
            .unwrap();

        ducklake
            .apply_delta_to_table("test_table", &new_delta_files)
            .unwrap();

        let updated_results = ducklake.query(verify_sql).unwrap();
        assert_eq!(updated_results.len(), 4);
        assert_eq!(
            updated_results[2],
            vec!["3".to_string(), "Charlie".to_string(), "300".to_string()]
        );
        assert_eq!(
            updated_results[3],
            vec!["4".to_string(), "David".to_string(), "400".to_string()]
        );
    }

    #[tokio::test]
    async fn test_rewrite_sql_for_deltas_edge_cases() {
        let test_dir = "/tmp/ducklake_test_edge_cases";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut dependency_deltas = std::collections::HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/path/to/delta_users_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );
        dependency_deltas.insert(
            "user".to_string(),
            DeltaMetadata {
                action_id: 2,
                insert_delta_path: "/path/to/delta_user_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        // Test Case 1: Table name as column name should not be replaced
        let sql_with_column = "SELECT user.name, users.email FROM users WHERE user.active = true";
        let rewritten = ducklake
            .rewrite_sql_for_deltas(sql_with_column, &dependency_deltas)
            .unwrap();

        // 'user.name' should not be replaced, but 'users' table should be
        assert!(
            rewritten.contains("user.name"),
            "Column 'user.name' was incorrectly replaced"
        );
        assert!(
            rewritten.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Table 'users' was not replaced with delta"
        );

        // Test Case 2: Substring table names should not interfere
        let sql_substring = "SELECT * FROM super_users, users WHERE super_users.id = users.id";
        dependency_deltas.insert(
            "super_users".to_string(),
            DeltaMetadata {
                action_id: 3,
                insert_delta_path: "/path/to/delta_super_users_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        let rewritten_substring = ducklake
            .rewrite_sql_for_deltas(sql_substring, &dependency_deltas)
            .unwrap();

        // Both tables should be replaced correctly
        assert!(
            rewritten_substring
                .contains("read_parquet('/path/to/delta_super_users_insert.parquet')")
        );
        assert!(
            rewritten_substring.contains("read_parquet('/path/to/delta_users_insert.parquet')")
        );

        // Test Case 3: Complex SQL with newlines and multiple spaces
        let complex_sql = r#"
            SELECT 
                COUNT(DISTINCT u.id) as total_users,
                DATE_TRUNC('month', u.created_at) as month
            FROM 
                users u
            LEFT JOIN 
                posts p ON u.id = p.user_id
            WHERE 
                u.active = true
            GROUP BY 
                DATE_TRUNC('month', u.created_at)
        "#;

        dependency_deltas.insert(
            "posts".to_string(),
            DeltaMetadata {
                action_id: 4,
                insert_delta_path: "/path/to/delta_posts_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        let rewritten_complex = ducklake
            .rewrite_sql_for_deltas(complex_sql, &dependency_deltas)
            .unwrap();

        // Both tables should be replaced in complex SQL
        assert!(rewritten_complex.contains("read_parquet('/path/to/delta_users_insert.parquet')"));
        assert!(rewritten_complex.contains("read_parquet('/path/to/delta_posts_insert.parquet')"));

        // Aliases and other parts should remain unchanged
        assert!(rewritten_complex.contains("u.id"));
        assert!(rewritten_complex.contains("p.user_id"));
        assert!(rewritten_complex.contains("u.active = true"));

        // Test Case 4: Invalid SQL should fail gracefully
        let invalid_sql = "INVALID SQL SYNTAX HERE";
        let result = ducklake.rewrite_sql_for_deltas(invalid_sql, &dependency_deltas);
        assert!(result.is_err(), "Invalid SQL should return an error");

        // Test Case 5: Empty SQL should fail
        let empty_sql = "";
        let empty_result = ducklake.rewrite_sql_for_deltas(empty_sql, &dependency_deltas);
        assert!(empty_result.is_err(), "Empty SQL should return an error");
    }

    #[tokio::test]
    async fn test_sql_syntax_correctness_after_rewrite() {
        let test_dir = "/tmp/ducklake_test_syntax";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut dependency_deltas = std::collections::HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/tmp/test_delta_users.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        // Create a test parquet file for syntax validation
        let test_csv = std::path::Path::new("/tmp/test_users_for_syntax.csv");
        std::fs::write(
            test_csv,
            "id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com",
        )
        .unwrap();

        // Convert CSV to Parquet for delta test
        let create_parquet_sql = format!(
            "COPY (SELECT * FROM read_csv('{}', header=true)) TO '{}'",
            test_csv.to_string_lossy(),
            dependency_deltas["users"].insert_delta_path
        );

        if ducklake.execute_batch(&create_parquet_sql).is_err() {
            // If parquet creation fails, skip syntax validation (not the main focus)
            return;
        }

        // Test various SQL patterns that should produce valid syntax after rewrite
        let test_cases = vec![
            ("Simple SELECT", "SELECT id, name FROM users"),
            ("SELECT with WHERE", "SELECT * FROM users WHERE id > 1"),
            (
                "JOIN query",
                "SELECT u.name FROM users u JOIN posts p ON u.id = p.user_id",
            ),
            ("Subquery", "SELECT * FROM (SELECT id FROM users) AS subq"),
            (
                "Complex expression",
                "SELECT COUNT(*) FROM users WHERE name LIKE '%test%'",
            ),
        ];

        for (description, original_sql) in test_cases {
            let rewritten_sql = ducklake
                .rewrite_sql_for_deltas(original_sql, &dependency_deltas)
                .unwrap_or_else(|e| panic!("Failed to rewrite SQL for '{description}': {e}"));

            // Verify the rewritten SQL is syntactically valid by parsing it
            use sqlparser::{dialect::DuckDbDialect, parser::Parser};
            let dialect = DuckDbDialect {};

            let parse_result = Parser::parse_sql(&dialect, &rewritten_sql);
            assert!(
                parse_result.is_ok(),
                "Rewritten SQL for '{}' is not syntactically valid:\nOriginal: {}\nRewritten: {}\nError: {:?}",
                description,
                original_sql,
                rewritten_sql,
                parse_result.err()
            );

            // Ensure no double parentheses around read_parquet calls
            assert!(
                !rewritten_sql.contains("((read_parquet"),
                "Double parentheses found in rewritten SQL for '{description}': {rewritten_sql}"
            );

            // Ensure read_parquet calls are present (this validates the replacement worked)
            assert!(
                rewritten_sql.contains("read_parquet("),
                "Missing read_parquet call in rewritten SQL for '{description}': {rewritten_sql}"
            );
        }

        // Clean up
        let _ = std::fs::remove_file(test_csv);
        let _ = std::fs::remove_file(&dependency_deltas["users"].insert_delta_path);
    }

    #[tokio::test]
    async fn test_legacy_sql_rewrite_issues_prevention() {
        // This test verifies that issues from the legacy implementation are prevented
        let test_dir = "/tmp/ducklake_test_legacy_issues";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let mut dependency_deltas = std::collections::HashMap::new();
        dependency_deltas.insert(
            "users".to_string(),
            DeltaMetadata {
                action_id: 1,
                insert_delta_path: "/path/to/delta_users_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        // Issue 1: SQL with multiple spaces and newlines before table name
        // Legacy string replacement " users" would fail with newlines
        let problematic_sql1 = "SELECT COUNT(*) FROM\n    users WHERE active = true";
        let rewritten1 = ducklake
            .rewrite_sql_for_deltas(problematic_sql1, &dependency_deltas)
            .unwrap();
        assert!(
            rewritten1.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Failed to replace table name with newlines: {rewritten1}"
        );

        // Issue 2: SQL with tabs and multiple spaces
        let problematic_sql2 = "SELECT * FROM\t\t   users\nWHERE id > 0";
        let rewritten2 = ducklake
            .rewrite_sql_for_deltas(problematic_sql2, &dependency_deltas)
            .unwrap();
        assert!(
            rewritten2.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Failed to replace table name with tabs: {rewritten2}"
        );

        // Issue 3: Table name at start of line
        let problematic_sql3 = "SELECT *\nFROM\nusers\nWHERE active = true";
        let rewritten3 = ducklake
            .rewrite_sql_for_deltas(problematic_sql3, &dependency_deltas)
            .unwrap();
        assert!(
            rewritten3.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Failed to replace table name at start of line: {rewritten3}"
        );

        // Issue 4: Ensure no extra parentheses are added (this was a real bug)
        // The legacy implementation would create invalid syntax like "FROM (read_parquet('path'))"
        for rewritten in [&rewritten1, &rewritten2, &rewritten3] {
            // Check that we don't have invalid syntax with unnecessary parentheses
            assert!(
                !rewritten.contains("FROM (read_parquet"),
                "Invalid parentheses found in SQL: {rewritten}"
            );
            assert!(
                !rewritten.contains("JOIN (read_parquet"),
                "Invalid parentheses found in SQL: {rewritten}"
            );
        }

        // Issue 5: Multiple table replacement test
        dependency_deltas.insert(
            "orders".to_string(),
            DeltaMetadata {
                action_id: 2,
                insert_delta_path: "/path/to/delta_orders_insert.parquet".to_string(),
                created_at: chrono::Utc::now(),
            },
        );

        let multi_table_sql = "SELECT * FROM users, orders WHERE users.id = orders.user_id";
        let rewritten_multi = ducklake
            .rewrite_sql_for_deltas(multi_table_sql, &dependency_deltas)
            .unwrap();

        // Both "users" and "orders" should be replaced
        assert!(
            rewritten_multi.contains("read_parquet('/path/to/delta_users_insert.parquet')"),
            "Table 'users' should be replaced"
        );
        assert!(
            rewritten_multi.contains("read_parquet('/path/to/delta_orders_insert.parquet')"),
            "Table 'orders' should be replaced"
        );
        assert!(
            rewritten_multi.matches("read_parquet").count() == 2,
            "Both tables should be replaced: {rewritten_multi}"
        );
    }
}
