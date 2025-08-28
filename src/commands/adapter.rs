use anyhow::{Context, Result};
use duckdb;
use glob;
use inquire::{Confirm, Select, Text};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::{render_adapter_template, validate_name};
use crate::commands::workspace::{ensure_project_directory, find_project_root};
use crate::config::adapter::{
    AdapterConfig, AdapterSource, ColumnConfig, FileConfig, FormatConfig,
};
use crate::config::project::{ConnectionConfig, ProjectConfig};

pub fn execute_adapter_new(name: &str, project_path: &std::path::Path) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(Some(project_path))?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if adapter_file.exists() {
        return Err(anyhow::anyhow!("Adapter '{}' already exists", name));
    }

    let template = render_adapter_template(name);
    fs::write(&adapter_file, template)
        .with_context(|| format!("Failed to create adapter file: {adapter_file:?}"))?;

    println!("Created adapter: {name}");
    Ok(())
}

pub fn execute_adapter_delete(name: &str, project_path: &std::path::Path) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(Some(project_path))?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(anyhow::anyhow!("Adapter '{}' does not exist", name));
    }

    fs::remove_file(&adapter_file)
        .with_context(|| format!("Failed to delete adapter file: {adapter_file:?}"))?;

    println!("Deleted adapter: {name}");
    Ok(())
}

pub async fn execute_adapter_interactive(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    let adapter_name = Text::new("Adapter name:").prompt()?;
    validate_name(&adapter_name)?;

    let adapter_file = project_root
        .join("adapters")
        .join(format!("{adapter_name}.yml"));
    if adapter_file.exists()
        && !Confirm::new(&format!(
            "Adapter '{adapter_name}' already exists. Overwrite?"
        ))
        .prompt()?
    {
        println!("Adapter creation cancelled.");
        return Ok(());
    }

    let project_config = load_project_config(&project_root)?;
    let connection_name = select_connection(&project_config.connections)?;
    let connection_config = project_config.connections.get(&connection_name).unwrap();

    let (source_config, format_config) = configure_source_with_test(connection_config).await?;

    println!("\nGenerating schema...\n");
    let columns = generate_schema(&source_config, &format_config, connection_config).await?;

    if !columns.is_empty() {
        println!("Generated schema:");
        for (i, column) in columns.iter().enumerate() {
            println!("  {}: {} ({})", i + 1, column.name, column.ty);
        }
        println!();
    }

    let adapter_config = AdapterConfig {
        connection: connection_name,
        description: Some(format!("Generated adapter for {adapter_name}")),
        source: source_config,
        columns,
    };

    let yaml_content = serde_yml::to_string(&adapter_config)?;

    if let Some(parent) = adapter_file.parent() {
        fs::create_dir_all(parent).with_context(|| "Failed to create adapters directory")?;
    }

    fs::write(&adapter_file, yaml_content)
        .with_context(|| format!("Failed to save adapter file: {adapter_file:?}"))?;

    println!("✓ Adapter '{adapter_name}' created successfully");
    Ok(())
}

#[derive(Debug, Clone)]
enum SourceType {
    File,
    Database,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            SourceType::File => "File",
            SourceType::Database => "Database",
        };
        write!(f, "{name}")
    }
}

#[derive(Debug, Clone)]
enum FileFormatType {
    Csv,
    Json,
    Parquet,
}

impl std::fmt::Display for FileFormatType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            FileFormatType::Csv => "CSV",
            FileFormatType::Json => "JSON",
            FileFormatType::Parquet => "Parquet",
        };
        write!(f, "{name}")
    }
}

fn load_project_config(project_root: &Path) -> Result<ProjectConfig> {
    let project_yml = project_root.join("project.yml");
    let content = fs::read_to_string(&project_yml)
        .with_context(|| "Failed to read project.yml. Run 'fbox init' first.")?;

    crate::config::project::parse_project_config(&content)
}

fn select_connection(connections: &HashMap<String, ConnectionConfig>) -> Result<String> {
    if connections.is_empty() {
        return Err(anyhow::anyhow!(
            "No connections found. Create a connection first using 'fbox connection'"
        ));
    }

    let connection_names: Vec<String> = connections.keys().cloned().collect();
    let selected = Select::new("Select connection:", connection_names).prompt()?;

    Ok(selected)
}

async fn configure_source_with_test(
    connection_config: &ConnectionConfig,
) -> Result<(AdapterSource, FormatConfig)> {
    let source_type = match connection_config {
        ConnectionConfig::LocalFile { .. } | ConnectionConfig::S3(_) => SourceType::File,
        ConnectionConfig::Sqlite { .. }
        | ConnectionConfig::MySql { .. }
        | ConnectionConfig::PostgreSql { .. } => SourceType::Database,
    };

    match source_type {
        SourceType::File => {
            let path = Text::new("File path pattern:").prompt()?;

            test_connection_simple(connection_config).await?;

            let format_types = vec![
                FileFormatType::Csv,
                FileFormatType::Json,
                FileFormatType::Parquet,
            ];
            let format_type = Select::new("Select file format:", format_types).prompt()?;

            let file_config = FileConfig {
                path,
                compression: None,
                max_batch_size: None,
            };

            let format_config = match format_type {
                FileFormatType::Csv => {
                    let has_header = Some(true);

                    FormatConfig {
                        ty: "csv".to_string(),
                        delimiter: Some(",".to_string()),
                        null_value: None,
                        has_header,
                    }
                }
                FileFormatType::Json => FormatConfig {
                    ty: "json".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: None,
                },
                FileFormatType::Parquet => FormatConfig {
                    ty: "parquet".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: None,
                },
            };

            Ok((
                AdapterSource::File {
                    file: file_config,
                    format: format_config.clone(),
                },
                format_config,
            ))
        }
        SourceType::Database => {
            let table_name = Text::new("Table name:").prompt()?;

            let format_config = FormatConfig {
                ty: "database".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            };

            Ok((AdapterSource::Database { table_name }, format_config))
        }
    }
}

async fn generate_schema(
    source_config: &AdapterSource,
    format_config: &FormatConfig,
    connection_config: &ConnectionConfig,
) -> Result<Vec<ColumnConfig>> {
    match source_config {
        AdapterSource::File { file, .. } => {
            generate_file_schema(&file.path, &format_config.ty, connection_config).await
        }
        AdapterSource::Database { table_name } => {
            generate_database_schema(table_name, connection_config).await
        }
    }
}

async fn generate_file_schema(
    file_path: &str,
    format_type: &str,
    connection_config: &ConnectionConfig,
) -> Result<Vec<ColumnConfig>> {
    if file_path.contains('*') || file_path.contains('?') {
        return generate_schema_from_wildcard_pattern(file_path, format_type, connection_config)
            .await;
    }

    let actual_path = resolve_file_path(file_path, connection_config)?;

    if !actual_path.exists() {
        return Err(anyhow::anyhow!(
            "File not found: {}. Please ensure the file exists before generating schema.",
            actual_path.display()
        ));
    }

    let connection = create_duckdb_connection().await?;

    let file_path_str = actual_path.to_string_lossy();
    let describe_sql = format!("DESCRIBE SELECT * FROM '{file_path_str}' LIMIT 0");

    match query_duckdb_schema(&connection, &describe_sql) {
        Ok(schema_rows) => {
            let columns: Vec<ColumnConfig> = schema_rows
                .into_iter()
                .map(|row| {
                    let column_name = row.first().unwrap_or(&"unknown".to_string()).clone();
                    let duckdb_type = row.get(1).unwrap_or(&"VARCHAR".to_string()).clone();
                    let featherbox_type = map_duckdb_type_to_featherbox(&duckdb_type);

                    ColumnConfig {
                        name: column_name.clone(),
                        ty: featherbox_type.clone(),
                        description: Some(format!("Auto-detected `{column_name}` column")),
                    }
                })
                .collect();

            Ok(columns)
        }
        Err(e) => Err(anyhow::anyhow!(
            "Failed to generate schema from {} file '{}': {}. Please check if the file exists and is properly formatted.",
            format_type.to_uppercase(),
            actual_path.display(),
            e
        )),
    }
}

async fn create_duckdb_connection() -> Result<duckdb::Connection> {
    let connection = duckdb::Connection::open_in_memory()?;
    Ok(connection)
}

fn query_duckdb_schema(connection: &duckdb::Connection, sql: &str) -> Result<Vec<Vec<String>>> {
    let mut stmt = connection.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let column_count = rows.as_ref().unwrap().column_count();

    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let mut row_data = Vec::new();
        for i in 0..column_count {
            use duckdb::types::Value;
            let value: Result<Value, _> = row.get(i);
            let string_value = match value {
                Ok(Value::Text(s)) => s,
                Ok(v) => format!("{v:?}"),
                Err(_) => "ERROR".to_string(),
            };
            row_data.push(string_value);
        }
        results.push(row_data);
    }

    Ok(results)
}

async fn generate_schema_from_wildcard_pattern(
    pattern: &str,
    format_type: &str,
    connection_config: &ConnectionConfig,
) -> Result<Vec<ColumnConfig>> {
    match connection_config {
        ConnectionConfig::LocalFile { base_path } => {
            let base = Path::new(base_path);
            let search_pattern = if pattern.starts_with('/') {
                pattern.to_string()
            } else {
                format!("{}/{}", base.display(), pattern)
            };

            let matches: Vec<_> = glob::glob(&search_pattern)
                .context("Failed to execute glob pattern")?
                .filter_map(Result::ok)
                .filter(|p| p.is_file())
                .collect();

            if matches.is_empty() {
                return Err(anyhow::anyhow!(
                    "No files found matching pattern: {}",
                    pattern
                ));
            }

            let first_file = &matches[0];
            let connection = create_duckdb_connection().await?;
            let file_path_str = first_file.to_string_lossy();
            let describe_sql = format!("DESCRIBE SELECT * FROM '{file_path_str}' LIMIT 0");

            match query_duckdb_schema(&connection, &describe_sql) {
                Ok(schema_rows) => {
                    let columns: Vec<ColumnConfig> = schema_rows
                        .into_iter()
                        .map(|row| {
                            let column_name = row.first().unwrap_or(&"unknown".to_string()).clone();
                            let duckdb_type = row.get(1).unwrap_or(&"VARCHAR".to_string()).clone();
                            let featherbox_type = map_duckdb_type_to_featherbox(&duckdb_type);

                            ColumnConfig {
                                name: column_name.clone(),
                                ty: featherbox_type.clone(),
                                description: Some(format!("Auto-detected `{column_name}` column")),
                            }
                        })
                        .collect();

                    Ok(columns)
                }
                Err(e) => Err(anyhow::anyhow!(
                    "Failed to generate schema from {} file pattern '{}': {}. Please check if files exist and are properly formatted.",
                    format_type.to_uppercase(),
                    pattern,
                    e
                )),
            }
        }
        _ => Err(anyhow::anyhow!(
            "Wildcard patterns are only supported for local files"
        )),
    }
}

fn map_duckdb_type_to_featherbox(duckdb_type: &str) -> String {
    let uppercase_type = duckdb_type.to_uppercase();
    match uppercase_type.as_str() {
        t if t.starts_with("VARCHAR") || t.starts_with("TEXT") || t.starts_with("STRING") => {
            "STRING".to_string()
        }
        "TINYINT" | "SMALLINT" | "INTEGER" | "INT" | "BIGINT" | "INT8" | "INT4" | "INT2" => {
            "INTEGER".to_string()
        }
        "HUGEINT" | "UINTEGER" | "UBIGINT" | "USMALLINT" | "UTINYINT" => "INTEGER".to_string(),
        "DOUBLE" | "REAL" | "FLOAT" | "DECIMAL" | "NUMERIC" => "FLOAT".to_string(),
        "BOOLEAN" | "BOOL" => "BOOLEAN".to_string(),
        "DATE" => "DATETIME".to_string(),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" | "TIMESTAMP_TZ" => "DATETIME".to_string(),
        "TIME" | "TIMETZ" | "TIME_TZ" => "STRING".to_string(),
        "JSON" => "JSON".to_string(),
        "UUID" | "BLOB" | "BITSTRING" | "BIT" => "STRING".to_string(),
        t if t.starts_with("ARRAY") || t.starts_with("LIST") => "JSON".to_string(),
        t if t.starts_with("STRUCT") || t.starts_with("MAP") => "JSON".to_string(),
        _ => "STRING".to_string(),
    }
}

fn resolve_file_path(
    pattern: &str,
    connection_config: &ConnectionConfig,
) -> Result<std::path::PathBuf> {
    match connection_config {
        ConnectionConfig::LocalFile { base_path } => {
            let base = Path::new(base_path);
            let pattern_without_wildcards = pattern.split('*').next().unwrap_or(pattern);
            let pattern_without_placeholders = pattern_without_wildcards
                .replace("<YYYY>", "2024")
                .replace("<MM>", "01")
                .replace("<DD>", "01")
                .replace("<HH>", "00")
                .replace("<mm>", "00");

            let mut full_path = base.join(pattern_without_placeholders);

            if !full_path.exists() && !pattern.contains('/') {
                full_path = base.join("data").join(pattern);
            }

            Ok(full_path)
        }
        _ => Err(anyhow::anyhow!(
            "File schema generation is only supported for local files"
        )),
    }
}

async fn generate_database_schema(
    table_name: &str,
    connection_config: &ConnectionConfig,
) -> Result<Vec<ColumnConfig>> {
    match connection_config {
        ConnectionConfig::Sqlite { .. } => {
            generate_sqlite_schema(table_name, connection_config).await
        }
        ConnectionConfig::MySql { .. } | ConnectionConfig::PostgreSql { .. } => {
            Err(anyhow::anyhow!(
                "Remote database schema generation is not yet implemented. Please manually define the schema for table '{}'.",
                table_name
            ))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported connection type for database schema generation"
        )),
    }
}

async fn generate_sqlite_schema(
    table_name: &str,
    connection_config: &ConnectionConfig,
) -> Result<Vec<ColumnConfig>> {
    let connection = create_duckdb_connection().await?;

    let connection_string = match connection_config {
        ConnectionConfig::Sqlite { path } => {
            format!("ATTACH 'sqlite://{path}' AS source")
        }
        _ => unreachable!(),
    };

    match connection.execute(&connection_string, []) {
        Ok(_) => {}
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Failed to attach SQLite database: {}. Path: {}. Please check if the database file exists and is accessible.",
                e,
                match connection_config {
                    ConnectionConfig::Sqlite { path } => path,
                    _ => unreachable!(),
                }
            ));
        }
    }

    let table_exists_sql =
        format!("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table_name}'");
    let mut stmt = connection.prepare(&table_exists_sql)?;
    let mut rows = stmt.query([])?;

    let count: i32 = if let Some(row) = rows.next()? {
        row.get(0)?
    } else {
        0
    };

    if count == 0 {
        return Err(anyhow::anyhow!(
            "Table '{}' does not exist in the SQLite database",
            table_name
        ));
    }

    let schema_sql = format!("DESCRIBE SELECT * FROM source.{table_name} LIMIT 0");
    let schema_rows = query_duckdb_schema(&connection, &schema_sql)?;

    let columns: Vec<ColumnConfig> = schema_rows
        .into_iter()
        .map(|row| {
            let column_name = row.first().unwrap_or(&"unknown".to_string()).clone();
            let duckdb_type = row.get(1).unwrap_or(&"VARCHAR".to_string()).clone();
            let featherbox_type = map_duckdb_type_to_featherbox(&duckdb_type);

            ColumnConfig {
                name: column_name.clone(),
                ty: featherbox_type,
                description: Some(format!(
                    "Auto-detected `{column_name}` column from database"
                )),
            }
        })
        .collect();

    Ok(columns)
}

async fn test_connection_simple(connection_config: &ConnectionConfig) -> Result<()> {
    match connection_config {
        ConnectionConfig::LocalFile { base_path } => {
            if Path::new(base_path).exists() {
                println!("✓ Local file path is accessible");
            } else {
                return Err(anyhow::anyhow!(
                    "✗ Local file path does not exist: {}",
                    base_path
                ));
            }
        }
        ConnectionConfig::Sqlite { path } => {
            if let Some(parent) = Path::new(path).parent() {
                if parent.exists() {
                    println!("✓ SQLite database path is valid");
                } else {
                    return Err(anyhow::anyhow!(
                        "✗ SQLite database parent directory does not exist"
                    ));
                }
            }
        }
        ConnectionConfig::S3(s3_config) => {
            println!("✓ S3 bucket configuration: {}", s3_config.bucket);
        }
        ConnectionConfig::MySql { host, port, .. } => {
            println!("✓ MySQL connection configured at {host}:{port}");
        }
        ConnectionConfig::PostgreSql { host, port, .. } => {
            println!("✓ PostgreSQL connection configured at {host}:{port}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile;

    fn setup_test_project() -> Result<tempfile::TempDir> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
        fs::write(project_path.join("project.yml"), "test")?;

        Ok(temp_dir)
    }

    #[test]
    fn test_execute_adapter_new_success() -> Result<()> {
        let temp_dir = setup_test_project()?;

        let result = execute_adapter_new("test_logs", temp_dir.path());

        assert!(result.is_ok());

        let adapter_file = temp_dir.path().join("adapters/test_logs.yml");
        assert!(adapter_file.exists());

        let content = fs::read_to_string(adapter_file)?;
        assert!(content.contains("Generated adapter for test_logs"));
        assert!(content.contains("connection:"));
        assert!(content.contains("format:"));

        Ok(())
    }

    #[test]
    fn test_execute_adapter_new_already_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;

        fs::write(temp_dir.path().join("adapters/existing.yml"), "test")?;

        let result = execute_adapter_new("existing", temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_execute_adapter_delete_success() -> Result<()> {
        let temp_dir = setup_test_project()?;

        fs::write(temp_dir.path().join("adapters/to_delete.yml"), "test")?;

        let result = execute_adapter_delete("to_delete", temp_dir.path());

        assert!(result.is_ok());
        assert!(!temp_dir.path().join("adapters/to_delete.yml").exists());

        Ok(())
    }

    #[test]
    fn test_execute_adapter_delete_not_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;

        let result = execute_adapter_delete("nonexistent", temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_render_adapter_template() {
        let template = super::render_adapter_template("test_adapter");

        assert!(template.contains("Generated adapter for test_adapter"));
        assert!(template.contains("connection: <CONNECTION_NAME>"));
        assert!(template.contains("type: csv"));
        assert!(template.contains("has_header: true"));
    }

    #[test]
    fn test_load_project_config() {
        let temp_dir = setup_test_project().unwrap();
        let project_yml_content = r#"
storage:
  type: local
  path: ./storage
database:
  type: sqlite
  path: ./database.db
deployments:
  timeout: 600
connections:
  test_connection:
    type: localfile
    base_path: /tmp/test
"#;
        fs::write(temp_dir.path().join("project.yml"), project_yml_content).unwrap();

        let config = load_project_config(temp_dir.path()).unwrap();
        assert_eq!(config.connections.len(), 1);
        assert!(config.connections.contains_key("test_connection"));
    }

    #[tokio::test]
    async fn test_generate_file_schema_csv() {
        let temp_dir = setup_test_project().unwrap();
        let csv_content =
            "id,name,email,created_at\n1,John Doe,john@example.com,2024-01-01 10:00:00\n";
        let csv_path = temp_dir.path().join("data/test.csv");
        fs::create_dir_all(csv_path.parent().unwrap()).unwrap();
        fs::write(&csv_path, csv_content).unwrap();

        let connection_config = ConnectionConfig::LocalFile {
            base_path: temp_dir.path().to_string_lossy().to_string(),
        };

        let schema = generate_file_schema("data/test.csv", "csv", &connection_config)
            .await
            .unwrap();

        assert!(!schema.is_empty());
        assert!(schema.iter().all(|col| col.description.is_some()));
        assert!(schema.iter().all(|col| {
            col.description
                .as_ref()
                .unwrap()
                .starts_with("Auto-detected")
        }));
    }

    #[tokio::test]
    async fn test_generate_file_schema_json() {
        let temp_dir = setup_test_project().unwrap();
        let json_content = r#"{"id": 1, "name": "test", "value": 42.5}
{"id": 2, "name": "another", "value": 84.0}"#;
        let json_path = temp_dir.path().join("data/test.json");
        fs::create_dir_all(json_path.parent().unwrap()).unwrap();
        fs::write(&json_path, json_content).unwrap();

        let connection_config = ConnectionConfig::LocalFile {
            base_path: temp_dir.path().to_string_lossy().to_string(),
        };

        let schema = generate_file_schema("data/test.json", "json", &connection_config)
            .await
            .unwrap();

        assert!(!schema.is_empty());
        assert!(schema.iter().all(|col| col.description.is_some()));
        assert!(schema.iter().all(|col| {
            col.description
                .as_ref()
                .unwrap()
                .starts_with("Auto-detected")
        }));
    }

    #[tokio::test]
    async fn test_generate_file_schema_nonexistent_file() {
        let temp_dir = setup_test_project().unwrap();

        let connection_config = ConnectionConfig::LocalFile {
            base_path: temp_dir.path().to_string_lossy().to_string(),
        };

        let result = generate_file_schema("nonexistent.csv", "csv", &connection_config).await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("File not found"));
    }

    #[tokio::test]
    async fn test_generate_file_schema_invalid_file() {
        let temp_dir = setup_test_project().unwrap();
        let invalid_content = "This is not valid CSV content without proper structure";
        let csv_path = temp_dir.path().join("data/invalid.csv");
        fs::create_dir_all(csv_path.parent().unwrap()).unwrap();
        fs::write(&csv_path, invalid_content).unwrap();

        let connection_config = ConnectionConfig::LocalFile {
            base_path: temp_dir.path().to_string_lossy().to_string(),
        };

        let result = generate_file_schema("data/invalid.csv", "csv", &connection_config).await;

        if result.is_err() {
            let error_message = result.unwrap_err().to_string();
            assert!(error_message.contains("Failed to generate schema from CSV file"));
        }
    }

    #[test]
    fn test_map_duckdb_type_to_featherbox() {
        assert_eq!(map_duckdb_type_to_featherbox("VARCHAR"), "STRING");
        assert_eq!(map_duckdb_type_to_featherbox("TEXT"), "STRING");
        assert_eq!(map_duckdb_type_to_featherbox("STRING"), "STRING");

        assert_eq!(map_duckdb_type_to_featherbox("TINYINT"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("SMALLINT"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("INTEGER"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("BIGINT"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("INT8"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("HUGEINT"), "INTEGER");
        assert_eq!(map_duckdb_type_to_featherbox("UINTEGER"), "INTEGER");

        assert_eq!(map_duckdb_type_to_featherbox("DOUBLE"), "FLOAT");
        assert_eq!(map_duckdb_type_to_featherbox("REAL"), "FLOAT");
        assert_eq!(map_duckdb_type_to_featherbox("FLOAT"), "FLOAT");
        assert_eq!(map_duckdb_type_to_featherbox("DECIMAL"), "FLOAT");

        assert_eq!(map_duckdb_type_to_featherbox("BOOLEAN"), "BOOLEAN");
        assert_eq!(map_duckdb_type_to_featherbox("BOOL"), "BOOLEAN");

        assert_eq!(map_duckdb_type_to_featherbox("DATE"), "DATETIME");
        assert_eq!(map_duckdb_type_to_featherbox("TIMESTAMP"), "DATETIME");
        assert_eq!(map_duckdb_type_to_featherbox("DATETIME"), "DATETIME");
        assert_eq!(map_duckdb_type_to_featherbox("TIMESTAMPTZ"), "DATETIME");

        assert_eq!(map_duckdb_type_to_featherbox("TIME"), "STRING");
        assert_eq!(map_duckdb_type_to_featherbox("TIMETZ"), "STRING");

        assert_eq!(map_duckdb_type_to_featherbox("JSON"), "JSON");
        assert_eq!(map_duckdb_type_to_featherbox("ARRAY<INTEGER>"), "JSON");
        assert_eq!(map_duckdb_type_to_featherbox("LIST<VARCHAR>"), "JSON");
        assert_eq!(
            map_duckdb_type_to_featherbox("STRUCT<name VARCHAR>"),
            "JSON"
        );

        assert_eq!(map_duckdb_type_to_featherbox("UUID"), "STRING");
        assert_eq!(map_duckdb_type_to_featherbox("BLOB"), "STRING");
        assert_eq!(map_duckdb_type_to_featherbox("UNKNOWN_TYPE"), "STRING");
    }
}
