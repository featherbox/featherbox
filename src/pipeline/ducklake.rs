use crate::config::project::{ConnectionConfig, S3AuthMethod};
use anyhow::{Context, Result};
use duckdb::Connection;
use std::collections::HashMap;
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

    pub async fn new_with_connections(
        catalog_config: CatalogConfig,
        storage_config: StorageConfig,
        connections: HashMap<String, ConnectionConfig>,
    ) -> Result<Self> {
        let connection =
            Connection::open_in_memory().context("Failed to create DuckDB connection")?;

        let instance = Self {
            catalog_config,
            storage_config,
            connection,
        };

        instance.initialize_with_connections(&connections).await?;
        Ok(instance)
    }

    async fn initialize(&self) -> Result<()> {
        self.initialize_base().await?;
        self.setup_catalog_and_storage().await?;
        Ok(())
    }

    async fn initialize_with_connections(
        &self,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<()> {
        self.initialize_base().await?;
        self.configure_s3_connections(connections).await?;
        self.setup_catalog_and_storage().await?;
        Ok(())
    }

    async fn initialize_base(&self) -> Result<()> {
        self.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("Failed to install and load extensions")?;
        Ok(())
    }

    async fn configure_s3_connections(
        &self,
        connections: &HashMap<String, ConnectionConfig>,
    ) -> Result<()> {
        let has_s3_connection = connections
            .values()
            .any(|conn| matches!(conn, ConnectionConfig::S3 { .. }));

        if has_s3_connection {
            self.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("Failed to install and load httpfs extension for S3")?;

            for connection in connections.values() {
                if let ConnectionConfig::S3 {
                    bucket: _,
                    region,
                    endpoint_url,
                    auth_method,
                    access_key_id,
                    secret_access_key,
                    session_token,
                } = connection
                {
                    self.apply_s3_authentication(
                        region,
                        endpoint_url.as_ref(),
                        auth_method,
                        access_key_id,
                        secret_access_key,
                        session_token.as_ref(),
                    )
                    .await?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn apply_s3_authentication(
        &self,
        region: &str,
        endpoint_url: Option<&String>,
        auth_method: &S3AuthMethod,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&String>,
    ) -> Result<()> {
        match auth_method {
            S3AuthMethod::CredentialChain => {
                self.execute_batch("INSTALL aws; LOAD aws;")
                    .context("Failed to install and load aws extension for credential chain")?;

                let mut create_secret_parts = vec![
                    "CREATE OR REPLACE SECRET s3_secret (".to_string(),
                    "    TYPE S3,".to_string(),
                    format!("    REGION '{}'", region),
                ];

                if let Some(endpoint) = endpoint_url {
                    create_secret_parts.push(format!(",    ENDPOINT '{endpoint}'"));
                }

                create_secret_parts.push(");".to_string());

                let create_secret_sql = create_secret_parts.join("\n");
                self.execute_batch(&create_secret_sql)
                    .context("Failed to create S3 secret with credential chain")?;
            }
            S3AuthMethod::Explicit => {
                let mut s3_settings = vec![
                    format!("SET s3_region = '{}';", region),
                    format!("SET s3_access_key_id = '{}';", access_key_id),
                    format!("SET s3_secret_access_key = '{}';", secret_access_key),
                ];

                if let Some(endpoint) = endpoint_url {
                    s3_settings.push(format!("SET s3_endpoint = '{endpoint}';"));
                }

                if let Some(token) = session_token {
                    s3_settings.push(format!("SET s3_session_token = '{token}';"));
                }

                let settings_sql = s3_settings.join(" ");
                self.execute_batch(&settings_sql)
                    .context("Failed to configure S3 authentication settings")?;
            }
        }

        Ok(())
    }

    async fn setup_catalog_and_storage(&self) -> Result<()> {
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

                self.execute_batch("INSTALL sqlite; LOAD sqlite;")
                    .context("Failed to install and load SQLite extension")?;

                let attach_sql = format!(
                    "ATTACH 'ducklake:sqlite:{path}' AS db (DATA_PATH '{data_path}'); USE db;"
                );
                self.execute_batch(&attach_sql)
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

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        tracing::info!("DuckDB executing batch SQL: {}", sql);
        let result = self
            .connection
            .execute_batch(sql)
            .context("Failed to execute batch SQL");
        match &result {
            Ok(_) => tracing::info!("DuckDB batch SQL executed successfully"),
            Err(e) => tracing::error!("DuckDB batch SQL execution failed: {}", e),
        }
        result?;
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        tracing::info!("DuckDB executing query: {}", sql);
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

        tracing::info!("DuckDB query returned {} rows", results.len());
        Ok(results)
    }

    pub fn create_table_from_query(&self, table_name: &str, query: &str) -> Result<()> {
        let sql = format!("CREATE OR REPLACE TABLE {table_name} AS ({query});");
        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create table '{table_name}' from query"))
    }

    pub fn create_table(&self, table_name: &str, columns: &[(String, String)]) -> Result<()> {
        if columns.is_empty() {
            return Err(anyhow::anyhow!(
                "Cannot create table without column definitions"
            ));
        }

        let column_definitions: Vec<String> = columns
            .iter()
            .map(|(name, data_type)| format!("{name} {data_type}"))
            .collect();

        let columns_sql = column_definitions.join(", ");
        let sql = format!("CREATE OR REPLACE TABLE {table_name} ({columns_sql});");

        tracing::info!("Creating empty table with SQL: {}", sql);
        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create empty table '{table_name}'"))
    }

    pub fn generate_temp_table_name(prefix: &str) -> String {
        format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
    }

    pub fn drop_temp_table(&self, table_name: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table_name};");
        self.execute_batch(&drop_sql)
            .with_context(|| format!("Failed to drop temporary table: {table_name}"))
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );
        let results = self.query(&sql)?;
        let exists = if let Some(row) = results.first() {
            if let Some(count_str) = row.first() {
                count_str.parse::<i64>().unwrap_or(0) > 0
            } else {
                false
            }
        } else {
            false
        };
        Ok(exists)
    }

    pub fn table_schema(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        );
        let results = self.query(&sql)?;
        let columns = results
            .into_iter()
            .filter_map(|row| {
                if row.len() >= 2 {
                    Some((row[0].clone(), row[1].clone()))
                } else {
                    None
                }
            })
            .collect();
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::ConnectionConfig;

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
        assert!(ducklake.is_ok());
    }

    #[tokio::test]
    async fn test_create_table_from_query() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_query";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let query = "SELECT 1 as id, 'Alice' as name";
        ducklake
            .create_table_from_query("test_table", query)
            .unwrap();

        let results = ducklake.query("SELECT * FROM test_table").unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec!["1", "Alice"]);
    }

    #[tokio::test]
    async fn test_execute_batch_and_query() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_exec";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        ducklake.execute_batch(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR); INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');"
        ).unwrap();

        let results = ducklake
            .query("SELECT * FROM test_table ORDER BY id")
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec!["1", "Alice"]);
        assert_eq!(results[1], vec!["2", "Bob"]);
    }

    #[tokio::test]
    async fn test_ducklake_new_with_s3_connection() {
        use std::fs;

        let test_dir = "/tmp/ducklake_s3_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: Some("https://s3.amazonaws.com".to_string()),
                auth_method: S3AuthMethod::Explicit,
                access_key_id: "test_access_key".to_string(),
                secret_access_key: "test_secret_key".to_string(),
                session_token: Some("test_session_token".to_string()),
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections).await;
        assert!(ducklake.is_ok());

        let ducklake = ducklake.unwrap();
        let result = ducklake.query("SELECT current_setting('s3_region')");
        assert!(
            result.is_ok(),
            "Failed to query s3_region setting: {result:?}"
        );
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "us-east-1");
    }

    #[tokio::test]
    async fn test_ducklake_new_with_mixed_connections() {
        use std::fs;

        let test_dir = "/tmp/ducklake_mixed_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "local_connection".to_string(),
            ConnectionConfig::LocalFile {
                base_path: "/tmp/local".to_string(),
            },
        );
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-west-2".to_string(),
                endpoint_url: None,
                auth_method: S3AuthMethod::Explicit,
                access_key_id: "test_access_key".to_string(),
                secret_access_key: "test_secret_key".to_string(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections).await;
        assert!(ducklake.is_ok());

        let ducklake = ducklake.unwrap();
        let result = ducklake.query("SELECT current_setting('s3_region')");
        assert!(
            result.is_ok(),
            "Failed to query s3_region setting: {result:?}"
        );
        let results = result.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "us-west-2");
    }

    #[tokio::test]
    async fn test_ducklake_new_without_s3_connections() {
        use std::fs;

        let test_dir = "/tmp/ducklake_no_s3_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "local_connection".to_string(),
            ConnectionConfig::LocalFile {
                base_path: "/tmp/local".to_string(),
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections).await;
        assert!(ducklake.is_ok());

        let ducklake = ducklake.unwrap();

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
    }

    #[tokio::test]
    async fn test_ducklake_new_with_s3_credential_chain() {
        use std::fs;

        let test_dir = "/tmp/ducklake_credential_chain_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "eu-west-1".to_string(),
                endpoint_url: Some("https://s3.eu-west-1.amazonaws.com".to_string()),
                auth_method: S3AuthMethod::CredentialChain,
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections).await;

        match ducklake {
            Ok(ducklake) => {
                let result = ducklake.query("SELECT 1 as test_query");
                assert!(
                    result.is_ok(),
                    "Basic query should work with credential chain"
                );
            }
            Err(e) => {
                println!("Expected error due to missing AWS credentials or extension: {e}");
                assert!(
                    e.to_string().contains("aws")
                        || e.to_string().contains("extension")
                        || e.to_string().contains("install")
                        || e.to_string().contains("load"),
                    "Error should be related to AWS extension loading: {e}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_ducklake_new_with_s3_credential_chain_basic() {
        use std::fs;

        let test_dir = "/tmp/ducklake_credential_chain_basic_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let mut connections = HashMap::new();
        connections.insert(
            "s3_connection".to_string(),
            ConnectionConfig::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-west-2".to_string(),
                endpoint_url: None,
                auth_method: S3AuthMethod::CredentialChain,
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            },
        );

        let ducklake =
            DuckLake::new_with_connections(catalog_config, storage_config, connections).await;

        match ducklake {
            Ok(ducklake) => {
                let result = ducklake.query("SELECT 1 as test_query");
                assert!(result.is_ok(), "Basic query should work");
            }
            Err(e) => {
                println!("Expected error due to missing AWS extension: {e}");
                assert!(
                    e.to_string().contains("aws") || e.to_string().contains("extension"),
                    "Error should be related to AWS extension: {e}"
                );
            }
        }
    }
}
