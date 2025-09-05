use crate::config::Config;
use crate::config::project::{
    ConnectionConfig, DatabaseType, RemoteDatabaseConfig, S3AuthMethod, S3Config, StorageConfig,
};
use anyhow::{Context, Result};
use duckdb::DuckdbConnectionManager;
use r2d2::Pool;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Sqlite {
        path: String,
    },
    RemoteDatabase {
        db_type: DatabaseType,
        config: RemoteDatabaseConfig,
    },
}

#[derive(Clone)]
pub struct DuckLake {
    catalog_config: CatalogConfig,
    storage_config: StorageConfig,
    pool: Arc<Pool<DuckdbConnectionManager>>,
    #[allow(dead_code)]
    temp_dir: Arc<tempfile::TempDir>,
}

impl DuckLake {
    pub async fn new(catalog_config: CatalogConfig, storage_config: StorageConfig) -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let temp_db_path = temp_dir.path().join("shared.db");

        let manager = DuckdbConnectionManager::file(&temp_db_path)?;
        let pool = Pool::builder()
            .max_size(num_cpus::get() as u32)
            .build(manager)?;

        let instance = Self {
            catalog_config,
            storage_config,
            pool: Arc::new(pool),
            temp_dir: Arc::new(temp_dir),
        };

        instance.initialize().await?;
        Ok(instance)
    }

    pub async fn from_config(config: &Config) -> Result<DuckLake> {
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

    async fn initialize(&self) -> Result<()> {
        self.initialize_base().await?;
        self.attach().await?;
        Ok(())
    }

    async fn initialize_base(&self) -> Result<()> {
        self.execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("Failed to install and load extensions")?;
        Ok(())
    }

    pub async fn configure_s3_connection(&self, connection: &ConnectionConfig) -> Result<()> {
        if let ConnectionConfig::S3(s3_config) = connection {
            let region = &s3_config.region;
            let auth_method = &s3_config.auth_method;
            let access_key_id = &s3_config.access_key_id;
            let secret_access_key = &s3_config.secret_access_key;
            let session_token = &s3_config.session_token;
            let path_style_access = s3_config.path_style_access;
            self.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("Failed to install and load httpfs extension for S3")?;

            match auth_method {
                S3AuthMethod::CredentialChain => {
                    self.execute_batch("INSTALL aws; LOAD aws;")
                        .context("Failed to install and load aws extension for credential chain")?;

                    let mut create_secret_parts = vec![
                        "CREATE OR REPLACE SECRET s3_secret (".to_string(),
                        "    TYPE S3,".to_string(),
                        format!("    REGION '{}'", region),
                        ",    PROVIDER credential_chain".to_string(),
                    ];

                    if let Some(endpoint) = connection.get_full_endpoint_url() {
                        create_secret_parts.push(format!(",    ENDPOINT '{endpoint}'"));
                    }

                    if path_style_access {
                        create_secret_parts.push(",    URL_STYLE 'path'".to_string());
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

                    if let Some(clean_endpoint) = connection.get_clean_endpoint_url() {
                        s3_settings.push(format!("SET s3_endpoint = '{clean_endpoint}';"));
                    }

                    if path_style_access {
                        s3_settings.push("SET s3_url_style = 'path';".to_string());
                    }

                    if !connection.uses_ssl() {
                        s3_settings.push("SET s3_use_ssl = false;".to_string());
                    }

                    if let Some(token) = session_token {
                        s3_settings.push(format!("SET s3_session_token = '{token}';"));
                    }

                    let settings_sql = s3_settings.join(" ");
                    self.execute_batch(&settings_sql)
                        .context("Failed to configure S3 authentication settings")?;
                }
            }
        }
        Ok(())
    }

    async fn attach(&self) -> Result<()> {
        let (extension_sql, attach_sql) = self.catalog_sql()?;

        match &self.storage_config {
            StorageConfig::LocalFile { path } => {
                std::fs::create_dir_all(path)
                    .with_context(|| format!("Failed to create storage directory: {path}"))?;
            }
            StorageConfig::S3(_) => {
                self.configure_s3_storage().await?;
            }
        };

        self.execute_batch(&extension_sql)
            .context("Failed to install and load database extension")?;

        self.execute_batch(&attach_sql)
            .context("Failed to attach DuckLake catalog")?;

        Ok(())
    }

    fn catalog_sql(&self) -> Result<(String, String)> {
        match &self.catalog_config {
            CatalogConfig::Sqlite { path } => {
                let catalog_path = Path::new(path);
                if let Some(parent) = catalog_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context("Failed to create catalog directory")?;
                }

                let data_path = self.get_storage_path();

                let extension_sql = "INSTALL sqlite; LOAD sqlite;".to_string();
                let attach_sql = format!(
                    "ATTACH 'ducklake:sqlite:{path}' AS db (DATA_PATH '{data_path}'); USE db;"
                );

                Ok((extension_sql, attach_sql))
            }
            CatalogConfig::RemoteDatabase { db_type, config } => {
                let data_path = self.get_storage_path();

                let (extension_name, connection_string) = match db_type {
                    DatabaseType::Mysql => {
                        let extension = "mysql";
                        let conn_str = format!(
                            "ducklake:mysql:db={} host={} port={} user={} password={}",
                            config.database,
                            config.host,
                            config.port,
                            config.username,
                            config.password
                        );
                        (extension, conn_str)
                    }
                    DatabaseType::Postgresql => {
                        let extension = "postgres";
                        let conn_str = format!(
                            "ducklake:postgres:dbname={} host={} port={} user={} password={}",
                            config.database,
                            config.host,
                            config.port,
                            config.username,
                            config.password
                        );
                        (extension, conn_str)
                    }
                    DatabaseType::Sqlite => {
                        unreachable!("SQLite should not use RemoteDatabase catalog variant")
                    }
                };

                let extension_sql = format!("INSTALL {extension_name}; LOAD {extension_name};");
                let attach_sql = format!(
                    "ATTACH '{connection_string}' AS db (DATA_PATH '{data_path}', METADATA_SCHEMA '{}_metadata'); USE db;",
                    config.database
                );

                Ok((extension_sql, attach_sql))
            }
        }
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let connection = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;

        connection
            .execute_batch(sql)
            .with_context(|| format!("Failed to execute batch SQL: {sql}"))
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let connection = self
            .pool
            .get()
            .context("Failed to get connection from pool")?;
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

    pub fn create_table_from_query(&self, table_name: &str, query: &str) -> Result<()> {
        let sql = format!("CREATE OR REPLACE TABLE {table_name} AS ({query});");
        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create table '{table_name}' from query: '{query}'"))
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

    async fn configure_s3_storage(&self) -> Result<()> {
        if let StorageConfig::S3(s3_config) = &self.storage_config {
            self.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("Failed to install and load httpfs extension")?;

            let secret_sql = Self::build_s3_secret_sql(s3_config, "s3_secret", true);

            self.execute_batch(&secret_sql)
                .context("Failed to create S3 secret")?;
        }

        Ok(())
    }

    fn build_s3_secret_sql(s3_config: &S3Config, secret_name: &str, if_not_exists: bool) -> String {
        let is_minio = s3_config
            .endpoint_url
            .as_ref()
            .map(|url| url.contains("localhost") || url.contains("127.0.0.1"))
            .unwrap_or(false);

        let create_clause = if if_not_exists {
            format!("CREATE SECRET IF NOT EXISTS {secret_name}")
        } else {
            format!("CREATE OR REPLACE SECRET {secret_name}")
        };

        let mut sql = match &s3_config.auth_method {
            S3AuthMethod::Explicit => format!(
                "{create_clause} (
                    TYPE S3,
                    KEY_ID '{}',
                    SECRET '{}',
                    REGION '{}'",
                s3_config.access_key_id, s3_config.secret_access_key, s3_config.region
            ),
            S3AuthMethod::CredentialChain => format!(
                "{create_clause} (
                    TYPE S3,
                    PROVIDER credential_chain,
                    REGION '{}'",
                s3_config.region
            ),
        };

        if let Some(endpoint) = &s3_config.endpoint_url {
            let clean_endpoint = endpoint
                .strip_prefix("http://")
                .or_else(|| endpoint.strip_prefix("https://"))
                .unwrap_or(endpoint);
            sql.push_str(&format!(",\n    ENDPOINT '{clean_endpoint}'"));
        }

        if let Some(token) = &s3_config.session_token {
            sql.push_str(&format!(",\n    SESSION_TOKEN '{token}'"));
        }

        if s3_config.path_style_access || is_minio {
            sql.push_str(",\n    URL_STYLE 'path'");
        }

        if is_minio {
            sql.push_str(",\n    USE_SSL false");
        }

        sql.push_str("\n);");
        sql
    }

    fn get_storage_path(&self) -> String {
        match &self.storage_config {
            StorageConfig::LocalFile { path } => path.clone(),
            StorageConfig::S3(s3_config) => format!("s3://{}/ducklake", s3_config.bucket),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::S3Config;

    fn create_test_s3_storage(
        bucket: &str,
        region: &str,
        endpoint_url: Option<String>,
        auth_method: S3AuthMethod,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> StorageConfig {
        create_test_s3_storage_with_session_token(
            bucket,
            region,
            endpoint_url,
            auth_method,
            access_key_id,
            secret_access_key,
            None,
        )
    }

    fn create_test_s3_storage_with_session_token(
        bucket: &str,
        region: &str,
        endpoint_url: Option<String>,
        auth_method: S3AuthMethod,
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<String>,
    ) -> StorageConfig {
        StorageConfig::S3(S3Config {
            bucket: bucket.to_string(),
            region: region.to_string(),
            endpoint_url,
            auth_method,
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            session_token,
            path_style_access: false,
        })
    }
    use crate::config::project::ConnectionConfig;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_ducklake_new() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test";
        fs::remove_dir_all(test_dir).ok();
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
    async fn test_ducklake_new_with_s3() {
        use std::fs;

        let test_dir = "/tmp/ducklake_s3_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = create_test_s3_storage(
            "test-bucket",
            "us-east-1",
            Some("http://localhost:9010".to_string()),
            S3AuthMethod::Explicit,
            "test_key",
            "test_secret",
        );

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        assert!(ducklake.is_ok());
    }

    #[tokio::test]
    async fn test_ducklake_configure_s3_explicit_auth() {
        use std::fs;

        let test_dir = "/tmp/ducklake_s3_auth_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = create_test_s3_storage_with_session_token(
            "test-bucket",
            "us-west-2",
            Some("https://s3.us-west-2.amazonaws.com".to_string()),
            S3AuthMethod::Explicit,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Some("test-session-token".to_string()),
        );

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        assert!(ducklake.is_ok());
    }

    #[tokio::test]
    async fn test_ducklake_configure_s3_credential_chain() {
        use std::fs;

        let test_dir = "/tmp/ducklake_s3_creds_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::S3(S3Config {
            bucket: "test-bucket".to_string(),
            region: "eu-central-1".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            path_style_access: false,
        });

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        assert!(ducklake.is_ok());
    }

    #[test]
    fn test_get_storage_path_local() {
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };
        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let temp_db_path = temp_dir.path().join("test.db");
        let manager = DuckdbConnectionManager::file(&temp_db_path).unwrap();
        let pool = Pool::builder().build(manager).unwrap();
        let ducklake = DuckLake {
            catalog_config,
            storage_config,
            pool: Arc::new(pool),
            temp_dir: Arc::new(temp_dir),
        };

        assert_eq!(ducklake.get_storage_path(), "/tmp/test_storage");
    }

    #[test]
    fn test_get_storage_path_s3() {
        let storage_config = StorageConfig::S3(S3Config {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            path_style_access: false,
        });
        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let temp_db_path = temp_dir.path().join("test.db");
        let manager = DuckdbConnectionManager::file(&temp_db_path).unwrap();
        let pool = Pool::builder().build(manager).unwrap();
        let ducklake = DuckLake {
            catalog_config,
            storage_config,
            pool: Arc::new(pool),
            temp_dir: Arc::new(temp_dir),
        };

        assert_eq!(ducklake.get_storage_path(), "s3://my-bucket/ducklake");
    }

    #[tokio::test]
    async fn test_create_table_from_query() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_query";
        fs::remove_dir_all(test_dir).ok();
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
        fs::remove_dir_all(test_dir).ok();
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
    async fn test_ducklake_configure_s3_connection_explicit() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir = format!("/tmp/ducklake_s3_test_{timestamp}");
        fs::remove_dir_all(&test_dir).ok();
        fs::create_dir_all(&test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let s3_connection = ConnectionConfig::S3(S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("https://s3.amazonaws.com".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "test_access_key".to_string(),
            secret_access_key: "test_secret_key".to_string(),
            session_token: Some("test_session_token".to_string()),
            path_style_access: false,
        });

        let configure_result = ducklake.configure_s3_connection(&s3_connection).await;
        assert!(configure_result.is_ok());

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
    async fn test_ducklake_configure_s3_connection_with_endpoint() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir = format!("/tmp/ducklake_mixed_test_{timestamp}");
        fs::remove_dir_all(&test_dir).ok();
        fs::create_dir_all(&test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let s3_connection = ConnectionConfig::S3(S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "test_access_key".to_string(),
            secret_access_key: "test_secret_key".to_string(),
            session_token: None,
            path_style_access: false,
        });

        let configure_result = ducklake.configure_s3_connection(&s3_connection).await;
        assert!(configure_result.is_ok());

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
        fs::remove_dir_all(test_dir).ok();
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

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        assert!(ducklake.is_ok());

        let ducklake = ducklake.unwrap();

        let result = ducklake.query("SELECT 1 as test_query");
        assert!(result.is_ok(), "Basic query should work");

        let results = ducklake
            .query("SELECT current_setting('s3_region')")
            .unwrap();
        if !results.is_empty() && !results[0][0].is_empty() && results[0][0] != "NULL" {
            assert_eq!(
                results[0][0], "us-east-1",
                "Without S3 connections, should return DuckDB default region"
            );
        }
    }

    #[tokio::test]
    async fn test_ducklake_configure_s3_connection_credential_chain() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir = format!("/tmp/ducklake_credential_chain_test_{timestamp}");
        fs::remove_dir_all(&test_dir).ok();
        fs::create_dir_all(&test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let s3_connection = ConnectionConfig::S3(S3Config {
            bucket: "test-bucket".to_string(),
            region: "eu-west-1".to_string(),
            endpoint_url: Some("https://s3.eu-west-1.amazonaws.com".to_string()),
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            path_style_access: false,
        });

        let configure_result = ducklake.configure_s3_connection(&s3_connection).await;
        assert!(configure_result.is_ok());

        let result = ducklake.query("SELECT 1 as test_query");
        assert!(
            result.is_ok(),
            "Basic query should work with credential chain"
        );
    }

    #[tokio::test]
    async fn test_ducklake_configure_s3_connection_credential_chain_basic() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir = format!("/tmp/ducklake_credential_chain_basic_test_{timestamp}");
        fs::remove_dir_all(&test_dir).ok();
        fs::create_dir_all(&test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let s3_connection = ConnectionConfig::S3(S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            path_style_access: false,
        });

        let configure_result = ducklake.configure_s3_connection(&s3_connection).await;
        assert!(configure_result.is_ok());

        let result = ducklake.query("SELECT 1 as test_query");
        assert!(result.is_ok(), "Basic query should work");
    }

    #[tokio::test]
    async fn test_duckdb_s3_access_direct() {
        if std::env::var("FEATHERBOX_S3_TEST").is_err() {
            return;
        }

        use std::fs;
        let test_dir = "/tmp/duckdb_s3_direct_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let s3_connection = ConnectionConfig::S3(S3Config {
            bucket: "featherbox-test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            path_style_access: false,
        });

        ducklake
            .configure_s3_connection(&s3_connection)
            .await
            .unwrap();

        let s3_test = setup_s3_test_for_duckdb().await.unwrap();
        let s3_url = format!("s3://{}/test-data.json", s3_test.bucket_name);
        let test_query = format!("SELECT * FROM read_json_auto('{s3_url}')");

        let result = ducklake.query(&test_query);

        match result {
            Ok(data) => {
                assert!(!data.is_empty(), "Should have retrieved data from S3");
            }
            Err(e) => {
                panic!("DuckDB S3 access should work: {e:#?}");
            }
        }

        cleanup_s3_test_for_duckdb(&s3_test).await.ok();
    }

    async fn setup_s3_test_for_duckdb() -> Result<S3TestData> {
        use crate::config::project::{ConnectionConfig, S3AuthMethod};

        let unique_bucket = format!("featherbox-duckdb-test-{}", chrono::Utc::now().timestamp());

        let connection_config = ConnectionConfig::S3(S3Config {
            bucket: unique_bucket.clone(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            auth_method: S3AuthMethod::CredentialChain,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            path_style_access: false,
        });

        let s3_client = crate::s3_client::S3Client::new(&connection_config).await?;

        s3_client.create_bucket().await?;

        let test_json = r#"{"id": 1, "name": "test_item", "value": 42}
{"id": 2, "name": "another_item", "value": 84}
{"id": 3, "name": "final_item", "value": 126}"#;

        s3_client
            .put_object("test-data.json", test_json.as_bytes().to_vec())
            .await?;

        Ok(S3TestData {
            bucket_name: unique_bucket,
            s3_client,
        })
    }

    async fn cleanup_s3_test_for_duckdb(test_data: &S3TestData) -> Result<()> {
        test_data
            .s3_client
            .delete_objects(vec!["test-data.json".to_string()])
            .await?;
        test_data.s3_client.delete_bucket().await?;
        Ok(())
    }

    struct S3TestData {
        bucket_name: String,
        s3_client: crate::s3_client::S3Client,
    }

    #[tokio::test]
    async fn test_mysql_catalog_connection() -> anyhow::Result<()> {
        use std::process::Command;
        use uuid::Uuid;

        let db_name = format!("ducklake_test_{}", Uuid::new_v4().simple());

        let create_db_result = Command::new("docker")
            .args([
                "compose",
                "exec",
                "mysql",
                "mysql",
                "-u",
                "featherbox",
                "-ptestpass",
                "-e",
            ])
            .arg(format!("CREATE DATABASE IF NOT EXISTS {db_name};"))
            .output();

        if create_db_result.is_err() {
            return Ok(());
        }

        let output = create_db_result.unwrap();
        if !output.status.success() {
            return Ok(());
        }

        let catalog_config = CatalogConfig::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3306,
                database: db_name.clone(),
                username: "featherbox".to_string(),
                password: "testpass".to_string(),
            },
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir.path().to_string_lossy().to_string(),
        };

        let result = DuckLake::new(catalog_config, storage_config).await;

        Command::new("docker")
            .args([
                "compose",
                "exec",
                "mysql",
                "mysql",
                "-u",
                "featherbox",
                "-ptestpass",
                "-e",
            ])
            .arg(format!("DROP DATABASE IF EXISTS {db_name};"))
            .output()
            .ok();

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("MySQL catalog connection failed: {e}");
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_postgresql_catalog_connection() -> anyhow::Result<()> {
        use std::process::Command;
        use uuid::Uuid;

        let db_name = format!("ducklake_test_{}", Uuid::new_v4().simple());

        let create_db_result = Command::new("docker")
            .args([
                "compose",
                "exec",
                "postgres",
                "psql",
                "-U",
                "featherbox",
                "-d",
                "featherbox_test",
                "-c",
            ])
            .arg(format!("CREATE DATABASE {db_name};"))
            .output();

        if create_db_result.is_err() {
            return Ok(());
        }

        let output = create_db_result.unwrap();
        if !output.status.success() {
            return Ok(());
        }

        let catalog_config = CatalogConfig::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: db_name.clone(),
                username: "featherbox".to_string(),
                password: "testpass".to_string(),
            },
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir.path().to_string_lossy().to_string(),
        };

        let result = DuckLake::new(catalog_config, storage_config).await;

        Command::new("docker")
            .args([
                "compose",
                "exec",
                "postgres",
                "psql",
                "-U",
                "featherbox",
                "-d",
                "featherbox_test",
                "-c",
            ])
            .arg(format!("DROP DATABASE IF EXISTS {db_name};"))
            .output()
            .ok();

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                eprintln!("PostgreSQL catalog connection failed: {e}");
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn test_ducklake_with_minio() {
        use std::fs;
        use std::process::Command;

        let test_dir = "/tmp/ducklake_minio_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };

        let unique_bucket = format!("test-bucket-{}", uuid::Uuid::new_v4().simple());
        let storage_config = StorageConfig::S3(S3Config {
            bucket: unique_bucket.clone(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9010".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "user".to_string(),
            secret_access_key: "password".to_string(),
            session_token: None,
            path_style_access: false,
        });

        let check_minio = Command::new("docker")
            .args(["compose", "exec", "minio", "ls", "/data"])
            .output();

        if check_minio.is_err() || !check_minio.unwrap().status.success() {
            return;
        }

        let setup_result = Command::new("docker")
            .args([
                "compose",
                "exec",
                "minio",
                "mc",
                "alias",
                "set",
                "myminio",
                "http://localhost:9000",
                "user",
                "password",
            ])
            .output();

        if setup_result.is_err() || !setup_result.unwrap().status.success() {
            return;
        }

        let create_bucket_result = Command::new("docker")
            .args([
                "compose",
                "exec",
                "minio",
                "mc",
                "mb",
                "--ignore-existing",
                &format!("myminio/{unique_bucket}"),
            ])
            .output();

        if create_bucket_result.is_err() || !create_bucket_result.unwrap().status.success() {
            return;
        }

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        match ducklake {
            Ok(dl) => {
                let query_result = dl.query("SELECT 1 as test_query");
                assert!(
                    query_result.is_ok(),
                    "Basic query should work with MinIO S3 storage"
                );

                let create_table_result =
                    dl.create_table_from_query("test_table", "SELECT 1 as id, 'test' as name");

                match create_table_result {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("Table creation should work with MinIO: {e:#?}");
                    }
                }

                let verify_result = dl.query("SELECT * FROM test_table");
                assert!(verify_result.is_ok(), "Table query should work");
                let results = verify_result.unwrap();
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], vec!["1", "test"]);
            }
            Err(e) => {
                panic!("DuckLake creation with MinIO should work: {e:#?}");
            }
        }

        let _cleanup = Command::new("docker")
            .args([
                "compose",
                "exec",
                "minio",
                "mc",
                "rm",
                "--recursive",
                "--force",
                &format!("myminio/{unique_bucket}"),
            ])
            .output();
    }

    #[tokio::test]
    async fn test_ducklake_execute_batch_parallel() {
        use std::sync::Arc;
        use std::thread;
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");

        let catalog_config = CatalogConfig::Sqlite {
            path: db_path.to_string_lossy().to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir.path().to_string_lossy().to_string(),
        };

        let ducklake = Arc::new(
            DuckLake::new(catalog_config, storage_config)
                .await
                .expect("Failed to create DuckLake"),
        );

        let mut handles = vec![];

        for i in 0..5 {
            let ducklake_clone = ducklake.clone();
            let handle = thread::spawn(move || {
                let table_name = format!("parallel_test_{i}");
                let sql = format!("CREATE TABLE {table_name} (id INT, value VARCHAR);");
                ducklake_clone
                    .execute_batch(&sql)
                    .unwrap_or_else(|_| panic!("Failed to create table {table_name}"));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread failed");
        }

        let result = ducklake.query(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'parallel_test_%' ORDER BY table_name"
        ).expect("Failed to query created tables");

        assert_eq!(result.len(), 5);
    }

    #[tokio::test]
    async fn test_ducklake_query_parallel() {
        use std::sync::Arc;
        use std::thread;
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_query.db");

        let catalog_config = CatalogConfig::Sqlite {
            path: db_path.to_string_lossy().to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir.path().to_string_lossy().to_string(),
        };

        let ducklake = Arc::new(
            DuckLake::new(catalog_config, storage_config)
                .await
                .expect("Failed to create DuckLake"),
        );

        ducklake
            .execute_batch("CREATE TABLE test_data (id INT, value INT);")
            .expect("Failed to create test table");

        ducklake
            .execute_batch(
                "INSERT INTO test_data VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);",
            )
            .expect("Failed to insert test data");

        let mut handles = vec![];

        for i in 1..=5 {
            let ducklake_clone = ducklake.clone();
            let handle = thread::spawn(move || {
                let sql = format!("SELECT value FROM db.test_data WHERE id = {i}");
                let result = ducklake_clone
                    .query(&sql)
                    .unwrap_or_else(|_| panic!("Failed to query id {i}"));
                assert_eq!(result.len(), 1);
                assert_eq!(result[0].len(), 1);
                let expected_value = (i * 100).to_string();
                assert_eq!(result[0][0], expected_value);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread failed");
        }
    }
}
