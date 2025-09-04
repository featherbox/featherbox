use crate::{
    config::{
        adapter::{AdapterConfig, AdapterSource},
        project::{ConnectionConfig, DatabaseType},
    },
    pipeline::{
        ducklake::DuckLake,
        file_processor::{FileProcessor, FileSystem},
    },
};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum DatabaseSystem {
    Sqlite {
        path: String,
    },
    RemoteDatabase {
        db_type: DatabaseType,
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    },
}

impl DatabaseSystem {
    pub fn from_connection(connection: &ConnectionConfig) -> Result<Self> {
        match connection {
            ConnectionConfig::Sqlite { path } => Ok(Self::Sqlite { path: path.clone() }),
            ConnectionConfig::MySql {
                host,
                port,
                database,
                username,
                password,
            } => Ok(Self::RemoteDatabase {
                db_type: DatabaseType::Mysql,
                host: host.clone(),
                port: *port,
                database: database.clone(),
                username: username.clone(),
                password: password.clone(),
            }),
            ConnectionConfig::PostgreSql {
                host,
                port,
                database,
                username,
                password,
            } => Ok(Self::RemoteDatabase {
                db_type: DatabaseType::Postgresql,
                host: host.clone(),
                port: *port,
                database: database.clone(),
                username: username.clone(),
                password: password.clone(),
            }),
            ConnectionConfig::LocalFile { .. } | ConnectionConfig::S3(_) => Err(anyhow::anyhow!(
                "Connection type not supported for database operations"
            )),
        }
    }

    pub fn generate_connection_string(&self) -> String {
        match self {
            Self::Sqlite { path } => format!("sqlite:{}", path),
            Self::RemoteDatabase {
                db_type,
                host,
                port,
                database,
                username,
                password,
            } => match db_type {
                DatabaseType::Mysql => {
                    format!(
                        "mysql://{}:{}@{}:{}/{}",
                        username, password, host, port, database
                    )
                }
                DatabaseType::Postgresql => {
                    format!(
                        "postgresql://{}:{}@{}:{}/{}",
                        username, password, host, port, database
                    )
                }
                _ => unreachable!(),
            },
        }
    }

    pub fn generate_alias(&self) -> String {
        match self {
            Self::Sqlite { .. } => "sqlite_db".to_string(),
            Self::RemoteDatabase { db_type, .. } => match db_type {
                DatabaseType::Mysql => "mysql_db".to_string(),
                DatabaseType::Postgresql => "postgres_db".to_string(),
                _ => "remote_db".to_string(),
            },
        }
    }

    pub fn build_read_query(&self, db_alias: &str, table_name: &str) -> String {
        match self {
            Self::Sqlite { path } => {
                format!("SELECT * FROM sqlite_scan('{}', '{}')", path, table_name)
            }
            Self::RemoteDatabase { .. } => {
                format!("SELECT * FROM {}.{}", db_alias, table_name)
            }
        }
    }

    pub fn build_attach_query(&self, db_alias: &str) -> Result<String> {
        match self {
            Self::Sqlite { .. } => Ok("INSTALL sqlite_scanner; LOAD sqlite_scanner;".to_string()),
            Self::RemoteDatabase { .. } => {
                let connection_string = self.generate_connection_string();
                Ok(format!("ATTACH '{}' AS {}", connection_string, db_alias))
            }
        }
    }

    pub fn build_detach_query(&self, db_alias: &str) -> Result<String> {
        Ok(format!("DETACH {}", db_alias))
    }

    pub fn validate_table_exists(&self, db_alias: &str, table_name: &str) -> Result<String> {
        match self {
            Self::Sqlite { path } => Ok(format!(
                "SELECT COUNT(*) as count FROM sqlite_scan('{}', '{}')",
                path, table_name
            )),
            Self::RemoteDatabase { db_type, .. } => match db_type {
                DatabaseType::Mysql => Ok(format!(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'",
                    table_name
                )),
                DatabaseType::Postgresql => Ok(format!(
                    "SELECT tablename FROM pg_tables WHERE tablename = '{}'",
                    table_name
                )),
                _ => Err(anyhow::anyhow!(
                    "Unsupported database type for table validation"
                )),
            },
        }
    }
}

#[derive(Clone)]
pub struct Adapter {
    config: AdapterConfig,
    ducklake: Arc<DuckLake>,
}

impl Adapter {
    pub fn new(config: AdapterConfig, ducklake: Arc<DuckLake>) -> Self {
        Self { config, ducklake }
    }

    pub async fn execute_import(
        &self,
        table_name: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        match &self.config.source {
            AdapterSource::File { .. } => self.execute_file_import(table_name, connections).await,
            AdapterSource::Database {
                table_name: source_table,
            } => {
                self.execute_database_import(source_table, table_name, connections)
                    .await
            }
        }
    }

    async fn execute_file_import(
        &self,
        table_name: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        let filesystem = self.create_filesystem(connections).await?;

        if let Some(connection) = self.get_connection_if_exists(connections)
            && matches!(connection, ConnectionConfig::S3(_))
        {
            self.ducklake.configure_s3_connection(connection).await?;
        }

        let file_paths = FileProcessor::files_for_processing(&self.config, &filesystem).await?;

        if file_paths.is_empty() {
            return Ok(());
        }

        let query = self.build_import_query_multiple(&self.config, &file_paths)?;

        self.ducklake.create_table_from_query(table_name, &query)?;

        Ok(())
    }

    fn build_import_query_multiple(
        &self,
        adapter: &AdapterConfig,
        file_paths: &[String],
    ) -> Result<String> {
        if file_paths.is_empty() {
            return Err(anyhow::anyhow!("No files to load"));
        }

        if file_paths.len() == 1 {
            let file_path = &file_paths[0];
            return self.build_import_query_single(adapter, file_path);
        }

        let file_paths_str = file_paths
            .iter()
            .map(|p| format!("'{p}'"))
            .collect::<Vec<_>>()
            .join(", ");

        match &adapter.source {
            crate::config::adapter::AdapterSource::File { format, .. } => {
                match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        let query = format!(
                            "SELECT * FROM read_csv_auto([{file_paths_str}], header={has_header})"
                        );
                        Ok(query)
                    }
                    "parquet" => {
                        let query = format!("SELECT * FROM read_parquet([{file_paths_str}])");
                        Ok(query)
                    }
                    "json" => {
                        let query = format!("SELECT * FROM read_json_auto([{file_paths_str}])");
                        Ok(query)
                    }
                    _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Only file sources are supported in delta processing"
            )),
        }
    }

    fn build_import_query_single(
        &self,
        adapter: &AdapterConfig,
        file_path: &str,
    ) -> Result<String> {
        match &adapter.source {
            crate::config::adapter::AdapterSource::File { format, .. } => {
                match format.ty.as_str() {
                    "csv" => {
                        let has_header = format.has_header.unwrap_or(true);
                        let query = format!(
                            "SELECT * FROM read_csv_auto('{file_path}', header={has_header})"
                        );
                        Ok(query)
                    }
                    "parquet" => {
                        let query = format!("SELECT * FROM read_parquet('{file_path}')");
                        Ok(query)
                    }
                    "json" => {
                        let query = format!("SELECT * FROM read_json_auto('{file_path}')");
                        Ok(query)
                    }
                    _ => Err(anyhow::anyhow!("Unsupported format: {}", format.ty)),
                }
            }
            _ => Err(anyhow::anyhow!(
                "Only file sources are supported in delta processing"
            )),
        }
    }

    async fn execute_database_import(
        &self,
        source_table: &str,
        target_table: &str,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<()> {
        let connection = self.get_connection(connections)?;
        let db_system = DatabaseSystem::from_connection(connection)?;

        let db_alias = &db_system.generate_alias();
        let attach_result = self.attach_and_validate_database(db_alias, &db_system, source_table);
        if let Err(e) = attach_result {
            if let Err(detach_err) = self.detach_database(db_alias, &db_system) {
                eprintln!("Warning: Failed to detach database during cleanup: {detach_err}");
            }
            return Err(e);
        }

        let query = db_system.build_read_query(db_alias, source_table);

        self.ducklake
            .create_table_from_query(target_table, &query)?;

        Ok(())
    }

    fn detach_database(&self, db_alias: &str, db_system: &DatabaseSystem) -> Result<()> {
        self.ducklake
            .execute_batch(&db_system.build_detach_query(db_alias)?)
            .with_context(|| format!("Failed to detach SQLite database: {db_alias}"))
    }

    fn attach_and_validate_database(
        &self,
        db_alias: &str,
        db_system: &DatabaseSystem,
        source_table: &str,
    ) -> Result<()> {
        let attach_query = db_system.build_attach_query(db_alias)?;

        self.ducklake
            .execute_batch(&attach_query)
            .with_context(|| format!("Failed to attach SQLite database. Query: {attach_query}"))?;

        let validation_query = db_system.validate_table_exists(db_alias, source_table)?;
        let validation_result = self
            .ducklake
            .query(&validation_query)
            .with_context(|| format!("Failed to validate table existence for: {source_table}"))?;

        let table_exists = !validation_result.is_empty()
            && !validation_result[0].is_empty()
            && validation_result[0][0] != "0";

        if !table_exists {
            return Err(anyhow::anyhow!(
                "Table '{}' does not exist in the source database",
                source_table
            ));
        }

        Ok(())
    }

    fn get_connection<'a>(
        &self,
        connections: Option<&'a HashMap<String, ConnectionConfig>>,
    ) -> Result<&'a ConnectionConfig> {
        if let Some(connections) = connections {
            if let Some(connection) = connections.get(&self.config.connection) {
                Ok(connection)
            } else {
                Err(anyhow::anyhow!(
                    "Connection '{}' not found",
                    self.config.connection
                ))
            }
        } else {
            Err(anyhow::anyhow!("No connections provided"))
        }
    }

    async fn create_filesystem(
        &self,
        connections: Option<&HashMap<String, ConnectionConfig>>,
    ) -> Result<FileSystem> {
        if let Some(connections) = connections
            && let Some(connection) = connections.get(&self.config.connection)
        {
            return FileSystem::from_connection(connection).await;
        }
        Ok(FileSystem::new_local(None))
    }

    fn get_connection_if_exists<'a>(
        &self,
        connections: Option<&'a HashMap<String, ConnectionConfig>>,
    ) -> Option<&'a ConnectionConfig> {
        connections?.get(&self.config.connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig};
    use crate::config::project::StorageConfig;
    use crate::pipeline::ducklake::{CatalogConfig, DuckLake};
    use tempfile;

    fn create_test_adapter_config() -> AdapterConfig {
        AdapterConfig {
            connection: "local".to_string(),
            description: None,
            source: AdapterSource::File {
                file: FileConfig {
                    path: "test_data/*.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: Some(true),
                },
            },
            columns: vec![],
        }
    }

    #[tokio::test]
    async fn test_adapter_creation() {
        let config = create_test_adapter_config();

        let catalog_config = CatalogConfig::Sqlite {
            path: "/tmp/test_catalog.sqlite".to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: "/tmp/test_storage".to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());

        let adapter = Adapter::new(config, ducklake);
        assert_eq!(adapter.config.connection, "local");
    }

    #[tokio::test]
    async fn test_sqlite_database_system_attach_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let connection = rusqlite::Connection::open(&db_path).unwrap();
        connection
            .execute(
                "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO test_table (name) VALUES ('test1'), ('test2')",
                [],
            )
            .unwrap();
        drop(connection);

        let db_system = DatabaseSystem::Sqlite {
            path: db_path.to_str().unwrap().to_string(),
        };

        let attach_query = db_system.build_attach_query("test_db").unwrap();
        assert!(attach_query.contains("INSTALL sqlite_scanner"));
        assert!(attach_query.contains("LOAD sqlite_scanner"));

        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("catalog.sqlite")
                .to_str()
                .unwrap()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir.path().to_str().unwrap().to_string(),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let result = ducklake.execute_batch(&attach_query);
        assert!(
            result.is_ok(),
            "Failed to install sqlite_scanner: {:?}",
            result
        );

        let read_query = db_system.build_read_query("test_db", "test_table");
        let read_result = ducklake.query(&read_query);
        assert!(
            read_result.is_ok(),
            "Failed to read from table: {:?}",
            read_result
        );
    }
}
