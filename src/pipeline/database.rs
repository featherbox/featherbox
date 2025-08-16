use crate::config::project::{ConnectionConfig, DatabaseType, RemoteDatabaseConfig};
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum DatabaseSystem {
    Sqlite {
        path: String,
    },
    RemoteDatabase {
        db_type: DatabaseType,
        config: RemoteDatabaseConfig,
    },
}

impl DatabaseSystem {
    pub fn from_connection(connection: &ConnectionConfig) -> Result<Self> {
        match connection {
            ConnectionConfig::Sqlite { path } => Ok(Self::Sqlite { path: path.clone() }),
            ConnectionConfig::RemoteDatabase { db_type, config } => Ok(Self::RemoteDatabase {
                db_type: db_type.clone(),
                config: config.clone(),
            }),
            _ => Err(anyhow::anyhow!(
                "Unsupported connection type for database system"
            )),
        }
    }

    pub fn generate_alias(&self) -> String {
        match self {
            Self::Sqlite { .. } => {
                let uuid_suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
                format!("sqlite_db_{uuid_suffix}")
            }
            Self::RemoteDatabase { db_type, .. } => {
                let uuid_suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
                match db_type {
                    DatabaseType::Mysql => format!("mysql_db_{uuid_suffix}"),
                    DatabaseType::Postgresql => format!("postgres_db_{uuid_suffix}"),
                    DatabaseType::Sqlite => {
                        unreachable!("SQLite should not use RemoteDatabase variant")
                    }
                }
            }
        }
    }

    pub fn build_attach_query(&self, alias: &str) -> Result<String> {
        match self {
            Self::Sqlite { path } => {
                let query = format!("ATTACH '{path}' AS {alias} (TYPE sqlite);");
                Ok(query)
            }
            Self::RemoteDatabase { db_type, config } => {
                let db_type_str = match db_type {
                    DatabaseType::Mysql => "mysql",
                    DatabaseType::Postgresql => "postgres",
                    DatabaseType::Sqlite => {
                        unreachable!("SQLite should not use RemoteDatabase variant")
                    }
                };
                let query = format!(
                    "ATTACH 'host={} port={} database={} user={} password={}' AS {alias} (TYPE {db_type_str});",
                    config.host, config.port, config.database, config.username, config.password
                );
                Ok(query)
            }
        }
    }

    pub fn build_detach_query(&self, alias: &str) -> Result<String> {
        let query = format!("DETACH {alias};");
        Ok(query)
    }

    pub fn build_read_query(&self, alias: &str, table_name: &str) -> Result<String> {
        let query = format!("SELECT * FROM {alias}.{table_name}");
        Ok(query)
    }

    pub fn validate_table_exists(&self, alias: &str, table_name: &str) -> Result<String> {
        match self {
            Self::Sqlite { .. } => {
                let query = format!(
                    "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_catalog = '{alias}' AND table_name = '{table_name}'"
                );
                Ok(query)
            }
            Self::RemoteDatabase { db_type, config } => {
                let query = match db_type {
                    DatabaseType::Mysql => format!(
                        "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{table_name}'",
                        config.database
                    ),
                    DatabaseType::Postgresql => format!(
                        "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = '{}' AND table_name = '{table_name}'",
                        config.database
                    ),
                    DatabaseType::Sqlite => {
                        unreachable!("SQLite should not use RemoteDatabase variant")
                    }
                };
                Ok(query)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_system_creation() {
        let connection = ConnectionConfig::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let db_system = DatabaseSystem::from_connection(&connection).unwrap();

        match db_system {
            DatabaseSystem::Sqlite { path } => {
                assert_eq!(path, "/tmp/test.db");
            }
            _ => panic!("Expected SQLite system"),
        }
    }

    #[test]
    fn test_mysql_system_creation() {
        let connection = ConnectionConfig::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3307,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let db_system = DatabaseSystem::from_connection(&connection).unwrap();

        match db_system {
            DatabaseSystem::RemoteDatabase { db_type, config } => {
                assert_eq!(db_type, DatabaseType::Mysql);
                assert_eq!(config.host, "localhost");
                assert_eq!(config.port, 3307);
                assert_eq!(config.database, "datasource_test");
                assert_eq!(config.username, "datasource");
                assert_eq!(config.password, "datasourcepass");
            }
            _ => panic!("Expected MySQL system"),
        }
    }

    #[test]
    fn test_sqlite_attach_detach_queries() {
        let db_system = DatabaseSystem::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let alias = "test_db";
        let attach_query = db_system.build_attach_query(alias).unwrap();
        assert_eq!(
            attach_query,
            "ATTACH '/tmp/test.db' AS test_db (TYPE sqlite);"
        );

        let detach_query = db_system.build_detach_query(alias).unwrap();
        assert_eq!(detach_query, "DETACH test_db;");
    }

    #[test]
    fn test_mysql_attach_detach_queries() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3307,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "mysql_db";
        let attach_query = db_system.build_attach_query(alias).unwrap();
        assert_eq!(
            attach_query,
            "ATTACH 'host=localhost port=3307 database=datasource_test user=datasource password=datasourcepass' AS mysql_db (TYPE mysql);"
        );

        let detach_query = db_system.build_detach_query(alias).unwrap();
        assert_eq!(detach_query, "DETACH mysql_db;");
    }

    #[test]
    fn test_sqlite_read_query() {
        let db_system = DatabaseSystem::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let alias = "test_db";
        let query = db_system.build_read_query(alias, "users").unwrap();
        assert_eq!(query, "SELECT * FROM test_db.users");
    }

    #[test]
    fn test_mysql_read_query() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3307,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "mysql_db";
        let query = db_system.build_read_query(alias, "users").unwrap();
        assert_eq!(query, "SELECT * FROM mysql_db.users");
    }

    #[test]
    fn test_sqlite_validate_table_exists() {
        let db_system = DatabaseSystem::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let alias = "test_db";
        let query = db_system.validate_table_exists(alias, "users").unwrap();
        assert_eq!(
            query,
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_catalog = 'test_db' AND table_name = 'users'"
        );
    }

    #[test]
    fn test_mysql_validate_table_exists() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3307,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "mysql_db";
        let query = db_system.validate_table_exists(alias, "users").unwrap();
        assert_eq!(
            query,
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'datasource_test' AND table_name = 'users'"
        );
    }

    #[test]
    fn test_postgres_system_creation() {
        let connection = ConnectionConfig::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let db_system = DatabaseSystem::from_connection(&connection).unwrap();

        match db_system {
            DatabaseSystem::RemoteDatabase { db_type, config } => {
                assert_eq!(db_type, DatabaseType::Postgresql);
                assert_eq!(config.host, "localhost");
                assert_eq!(config.port, 5433);
                assert_eq!(config.database, "datasource_test");
                assert_eq!(config.username, "datasource");
                assert_eq!(config.password, "datasourcepass");
            }
            _ => panic!("Expected PostgreSQL system"),
        }
    }

    #[test]
    fn test_postgres_attach_detach_queries() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "postgres_db";
        let attach_query = db_system.build_attach_query(alias).unwrap();
        assert_eq!(
            attach_query,
            "ATTACH 'host=localhost port=5433 database=datasource_test user=datasource password=datasourcepass' AS postgres_db (TYPE postgres);"
        );

        let detach_query = db_system.build_detach_query(alias).unwrap();
        assert_eq!(detach_query, "DETACH postgres_db;");
    }

    #[test]
    fn test_postgres_read_query() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "postgres_db";
        let query = db_system.build_read_query(alias, "users").unwrap();
        assert_eq!(query, "SELECT * FROM postgres_db.users");
    }

    #[test]
    fn test_postgres_validate_table_exists() {
        let db_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let alias = "postgres_db";
        let query = db_system.validate_table_exists(alias, "users").unwrap();
        assert_eq!(
            query,
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = 'datasource_test' AND table_name = 'users'"
        );
    }

    #[test]
    fn test_generate_alias() {
        let sqlite_system = DatabaseSystem::Sqlite {
            path: "/tmp/test.db".to_string(),
        };

        let sqlite_alias = sqlite_system.generate_alias();
        assert!(sqlite_alias.starts_with("sqlite_db_"));
        assert_eq!(sqlite_alias.len(), "sqlite_db_".len() + 8);

        let mysql_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Mysql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 3307,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let mysql_alias = mysql_system.generate_alias();
        assert!(mysql_alias.starts_with("mysql_db_"));
        assert_eq!(mysql_alias.len(), "mysql_db_".len() + 8);

        let postgres_system = DatabaseSystem::RemoteDatabase {
            db_type: DatabaseType::Postgresql,
            config: RemoteDatabaseConfig {
                host: "localhost".to_string(),
                port: 5433,
                database: "datasource_test".to_string(),
                username: "datasource".to_string(),
                password: "datasourcepass".to_string(),
            },
        };

        let postgres_alias = postgres_system.generate_alias();
        assert!(postgres_alias.starts_with("postgres_db_"));
        assert_eq!(postgres_alias.len(), "postgres_db_".len() + 8);
    }

    #[test]
    fn test_unsupported_connection_type() {
        let connection = ConnectionConfig::LocalFile {
            base_path: "/tmp".to_string(),
        };

        let result = DatabaseSystem::from_connection(&connection);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported connection type for database system")
        );
    }

    #[tokio::test]
    async fn test_sqlite_attach_with_real_database() {
        use crate::pipeline::ducklake::{CatalogConfig, DuckLake, StorageConfig};
        use std::sync::Arc;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let sqlite_path = temp_dir.path().join("test.db");

        let connection = rusqlite::Connection::open(&sqlite_path).unwrap();
        connection
            .execute(
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )",
                [],
            )
            .unwrap();

        connection
            .execute(
                "INSERT INTO users (name, email, created_at) VALUES 
                 ('Alice', 'alice@example.com', '2024-01-01T10:00:00Z'),
                 ('Bob', 'bob@example.com', '2024-01-01T11:00:00Z')",
                [],
            )
            .unwrap();

        connection.close().unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("catalog.db")
                .to_string_lossy()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir
                .path()
                .join("storage")
                .to_string_lossy()
                .to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());

        ducklake.execute_batch("LOAD sqlite;").unwrap();

        let db_system = DatabaseSystem::Sqlite {
            path: sqlite_path.to_string_lossy().to_string(),
        };

        let alias = db_system.generate_alias();

        let attach_query = db_system.build_attach_query(&alias).unwrap();
        println!("Executing ATTACH query: {attach_query}");
        ducklake.execute_batch(&attach_query).unwrap();

        let list_attached = ducklake.query("PRAGMA database_list;").unwrap();
        println!("Attached databases: {list_attached:?}");

        let validation_query = db_system.validate_table_exists(&alias, "users").unwrap();
        println!("Validation query: {validation_query}");
        let validation_result = ducklake.query(&validation_query).unwrap();
        assert!(!validation_result.is_empty());
        assert_eq!(validation_result[0][0], "1");

        let read_query = db_system.build_read_query(&alias, "users").unwrap();
        let results = ducklake.query(&read_query).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][1], "Alice");
        assert_eq!(results[1][1], "Bob");

        let detach_query = db_system.build_detach_query(&alias).unwrap();
        ducklake.execute_batch(&detach_query).unwrap();
    }

    #[tokio::test]
    async fn test_sqlite_attach_with_test_fixture() {
        use crate::pipeline::ducklake::{CatalogConfig, DuckLake, StorageConfig};
        use std::sync::Arc;
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("catalog.db")
                .to_string_lossy()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir
                .path()
                .join("storage")
                .to_string_lossy()
                .to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());

        ducklake.execute_batch("LOAD sqlite;").unwrap();

        let test_db_path = std::path::Path::new("tests/fixtures/test_data/source.db");
        if !test_db_path.exists() {
            panic!("Test fixture SQLite database not found at: {test_db_path:?}");
        }

        let db_system = DatabaseSystem::Sqlite {
            path: test_db_path.to_string_lossy().to_string(),
        };

        let alias = db_system.generate_alias();

        let attach_query = db_system.build_attach_query(&alias).unwrap();
        println!("Attach query: {attach_query}");
        ducklake.execute_batch(&attach_query).unwrap();

        let validation_query = db_system.validate_table_exists(&alias, "users").unwrap();
        println!("Validation query: {validation_query}");
        let validation_result = ducklake.query(&validation_query).unwrap();
        println!("Validation result: {validation_result:?}");
        assert!(!validation_result.is_empty());
        assert_eq!(validation_result[0][0], "1");

        let read_query = db_system.build_read_query(&alias, "users").unwrap();
        println!("Read query: {read_query}");
        let results = ducklake.query(&read_query).unwrap();
        println!("Query results: {results:?}");
        assert_eq!(results.len(), 4);
        assert_eq!(results[0][1], "Alice Johnson");

        use uuid::Uuid;
        let temp_table_name = format!("temp_test_{}", Uuid::new_v4().simple());
        ducklake
            .create_table_from_query(&temp_table_name, &read_query)
            .unwrap();

        let verify_sql = format!("SELECT COUNT(*) FROM {temp_table_name}");
        let verify_results = ducklake.query(&verify_sql).unwrap();
        println!("Temp table verification: {verify_results:?}");
        assert_eq!(verify_results[0][0], "4");

        ducklake.drop_temp_table(&temp_table_name).unwrap();

        let detach_query = db_system.build_detach_query(&alias).unwrap();
        ducklake.execute_batch(&detach_query).unwrap();
    }
}
