use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectConfig {
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub deployments: DeploymentsConfig,
    pub connections: HashMap<String, ConnectionConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    pub ty: StorageType,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageType {
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub ty: DatabaseType,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseType {
    Sqlite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentsConfig {
    pub timeout: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionConfig {
    LocalFile { base_path: String },
}

pub fn parse_project_config(yaml: &yaml_rust2::Yaml) -> ProjectConfig {
    let storage = parse_storage(&yaml["storage"]);
    let database = parse_database(&yaml["database"]);
    let deployments = parse_deployments(&yaml["deployments"]);
    let connections = parse_connections(&yaml["connections"]);

    ProjectConfig {
        storage,
        database,
        deployments,
        connections,
    }
}

fn parse_storage(storage: &yaml_rust2::Yaml) -> StorageConfig {
    let ty = storage["type"]
        .as_str()
        .expect("Storage type is required")
        .to_string();
    let ty = match ty.as_str() {
        "local" => StorageType::Local,
        _ => panic!("Unsupported storage type: {ty}"),
    };

    let path = storage["path"]
        .as_str()
        .expect("Storage path is required")
        .to_string();
    StorageConfig { ty, path }
}

fn parse_database(database: &yaml_rust2::Yaml) -> DatabaseConfig {
    let ty = database["type"]
        .as_str()
        .expect("Database type is required")
        .to_string();
    let ty = match ty.as_str() {
        "sqlite" => DatabaseType::Sqlite,
        _ => panic!("Unsupported database type: {ty}"),
    };

    let path = database["path"]
        .as_str()
        .expect("Database path is required")
        .to_string();
    DatabaseConfig { ty, path }
}

fn parse_deployments(deployments: &yaml_rust2::Yaml) -> DeploymentsConfig {
    let default_timeout = 600;

    if deployments.is_null() || deployments["timeout"].is_badvalue() {
        return DeploymentsConfig {
            timeout: default_timeout,
        };
    }

    let timeout = match deployments["timeout"].as_i64() {
        Some(timeout) => timeout as u64,
        None => panic!("Timeout must be an integer"),
    };
    if timeout < 1 {
        panic!("Timeout must be greater than 0");
    }
    DeploymentsConfig { timeout }
}

fn parse_connections(connections: &yaml_rust2::Yaml) -> HashMap<String, ConnectionConfig> {
    let mut conn_map = HashMap::new();
    for (key, value) in connections.as_hash().expect("Connections must be a hash") {
        let key = key
            .as_str()
            .expect("Connection name must be a string")
            .to_string();
        let conn = match value["type"].as_str().expect("Connection type is required") {
            "localfile" => {
                let base_path = value["base_path"]
                    .as_str()
                    .expect("Base path is required")
                    .to_string();
                ConnectionConfig::LocalFile { base_path }
            }
            _ => panic!("Unsupported connection type"),
        };
        conn_map.insert(key, conn);
    }
    conn_map
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml_rust2::YamlLoader;

    #[test]
    fn test_parse_storage() {
        let yaml_str = r#"
            type: local
            path: /home/user/featherbox/storage
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_storage(yaml);
        assert_eq!(config.ty, StorageType::Local);
        assert_eq!(config.path, "/home/user/featherbox/storage");
    }

    #[test]
    #[should_panic(expected = "Storage type is required")]
    fn test_parse_storage_missing_type() {
        let yaml_str = r#"
            path: /home/user/featherbox/storage
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    #[should_panic(expected = "Storage path is required")]
    fn test_parse_storage_missing_path() {
        let yaml_str = r#"
            type: local
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    #[should_panic(expected = "Unsupported storage type: s3")]
    fn test_parse_storage_unsupported_type() {
        let yaml_str = r#"
            type: s3
            path: /some/path
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    fn test_parse_database() {
        let yaml_str = r#"
            type: sqlite
            path: /home/user/featherbox/database.db
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_database(yaml);
        assert_eq!(config.ty, DatabaseType::Sqlite);
        assert_eq!(config.path, "/home/user/featherbox/database.db");
    }

    #[test]
    #[should_panic(expected = "Database type is required")]
    fn test_parse_database_missing_type() {
        let yaml_str = r#"
            path: /home/user/featherbox/database.db
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_database(yaml);
    }

    #[test]
    #[should_panic(expected = "Database path is required")]
    fn test_parse_database_missing_path() {
        let yaml_str = r#"
            type: sqlite
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_database(yaml);
    }

    #[test]
    #[should_panic(expected = "Unsupported database type: mysql")]
    fn test_parse_database_unsupported_type() {
        let yaml_str = r#"
            type: mysql
            path: /some/path
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_database(yaml);
    }

    #[test]
    fn test_parse_deployments_with_timeout() {
        let yaml_str = r#"
            timeout: 300
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_deployments(yaml);
        assert_eq!(config.timeout, 300);
    }

    #[test]
    fn test_parse_deployments_default_timeout() {
        let yaml_str = r#"
            other_field: value
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_deployments(yaml);
        assert_eq!(config.timeout, 600);
    }

    #[test]
    #[should_panic(expected = "Timeout must be greater than 0")]
    fn test_parse_deployments_zero_timeout() {
        let yaml_str = r#"
            timeout: 0
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_deployments(yaml);
    }

    #[test]
    #[should_panic(expected = "Timeout must be an integer")]
    fn test_parse_deployments_invalid_timeout_type() {
        let yaml_str = r#"
            timeout: "300"
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_deployments(yaml);
    }

    #[test]
    fn test_parse_connections() {
        let yaml_str = r#"
            app_logs:
              type: localfile
              base_path: /var/logs
            data_files:
              type: localfile
              base_path: /data/files
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 2);

        match connections.get("app_logs").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/var/logs");
            }
        }

        match connections.get("data_files").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/data/files");
            }
        }
    }

    #[test]
    #[should_panic(expected = "Connections must be a hash")]
    fn test_parse_connections_not_hash() {
        let yaml_str = r#"
            - connection1
            - connection2
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "Connection type is required")]
    fn test_parse_connections_missing_type() {
        let yaml_str = r#"
            app_logs:
              base_path: /var/logs
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "Base path is required")]
    fn test_parse_connections_missing_base_path() {
        let yaml_str = r#"
            app_logs:
              type: localfile
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "Unsupported connection type")]
    fn test_parse_connections_unsupported_type() {
        let yaml_str = r#"
            app_logs:
              type: s3
              bucket: my-bucket
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    fn test_parse_project_config() {
        let yaml_str = r#"
            storage:
              type: local
              path: /home/user/featherbox/storage
            database:
              type: sqlite
              path: /home/user/featherbox/database.db
            deployments:
              timeout: 600
            connections:
              app_logs:
                type: localfile
                base_path: /var/logs
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_project_config(yaml);

        assert_eq!(config.storage.ty, StorageType::Local);
        assert_eq!(config.storage.path, "/home/user/featherbox/storage");

        assert_eq!(config.database.ty, DatabaseType::Sqlite);
        assert_eq!(config.database.path, "/home/user/featherbox/database.db");

        assert_eq!(config.deployments.timeout, 600);

        assert_eq!(config.connections.len(), 1);
        match config.connections.get("app_logs").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/var/logs");
            }
        }
    }
}
