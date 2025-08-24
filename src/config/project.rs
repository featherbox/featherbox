use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectConfig {
    pub name: Option<String>,
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub deployments: DeploymentsConfig,
    pub connections: HashMap<String, ConnectionConfig>,
    pub secret_key_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageConfig {
    LocalFile {
        path: String,
    },
    S3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        auth_method: S3AuthMethod,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub ty: DatabaseType,
    pub path: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentsConfig {
    pub timeout: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum S3AuthMethod {
    CredentialChain,
    #[default]
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseType {
    Sqlite,
    Mysql,
    Postgresql,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionConfig {
    LocalFile {
        base_path: String,
    },
    S3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        auth_method: S3AuthMethod,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        path_style_access: bool,
    },
    Sqlite {
        path: String,
    },
    RemoteDatabase {
        db_type: DatabaseType,
        config: RemoteDatabaseConfig,
    },
}

impl ConnectionConfig {
    pub fn get_full_endpoint_url(&self) -> Option<String> {
        match self {
            ConnectionConfig::S3 { endpoint_url, .. } => endpoint_url.clone(),
            _ => None,
        }
    }

    pub fn get_clean_endpoint_url(&self) -> Option<String> {
        match self {
            ConnectionConfig::S3 { endpoint_url, .. } => endpoint_url.as_ref().map(|url| {
                url.strip_prefix("https://")
                    .or_else(|| url.strip_prefix("http://"))
                    .unwrap_or(url)
                    .to_string()
            }),
            _ => None,
        }
    }

    pub fn uses_ssl(&self) -> bool {
        match self {
            ConnectionConfig::S3 { endpoint_url, .. } => endpoint_url
                .as_ref()
                .is_none_or(|url| !url.starts_with("http://")),
            _ => true,
        }
    }
}

fn expand_env_vars(value: &str) -> Result<String, String> {
    let mut result = value.to_string();

    while let Some(start) = result.find("${") {
        let end = result[start..]
            .find('}')
            .ok_or_else(|| format!("Unclosed environment variable reference in: {value}"))?;
        let end = start + end;

        let var_name = &result[start + 2..end];
        let env_value = env::var(var_name)
            .map_err(|_| format!("Environment variable not found: {var_name}"))?;

        result.replace_range(start..end + 1, &env_value);
    }

    Ok(result)
}

fn parse_remote_database_config(
    _db_type: DatabaseType,
    value: &yaml_rust2::Yaml,
    default_port: u16,
) -> RemoteDatabaseConfig {
    let host = value["host"]
        .as_str()
        .expect("Database host is required")
        .to_string();

    let port = value["port"]
        .as_i64()
        .map(|p| p as u16)
        .unwrap_or(default_port);

    let database = value["database"]
        .as_str()
        .expect("Database name is required")
        .to_string();

    let username_str = value["username"]
        .as_str()
        .expect("Database username is required");
    let username =
        expand_env_vars(username_str).unwrap_or_else(|e| panic!("Failed to expand username: {e}"));

    let password_str = value["password"]
        .as_str()
        .expect("Database password is required");
    let password =
        expand_env_vars(password_str).unwrap_or_else(|e| panic!("Failed to expand password: {e}"));

    RemoteDatabaseConfig {
        host,
        port,
        database,
        username,
        password,
    }
}

pub fn parse_project_config(yaml: &yaml_rust2::Yaml) -> ProjectConfig {
    let name = yaml["name"].as_str().map(|s| s.to_string());
    let storage = parse_storage(&yaml["storage"]);
    let database = parse_database(&yaml["database"]);
    let deployments = parse_deployments(&yaml["deployments"]);
    let connections = parse_connections(&yaml["connections"]);
    let secret_key_path = yaml["secret_key_path"].as_str().map(|s| s.to_string());

    ProjectConfig {
        name,
        storage,
        database,
        deployments,
        connections,
        secret_key_path,
    }
}

fn parse_s3_config(
    config: &yaml_rust2::Yaml,
) -> Result<
    (
        String,
        String,
        Option<String>,
        S3AuthMethod,
        String,
        String,
        Option<String>,
    ),
    String,
> {
    let bucket = config["bucket"]
        .as_str()
        .ok_or("S3 bucket is required")?
        .to_string();

    let region = config["region"].as_str().unwrap_or("us-east-1").to_string();

    let endpoint_url = config["endpoint_url"].as_str().map(|s| s.to_string());

    let auth_method = match config["auth_method"].as_str() {
        Some("credential_chain") => S3AuthMethod::CredentialChain,
        Some("explicit") => S3AuthMethod::Explicit,
        Some(unknown) => return Err(format!("Unknown S3 auth_method: {unknown}")),
        None => S3AuthMethod::default(),
    };

    let (access_key_id, secret_access_key) = match auth_method {
        S3AuthMethod::CredentialChain => (String::new(), String::new()),
        S3AuthMethod::Explicit => {
            let access_key_id_str = config["access_key_id"]
                .as_str()
                .ok_or("S3 access_key_id is required for explicit auth method")?;
            let access_key_id = expand_env_vars(access_key_id_str)
                .map_err(|e| format!("Failed to expand access_key_id: {e}"))?;

            let secret_access_key_str = config["secret_access_key"]
                .as_str()
                .ok_or("S3 secret_access_key is required for explicit auth method")?;
            let secret_access_key = expand_env_vars(secret_access_key_str)
                .map_err(|e| format!("Failed to expand secret_access_key: {e}"))?;

            (access_key_id, secret_access_key)
        }
    };

    let session_token = config["session_token"]
        .as_str()
        .map(expand_env_vars)
        .transpose()
        .map_err(|e| format!("Failed to expand session_token: {e}"))?;

    validate_s3_config(&bucket, &auth_method, &access_key_id, &secret_access_key)?;

    Ok((
        bucket,
        region,
        endpoint_url,
        auth_method,
        access_key_id,
        secret_access_key,
        session_token,
    ))
}

fn validate_s3_config(
    bucket: &str,
    auth_method: &S3AuthMethod,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<(), String> {
    if bucket.is_empty() {
        return Err("S3 bucket cannot be empty".to_string());
    }

    if bucket.contains('/') || bucket.contains('\\') {
        return Err("S3 bucket name cannot contain path separators".to_string());
    }

    match auth_method {
        S3AuthMethod::Explicit => {
            if access_key_id.is_empty() {
                return Err("S3 access_key_id cannot be empty for explicit auth method".to_string());
            }
            if secret_access_key.is_empty() {
                return Err(
                    "S3 secret_access_key cannot be empty for explicit auth method".to_string(),
                );
            }
        }
        S3AuthMethod::CredentialChain => {
            if !access_key_id.is_empty() {
                return Err(
                    "S3 access_key_id should not be specified for credential_chain auth method"
                        .to_string(),
                );
            }
            if !secret_access_key.is_empty() {
                return Err(
                    "S3 secret_access_key should not be specified for credential_chain auth method"
                        .to_string(),
                );
            }
        }
    }

    Ok(())
}

fn parse_storage(storage: &yaml_rust2::Yaml) -> StorageConfig {
    let ty = storage["type"]
        .as_str()
        .expect("Storage type is required")
        .to_string();

    match ty.as_str() {
        "local" => StorageConfig::LocalFile {
            path: storage["path"]
                .as_str()
                .expect("Storage path is required")
                .to_string(),
        },
        "s3" => {
            let (
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
            ) = parse_s3_config(storage).expect("Failed to parse S3 storage configuration");

            StorageConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
            }
        }
        _ => panic!("Unsupported storage type: {ty}"),
    }
}

fn parse_database(database: &yaml_rust2::Yaml) -> DatabaseConfig {
    let ty = database["type"]
        .as_str()
        .expect("Database type is required")
        .to_string();
    let ty = match ty.as_str() {
        "sqlite" => DatabaseType::Sqlite,
        "mysql" => DatabaseType::Mysql,
        "postgresql" => DatabaseType::Postgresql,
        _ => panic!("Unsupported database type: {ty}"),
    };

    match ty {
        DatabaseType::Sqlite => {
            let path = database["path"]
                .as_str()
                .expect("SQLite database path is required")
                .to_string();
            DatabaseConfig {
                ty,
                path: Some(path),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            }
        }
        DatabaseType::Mysql => {
            let default_port = 3306;
            let remote_config =
                parse_remote_database_config(DatabaseType::Mysql, database, default_port);

            DatabaseConfig {
                ty,
                path: None,
                host: Some(remote_config.host),
                port: Some(remote_config.port),
                database: Some(remote_config.database),
                username: Some(remote_config.username),
                password: Some(remote_config.password),
            }
        }
        DatabaseType::Postgresql => {
            let default_port = 5432;
            let remote_config =
                parse_remote_database_config(DatabaseType::Postgresql, database, default_port);

            DatabaseConfig {
                ty,
                path: None,
                host: Some(remote_config.host),
                port: Some(remote_config.port),
                database: Some(remote_config.database),
                username: Some(remote_config.username),
                password: Some(remote_config.password),
            }
        }
    }
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

    if connections.is_null() || connections.is_badvalue() {
        return conn_map;
    }

    let Some(connections_hash) = connections.as_hash() else {
        return conn_map;
    };

    for (key, value) in connections_hash {
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
            "sqlite" => {
                let path = value["path"]
                    .as_str()
                    .expect("SQLite path is required")
                    .to_string();
                ConnectionConfig::Sqlite { path }
            }
            "mysql" => {
                let config = parse_remote_database_config(DatabaseType::Mysql, value, 3306);
                ConnectionConfig::RemoteDatabase {
                    db_type: DatabaseType::Mysql,
                    config,
                }
            }
            "postgresql" => {
                let config = parse_remote_database_config(DatabaseType::Postgresql, value, 5432);
                ConnectionConfig::RemoteDatabase {
                    db_type: DatabaseType::Postgresql,
                    config,
                }
            }
            "s3" => {
                let (
                    bucket,
                    region,
                    endpoint_url,
                    auth_method,
                    access_key_id,
                    secret_access_key,
                    session_token,
                ) = parse_s3_config(value).expect("Failed to parse S3 connection configuration");

                let path_style_access = value["path_style_access"].as_bool().unwrap_or(false);

                ConnectionConfig::S3 {
                    bucket,
                    region,
                    endpoint_url,
                    auth_method,
                    access_key_id,
                    secret_access_key,
                    session_token,
                    path_style_access,
                }
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
            path: /tmp/foo/storage
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_storage(yaml);
        assert_eq!(
            config,
            StorageConfig::LocalFile {
                path: "/tmp/foo/storage".to_string()
            }
        );
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
    #[should_panic(expected = "Unsupported storage type: ftp")]
    fn test_parse_storage_unsupported_type() {
        let yaml_str = r#"
            type: ftp
            path: /some/path
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    fn test_parse_storage_s3_basic() {
        unsafe {
            env::set_var("TEST_S3_STORAGE_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_STORAGE_SECRET_KEY", "test_secret_key");
        }

        let yaml_str = r#"
            type: s3
            bucket: my-storage-bucket
            region: us-west-2
            access_key_id: ${TEST_S3_STORAGE_ACCESS_KEY}
            secret_access_key: ${TEST_S3_STORAGE_SECRET_KEY}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_storage(yaml);
        match config {
            StorageConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                assert_eq!(bucket, "my-storage-bucket");
                assert_eq!(region, "us-west-2");
                assert_eq!(endpoint_url, None);
                assert_eq!(auth_method, S3AuthMethod::Explicit);
                assert_eq!(access_key_id, "test_access_key");
                assert_eq!(secret_access_key, "test_secret_key");
                assert_eq!(session_token, None);
            }
            _ => panic!("Expected S3 storage config"),
        }

        unsafe {
            env::remove_var("TEST_S3_STORAGE_ACCESS_KEY");
            env::remove_var("TEST_S3_STORAGE_SECRET_KEY");
        }
    }

    #[test]
    fn test_parse_storage_s3_credential_chain() {
        let yaml_str = r#"
            type: s3
            bucket: my-storage-bucket
            region: eu-west-1
            endpoint_url: https://s3.eu-west-1.amazonaws.com
            auth_method: credential_chain
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_storage(yaml);
        match config {
            StorageConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                assert_eq!(bucket, "my-storage-bucket");
                assert_eq!(region, "eu-west-1");
                assert_eq!(
                    endpoint_url,
                    Some("https://s3.eu-west-1.amazonaws.com".to_string())
                );
                assert_eq!(auth_method, S3AuthMethod::CredentialChain);
                assert_eq!(access_key_id, "");
                assert_eq!(secret_access_key, "");
                assert_eq!(session_token, None);
            }
            _ => panic!("Expected S3 storage config"),
        }
    }

    #[test]
    fn test_parse_storage_s3_default_region() {
        unsafe {
            env::set_var("TEST_S3_STORAGE_DEFAULT_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_STORAGE_DEFAULT_SECRET_KEY", "test_secret_key");
        }

        let yaml_str = r#"
            type: s3
            bucket: my-storage-bucket
            access_key_id: ${TEST_S3_STORAGE_DEFAULT_ACCESS_KEY}
            secret_access_key: ${TEST_S3_STORAGE_DEFAULT_SECRET_KEY}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_storage(yaml);
        match config {
            StorageConfig::S3 { region, .. } => {
                assert_eq!(region, "us-east-1");
            }
            _ => panic!("Expected S3 storage config"),
        }

        unsafe {
            env::remove_var("TEST_S3_STORAGE_DEFAULT_ACCESS_KEY");
            env::remove_var("TEST_S3_STORAGE_DEFAULT_SECRET_KEY");
        }
    }

    #[test]
    #[should_panic(expected = "S3 bucket is required")]
    fn test_parse_storage_s3_missing_bucket() {
        let yaml_str = r#"
            type: s3
            region: us-east-1
            access_key_id: test_key
            secret_access_key: test_secret
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    #[should_panic(expected = "S3 access_key_id is required for explicit auth method")]
    fn test_parse_storage_s3_missing_access_key() {
        let yaml_str = r#"
            type: s3
            bucket: my-bucket
            secret_access_key: test_secret
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_storage(yaml);
    }

    #[test]
    #[should_panic(expected = "S3 secret_access_key is required for explicit auth method")]
    fn test_parse_storage_s3_missing_secret_key() {
        let yaml_str = r#"
            type: s3
            bucket: my-bucket
            access_key_id: test_key
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
        assert_eq!(
            config.path,
            Some("/home/user/featherbox/database.db".to_string())
        );
        assert_eq!(config.host, None);
        assert_eq!(config.port, None);
        assert_eq!(config.database, None);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
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
    #[should_panic(expected = "SQLite database path is required")]
    fn test_parse_database_missing_path() {
        let yaml_str = r#"
            type: sqlite
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_database(yaml);
    }

    #[test]
    fn test_parse_database_mysql() {
        unsafe {
            env::set_var("TEST_MYSQL_USER", "test_user");
            env::set_var("TEST_MYSQL_PASSWORD", "test_password");
        }

        let yaml_str = r#"
            type: mysql
            host: localhost
            port: 3306
            database: featherbox
            username: ${TEST_MYSQL_USER}
            password: ${TEST_MYSQL_PASSWORD}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_database(yaml);
        assert_eq!(config.ty, DatabaseType::Mysql);
        assert_eq!(config.path, None);
        assert_eq!(config.host, Some("localhost".to_string()));
        assert_eq!(config.port, Some(3306));
        assert_eq!(config.database, Some("featherbox".to_string()));
        assert_eq!(config.username, Some("test_user".to_string()));
        assert_eq!(config.password, Some("test_password".to_string()));

        unsafe {
            env::remove_var("TEST_MYSQL_USER");
            env::remove_var("TEST_MYSQL_PASSWORD");
        }
    }

    #[test]
    fn test_parse_database_postgresql() {
        unsafe {
            env::set_var("TEST_POSTGRES_USER", "postgres_user");
            env::set_var("TEST_POSTGRES_PASSWORD", "postgres_pass");
        }

        let yaml_str = r#"
            type: postgresql
            host: db.example.com
            port: 5432
            database: mydb
            username: ${TEST_POSTGRES_USER}
            password: ${TEST_POSTGRES_PASSWORD}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_database(yaml);
        assert_eq!(config.ty, DatabaseType::Postgresql);
        assert_eq!(config.path, None);
        assert_eq!(config.host, Some("db.example.com".to_string()));
        assert_eq!(config.port, Some(5432));
        assert_eq!(config.database, Some("mydb".to_string()));
        assert_eq!(config.username, Some("postgres_user".to_string()));
        assert_eq!(config.password, Some("postgres_pass".to_string()));

        unsafe {
            env::remove_var("TEST_POSTGRES_USER");
            env::remove_var("TEST_POSTGRES_PASSWORD");
        }
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
            _ => panic!("Expected LocalFile connection config"),
        }

        match connections.get("data_files").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/data/files");
            }
            _ => panic!("Expected LocalFile connection config"),
        }
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
    fn test_expand_env_vars() {
        unsafe {
            env::set_var("TEST_VAR", "test_value");
            env::set_var("ANOTHER_VAR", "another_value");
        }

        let result = expand_env_vars("${TEST_VAR}").unwrap();
        assert_eq!(result, "test_value");

        let result = expand_env_vars("prefix_${TEST_VAR}_suffix").unwrap();
        assert_eq!(result, "prefix_test_value_suffix");

        let result = expand_env_vars("${TEST_VAR}_${ANOTHER_VAR}").unwrap();
        assert_eq!(result, "test_value_another_value");

        let result = expand_env_vars("no_vars").unwrap();
        assert_eq!(result, "no_vars");

        unsafe {
            env::remove_var("TEST_VAR");
            env::remove_var("ANOTHER_VAR");
        }
    }

    #[test]
    fn test_expand_env_vars_missing_variable() {
        let result = expand_env_vars("${NONEXISTENT_VAR}");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Environment variable not found: NONEXISTENT_VAR")
        );
    }

    #[test]
    fn test_expand_env_vars_unclosed_reference() {
        let result = expand_env_vars("${UNCLOSED_VAR");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Unclosed environment variable reference")
        );
    }

    #[test]
    fn test_parse_connections_s3_basic() {
        unsafe {
            env::set_var("TEST_S3_BASIC_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_BASIC_SECRET_KEY", "test_secret_key");
        }

        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              region: us-west-2
              access_key_id: ${TEST_S3_BASIC_ACCESS_KEY}
              secret_access_key: ${TEST_S3_BASIC_SECRET_KEY}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);

        match connections.get("my_s3_data").unwrap() {
            ConnectionConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
                path_style_access,
            } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "us-west-2");
                assert_eq!(endpoint_url, &None);
                assert_eq!(auth_method, &S3AuthMethod::Explicit);
                assert_eq!(access_key_id, "test_access_key");
                assert_eq!(secret_access_key, "test_secret_key");
                assert_eq!(session_token, &None);
                assert!(!(*path_style_access));
            }
            _ => panic!("Expected S3 connection config"),
        }

        unsafe {
            env::remove_var("TEST_S3_BASIC_ACCESS_KEY");
            env::remove_var("TEST_S3_BASIC_SECRET_KEY");
        }
    }

    #[test]
    fn test_parse_connections_s3_with_all_fields() {
        unsafe {
            env::set_var("TEST_S3_ALL_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_ALL_SECRET_KEY", "test_secret_key");
            env::set_var("TEST_S3_ALL_SESSION_TOKEN", "test_session_token");
        }

        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              region: eu-west-1
              endpoint_url: https://s3.eu-west-1.amazonaws.com
              access_key_id: ${TEST_S3_ALL_ACCESS_KEY}
              secret_access_key: ${TEST_S3_ALL_SECRET_KEY}
              session_token: ${TEST_S3_ALL_SESSION_TOKEN}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        match connections.get("my_s3_data").unwrap() {
            ConnectionConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
                path_style_access,
            } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "eu-west-1");
                assert_eq!(
                    endpoint_url,
                    &Some("https://s3.eu-west-1.amazonaws.com".to_string())
                );
                assert_eq!(auth_method, &S3AuthMethod::Explicit);
                assert_eq!(access_key_id, "test_access_key");
                assert_eq!(secret_access_key, "test_secret_key");
                assert_eq!(session_token, &Some("test_session_token".to_string()));
                assert!(!(*path_style_access));
            }
            _ => panic!("Expected S3 connection config"),
        }

        unsafe {
            env::remove_var("TEST_S3_ALL_ACCESS_KEY");
            env::remove_var("TEST_S3_ALL_SECRET_KEY");
            env::remove_var("TEST_S3_ALL_SESSION_TOKEN");
        }
    }

    #[test]
    fn test_parse_connections_s3_default_region() {
        unsafe {
            env::set_var("TEST_S3_DEFAULT_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_DEFAULT_SECRET_KEY", "test_secret_key");
        }

        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              access_key_id: ${TEST_S3_DEFAULT_ACCESS_KEY}
              secret_access_key: ${TEST_S3_DEFAULT_SECRET_KEY}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        match connections.get("my_s3_data").unwrap() {
            ConnectionConfig::S3 {
                region,
                auth_method,
                ..
            } => {
                assert_eq!(region, "us-east-1");
                assert_eq!(auth_method, &S3AuthMethod::Explicit);
            }
            _ => panic!("Expected S3 connection config"),
        }

        unsafe {
            env::remove_var("TEST_S3_DEFAULT_ACCESS_KEY");
            env::remove_var("TEST_S3_DEFAULT_SECRET_KEY");
        }
    }

    #[test]
    #[should_panic(expected = "S3 bucket is required")]
    fn test_parse_connections_s3_missing_bucket() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              region: us-east-1
              access_key_id: test_key
              secret_access_key: test_secret
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "S3 access_key_id is required")]
    fn test_parse_connections_s3_missing_access_key() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-bucket
              secret_access_key: test_secret
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "S3 secret_access_key is required")]
    fn test_parse_connections_s3_missing_secret_key() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-bucket
              access_key_id: test_key
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    #[should_panic(expected = "Environment variable not found: NONEXISTENT_KEY")]
    fn test_parse_connections_s3_missing_env_var() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-bucket
              access_key_id: ${NONEXISTENT_KEY}
              secret_access_key: test_secret
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    fn test_parse_connections_sqlite() {
        let yaml_str = r#"
            my_database:
              type: sqlite
              path: /data/my_database.db
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);

        match connections.get("my_database").unwrap() {
            ConnectionConfig::Sqlite { path } => {
                assert_eq!(path, "/data/my_database.db");
            }
            _ => panic!("Expected SQLite connection config"),
        }
    }

    #[test]
    fn test_parse_connections_mysql() {
        unsafe {
            env::set_var("TEST_MYSQL_USER", "mysql_user");
            env::set_var("TEST_MYSQL_PASSWORD", "mysql_pass");
        }

        let yaml_str = r#"
            my_mysql_db:
              type: mysql
              host: localhost
              port: 3307
              database: datasource_test
              username: ${TEST_MYSQL_USER}
              password: ${TEST_MYSQL_PASSWORD}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);

        match connections.get("my_mysql_db").unwrap() {
            ConnectionConfig::RemoteDatabase { db_type, config } => {
                assert_eq!(db_type, &DatabaseType::Mysql);
                assert_eq!(config.host, "localhost");
                assert_eq!(config.port, 3307);
                assert_eq!(config.database, "datasource_test");
                assert_eq!(config.username, "mysql_user");
                assert_eq!(config.password, "mysql_pass");
            }
            _ => panic!("Expected MySQL connection config"),
        }

        unsafe {
            env::remove_var("TEST_MYSQL_USER");
            env::remove_var("TEST_MYSQL_PASSWORD");
        }
    }

    #[test]
    fn test_parse_connections_mysql_default_port() {
        unsafe {
            env::set_var("TEST_MYSQL_DEFAULT_USER", "mysql_user");
            env::set_var("TEST_MYSQL_DEFAULT_PASSWORD", "mysql_pass");
        }

        let yaml_str = r#"
            my_mysql_db:
              type: mysql
              host: localhost
              database: datasource_test
              username: ${TEST_MYSQL_DEFAULT_USER}
              password: ${TEST_MYSQL_DEFAULT_PASSWORD}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        match connections.get("my_mysql_db").unwrap() {
            ConnectionConfig::RemoteDatabase { db_type, config } => {
                assert_eq!(*db_type, DatabaseType::Mysql);
                assert_eq!(config.port, 3306);
            }
            _ => panic!("Expected MySQL connection config"),
        }

        unsafe {
            env::remove_var("TEST_MYSQL_DEFAULT_USER");
            env::remove_var("TEST_MYSQL_DEFAULT_PASSWORD");
        }
    }

    #[test]
    fn test_parse_connections_postgresql() {
        let yaml = &YamlLoader::load_from_str(
            r"
            postgres_db:
              type: postgresql
              host: localhost
              port: 5433
              database: test_db
              username: testuser
              password: testpass
            ",
        )
        .unwrap()[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);
        match &connections["postgres_db"] {
            ConnectionConfig::RemoteDatabase { db_type, config } => {
                assert_eq!(*db_type, DatabaseType::Postgresql);
                assert_eq!(config.host, "localhost");
                assert_eq!(config.port, 5433);
                assert_eq!(config.database, "test_db");
                assert_eq!(config.username, "testuser");
                assert_eq!(config.password, "testpass");
            }
            _ => panic!("Expected PostgreSQL connection"),
        }
    }

    #[test]
    fn test_parse_connections_postgresql_default_port() {
        let yaml = &YamlLoader::load_from_str(
            r"
            postgres_db:
              type: postgresql
              host: localhost
              database: test_db
              username: testuser
              password: testpass
            ",
        )
        .unwrap()[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);
        match &connections["postgres_db"] {
            ConnectionConfig::RemoteDatabase { db_type, config } => {
                assert_eq!(*db_type, DatabaseType::Postgresql);
                assert_eq!(config.port, 5432);
            }
            _ => panic!("Expected PostgreSQL connection"),
        }
    }

    #[test]
    fn test_parse_connections_mixed_types() {
        unsafe {
            env::set_var("TEST_S3_MIXED_ACCESS_KEY", "test_access_key");
            env::set_var("TEST_S3_MIXED_SECRET_KEY", "test_secret_key");
        }

        let yaml_str = r#"
            local_files:
              type: localfile
              base_path: /data/local
            my_database:
              type: sqlite
              path: /data/my_database.db
            s3_data:
              type: s3
              bucket: my-bucket
              access_key_id: ${TEST_S3_MIXED_ACCESS_KEY}
              secret_access_key: ${TEST_S3_MIXED_SECRET_KEY}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 3);

        match connections.get("local_files").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/data/local");
            }
            _ => panic!("Expected LocalFile connection config"),
        }

        match connections.get("my_database").unwrap() {
            ConnectionConfig::Sqlite { path } => {
                assert_eq!(path, "/data/my_database.db");
            }
            _ => panic!("Expected SQLite connection config"),
        }

        match connections.get("s3_data").unwrap() {
            ConnectionConfig::S3 {
                bucket,
                auth_method,
                ..
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(auth_method, &S3AuthMethod::Explicit);
            }
            _ => panic!("Expected S3 connection config"),
        }

        unsafe {
            env::remove_var("TEST_S3_MIXED_ACCESS_KEY");
            env::remove_var("TEST_S3_MIXED_SECRET_KEY");
        }
    }

    #[test]
    #[should_panic(expected = "Unsupported connection type")]
    fn test_parse_connections_unsupported_type() {
        let yaml_str = r#"
            app_logs:
              type: ftp
              host: example.com
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    fn test_parse_connections_s3_credential_chain() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              region: us-west-2
              auth_method: credential_chain
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        assert_eq!(connections.len(), 1);

        match connections.get("my_s3_data").unwrap() {
            ConnectionConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
                path_style_access,
            } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "us-west-2");
                assert_eq!(endpoint_url, &None);
                assert_eq!(auth_method, &S3AuthMethod::CredentialChain);
                assert_eq!(access_key_id, "");
                assert_eq!(secret_access_key, "");
                assert_eq!(session_token, &None);
                assert!(!(*path_style_access));
            }
            _ => panic!("Expected S3 connection config"),
        }
    }

    #[test]
    fn test_parse_connections_s3_credential_chain_with_endpoint() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              region: eu-central-1
              endpoint_url: https://s3.eu-central-1.amazonaws.com
              auth_method: credential_chain
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let connections = parse_connections(yaml);

        match connections.get("my_s3_data").unwrap() {
            ConnectionConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
                path_style_access,
            } => {
                assert_eq!(bucket, "my-data-bucket");
                assert_eq!(region, "eu-central-1");
                assert_eq!(
                    endpoint_url,
                    &Some("https://s3.eu-central-1.amazonaws.com".to_string())
                );
                assert_eq!(auth_method, &S3AuthMethod::CredentialChain);
                assert_eq!(access_key_id, "");
                assert_eq!(secret_access_key, "");
                assert_eq!(session_token, &None);
                assert!(!(*path_style_access));
            }
            _ => panic!("Expected S3 connection config"),
        }
    }

    #[test]
    #[should_panic(expected = "Unknown S3 auth_method: invalid")]
    fn test_parse_connections_s3_invalid_auth_method() {
        let yaml_str = r#"
            my_s3_data:
              type: s3
              bucket: my-data-bucket
              auth_method: invalid
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_connections(yaml);
    }

    #[test]
    fn test_parse_project_config() {
        let yaml_str = r#"
            name: test_project
            storage:
              type: local
              path: /tmp/foo/storage
            database:
              type: sqlite
              path: /tmp/foo/database.db
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

        assert_eq!(config.name, Some("test_project".to_string()));
        assert_eq!(
            config.storage,
            StorageConfig::LocalFile {
                path: "/tmp/foo/storage".to_string()
            }
        );

        assert_eq!(config.database.ty, DatabaseType::Sqlite);
        assert_eq!(
            config.database.path,
            Some("/tmp/foo/database.db".to_string())
        );

        assert_eq!(config.deployments.timeout, 600);

        assert_eq!(config.connections.len(), 1);
        match config.connections.get("app_logs").unwrap() {
            ConnectionConfig::LocalFile { base_path } => {
                assert_eq!(base_path, "/var/logs");
            }
            _ => panic!("Expected LocalFile connection config"),
        }

        assert_eq!(config.secret_key_path, None);
    }

    #[test]
    fn test_parse_project_config_with_secret_key_path() {
        let yaml_str = r#"
            name: project_with_secret
            storage:
              type: local
              path: /tmp/foo/storage
            database:
              type: sqlite
              path: /tmp/foo/database.db
            deployments:
              timeout: 600
            connections: {}
            secret_key_path: /custom/path/secret.key
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_project_config(yaml);

        assert_eq!(config.name, Some("project_with_secret".to_string()));
        assert_eq!(
            config.secret_key_path,
            Some("/custom/path/secret.key".to_string())
        );
    }

    #[test]
    fn test_connection_config_endpoint_url_methods() {
        let s3_connection_with_https = ConnectionConfig::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("https://example.com".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            path_style_access: false,
        };

        assert_eq!(
            s3_connection_with_https.get_full_endpoint_url(),
            Some("https://example.com".to_string())
        );
        assert_eq!(
            s3_connection_with_https.get_clean_endpoint_url(),
            Some("example.com".to_string())
        );
        assert!(s3_connection_with_https.uses_ssl());

        let s3_connection_with_http = ConnectionConfig::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: Some("http://localhost:9000".to_string()),
            auth_method: S3AuthMethod::Explicit,
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            path_style_access: false,
        };

        assert_eq!(
            s3_connection_with_http.get_full_endpoint_url(),
            Some("http://localhost:9000".to_string())
        );
        assert_eq!(
            s3_connection_with_http.get_clean_endpoint_url(),
            Some("localhost:9000".to_string())
        );
        assert!(!s3_connection_with_http.uses_ssl());

        let local_connection = ConnectionConfig::LocalFile {
            base_path: "/tmp".to_string(),
        };

        assert_eq!(local_connection.get_full_endpoint_url(), None);
        assert_eq!(local_connection.get_clean_endpoint_url(), None);
        assert!(local_connection.uses_ssl());
    }

    #[test]
    fn test_parse_project_config_without_name() {
        let yaml_str = r#"
            storage:
              type: local
              path: /tmp/foo/storage
            database:
              type: sqlite
              path: /tmp/foo/database.db
            deployments:
              timeout: 600
            connections: {}
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_project_config(yaml);

        assert_eq!(config.name, None);
        assert_eq!(
            config.storage,
            StorageConfig::LocalFile {
                path: "/tmp/foo/storage".to_string()
            }
        );
    }
}
