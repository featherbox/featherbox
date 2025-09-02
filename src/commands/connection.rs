use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use inquire::{Select, Text};
use sea_orm::{Database, DbErr};
use std::path::Path;

use crate::commands::workspace::find_project_root;
use crate::config::project::{ConnectionConfig, S3AuthMethod, S3Config};

#[derive(Debug, Clone)]
enum ConnectionType {
    LocalFile,
    Sqlite,
    S3,
    MySql,
    PostgreSql,
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ConnectionType::LocalFile => "Local File",
            ConnectionType::Sqlite => "SQLite",
            ConnectionType::S3 => "S3",
            ConnectionType::MySql => "MySQL",
            ConnectionType::PostgreSql => "PostgreSQL",
        };
        write!(f, "{name}")
    }
}

impl ConnectionType {
    fn all() -> Vec<ConnectionType> {
        vec![
            ConnectionType::LocalFile,
            ConnectionType::Sqlite,
            ConnectionType::S3,
            ConnectionType::MySql,
            ConnectionType::PostgreSql,
        ]
    }
}

pub async fn execute_connection(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    let connection_name = Text::new("Connection name:").prompt()?;

    if connection_name.trim().is_empty() {
        println!("Connection creation cancelled.");
        return Ok(());
    }

    let connection_types = ConnectionType::all();
    let connection_type = Select::new("Select connection type:", connection_types).prompt()?;

    let config = match connection_type {
        ConnectionType::LocalFile => {
            let path = Text::new("Path:").prompt()?;
            ConnectionConfig::LocalFile { base_path: path }
        }
        ConnectionType::Sqlite => {
            let path = Text::new("Database Path:").prompt()?;
            ConnectionConfig::Sqlite { path }
        }
        ConnectionType::S3 => {
            let bucket = Text::new("Bucket:").prompt()?.trim().to_string();

            let auth_methods = vec!["AWS Profile (credential chain)", "Explicit Access Keys"];
            let auth_choice = Select::new("Authentication method:", auth_methods).prompt()?;

            let (auth_method, access_key_id, secret_access_key, region) = match auth_choice {
                "AWS Profile (credential chain)" => {
                    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
                    let auto_region = config
                        .region()
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "us-east-1".to_string());
                    let region = Text::new("Region:").with_default(&auto_region).prompt()?;
                    (
                        S3AuthMethod::CredentialChain,
                        String::new(),
                        String::new(),
                        region,
                    )
                }
                "Explicit Access Keys" => {
                    let region = Text::new("Region:").with_default("us-east-1").prompt()?;
                    let access_key_id = Text::new("Access Key ID:").prompt()?;
                    let secret_access_key = Text::new("Secret Access Key:").prompt()?;
                    (
                        S3AuthMethod::Explicit,
                        access_key_id,
                        secret_access_key,
                        region,
                    )
                }
                _ => unreachable!(),
            };

            ConnectionConfig::S3(S3Config {
                bucket,
                region,
                endpoint_url: None,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token: None,
                path_style_access: false,
            })
        }
        ConnectionType::MySql => {
            let host = Text::new("Host:").prompt()?;
            let port = Text::new("Port:").with_default("3306").prompt()?;
            let database = Text::new("Database:").prompt()?;
            let username = Text::new("Username:").prompt()?;
            let password = Text::new("Password:").prompt()?;
            ConnectionConfig::MySql {
                host,
                port: port.parse()?,
                database,
                username,
                password,
            }
        }
        ConnectionType::PostgreSql => {
            let host = Text::new("Host:").prompt()?;
            let port = Text::new("Port:").with_default("5432").prompt()?;
            let database = Text::new("Database:").prompt()?;
            let username = Text::new("Username:").prompt()?;
            let password = Text::new("Password:").prompt()?;
            ConnectionConfig::PostgreSql {
                host,
                port: port.parse()?,
                database,
                username,
                password,
            }
        }
    };

    println!("Testing connection...");
    match test_connection(&config).await {
        Ok(msg) => {
            println!("✓ {msg}");
        }
        Err(e) => {
            println!("✗ Connection test failed: {e}");
            return Ok(());
        }
    }

    save_connection(&project_root, &connection_name, &config)?;
    println!("✓ Connection '{connection_name}' saved to project.yml");

    Ok(())
}

async fn test_connection(config: &ConnectionConfig) -> Result<String> {
    match config {
        ConnectionConfig::LocalFile { base_path } => {
            if Path::new(base_path).exists() {
                Ok("Local file path exists".to_string())
            } else {
                Err(anyhow::anyhow!("Path does not exist"))
            }
        }
        ConnectionConfig::Sqlite { path } => {
            if Path::new(path)
                .parent()
                .map(|p| p.exists())
                .unwrap_or(false)
            {
                Ok("SQLite database path is valid".to_string())
            } else {
                Err(anyhow::anyhow!("Parent directory does not exist"))
            }
        }
        ConnectionConfig::S3(s3_config) => {
            test_s3_connection(
                &s3_config.bucket,
                &s3_config.region,
                &s3_config.auth_method,
                &s3_config.access_key_id,
                &s3_config.secret_access_key,
                &s3_config.endpoint_url,
            )
            .await
        }
        ConnectionConfig::MySql {
            host,
            port,
            database,
            username,
            password,
        } => test_mysql_connection(host, *port, database, username, password).await,
        ConnectionConfig::PostgreSql {
            host,
            port,
            database,
            username,
            password,
        } => test_postgresql_connection(host, *port, database, username, password).await,
    }
}

async fn test_mysql_connection(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let database_url = format!("mysql://{username}:{password}@{host}:{port}/{database}");

    match Database::connect(&database_url).await {
        Ok(_) => Ok("MySQL connection successful".to_string()),
        Err(DbErr::Conn(err)) => Err(anyhow::anyhow!("Connection failed: {}", err)),
        Err(err) => Err(anyhow::anyhow!("Database error: {}", err)),
    }
}

async fn test_postgresql_connection(
    host: &str,
    port: u16,
    database: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let database_url = format!("postgres://{username}:{password}@{host}:{port}/{database}");

    match Database::connect(&database_url).await {
        Ok(_) => Ok("PostgreSQL connection successful".to_string()),
        Err(DbErr::Conn(err)) => Err(anyhow::anyhow!("Connection failed: {}", err)),
        Err(err) => Err(anyhow::anyhow!("Database error: {}", err)),
    }
}

fn save_connection(project_root: &Path, name: &str, config: &ConnectionConfig) -> Result<()> {
    let project_yml_path = project_root.join("project.yml");
    let content = std::fs::read_to_string(&project_yml_path)?;

    let mut project_config: crate::config::ProjectConfig = serde_yml::from_str(&content)?;

    project_config
        .connections
        .insert(name.to_string(), config.clone());

    let updated_content = serde_yml::to_string(&project_config)?;
    std::fs::write(&project_yml_path, updated_content)?;

    Ok(())
}

async fn test_s3_connection(
    bucket: &str,
    region: &str,
    auth_method: &S3AuthMethod,
    access_key_id: &str,
    secret_access_key: &str,
    endpoint_url: &Option<String>,
) -> Result<String> {
    let config = match auth_method {
        S3AuthMethod::CredentialChain => {
            aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(region.to_string()))
                .load()
                .await
        }
        S3AuthMethod::Explicit => {
            let credentials =
                Credentials::new(access_key_id, secret_access_key, None, None, "explicit");

            let mut config_builder = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(region.to_string()))
                .credentials_provider(credentials);

            if let Some(endpoint) = endpoint_url {
                config_builder = config_builder.endpoint_url(endpoint);
            }

            config_builder.load().await
        }
    };

    let s3_client = S3Client::new(&config);

    let result = s3_client.head_bucket().bucket(bucket).send().await;

    match result {
        Ok(_) => Ok(format!("S3 bucket '{bucket}' is accessible")),
        Err(sdk_error) => match sdk_error {
            aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
                let err = service_err.err();
                let meta = err.meta();
                let error_code = meta.code();
                let error_message = meta.message();
                let status_code = service_err.raw().status();

                match (error_code, status_code.as_u16()) {
                    (Some("NoSuchBucket"), _) => {
                        Err(anyhow::anyhow!("Bucket '{}' does not exist", bucket))
                    }
                    (Some("AccessDenied"), _) | (Some("Forbidden"), _) => Err(anyhow::anyhow!(
                        "Access denied to bucket '{}'. Check your permissions.",
                        bucket
                    )),
                    (Some("InvalidAccessKeyId"), _) => {
                        Err(anyhow::anyhow!("Invalid AWS Access Key ID"))
                    }
                    (Some("SignatureDoesNotMatch"), _) => {
                        Err(anyhow::anyhow!("Invalid AWS Secret Access Key"))
                    }
                    (Some("InvalidBucketName"), _) => {
                        Err(anyhow::anyhow!("Invalid bucket name: '{}'", bucket))
                    }
                    (_, 400) => Err(anyhow::anyhow!(
                        "Invalid bucket name '{}'. Bucket names must be 3-63 characters, contain only lowercase letters, numbers, and hyphens, and cannot start/end with a hyphen.",
                        bucket
                    )),
                    (_, 404) => Err(anyhow::anyhow!("Bucket '{}' not found", bucket)),
                    (_, 403) => Err(anyhow::anyhow!(
                        "Access denied to bucket '{}'. Check IAM permissions or bucket policy.",
                        bucket
                    )),
                    _ => {
                        let msg = error_message.unwrap_or("Unknown error");
                        Err(anyhow::anyhow!(
                            "S3 error (HTTP {}): {} (code: {})",
                            status_code.as_u16(),
                            msg,
                            error_code.unwrap_or("unknown")
                        ))
                    }
                }
            }
            _ => Err(anyhow::anyhow!("S3 connection error: {}", sdk_error)),
        },
    }
}

pub async fn execute_connection_delete(current_dir: &Path) -> Result<()> {
    use inquire::Select;
    use std::fs;

    let project_root = find_project_root(Some(current_dir))?;
    let project_yml = project_root.join("project.yml");

    if !project_yml.exists() {
        return Err(anyhow::anyhow!(
            "project.yml not found. Run 'featherbox init' first."
        ));
    }

    let content = fs::read_to_string(&project_yml)?;
    let mut project_config: crate::config::ProjectConfig = serde_yml::from_str(&content)?;

    let connection_names: Vec<String> = project_config.connections.keys().cloned().collect();

    if connection_names.is_empty() {
        println!("No connections found.");
        return Ok(());
    }

    let selected = Select::new("Select connection to delete:", connection_names).prompt()?;

    if project_config.connections.remove(&selected).is_some() {
        println!("✓ Connection '{selected}' removed successfully");

        let updated_content = serde_yml::to_string(&project_config)?;
        fs::write(&project_yml, updated_content)?;
    } else {
        return Err(anyhow::anyhow!("Connection '{}' not found", selected));
    }

    Ok(())
}
