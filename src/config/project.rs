use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub deployments: DeploymentsConfig,
    pub connections: HashMap<String, ConnectionConfig>,
    pub secret_key_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "local")]
    LocalFile { path: String },
    #[serde(rename = "s3")]
    S3(S3Config),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub auth_method: S3AuthMethod,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub path_style_access: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(rename = "type")]
    pub ty: DatabaseType,
    pub path: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentsConfig {
    pub timeout: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum S3AuthMethod {
    CredentialChain,
    #[default]
    Explicit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteDatabaseConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabaseType {
    #[serde(rename = "sqlite")]
    Sqlite,
    #[serde(rename = "mysql")]
    Mysql,
    #[serde(rename = "postgresql")]
    Postgresql,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ConnectionConfig {
    #[serde(rename = "localfile")]
    LocalFile { base_path: String },
    #[serde(rename = "s3")]
    S3(S3Config),
    #[serde(rename = "sqlite")]
    Sqlite { path: String },
    #[serde(rename = "mysql")]
    MySql { config: RemoteDatabaseConfig },
    #[serde(rename = "postgresql")]
    PostgreSql { config: RemoteDatabaseConfig },
}

impl ConnectionConfig {
    pub fn get_full_endpoint_url(&self) -> Option<String> {
        match self {
            ConnectionConfig::S3(config) => config.endpoint_url.clone(),
            _ => None,
        }
    }

    pub fn get_clean_endpoint_url(&self) -> Option<String> {
        match self {
            ConnectionConfig::S3(config) => config.endpoint_url.as_ref().map(|url| {
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
            ConnectionConfig::S3(config) => config
                .endpoint_url
                .as_ref()
                .is_none_or(|url| !url.starts_with("http://")),
            _ => true,
        }
    }
}

fn expand_env_vars(value: &str) -> Result<String, anyhow::Error> {
    let re = regex::Regex::new(r"\$\{([^}]+)\}").unwrap();
    let mut result = value.to_string();

    for cap in re.captures_iter(value) {
        let env_var_with_braces = &cap[0];
        let env_var = &cap[1];

        match std::env::var(env_var) {
            Ok(env_value) => {
                result = result.replace(env_var_with_braces, &env_value);
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "Environment variable '{}' not found or not accessible",
                    env_var
                ));
            }
        }
    }

    if result.contains("${") {
        return Err(anyhow::anyhow!(
            "Unclosed environment variable reference in: {}",
            value
        ));
    }

    Ok(result)
}

pub fn parse_project_config(yaml_str: &str) -> anyhow::Result<ProjectConfig> {
    let expanded_yaml = expand_env_vars(yaml_str)?;
    serde_yml::from_str(&expanded_yaml)
        .map_err(|e| anyhow::anyhow!("Failed to parse project config: {}", e))
}
