use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub connections: HashMap<String, ConnectionConfig>,
}

impl ProjectConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<()> {
        if let StorageConfig::LocalFile { path } = &self.storage
            && path.is_empty()
        {
            return Err(anyhow::anyhow!("Storage path cannot be empty"));
        }

        if let StorageConfig::S3(s3_config) = &self.storage {
            if s3_config.bucket.is_empty() {
                return Err(anyhow::anyhow!("S3 bucket name cannot be empty"));
            }
            if s3_config.region.is_empty() {
                return Err(anyhow::anyhow!("S3 region cannot be empty"));
            }
        }

        match self.database.ty {
            DatabaseType::Sqlite => {
                if self.database.path.as_ref().is_none_or(|p| p.is_empty()) {
                    return Err(anyhow::anyhow!("SQLite database path cannot be empty"));
                }
            }
            DatabaseType::Mysql | DatabaseType::Postgresql => {
                if self.database.host.as_ref().is_none_or(|h| h.is_empty()) {
                    return Err(anyhow::anyhow!("Database host cannot be empty"));
                }
                if self.database.database.as_ref().is_none_or(|d| d.is_empty()) {
                    return Err(anyhow::anyhow!("Database name cannot be empty"));
                }
            }
        }

        Ok(())
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::LocalFile {
                path: "./storage".to_string(),
            },
            database: DatabaseConfig {
                ty: DatabaseType::Sqlite,
                path: Some("./database.db".to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            connections: HashMap::new(),
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum S3AuthMethod {
    #[serde(rename = "credential_chain")]
    CredentialChain,
    #[default]
    #[serde(rename = "explicit")]
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
    MySql {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    },
    #[serde(rename = "postgresql")]
    PostgreSql {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
    },
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
