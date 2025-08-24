use crate::config::project::ProjectConfig;
use age::secrecy::ExposeSecret;
use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SecretData {
    secrets: HashMap<String, String>,
}

pub struct SecretManager {
    key_file_path: PathBuf,
    secrets_file_path: PathBuf,
}

impl SecretManager {
    pub fn new(project_config: &ProjectConfig, project_root: &Path) -> Result<Self> {
        let key_file_path = match &project_config.secret_key_path {
            Some(path) => PathBuf::from(path),
            None => {
                let home_dir = dirs::home_dir().context("Unable to find home directory")?;
                let featherbox_dir = home_dir.join(".featherbox");

                fs::create_dir_all(&featherbox_dir).with_context(|| {
                    format!("Failed to create directory: {}", featherbox_dir.display())
                })?;

                featherbox_dir.join("secret.key")
            }
        };

        let secrets_file_path = project_root.join("secrets.enc");

        if let Some(parent) = key_file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        Ok(Self {
            key_file_path,
            secrets_file_path,
        })
    }

    pub fn new_for_project_root(project_root: &Path) -> Result<Self> {
        let key_file_path = project_root.join("secret.key");
        let secrets_file_path = project_root.join("secrets.enc");

        if let Some(parent) = key_file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        Ok(Self {
            key_file_path,
            secrets_file_path,
        })
    }

    pub fn generate_key(&self) -> Result<()> {
        let passphrase = age::secrecy::Secret::new(
            std::iter::repeat_with(fastrand::alphanumeric)
                .take(32)
                .collect::<String>(),
        );

        let key_content = format!(
            "# FeatherBox Secret Key File\n# \n# This file contains the encryption key for your project secrets.\n# \n# SECURITY WARNINGS:\n# - DO NOT commit this file to version control (Git, SVN, etc.)\n# - DO NOT share via email, chat, or public platforms\n# - DO NOT copy to shared drives or cloud storage\n# \n# TEAM SHARING:\n# - Share this key securely through encrypted channels only\n# - All team members need the same key to access project secrets\n# - Store backup copies in secure password managers\n# \n# USAGE:\n# - Keep this file at ~/.featherbox/secret.key\n# - Use 'fbox secret' commands to manage encrypted credentials\n# - If lost, use 'fbox secret gen-key' to regenerate (existing secrets will be lost)\n# \n# Generated: {}\n\n{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            passphrase.expose_secret()
        );

        fs::write(&self.key_file_path, key_content).with_context(|| {
            format!("Failed to write key file: {}", self.key_file_path.display())
        })?;

        Ok(())
    }

    pub fn key_exists(&self) -> bool {
        self.key_file_path.exists()
    }

    fn load_passphrase(&self) -> Result<age::secrecy::Secret<String>> {
        if !self.key_exists() {
            return Err(anyhow::anyhow!(
                "Secret key not found. Run 'fbox secret gen-key' first."
            ));
        }

        let content = fs::read_to_string(&self.key_file_path).with_context(|| {
            format!("Failed to read key file: {}", self.key_file_path.display())
        })?;

        let key_line = content
            .lines()
            .find(|line| !line.trim().starts_with('#') && !line.trim().is_empty())
            .context("No valid key found in secret file")?;

        Ok(age::secrecy::Secret::new(key_line.to_string()))
    }

    fn load_secrets(&self) -> Result<SecretData> {
        if !self.secrets_file_path.exists() {
            return Ok(SecretData::default());
        }

        let encrypted_content = fs::read(&self.secrets_file_path).with_context(|| {
            format!(
                "Failed to read secrets file: {}",
                self.secrets_file_path.display()
            )
        })?;

        if encrypted_content.is_empty() {
            return Ok(SecretData::default());
        }

        let passphrase = self.load_passphrase()?;
        let decryptor = match age::Decryptor::new(&encrypted_content[..]) {
            Ok(age::Decryptor::Passphrase(d)) => d,
            Ok(_) => return Err(anyhow::anyhow!("Unexpected decryptor type")),
            Err(e) => return Err(anyhow::anyhow!("Failed to create decryptor: {}", e)),
        };

        let mut decrypted_content = Vec::new();
        let mut reader = decryptor
            .decrypt(&passphrase, None)
            .context("Failed to decrypt secrets")?;

        reader
            .read_to_end(&mut decrypted_content)
            .context("Failed to read decrypted content")?;

        let json_str =
            String::from_utf8(decrypted_content).context("Decrypted content is not valid UTF-8")?;

        serde_json::from_str(&json_str).context("Failed to parse secrets JSON")
    }

    fn save_secrets(&self, data: &SecretData) -> Result<()> {
        let json_content =
            serde_json::to_string_pretty(data).context("Failed to serialize secrets to JSON")?;

        let passphrase = self.load_passphrase()?;
        let encryptor = age::Encryptor::with_user_passphrase(passphrase.clone());

        let mut encrypted_content = Vec::new();
        let mut writer = encryptor
            .wrap_output(&mut encrypted_content)
            .context("Failed to create encrypted writer")?;

        writer
            .write_all(json_content.as_bytes())
            .context("Failed to write content to encryptor")?;

        writer.finish().context("Failed to finalize encryption")?;

        fs::write(&self.secrets_file_path, encrypted_content).with_context(|| {
            format!(
                "Failed to write encrypted secrets: {}",
                self.secrets_file_path.display()
            )
        })?;

        Ok(())
    }

    pub fn get_secret(&self, key: &str) -> Result<Option<String>> {
        let data = self.load_secrets()?;
        Ok(data.secrets.get(key).cloned())
    }

    pub fn set_secret(&self, key: &str, value: &str) -> Result<()> {
        let mut data = self.load_secrets()?;
        data.secrets.insert(key.to_string(), value.to_string());
        self.save_secrets(&data)
    }

    pub fn delete_secret(&self, key: &str) -> Result<bool> {
        let mut data = self.load_secrets()?;
        let removed = data.secrets.remove(key).is_some();
        if removed {
            self.save_secrets(&data)?;
        }
        Ok(removed)
    }

    pub fn list_secrets(&self) -> Result<Vec<String>> {
        let data = self.load_secrets()?;
        let mut keys: Vec<String> = data.secrets.keys().cloned().collect();
        keys.sort();
        Ok(keys)
    }

    pub fn get_all_secrets(&self) -> Result<HashMap<String, String>> {
        let data = self.load_secrets()?;
        Ok(data.secrets)
    }
}

pub fn expand_secrets_in_text(
    text: &str,
    project_config: &ProjectConfig,
    project_root: &Path,
) -> Result<String> {
    let secret_regex =
        Regex::new(r"\$\{SECRET_([a-zA-Z][a-zA-Z0-9_]*)\}").expect("Invalid regex pattern");

    if !secret_regex.is_match(text) {
        return Ok(text.to_string());
    }

    let manager = SecretManager::new(project_config, project_root)?;
    let secrets = manager.get_all_secrets()?;

    let mut result = text.to_string();
    for captures in secret_regex.captures_iter(text) {
        let full_match = captures.get(0).unwrap().as_str();
        let key = captures.get(1).unwrap().as_str();

        if let Some(value) = secrets.get(key) {
            result = result.replace(full_match, value);
        } else {
            return Err(anyhow::anyhow!(
                "Secret '{}' not found. Use 'fbox secret new' to add it.",
                key
            ));
        }
    }

    Ok(result)
}

pub fn expand_secrets_in_hash_map(
    map: &HashMap<String, String>,
    project_config: &ProjectConfig,
    project_root: &Path,
) -> Result<HashMap<String, String>> {
    let mut expanded = HashMap::new();

    for (key, value) in map {
        let expanded_value = expand_secrets_in_text(value, project_config, project_root)?;
        expanded.insert(key.clone(), expanded_value);
    }

    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use crate::config::project::StorageConfig;

    use super::*;
    use tempfile::TempDir;

    fn create_test_project_config(
        secret_key_path: Option<String>,
    ) -> crate::config::project::ProjectConfig {
        crate::config::project::ProjectConfig {
            name: None,
            storage: StorageConfig::LocalFile {
                path: "./storage".to_string(),
            },

            database: crate::config::project::DatabaseConfig {
                ty: crate::config::project::DatabaseType::Sqlite,
                path: Some("./database.db".to_string()),
                host: None,
                port: None,
                database: None,
                username: None,
                password: None,
            },
            deployments: crate::config::project::DeploymentsConfig { timeout: 600 },
            connections: std::collections::HashMap::new(),
            secret_key_path,
        }
    }

    #[test]
    fn test_secret_manager_generate_and_load_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;

        assert!(!manager.key_exists());

        manager.generate_key()?;
        assert!(manager.key_exists());

        let _passphrase = manager.load_passphrase()?;

        Ok(())
    }

    #[test]
    fn test_secret_manager_operations() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        let keys = manager.list_secrets()?;
        assert!(keys.is_empty());

        manager.set_secret("TEST_KEY", "test_value")?;

        let keys = manager.list_secrets()?;
        assert_eq!(keys, vec!["TEST_KEY"]);

        let value = manager.get_secret("TEST_KEY")?;
        assert_eq!(value, Some("test_value".to_string()));

        manager.set_secret("TEST_KEY", "new_value")?;

        let value = manager.get_secret("TEST_KEY")?;
        assert_eq!(value, Some("new_value".to_string()));

        let removed = manager.delete_secret("TEST_KEY")?;
        assert!(removed);

        let keys = manager.list_secrets()?;
        assert!(keys.is_empty());

        Ok(())
    }

    #[test]
    fn test_secret_expansion() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        manager.set_secret("DB_HOST", "localhost")?;
        manager.set_secret("DB_PORT", "5432")?;

        let text = "host: ${SECRET_DB_HOST}\nport: ${SECRET_DB_PORT}";
        let expanded = expand_secrets_in_text(text, &project_config, temp_dir.path())?;

        assert_eq!(expanded, "host: localhost\nport: 5432");

        Ok(())
    }

    #[test]
    fn test_secret_expansion_missing_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));

        let text = "host: ${SECRET_MISSING_KEY}";
        let result = expand_secrets_in_text(text, &project_config, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_secret_expansion_no_match() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(None);

        let text = "host: localhost";
        let expanded = expand_secrets_in_text(text, &project_config, temp_dir.path())?;

        assert_eq!(expanded, "host: localhost");

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_multiple_secrets() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        manager.set_secret("API_KEY", "secret123")?;
        manager.set_secret("ENDPOINT", "https://api.example.com")?;
        manager.set_secret("VERSION", "v2")?;

        let text = "url: ${SECRET_ENDPOINT}/${SECRET_VERSION}/data?key=${SECRET_API_KEY}";
        let expanded = expand_secrets_in_text(text, &project_config, temp_dir.path())?;

        assert_eq!(
            expanded,
            "url: https://api.example.com/v2/data?key=secret123"
        );

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_same_secret_multiple_times() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        manager.set_secret("TOKEN", "abc123")?;

        let text = "auth_header: Bearer ${SECRET_TOKEN}\nbackup_token: ${SECRET_TOKEN}";
        let expanded = expand_secrets_in_text(text, &project_config, temp_dir.path())?;

        assert_eq!(expanded, "auth_header: Bearer abc123\nbackup_token: abc123");

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_invalid_secret_name() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        let text = "value: ${SECRET_NONEXISTENT_KEY}";
        let result = expand_secrets_in_text(text, &project_config, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_success() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        manager.set_secret("USERNAME", "testuser")?;
        manager.set_secret("PASSWORD", "testpass")?;

        let mut input_map = HashMap::new();
        input_map.insert("user".to_string(), "${SECRET_USERNAME}".to_string());
        input_map.insert("pass".to_string(), "${SECRET_PASSWORD}".to_string());
        input_map.insert("host".to_string(), "localhost".to_string());

        let expanded = expand_secrets_in_hash_map(&input_map, &project_config, temp_dir.path())?;

        assert_eq!(expanded.get("user").unwrap(), "testuser");
        assert_eq!(expanded.get("pass").unwrap(), "testpass");
        assert_eq!(expanded.get("host").unwrap(), "localhost");

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_empty_map() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let input_map = HashMap::new();

        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let expanded = expand_secrets_in_hash_map(&input_map, &project_config, temp_dir.path())?;

        assert!(expanded.is_empty());

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_no_secrets() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;

        let mut input_map = HashMap::new();
        input_map.insert("config1".to_string(), "value1".to_string());
        input_map.insert("config2".to_string(), "value2".to_string());

        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let expanded = expand_secrets_in_hash_map(&input_map, &project_config, temp_dir.path())?;

        assert_eq!(expanded.get("config1").unwrap(), "value1");
        assert_eq!(expanded.get("config2").unwrap(), "value2");

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_missing_secret() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        let mut input_map = HashMap::new();
        input_map.insert("key".to_string(), "${SECRET_NONEXISTENT}".to_string());

        let result = expand_secrets_in_hash_map(&input_map, &project_config, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_mixed_values() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let project_config = create_test_project_config(Some(
            temp_dir
                .path()
                .join("secret.key")
                .to_string_lossy()
                .to_string(),
        ));
        let manager = SecretManager::new(&project_config, temp_dir.path())?;
        manager.generate_key()?;

        manager.set_secret("DATABASE_URL", "postgres://localhost:5432/db")?;

        let mut input_map = HashMap::new();
        input_map.insert("db_url".to_string(), "${SECRET_DATABASE_URL}".to_string());
        input_map.insert("timeout".to_string(), "30".to_string());
        input_map.insert("ssl_mode".to_string(), "require".to_string());
        input_map.insert(
            "connection_string".to_string(),
            "server=${SECRET_DATABASE_URL};timeout=30".to_string(),
        );

        let expanded = expand_secrets_in_hash_map(&input_map, &project_config, temp_dir.path())?;

        assert_eq!(
            expanded.get("db_url").unwrap(),
            "postgres://localhost:5432/db"
        );
        assert_eq!(expanded.get("timeout").unwrap(), "30");
        assert_eq!(expanded.get("ssl_mode").unwrap(), "require");
        assert_eq!(
            expanded.get("connection_string").unwrap(),
            "server=postgres://localhost:5432/db;timeout=30"
        );

        Ok(())
    }
}
