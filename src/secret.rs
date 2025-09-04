use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use regex::Regex;
use ring::aead::{
    AES_256_GCM, Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey,
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::workspace::find_project_root;

struct CountingNonceSequence(u32);

impl NonceSequence for CountingNonceSequence {
    fn advance(&mut self) -> Result<Nonce, ring::error::Unspecified> {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[8..].copy_from_slice(&self.0.to_be_bytes());
        self.0 += 1;
        Nonce::try_assume_unique_for_key(&nonce_bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SecretData {
    #[serde(flatten)]
    secrets: HashMap<String, String>,
}

pub struct SecretManager {
    key_file_path: PathBuf,
    secrets_file_path: PathBuf,
}

impl SecretManager {
    pub fn new() -> Result<Self> {
        let project_root = find_project_root()?;
        let key_file_path = project_root.join(".secret.key");
        let secrets_file_path = project_root.join("secrets.yml");

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
        let mut key_bytes = [0u8; 32];
        let rng = SystemRandom::new();
        rng.fill(&mut key_bytes)
            .map_err(|_| anyhow::anyhow!("Failed to generate random key"))?;

        let key_base64 = BASE64.encode(key_bytes);

        let key_content = format!(
            "# FeatherBox Secret Key File\n# \n# This file contains the encryption key for your project secrets.\n# \n# SECURITY WARNINGS:\n# - DO NOT commit this file to version control (Git, SVN, etc.)\n# - DO NOT share via email, chat, or public platforms\n# - DO NOT copy to shared drives or cloud storage\n# \n# TEAM SHARING:\n# - Share this key securely through encrypted channels only\n# - All team members need the same key to access project secrets\n# - Store backup copies in secure password managers\n# \n# USAGE:\n# - Keep this file at <project-directory>/.secret.key\n# - Use 'featherbox secret' commands to manage encrypted credentials\n# - If lost, use 'featherbox secret gen-key' to regenerate (existing secrets will be lost)\n# \n# Generated: {}\n\n{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            key_base64
        );

        fs::write(&self.key_file_path, key_content).with_context(|| {
            format!("Failed to write key file: {}", self.key_file_path.display())
        })?;

        Ok(())
    }

    pub fn key_exists(&self) -> bool {
        self.key_file_path.exists()
    }

    fn load_key(&self) -> Result<[u8; 32]> {
        if !self.key_exists() {
            return Err(anyhow::anyhow!(
                "Secret key not found. Run 'featherbox secret gen-key' first."
            ));
        }

        let content = fs::read_to_string(&self.key_file_path).with_context(|| {
            format!("Failed to read key file: {}", self.key_file_path.display())
        })?;

        let key_line = content
            .lines()
            .find(|line| !line.trim().starts_with('#') && !line.trim().is_empty())
            .context("No valid key found in secret file")?;

        let key_bytes = BASE64
            .decode(key_line.trim())
            .context("Failed to decode base64 key")?;

        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length, expected 32 bytes"));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&key_bytes);
        Ok(key)
    }

    fn encrypt(&self, data: &str) -> Result<String> {
        let key = self.load_key()?;
        let unbound_key = UnboundKey::new(&AES_256_GCM, &key)
            .map_err(|_| anyhow::anyhow!("Failed to create encryption key"))?;

        let mut nonce_bytes = [0u8; 12];
        let rng = SystemRandom::new();
        rng.fill(&mut nonce_bytes)
            .map_err(|_| anyhow::anyhow!("Failed to generate nonce"))?;

        let mut sealing_key = SealingKey::new(unbound_key, CountingNonceSequence(0));

        let mut in_out = data.as_bytes().to_vec();
        let tag = sealing_key
            .seal_in_place_separate_tag(Aad::empty(), &mut in_out)
            .map_err(|_| anyhow::anyhow!("Failed to encrypt data"))?;

        let mut payload = Vec::new();
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&in_out);
        payload.extend_from_slice(tag.as_ref());

        Ok(BASE64.encode(payload))
    }

    fn decrypt(&self, encrypted_data: &str) -> Result<String> {
        let key = self.load_key()?;
        let payload = BASE64
            .decode(encrypted_data.trim())
            .context("Failed to decode base64 encrypted data")?;

        if payload.len() < 28 {
            return Err(anyhow::anyhow!("Invalid encrypted data: too short"));
        }

        let tag_start = payload.len() - 16;
        let ciphertext = &payload[12..tag_start];

        let unbound_key = UnboundKey::new(&AES_256_GCM, &key)
            .map_err(|_| anyhow::anyhow!("Failed to create decryption key"))?;
        let mut opening_key = OpeningKey::new(unbound_key, CountingNonceSequence(0));

        let mut ciphertext_and_tag = Vec::new();
        ciphertext_and_tag.extend_from_slice(ciphertext);
        ciphertext_and_tag.extend_from_slice(&payload[tag_start..]);

        let decrypted = opening_key
            .open_in_place(Aad::empty(), &mut ciphertext_and_tag)
            .map_err(|_| anyhow::anyhow!("Failed to decrypt data"))?;

        String::from_utf8(decrypted.to_vec()).context("Decrypted data is not valid UTF-8")
    }

    fn load_secrets(&self) -> Result<SecretData> {
        if !self.secrets_file_path.exists() {
            return Ok(SecretData::default());
        }

        let yaml_content = fs::read_to_string(&self.secrets_file_path).with_context(|| {
            format!(
                "Failed to read secrets file: {}",
                self.secrets_file_path.display()
            )
        })?;

        if yaml_content.trim().is_empty() {
            return Ok(SecretData::default());
        }

        let yaml_value: serde_yml::Value =
            serde_yml::from_str(&yaml_content).context("Failed to parse secrets YAML")?;

        let mut secrets = HashMap::new();

        if let serde_yml::Value::Mapping(map) = yaml_value {
            for (key, value) in map {
                if let (
                    serde_yml::Value::String(key_str),
                    serde_yml::Value::String(encrypted_value),
                ) = (key, value)
                {
                    let decrypted_value = self.decrypt(&encrypted_value)?;
                    secrets.insert(key_str, decrypted_value);
                }
            }
        }

        Ok(SecretData { secrets })
    }

    fn save_secrets(&self, data: &SecretData) -> Result<()> {
        let mut yaml_map = serde_yml::Mapping::new();

        for (key, value) in &data.secrets {
            let encrypted_value = self.encrypt(value)?;
            yaml_map.insert(
                serde_yml::Value::String(key.clone()),
                serde_yml::Value::String(encrypted_value),
            );
        }

        let yaml_value = serde_yml::Value::Mapping(yaml_map);
        let yaml_content =
            serde_yml::to_string(&yaml_value).context("Failed to serialize secrets to YAML")?;

        fs::write(&self.secrets_file_path, yaml_content).with_context(|| {
            format!(
                "Failed to write secrets file: {}",
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

pub fn expand_secrets_in_text(text: &str, _project_root: &Path) -> Result<String> {
    let secret_regex =
        Regex::new(r"\$\{SECRET_([a-zA-Z][a-zA-Z0-9_]*)\}").expect("Invalid regex pattern");

    if !secret_regex.is_match(text) {
        return Ok(text.to_string());
    }

    let manager = SecretManager::new()?;
    let secrets = manager.get_all_secrets()?;

    let mut result = text.to_string();
    for captures in secret_regex.captures_iter(text) {
        let full_match = captures.get(0).unwrap().as_str();
        let key = captures.get(1).unwrap().as_str();

        if let Some(value) = secrets.get(key) {
            result = result.replace(full_match, value);
        } else {
            return Err(anyhow::anyhow!(
                "Secret '{}' not found. Use 'featherbox secret new' to add it.",
                key
            ));
        }
    }

    Ok(result)
}

pub fn expand_secrets_in_hash_map(
    map: &HashMap<String, String>,
    project_root: &Path,
) -> Result<HashMap<String, String>> {
    let mut expanded = HashMap::new();

    for (key, value) in map {
        let expanded_value = expand_secrets_in_text(value, project_root)?;
        expanded.insert(key.clone(), expanded_value);
    }

    Ok(expanded)
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_secret_manager_generate_and_load_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;

        assert!(!manager.key_exists());

        manager.generate_key()?;
        assert!(manager.key_exists());

        let _key = manager.load_key()?;

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_secret_manager_operations() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
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

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_secret_expansion() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("DB_HOST", "localhost")?;
        manager.set_secret("DB_PORT", "5432")?;

        let text = "host: ${SECRET_DB_HOST}\nport: ${SECRET_DB_PORT}";
        let expanded = expand_secrets_in_text(text, temp_dir.path())?;

        assert_eq!(expanded, "host: localhost\nport: 5432");

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_secret_expansion_missing_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let text = "host: ${SECRET_MISSING_KEY}";
        let result = expand_secrets_in_text(text, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_secret_expansion_no_match() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let text = "host: localhost";
        let expanded = expand_secrets_in_text(text, temp_dir.path())?;

        assert_eq!(expanded, "host: localhost");

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_multiple_secrets() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("API_KEY", "secret123")?;
        manager.set_secret("ENDPOINT", "https://api.example.com")?;
        manager.set_secret("VERSION", "v2")?;

        let text = "url: ${SECRET_ENDPOINT}/${SECRET_VERSION}/data?key=${SECRET_API_KEY}";
        let expanded = expand_secrets_in_text(text, temp_dir.path())?;

        assert_eq!(
            expanded,
            "url: https://api.example.com/v2/data?key=secret123"
        );

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_same_secret_multiple_times() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("TOKEN", "abc123")?;

        let text = "auth_header: Bearer ${SECRET_TOKEN}\nbackup_token: ${SECRET_TOKEN}";
        let expanded = expand_secrets_in_text(text, temp_dir.path())?;

        assert_eq!(expanded, "auth_header: Bearer abc123\nbackup_token: abc123");

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_text_invalid_secret_name() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        let text = "value: ${SECRET_NONEXISTENT_KEY}";
        let result = expand_secrets_in_text(text, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_success() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("USERNAME", "testuser")?;
        manager.set_secret("PASSWORD", "testpass")?;

        let mut input_map = HashMap::new();
        input_map.insert("user".to_string(), "${SECRET_USERNAME}".to_string());
        input_map.insert("pass".to_string(), "${SECRET_PASSWORD}".to_string());
        input_map.insert("host".to_string(), "localhost".to_string());

        let expanded = expand_secrets_in_hash_map(&input_map, temp_dir.path())?;

        assert_eq!(expanded.get("user").unwrap(), "testuser");
        assert_eq!(expanded.get("pass").unwrap(), "testpass");
        assert_eq!(expanded.get("host").unwrap(), "localhost");

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_empty_map() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let input_map = HashMap::new();

        let expanded = expand_secrets_in_hash_map(&input_map, temp_dir.path())?;

        assert!(expanded.is_empty());

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_no_secrets() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let mut input_map = HashMap::new();
        input_map.insert("config1".to_string(), "value1".to_string());
        input_map.insert("config2".to_string(), "value2".to_string());

        let expanded = expand_secrets_in_hash_map(&input_map, temp_dir.path())?;

        assert_eq!(expanded.get("config1").unwrap(), "value1");
        assert_eq!(expanded.get("config2").unwrap(), "value2");

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_missing_secret() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        let mut input_map = HashMap::new();
        input_map.insert("key".to_string(), "${SECRET_NONEXISTENT}".to_string());

        let result = expand_secrets_in_hash_map(&input_map, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_expand_secrets_in_hash_map_mixed_values() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
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

        let expanded = expand_secrets_in_hash_map(&input_map, temp_dir.path())?;

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

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_yaml_secret_file_format() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("API_KEY", "secret123")?;
        manager.set_secret("DB_PASSWORD", "password456")?;

        let yaml_content = std::fs::read_to_string(temp_dir.path().join("secrets.yml"))?;

        assert!(yaml_content.contains("API_KEY:"));
        assert!(yaml_content.contains("DB_PASSWORD:"));
        assert!(!yaml_content.contains("secret123"));
        assert!(!yaml_content.contains("password456"));

        let retrieved_api_key = manager.get_secret("API_KEY")?;
        let retrieved_db_password = manager.get_secret("DB_PASSWORD")?;

        assert_eq!(retrieved_api_key, Some("secret123".to_string()));
        assert_eq!(retrieved_db_password, Some("password456".to_string()));

        crate::workspace::clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_yaml_file_manual_inspection() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        std::fs::write(temp_dir.path().join("project.yml"), "test: true")?;
        crate::workspace::set_project_dir_override(temp_dir.path().to_path_buf());

        let manager = SecretManager::new()?;
        manager.generate_key()?;

        manager.set_secret("AWS_SECRET_KEY", "AKIA1234567890")?;
        manager.set_secret("DB_PASSWORD", "mypassword")?;

        let yaml_content = std::fs::read_to_string(temp_dir.path().join("secrets.yml"))?;
        let yaml_lines: Vec<&str> = yaml_content.lines().collect();

        assert_eq!(yaml_lines.len(), 2);

        let mut found_aws = false;
        let mut found_db = false;

        for line in &yaml_lines {
            if line.starts_with("AWS_SECRET_KEY: ") {
                found_aws = true;
                assert!(!line.contains("AKIA1234567890"));
                let encrypted_part = line.strip_prefix("AWS_SECRET_KEY: ").unwrap();
                assert!(!encrypted_part.is_empty());
                assert_ne!(encrypted_part, "AKIA1234567890");
            } else if line.starts_with("DB_PASSWORD: ") {
                found_db = true;
                assert!(!line.contains("mypassword"));
                let encrypted_part = line.strip_prefix("DB_PASSWORD: ").unwrap();
                assert!(!encrypted_part.is_empty());
                assert_ne!(encrypted_part, "mypassword");
            }
        }

        assert!(found_aws, "AWS_SECRET_KEY not found in YAML");
        assert!(found_db, "DB_PASSWORD not found in YAML");

        let retrieved_aws = manager.get_secret("AWS_SECRET_KEY")?;
        let retrieved_db = manager.get_secret("DB_PASSWORD")?;
        assert_eq!(retrieved_aws, Some("AKIA1234567890".to_string()));
        assert_eq!(retrieved_db, Some("mypassword".to_string()));

        crate::workspace::clear_project_dir_override();
        Ok(())
    }
}
