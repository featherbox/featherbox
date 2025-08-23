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
    pub fn new(project_root: &Path) -> Result<Self> {
        let home_dir = dirs::home_dir().context("Unable to find home directory")?;
        let featherbox_dir = home_dir.join(".featherbox");

        fs::create_dir_all(&featherbox_dir)
            .with_context(|| format!("Failed to create directory: {}", featherbox_dir.display()))?;

        Ok(Self {
            key_file_path: featherbox_dir.join("secret.key"),
            secrets_file_path: project_root.join("secrets.enc"),
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

    #[cfg(test)]
    pub fn new_for_test(key_file_path: PathBuf, secrets_file_path: PathBuf) -> Self {
        Self {
            key_file_path,
            secrets_file_path,
        }
    }
}

pub fn expand_secrets_in_text(text: &str, project_root: &Path) -> Result<String> {
    let secret_regex =
        Regex::new(r"\$\{SECRET_([a-zA-Z][a-zA-Z0-9_]*)\}").expect("Invalid regex pattern");

    if !secret_regex.is_match(text) {
        return Ok(text.to_string());
    }

    let manager = SecretManager::new(project_root)?;
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
pub fn expand_secrets_in_text_with_manager(text: &str, manager: &SecretManager) -> Result<String> {
    let secret_regex =
        Regex::new(r"\$\{SECRET_([a-zA-Z][a-zA-Z0-9_]*)\}").expect("Invalid regex pattern");

    if !secret_regex.is_match(text) {
        return Ok(text.to_string());
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_secret_manager_generate_and_load_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let manager = SecretManager::new_for_test(
            temp_dir.path().join("test_secret.key"),
            temp_dir.path().join("test_secrets.enc"),
        );

        assert!(!manager.key_exists());

        manager.generate_key()?;
        assert!(manager.key_exists());

        let _passphrase = manager.load_passphrase()?;

        Ok(())
    }

    #[test]
    fn test_secret_manager_operations() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let manager = SecretManager::new_for_test(
            temp_dir.path().join("test_secret.key"),
            temp_dir.path().join("test_secrets.enc"),
        );
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
        let manager = SecretManager::new_for_test(
            temp_dir.path().join("test_secret.key"),
            temp_dir.path().join("test_secrets.enc"),
        );
        manager.generate_key()?;

        manager.set_secret("DB_HOST", "localhost")?;
        manager.set_secret("DB_PORT", "5432")?;

        let text = "host: ${SECRET_DB_HOST}\nport: ${SECRET_DB_PORT}";
        let expanded = expand_secrets_in_text_with_manager(text, &manager)?;

        assert_eq!(expanded, "host: localhost\nport: 5432");

        Ok(())
    }

    #[test]
    fn test_secret_expansion_missing_key() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let text = "host: ${SECRET_MISSING_KEY}";
        let result = expand_secrets_in_text(text, temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));

        Ok(())
    }

    #[test]
    fn test_secret_expansion_no_match() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let text = "host: localhost";
        let expanded = expand_secrets_in_text(text, temp_dir.path())?;

        assert_eq!(expanded, "host: localhost");

        Ok(())
    }
}
