use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::ProjectConfig;
use age::secrecy::ExposeSecret;

pub struct ProjectBuilder {
    pub project_name: String,
    pub config: ProjectConfig,
    current_dir: PathBuf,
}

impl ProjectBuilder {
    pub fn new(project_name: String, config: &ProjectConfig) -> Result<Self> {
        let current_dir = std::env::current_dir()?;
        Ok(Self {
            project_name,
            config: config.clone(),
            current_dir,
        })
    }

    pub fn with_current_dir(
        project_name: String,
        config: &ProjectConfig,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            project_name,
            config: config.clone(),
            current_dir,
        }
    }

    pub fn create_project_directory(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        if project_path.exists() {
            return Err(anyhow::anyhow!(
                "Directory '{}' already exists",
                self.project_name
            ));
        }
        self.create_directories(&project_path)
    }

    fn create_directories(&self, base_path: &Path) -> Result<()> {
        fs::create_dir_all(base_path).with_context(|| {
            format!(
                "Failed to create project directory '{}'.",
                base_path.display()
            )
        })?;

        fs::create_dir_all(base_path.join("adapters"))
            .context("Failed to create adapters directory")?;
        fs::create_dir_all(base_path.join("models"))
            .context("Failed to create models directory")?;

        Ok(())
    }

    pub fn create_secret_key(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        ensure_secret_key(&project_path)
    }

    pub fn save_project_config(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);

        let yaml_content = serde_yml::to_string(&self.config)
            .context("Failed to serialize project config to YAML")?;

        fs::write(project_path.join("project.yml"), yaml_content)
            .context("Failed to write project.yml")?;

        Ok(())
    }
}

fn ensure_secret_key(project_path: &std::path::Path) -> Result<()> {
    let key_path = project_path.join(".secret.key");

    fs::create_dir_all(project_path)
        .with_context(|| format!("Failed to create directory: {}", project_path.display()))?;

    if !key_path.exists() {
        generate_secret_key(&key_path)?;
    }
    Ok(())
}

fn generate_secret_key(key_path: &std::path::Path) -> Result<()> {
    let passphrase = age::secrecy::Secret::new(
        std::iter::repeat_with(fastrand::alphanumeric)
            .take(32)
            .collect::<String>(),
    );

    let key_content = format!(
        "# FeatherBox Secret Key\n# DO NOT share publicly\n# Generated: {}\n\n{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        passphrase.expose_secret()
    );

    fs::write(key_path, key_content)
        .with_context(|| format!("Failed to write key file: {}", key_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::StorageConfig;
    use tempfile;

    #[test]
    fn test_project_builder() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";
        let config = ProjectConfig::new();

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        builder.create_project_directory()?;
        builder.create_secret_key()?;
        builder.save_project_config()?;

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("project.yml").exists());
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        let content = fs::read_to_string(project_path.join("project.yml"))?;
        assert!(content.contains("storage:"));
        assert!(content.contains("database:"));
        assert!(content.contains("deployments:"));
        assert!(content.contains("connections: {}"));

        assert!(project_path.join(".secret.key").exists());

        Ok(())
    }

    #[test]
    fn test_project_builder_already_exists() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "existing_project";
        let config = ProjectConfig::new();

        fs::create_dir_all(temp_dir.path().join(project_name))?;

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        let result = builder.create_project_directory();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_project_builder_directories() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";
        let config = ProjectConfig::new();

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        builder.create_project_directory()?;

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        Ok(())
    }

    #[test]
    fn test_project_config_validate() -> Result<()> {
        let mut config = ProjectConfig::default();
        assert!(config.validate().is_ok());

        config.storage = StorageConfig::LocalFile {
            path: "".to_string(),
        };
        assert!(config.validate().is_err());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Storage path cannot be empty")
        );

        config.storage = StorageConfig::LocalFile {
            path: "./storage".to_string(),
        };
        config.deployments.timeout = 0;
        assert!(config.validate().is_err());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Deployment timeout must be greater than 0")
        );

        Ok(())
    }
}
