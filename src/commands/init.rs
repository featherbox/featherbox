use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::config::ProjectConfig;
use age::secrecy::ExposeSecret;
use inquire::Text;

pub fn create_new_project(
    project_name: &str,
    current_dir: &Path,
    secret_key_path: Option<&str>,
) -> Result<()> {
    let project_path = current_dir.join(project_name);

    if project_path.exists() {
        return Err(anyhow::anyhow!(
            "Directory '{}' already exists",
            project_name
        ));
    }

    let config = ProjectConfig::new(secret_key_path);

    config.validate()?;

    create_project_directory(current_dir, project_name)?;
    ensure_secret_key(secret_key_path)?;
    save_project_config(&config, &project_path)?;

    println!("✓ Project '{project_name}' initialized successfully");
    Ok(())
}

fn create_project_directory(current_dir: &Path, project_name: &str) -> Result<()> {
    let project_path = current_dir.join(project_name);

    fs::create_dir_all(&project_path)
        .with_context(|| format!("Failed to create project directory '{project_name}'"))?;

    fs::create_dir_all(project_path.join("adapters"))
        .context("Failed to create adapters directory")?;
    fs::create_dir_all(project_path.join("models")).context("Failed to create models directory")?;

    Ok(())
}

fn save_project_config(config: &ProjectConfig, project_path: &Path) -> Result<()> {
    let yaml_content =
        serde_yml::to_string(config).context("Failed to serialize project config to YAML")?;

    fs::write(project_path.join("project.yml"), yaml_content)
        .context("Failed to write project.yml")?;

    Ok(())
}

fn ensure_secret_key(secret_key_path: Option<&str>) -> Result<()> {
    let (key_path, key_dir) = match secret_key_path {
        Some(path) => {
            let key_path = std::path::PathBuf::from(path);
            let key_dir = key_path
                .parent()
                .ok_or_else(|| anyhow::anyhow!("Invalid secret key path: {}", path))?
                .to_path_buf();
            (key_path, key_dir)
        }
        None => {
            let home_dir = dirs::home_dir().context("Unable to find home directory")?;
            let config_dir = home_dir.join(".config").join("featherbox");
            let key_path = config_dir.join("secret.key");
            (key_path, config_dir)
        }
    };

    fs::create_dir_all(&key_dir)
        .with_context(|| format!("Failed to create directory: {}", key_dir.display()))?;

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

fn get_default_secret_key_path() -> Result<String> {
    let home_dir = dirs::home_dir().context("Unable to find home directory")?;
    let config_dir = home_dir.join(".config").join("featherbox");
    let secret_key_path = config_dir.join("secret.key");
    Ok(secret_key_path.to_string_lossy().to_string())
}

pub async fn execute_init_interactive(
    current_dir: &Path,
    default_project_name: Option<&str>,
    default_secret_key_path: Option<&str>,
) -> Result<()> {
    let mut project_name_prompt = Text::new("Project name:");
    if let Some(default_name) = default_project_name {
        project_name_prompt = project_name_prompt.with_default(default_name);
    }

    let project_name = project_name_prompt.prompt()?;

    if project_name.trim().is_empty() {
        println!("Project initialization cancelled.");
        return Ok(());
    }

    let default_key_path = match default_secret_key_path {
        Some(path) => path.to_string(),
        None => get_default_secret_key_path()?,
    };

    let secret_key_path_input = Text::new("Secret key path:")
        .with_initial_value(&default_key_path)
        .prompt()?;

    let secret_key_path = if secret_key_path_input.trim().is_empty() {
        None
    } else {
        Some(secret_key_path_input)
    };

    create_new_project(&project_name, current_dir, secret_key_path.as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::StorageConfig;
    use tempfile;

    #[test]
    fn test_create_new_project() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";

        let result = create_new_project(project_name, temp_dir.path(), None);

        assert!(result.is_ok());

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("project.yml").exists());
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        let content = fs::read_to_string(project_path.join("project.yml"))?;
        assert!(content.contains("storage:"));
        assert!(content.contains("database:"));
        assert!(content.contains("deployments:"));
        assert!(content.contains("connections: {}"));
        assert!(content.contains("secret_key_path:"));

        Ok(())
    }

    #[test]
    fn test_create_new_project_already_exists() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "existing_project";

        fs::create_dir_all(temp_dir.path().join(project_name))?;

        let result = create_new_project(project_name, temp_dir.path(), None);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_create_project_directory() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";

        create_project_directory(temp_dir.path(), project_name)?;

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        Ok(())
    }

    #[test]
    fn test_create_secret_key() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project_custom_secret";
        let custom_secret_path = temp_dir
            .path()
            .join("custom_secret.key")
            .to_string_lossy()
            .to_string();

        let result = create_new_project(project_name, temp_dir.path(), Some(&custom_secret_path));
        assert!(result.is_ok());

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("project.yml").exists());

        let content = fs::read_to_string(project_path.join("project.yml"))?;
        assert!(content.contains(&format!("secret_key_path: {custom_secret_path}")));

        assert!(temp_dir.path().join("custom_secret.key").exists());

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
