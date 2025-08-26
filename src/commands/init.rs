use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use super::render_project_template;
use age::secrecy::ExposeSecret;

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

    fs::create_dir_all(&project_path)
        .with_context(|| format!("Failed to create project directory '{project_name}'"))?;

    create_project_structure(&project_path)?;
    create_project_yml(&project_path, project_name, secret_key_path)?;
    ensure_secret_key(&project_path, secret_key_path)?;

    println!(
        "FeatherBox project '{}' initialized successfully at {}",
        project_name,
        project_path.display()
    );
    Ok(())
}

fn create_project_structure(project_path: &Path) -> Result<()> {
    fs::create_dir_all(project_path.join("adapters"))
        .context("Failed to create adapters directory")?;
    fs::create_dir_all(project_path.join("models")).context("Failed to create models directory")?;
    Ok(())
}

fn create_project_yml(
    project_path: &Path,
    project_name: &str,
    secret_key_path: Option<&str>,
) -> Result<()> {
    let key_path = match secret_key_path {
        Some(path) => path.to_string(),
        None => {
            let home_dir = dirs::home_dir().context("Unable to find home directory")?;
            let config_dir = home_dir.join(".config").join("featherbox");
            let secret_key_path = config_dir.join("secret.key");
            secret_key_path.to_string_lossy().to_string()
        }
    };

    create_project_yml_with_secret_path(project_path, project_name, &key_path)
}

fn create_project_yml_with_secret_path(
    project_path: &Path,
    project_name: &str,
    secret_key_path: &str,
) -> Result<()> {
    let project_yml_content = render_project_template(project_name, secret_key_path);

    fs::write(project_path.join("project.yml"), project_yml_content)
        .context("Failed to create project.yml")?;
    Ok(())
}

fn ensure_secret_key(_project_path: &Path, secret_key_path: Option<&str>) -> Result<()> {
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
        println!("✓ Secret key generated at {}", key_path.display());
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
    use tempfile;

    #[test]
    fn test_execute_init_success() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";

        let result = create_new_project(project_name, temp_dir.path(), None);

        assert!(result.is_ok());

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("project.yml").exists());
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        let content = fs::read_to_string(project_path.join("project.yml"))?;
        assert!(content.contains(&format!("name: {project_name}")));
        assert!(content.contains("storage:"));
        assert!(content.contains("database:"));
        assert!(content.contains("deployments:"));
        assert!(content.contains("connections: {}"));
        assert!(content.contains("secret_key_path:"));

        Ok(())
    }

    #[test]
    fn test_execute_init_already_exists() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "existing_project";

        fs::create_dir_all(temp_dir.path().join(project_name))?;

        let result = create_new_project(project_name, temp_dir.path(), None);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_create_project_structure() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        create_project_structure(temp_dir.path())?;

        assert!(temp_dir.path().join("adapters").is_dir());
        assert!(temp_dir.path().join("models").is_dir());

        Ok(())
    }

    #[test]
    fn test_create_project_yml() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_yml_project";
        let test_secret_path = temp_dir
            .path()
            .join("test_secret.key")
            .to_string_lossy()
            .to_string();

        create_project_yml_with_secret_path(temp_dir.path(), project_name, &test_secret_path)?;

        let content = fs::read_to_string(temp_dir.path().join("project.yml"))?;
        assert!(content.contains(&format!("name: {project_name}")));
        assert!(content.contains("storage:"));
        assert!(content.contains("type: local"));
        assert!(content.contains("type: sqlite"));
        assert!(content.contains("secret_key_path:"));

        Ok(())
    }

    #[test]
    fn test_project_name_in_yaml() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "my_awesome_project";

        let result = create_new_project(project_name, temp_dir.path(), None);
        assert!(result.is_ok());

        let project_path = temp_dir.path().join(project_name);
        let content = fs::read_to_string(project_path.join("project.yml"))?;

        assert!(content.starts_with(&format!("name: {project_name}")));

        Ok(())
    }

    #[test]
    fn test_execute_init_with_custom_secret_key_path() -> Result<()> {
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
        assert!(content.contains(&format!("name: {project_name}")));
        assert!(content.contains(&format!("secret_key_path: {custom_secret_path}")));

        assert!(temp_dir.path().join("custom_secret.key").exists());

        Ok(())
    }
}
