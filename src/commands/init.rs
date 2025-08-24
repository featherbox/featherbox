use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::secret::SecretManager;

pub fn execute_init(project_name: &str, current_dir: &Path) -> Result<()> {
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
    create_project_yml(&project_path, project_name)?;
    ensure_secret_key(&project_path)?;

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

fn create_project_yml(project_path: &Path, project_name: &str) -> Result<()> {
    let secret_key_path = project_path.join("secret.key");
    let project_yml_content = format!(
        r#"name: {}

storage:
  type: local
  path: ./storage

database:
  type: sqlite
  path: ./database.db

deployments:
  timeout: 600

connections: {{}}

secret_key_path: {}
"#,
        project_name,
        secret_key_path.to_string_lossy()
    );

    fs::write(project_path.join("project.yml"), project_yml_content)
        .context("Failed to create project.yml")?;
    Ok(())
}

fn ensure_secret_key(project_path: &Path) -> Result<()> {
    let key_manager = SecretManager::new_for_project_root(project_path)?;
    if !key_manager.key_exists() {
        key_manager.generate_key()?;
        println!(
            "✓ Secret key generated at {}",
            project_path.join("secret.key").display()
        );
    }
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

        let result = execute_init(project_name, temp_dir.path());

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

        let result = execute_init(project_name, temp_dir.path());

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

        create_project_yml(temp_dir.path(), project_name)?;

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

        let result = execute_init(project_name, temp_dir.path());
        assert!(result.is_ok());

        let project_path = temp_dir.path().join(project_name);
        let content = fs::read_to_string(project_path.join("project.yml"))?;

        assert!(content.starts_with(&format!("name: {project_name}")));

        Ok(())
    }
}
