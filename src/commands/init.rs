use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub fn execute_init(project_name: Option<&str>, project_path: &Path) -> Result<()> {
    if project_path.join("project.yml").exists() {
        return Err(anyhow::anyhow!(
            "FeatherBox project already exists in this directory"
        ));
    }

    create_project_structure(project_path)?;
    create_project_yml(project_path, project_name)?;

    println!("FeatherBox project initialized successfully");
    Ok(())
}

fn create_project_structure(project_path: &Path) -> Result<()> {
    fs::create_dir_all(project_path.join("adapters"))
        .context("Failed to create adapters directory")?;
    fs::create_dir_all(project_path.join("models")).context("Failed to create models directory")?;
    Ok(())
}

fn create_project_yml(project_path: &Path, _project_name: Option<&str>) -> Result<()> {
    let project_yml_content = r#"storage:
  type: local
  path: ./storage

database:
  type: sqlite
  path: ./database.db

deployments:
  timeout: 600

connections: {}
"#;

    fs::write(project_path.join("project.yml"), project_yml_content)
        .context("Failed to create project.yml")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_execute_init_success() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let result = execute_init(None, temp_dir.path());

        assert!(result.is_ok());

        // ファイル・ディレクトリの存在確認
        assert!(temp_dir.path().join("project.yml").exists());
        assert!(temp_dir.path().join("adapters").is_dir());
        assert!(temp_dir.path().join("models").is_dir());

        // project.yml内容の確認
        let content = fs::read_to_string(temp_dir.path().join("project.yml"))?;
        assert!(content.contains("storage:"));
        assert!(content.contains("database:"));
        assert!(content.contains("deployments:"));
        assert!(content.contains("connections: {}"));

        Ok(())
    }

    #[test]
    fn test_execute_init_already_exists() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        fs::write(temp_dir.path().join("project.yml"), "existing")?;

        let result = execute_init(None, temp_dir.path());

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

        create_project_yml(temp_dir.path(), None)?;

        let content = fs::read_to_string(temp_dir.path().join("project.yml"))?;
        assert!(content.contains("storage:"));
        assert!(content.contains("type: local"));
        assert!(content.contains("type: sqlite"));

        Ok(())
    }
}
