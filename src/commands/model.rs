use anyhow::{Context, Result};
use std::fs;

use super::{render_model_template, validate_name};
use crate::project::ensure_project_directory;

pub fn execute_model_new(name: &str, project_path: &std::path::Path) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(Some(project_path))?;
    let model_file = project_root.join("models").join(format!("{name}.yml"));

    if model_file.exists() {
        return Err(anyhow::anyhow!("Model '{}' already exists", name));
    }

    let template = render_model_template(name);
    fs::write(&model_file, template)
        .with_context(|| format!("Failed to create model file: {model_file:?}"))?;

    println!("Created model: {name}");
    Ok(())
}

pub fn execute_model_delete(name: &str, project_path: &std::path::Path) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(Some(project_path))?;
    let model_file = project_root.join("models").join(format!("{name}.yml"));

    if !model_file.exists() {
        return Err(anyhow::anyhow!("Model '{}' does not exist", name));
    }

    fs::remove_file(&model_file)
        .with_context(|| format!("Failed to delete model file: {model_file:?}"))?;

    println!("Deleted model: {name}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile;

    fn setup_test_project() -> Result<tempfile::TempDir> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::create_dir_all(project_path.join("adapters"))?;
        fs::create_dir_all(project_path.join("models"))?;
        fs::write(project_path.join("project.yml"), "test")?;

        Ok(temp_dir)
    }

    #[test]
    fn test_execute_model_new_success() -> Result<()> {
        let temp_dir = setup_test_project()?;

        let result = execute_model_new("user_stats", temp_dir.path());

        assert!(result.is_ok());

        let model_file = temp_dir.path().join("models/user_stats.yml");
        assert!(model_file.exists());

        let content = fs::read_to_string(model_file)?;
        assert!(content.contains("Generated model for user_stats"));
        assert!(content.contains("sql: |"));
        assert!(content.contains("max_age: 3600"));

        Ok(())
    }

    #[test]
    fn test_execute_model_new_already_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;

        fs::write(temp_dir.path().join("models/existing.yml"), "test")?;

        let result = execute_model_new("existing", temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_execute_model_delete_success() -> Result<()> {
        let temp_dir = setup_test_project()?;

        fs::write(temp_dir.path().join("models/to_delete.yml"), "test")?;

        let result = execute_model_delete("to_delete", temp_dir.path());

        assert!(result.is_ok());
        assert!(!temp_dir.path().join("models/to_delete.yml").exists());

        Ok(())
    }

    #[test]
    fn test_execute_model_delete_not_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;

        let result = execute_model_delete("nonexistent", temp_dir.path());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_render_model_template() {
        let template = super::render_model_template("test_model");

        assert!(template.contains("Generated model for test_model"));
        assert!(template.contains("sql: |"));
        assert!(template.contains("SELECT"));
        assert!(template.contains("max_age: 3600"));
    }
}
