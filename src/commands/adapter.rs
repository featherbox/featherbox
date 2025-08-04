use anyhow::{Context, Result};
use std::fs;

use super::{render_adapter_template, validate_name};
use crate::project::ensure_project_directory;

pub fn execute_adapter_new(name: &str) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(None)?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if adapter_file.exists() {
        return Err(anyhow::anyhow!("Adapter '{}' already exists", name));
    }

    let template = render_adapter_template(name);
    fs::write(&adapter_file, template)
        .with_context(|| format!("Failed to create adapter file: {adapter_file:?}"))?;

    println!("Created adapter: {name}");
    Ok(())
}

pub fn execute_adapter_delete(name: &str) -> Result<()> {
    validate_name(name)?;

    let project_root = ensure_project_directory(None)?;
    let adapter_file = project_root.join("adapters").join(format!("{name}.yml"));

    if !adapter_file.exists() {
        return Err(anyhow::anyhow!("Adapter '{}' does not exist", name));
    }

    fs::remove_file(&adapter_file)
        .with_context(|| format!("Failed to delete adapter file: {adapter_file:?}"))?;

    println!("Deleted adapter: {name}");
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
    fn test_execute_adapter_new_success() -> Result<()> {
        let temp_dir = setup_test_project()?;
        let original_dir = std::env::current_dir()?;

        let result = {
            std::env::set_current_dir(temp_dir.path())?;
            let result = execute_adapter_new("test_logs");
            std::env::set_current_dir(&original_dir)?;
            result
        };

        assert!(result.is_ok());

        let adapter_file = temp_dir.path().join("adapters/test_logs.yml");
        assert!(adapter_file.exists());

        let content = fs::read_to_string(adapter_file)?;
        assert!(content.contains("Generated adapter for test_logs"));
        assert!(content.contains("connection:"));
        assert!(content.contains("format:"));

        Ok(())
    }

    #[test]
    fn test_execute_adapter_new_already_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;
        let original_dir = std::env::current_dir()?;

        fs::write(temp_dir.path().join("adapters/existing.yml"), "test")?;

        std::env::set_current_dir(temp_dir.path())?;

        let result = execute_adapter_new("existing");

        std::env::set_current_dir(&original_dir)?;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_execute_adapter_delete_success() -> Result<()> {
        let temp_dir = setup_test_project()?;
        let original_dir = std::env::current_dir()?;

        fs::write(temp_dir.path().join("adapters/to_delete.yml"), "test")?;

        std::env::set_current_dir(temp_dir.path())?;

        let result = execute_adapter_delete("to_delete");

        std::env::set_current_dir(&original_dir)?;

        assert!(result.is_ok());
        assert!(!temp_dir.path().join("adapters/to_delete.yml").exists());

        Ok(())
    }

    #[test]
    fn test_execute_adapter_delete_not_exists() -> Result<()> {
        let temp_dir = setup_test_project()?;
        let original_dir = std::env::current_dir()?;

        std::env::set_current_dir(temp_dir.path())?;

        let result = execute_adapter_delete("nonexistent");

        std::env::set_current_dir(&original_dir)?;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));

        Ok(())
    }

    #[test]
    fn test_render_adapter_template() {
        let template = super::render_adapter_template("test_adapter");

        assert!(template.contains("Generated adapter for test_adapter"));
        assert!(template.contains("connection: <CONNECTION_NAME>"));
        assert!(template.contains("type: csv"));
        assert!(template.contains("has_header: true"));
    }
}
