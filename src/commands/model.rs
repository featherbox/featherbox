use anyhow::{Context, Result};
use inquire::{Confirm, Select, Text};
use std::fs;
use std::path::Path;

use super::validate_name;
use crate::commands::workspace::ensure_project_directory;

pub async fn execute_model_new(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;

    let model_name = Text::new("Model name:").prompt()?;

    if model_name.trim().is_empty() {
        println!("Model creation cancelled.");
        return Ok(());
    }

    validate_name(&model_name)?;

    let description = Text::new("Description:").with_default("").prompt()?;

    let model_file = project_root
        .join("models")
        .join(format!("{model_name}.yml"));

    if model_file.exists() {
        let overwrite = Confirm::new(&format!(
            "Model '{model_name}' already exists. Do you want to overwrite it?"
        ))
        .with_default(false)
        .prompt()?;

        if !overwrite {
            println!("Model creation cancelled.");
            return Ok(());
        }
    }

    let template = create_model_template(&model_name, &description);
    fs::write(&model_file, template)
        .with_context(|| format!("Failed to create model file: {model_file:?}"))?;

    println!("✓ Model '{model_name}' created successfully");
    Ok(())
}

pub async fn execute_model_delete(project_path: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(project_path))?;
    let models_dir = project_root.join("models");

    if !models_dir.exists() {
        println!("No models found to delete.");
        return Ok(());
    }

    let model_files = collect_model_files(&models_dir)?;

    if model_files.is_empty() {
        println!("No models found to delete.");
        return Ok(());
    }

    let selected_model = Select::new("Select model to delete:", model_files.clone()).prompt()?;

    let confirm = Confirm::new(&format!(
        "Are you sure you want to delete '{selected_model}'?"
    ))
    .with_default(false)
    .prompt()?;

    if !confirm {
        println!("Model deletion cancelled.");
        return Ok(());
    }

    let model_file_path = find_model_file_path(&models_dir, &selected_model)?;
    fs::remove_file(&model_file_path)
        .with_context(|| format!("Failed to delete model file: {model_file_path:?}"))?;

    println!("✓ Model '{selected_model}' deleted successfully");
    Ok(())
}

fn collect_model_files(models_dir: &Path) -> Result<Vec<String>> {
    let mut model_files = Vec::new();

    if models_dir.exists() {
        collect_yml_files_recursive(models_dir, models_dir, &mut model_files)?;
    }

    model_files.sort();
    Ok(model_files)
}

fn collect_yml_files_recursive(
    base_dir: &Path,
    current_dir: &Path,
    model_files: &mut Vec<String>,
) -> Result<()> {
    let entries = fs::read_dir(current_dir)
        .with_context(|| format!("Failed to read directory: {current_dir:?}"))?;

    for entry in entries {
        let entry =
            entry.with_context(|| format!("Failed to read directory entry in {current_dir:?}"))?;
        let path = entry.path();

        if path.is_dir() {
            collect_yml_files_recursive(base_dir, &path, model_files)?;
        } else if let Some(extension) = path.extension()
            && (extension == "yml" || extension == "yaml")
        {
            let relative_path = path
                .strip_prefix(base_dir)
                .with_context(|| "Failed to create relative path")?;
            let display_name = crate::config::generate_node_name_from_path(relative_path);
            model_files.push(display_name);
        }
    }

    Ok(())
}

fn find_model_file_path(models_dir: &Path, selected_model: &str) -> Result<std::path::PathBuf> {
    let yml_path = models_dir.join(format!("{selected_model}.yml"));
    let yaml_path = models_dir.join(format!("{selected_model}.yaml"));

    if yml_path.exists() {
        Ok(yml_path)
    } else if yaml_path.exists() {
        Ok(yaml_path)
    } else {
        Err(anyhow::anyhow!("Model file not found: {}", selected_model))
    }
}

fn create_model_template(name: &str, description: &str) -> String {
    let description = if description.is_empty() {
        format!("Generated model for {name}")
    } else {
        description.to_string()
    };

    format!(
        r#"description: '{description}'
sql: |
  SELECT COUNT(*) FROM source_table
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_model_template_with_description() {
        let template = create_model_template("test_model", "A test model");

        assert!(template.contains("description: 'A test model'"));
        assert!(template.contains("SELECT COUNT(*) FROM source_table"));
    }

    #[test]
    fn test_create_model_template_without_description() {
        let template = create_model_template("test_model", "");

        assert!(template.contains("description: 'Generated model for test_model'"));
        assert!(template.contains("SELECT COUNT(*) FROM source_table"));
    }

    #[test]
    fn test_create_model_template_edge_cases() {
        let template_special_chars = create_model_template("test_model", "Test with 'quotes'");
        assert!(template_special_chars.contains("description: 'Test with 'quotes''"));
    }

    #[test]
    fn test_template_consistency() {
        let template = create_model_template("test_model", "");

        assert!(template.contains("description: 'Generated model for test_model'"));
        assert!(template.contains("SELECT COUNT(*) FROM source_table"));
        assert!(template.contains("sql: |"));
    }

    #[test]
    fn test_collect_model_files() {
        let temp_dir = tempdir().unwrap();
        let models_dir = temp_dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        fs::write(models_dir.join("model1.yml"), "content").unwrap();
        fs::write(models_dir.join("model2.yaml"), "content").unwrap();

        let staging_dir = models_dir.join("staging");
        fs::create_dir_all(&staging_dir).unwrap();
        fs::write(staging_dir.join("staging_model.yml"), "content").unwrap();

        let files = collect_model_files(&models_dir).unwrap();

        assert_eq!(files.len(), 3);
        assert!(files.contains(&"model1".to_string()));
        assert!(files.contains(&"model2".to_string()));
        assert!(files.contains(&"staging_staging_model".to_string()));
    }

    #[test]
    fn test_collect_model_files_empty_directory() {
        let temp_dir = tempdir().unwrap();
        let models_dir = temp_dir.path().join("models");

        let files = collect_model_files(&models_dir).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_find_model_file_path() {
        let temp_dir = tempdir().unwrap();
        let models_dir = temp_dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        fs::write(models_dir.join("test.yml"), "content").unwrap();
        fs::write(models_dir.join("test2.yaml"), "content").unwrap();

        let yml_path = find_model_file_path(&models_dir, "test").unwrap();
        assert_eq!(yml_path, models_dir.join("test.yml"));

        let yaml_path = find_model_file_path(&models_dir, "test2").unwrap();
        assert_eq!(yaml_path, models_dir.join("test2.yaml"));

        let result = find_model_file_path(&models_dir, "nonexistent");
        assert!(result.is_err());
    }
}
