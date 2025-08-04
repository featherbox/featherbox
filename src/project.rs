use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub fn find_project_root(start_path: Option<&Path>) -> Result<PathBuf> {
    let current_dir = std::env::current_dir()?;
    let start = start_path.unwrap_or(&current_dir);

    let project_file = start.join("project.yml");
    if project_file.exists() {
        return Ok(start.to_path_buf());
    }

    Err(anyhow::anyhow!("Not in a FeatherBox project directory"))
}

pub fn ensure_project_directory(path: Option<&Path>) -> Result<PathBuf> {
    find_project_root(path).context("This command must be run inside a FeatherBox project")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile;

    #[test]
    fn test_find_project_root_in_project_directory() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::write(project_path.join("project.yml"), "test")?;

        let result = find_project_root(Some(project_path))?;
        assert_eq!(result, project_path);

        Ok(())
    }

    #[test]
    fn test_find_project_root_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path();

        let result = find_project_root(Some(project_path));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Not in a FeatherBox project")
        );
    }

    #[test]
    fn test_ensure_project_directory_success() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::write(project_path.join("project.yml"), "test")?;

        let result = ensure_project_directory(Some(project_path))?;
        assert_eq!(result, project_path);

        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let project_path = temp_dir.path();

        let result = ensure_project_directory(Some(project_path));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("This command must be run inside a FeatherBox project")
        );
    }
}
