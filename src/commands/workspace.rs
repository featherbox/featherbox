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

    #[test]
    fn test_find_project_root_with_none_path() -> Result<()> {
        let result = find_project_root(None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Not in a FeatherBox project")
        );

        Ok(())
    }

    #[test]
    fn test_find_project_root_absolute_path() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::write(project_path.join("project.yml"), "test_config: true")?;

        let result = find_project_root(Some(project_path))?;
        assert_eq!(result.canonicalize()?, project_path.canonicalize()?);

        Ok(())
    }

    #[test]
    fn test_find_project_root_nested_directory_structure() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let root_path = temp_dir.path();
        let nested_path = root_path.join("level1").join("level2").join("level3");
        fs::create_dir_all(&nested_path)?;

        fs::write(root_path.join("project.yml"), "project: test")?;

        let result = find_project_root(Some(root_path));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), root_path);

        Ok(())
    }

    #[test]
    fn test_find_project_root_no_project_file() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let result = find_project_root(Some(project_path));
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Not in a FeatherBox project"));

        Ok(())
    }

    #[test]
    fn test_find_project_root_empty_project_file() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        fs::write(project_path.join("project.yml"), "")?;

        let result = find_project_root(Some(project_path))?;
        assert_eq!(result, project_path);

        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_with_none_path() {
        let result = ensure_project_directory(None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("This command must be run inside a FeatherBox project")
        );
    }

    #[test]
    fn test_ensure_project_directory_context_message() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let result = ensure_project_directory(Some(project_path));
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("This command must be run inside a FeatherBox project"));

        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_preserves_original_error() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_path = temp_dir.path();

        let result = ensure_project_directory(Some(project_path));
        assert!(result.is_err());

        let error = result.unwrap_err();
        let error_chain: Vec<String> = error.chain().map(|e| e.to_string()).collect();
        assert!(
            error_chain
                .iter()
                .any(|msg| msg.contains("Not in a FeatherBox project"))
        );
        assert!(
            error_chain
                .iter()
                .any(|msg| msg.contains("This command must be run inside a FeatherBox project"))
        );

        Ok(())
    }
}
