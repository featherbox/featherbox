use anyhow::{Context, Result};
use std::cell::RefCell;
use std::path::PathBuf;

thread_local! {
    static PROJECT_DIR_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub fn set_project_dir_override(path: PathBuf) {
    PROJECT_DIR_OVERRIDE.with(|p| {
        *p.borrow_mut() = Some(path);
    });
}

#[cfg(test)]
pub fn clear_project_dir_override() {
    PROJECT_DIR_OVERRIDE.with(|p| {
        *p.borrow_mut() = None;
    });
}

pub fn project_dir() -> Result<PathBuf> {
    if let Some(path) = PROJECT_DIR_OVERRIDE.with(|p| p.borrow().clone()) {
        return Ok(path);
    }

    if let Ok(path) = std::env::var("FEATHERBOX_PROJECT_DIRECTORY") {
        return Ok(PathBuf::from(path));
    }
    Ok(std::env::current_dir()?)
}

pub fn find_project_root() -> Result<PathBuf> {
    let project_dir = project_dir()?;

    let project_file = project_dir.join("project.yml");
    if project_file.exists() {
        return Ok(project_dir.to_path_buf());
    }

    Err(anyhow::anyhow!("Not in a FeatherBox project directory"))
}

pub fn ensure_project_directory() -> Result<PathBuf> {
    find_project_root().context("This command must be run inside a FeatherBox project")
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

        set_project_dir_override(project_path.to_path_buf());
        let result = find_project_root()?;
        clear_project_dir_override();

        assert_eq!(result, project_path);

        Ok(())
    }

    #[test]
    fn test_find_project_root_not_found() {
        let result = find_project_root();
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
        set_project_dir_override(project_path.to_path_buf());

        let result = ensure_project_directory()?;
        assert_eq!(result, project_path);

        clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_failure() {
        let result = ensure_project_directory();
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
        let result = find_project_root();
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
        set_project_dir_override(project_path.to_path_buf());

        let result = find_project_root()?;
        assert_eq!(result.canonicalize()?, project_path.canonicalize()?);

        clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_find_project_root_nested_directory_structure() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let root_path = temp_dir.path();
        let nested_path = root_path.join("level1").join("level2").join("level3");
        fs::create_dir_all(&nested_path)?;

        fs::write(root_path.join("project.yml"), "project: test")?;
        set_project_dir_override(root_path.to_path_buf());

        let result = find_project_root();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), root_path);

        clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_find_project_root_no_project_file() -> Result<()> {
        let result = find_project_root();
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
        set_project_dir_override(project_path.to_path_buf());

        let result = find_project_root()?;
        assert_eq!(result, project_path);

        clear_project_dir_override();
        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_with_none_path() {
        let result = ensure_project_directory();
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
        let result = ensure_project_directory();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("This command must be run inside a FeatherBox project"));

        Ok(())
    }

    #[test]
    fn test_ensure_project_directory_preserves_original_error() -> Result<()> {
        let result = ensure_project_directory();
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
