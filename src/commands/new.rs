use anyhow::{Context, Result};
use std::{fs, path::Path};

pub fn create_gitignore(project_dir: &Path) -> Result<()> {
    let gitignore_content = ".secret.key\nstorage/\ndatabase.db\nsample_data/\n";

    fs::write(project_dir.join(".gitignore"), gitignore_content)
        .context("Failed to write .gitignore")?;

    Ok(())
}

pub fn create_secret_key(project_dir: &Path) -> Result<()> {
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
    use ring::rand::{SecureRandom, SystemRandom};

    let key_path = project_dir.join(".secret.key");

    let mut key_bytes = [0u8; 32];
    let rng = SystemRandom::new();
    rng.fill(&mut key_bytes)
        .map_err(|_| anyhow::anyhow!("Failed to generate random key"))?;

    let key_base64 = BASE64.encode(key_bytes);

    let key_content = format!(
        "# FeatherBox Secret Key\n# DO NOT share publicly\n\n{}",
        key_base64
    );

    fs::write(&key_path, key_content)
        .with_context(|| format!("Failed to write key file: {}", key_path.display()))?;

    Ok(())
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::project::StorageConfig;
//     use std::process::Command;
//     use tempfile;
//
//     fn get_featherbox_binary() -> PathBuf {
//         let manifest_dir = env!("CARGO_MANIFEST_DIR");
//         PathBuf::from(manifest_dir).join("target/debug/featherbox")
//     }
//
//     fn run_featherbox_command(
//         args: &[&str],
//         working_dir: &std::path::Path,
//     ) -> Result<(bool, String)> {
//         let featherbox_binary = get_featherbox_binary();
//
//         if !featherbox_binary.exists() {
//             let build_output = Command::new("cargo").arg("build").output()?;
//             if !build_output.status.success() {
//                 anyhow::bail!("Failed to build featherbox binary");
//             }
//         }
//
//         let mut cmd = Command::new(&featherbox_binary);
//         cmd.args(args).current_dir(working_dir);
//
//         let output = cmd.output()?;
//         let stdout = String::from_utf8_lossy(&output.stdout);
//         let stderr = String::from_utf8_lossy(&output.stderr);
//         let combined_output = format!("{stdout}{stderr}");
//
//         Ok((output.status.success(), combined_output))
//     }
//
//     #[test]
//     fn test_featherbox_new_command() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_name = "test_new_project";
//
//         let (success, output) = run_featherbox_command(&["new", project_name], temp_dir.path())?;
//
//         if !success {
//             anyhow::bail!("featherbox new failed: {}", output);
//         }
//
//         let project_dir = temp_dir.path().join(project_name);
//         assert!(project_dir.exists(), "Project directory should be created");
//         assert!(
//             project_dir.join("project.yml").exists(),
//             "project.yml should exist"
//         );
//         assert!(
//             project_dir.join(".secret.key").exists(),
//             "secret key should be created"
//         );
//         assert!(
//             project_dir.join("adapters").exists(),
//             "adapters directory should exist"
//         );
//         assert!(
//             project_dir.join("models").exists(),
//             "models directory should exist"
//         );
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_featherbox_help() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//
//         let (success, output) = run_featherbox_command(&["--help"], temp_dir.path())?;
//
//         assert!(success, "Help command should succeed");
//         assert!(output.contains("new"), "Help should mention 'new' command");
//         assert!(
//             output.contains("start"),
//             "Help should mention 'start' command"
//         );
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_featherbox_version() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//
//         let (success, output) = run_featherbox_command(&["--version"], temp_dir.path())?;
//
//         assert!(success, "Version command should succeed");
//         assert!(
//             output.contains("featherbox"),
//             "Version should mention featherbox"
//         );
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_project_structure_after_new() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_name = "structure_test_project";
//
//         let (success, _) = run_featherbox_command(&["new", project_name], temp_dir.path())?;
//         assert!(success, "Project creation should succeed");
//
//         let project_dir = temp_dir.path().join(project_name);
//
//         let project_yml_content = fs::read_to_string(project_dir.join("project.yml"))?;
//         assert!(project_yml_content.contains("storage:"));
//         assert!(project_yml_content.contains("database:"));
//         assert!(project_yml_content.contains("connections:"));
//
//         let gitignore_content = fs::read_to_string(project_dir.join(".gitignore"))?;
//         assert!(gitignore_content.contains("storage/"));
//         assert!(gitignore_content.contains("database.db"));
//
//         let secret_key_path = project_dir.join(".secret.key");
//         assert!(secret_key_path.exists());
//         let secret_key_content = fs::read_to_string(&secret_key_path)?;
//         assert!(
//             !secret_key_content.trim().is_empty(),
//             "Secret key should have content"
//         );
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_project_builder() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_name = "test_project";
//         let config = ProjectConfig::new();
//
//         let builder = ProjectBuilder::with_current_dir(
//             project_name.to_string(),
//             &config,
//             temp_dir.path().to_path_buf(),
//         );
//         builder.create_project_directory()?;
//         builder.create_secret_key()?;
//         builder.save_project_config()?;
//         builder.create_gitignore()?;
//
//         let project_path = temp_dir.path().join(project_name);
//         assert!(project_path.join("project.yml").exists());
//         assert!(project_path.join("adapters").is_dir());
//         assert!(project_path.join("models").is_dir());
//
//         let content = fs::read_to_string(project_path.join("project.yml"))?;
//         assert!(content.contains("storage:"));
//         assert!(content.contains("database:"));
//         assert!(content.contains("connections:"));
//         assert!(content.contains("local_files:"));
//         assert!(content.contains("sample_db:"));
//
//         assert!(project_path.join(".secret.key").exists());
//         assert!(project_path.join(".gitignore").exists());
//
//         let gitignore_content = fs::read_to_string(project_path.join(".gitignore"))?;
//         assert!(gitignore_content.contains(".secret.key"));
//         assert!(gitignore_content.contains("storage/"));
//         assert!(gitignore_content.contains("database.db"));
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_project_builder_already_exists() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_name = "existing_project";
//         let config = ProjectConfig::new();
//
//         fs::create_dir_all(temp_dir.path().join(project_name))?;
//
//         let builder = ProjectBuilder::with_current_dir(
//             project_name.to_string(),
//             &config,
//             temp_dir.path().to_path_buf(),
//         );
//         let result = builder.create_project_directory();
//
//         assert!(result.is_err());
//         assert!(result.unwrap_err().to_string().contains("already exists"));
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_project_builder_directories() -> Result<()> {
//         let temp_dir = tempfile::tempdir()?;
//         let project_name = "test_project";
//         let config = ProjectConfig::new();
//
//         let builder = ProjectBuilder::with_current_dir(
//             project_name.to_string(),
//             &config,
//             temp_dir.path().to_path_buf(),
//         );
//         builder.create_project_directory()?;
//
//         let project_path = temp_dir.path().join(project_name);
//         assert!(project_path.join("adapters").is_dir());
//         assert!(project_path.join("models").is_dir());
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_project_config_validate() -> Result<()> {
//         let mut config = ProjectConfig::default();
//         assert!(config.validate().is_ok());
//
//         config.storage = StorageConfig::LocalFile {
//             path: "".to_string(),
//         };
//         assert!(config.validate().is_err());
//         assert!(
//             config
//                 .validate()
//                 .unwrap_err()
//                 .to_string()
//                 .contains("Storage path cannot be empty")
//         );
//
//         config.storage = StorageConfig::LocalFile {
//             path: "./storage".to_string(),
//         };
//
//         Ok(())
//     }
// }
