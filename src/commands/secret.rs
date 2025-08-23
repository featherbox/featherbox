use anyhow::Result;
use inquire::{Password, Select, Text};
use std::path::Path;

use crate::commands::workspace::find_project_root;
use crate::secret::SecretManager;

pub async fn execute_secret_new(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    ensure_key_exists(&project_root)?;

    let key = Text::new("Secret key:")
        .with_help_message("Use letters, numbers, and underscores (e.g., db_password, API_KEY)")
        .prompt()?;

    if key.trim().is_empty() {
        println!("Secret creation cancelled.");
        return Ok(());
    }

    validate_secret_key(&key)?;

    let value = Password::new("Secret value:").prompt()?;

    if value.is_empty() {
        println!("Secret creation cancelled.");
        return Ok(());
    }

    let manager = SecretManager::new(&project_root)?;

    let existing = manager.get_secret(&key)?;
    if existing.is_some() {
        let options = vec!["Yes, overwrite", "No, cancel"];
        let choice = Select::new(
            &format!("Secret '{key}' already exists. Overwrite?"),
            options,
        )
        .prompt()?;

        if choice == "No, cancel" {
            println!("Secret creation cancelled.");
            return Ok(());
        }
    }

    manager.set_secret(&key, &value)?;
    println!("✓ Secret '{key}' saved successfully");

    Ok(())
}

pub async fn execute_secret_edit(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    ensure_key_exists(&project_root)?;

    let manager = SecretManager::new(&project_root)?;
    let keys = manager.list_secrets()?;

    if keys.is_empty() {
        println!("No secrets found. Use 'fbox secret new' to add secrets.");
        return Ok(());
    }

    let selected_key = Select::new("Select secret to edit:", keys).prompt()?;

    let current_value = manager
        .get_secret(&selected_key)?
        .unwrap_or_else(|| "".to_string());

    println!("Current value: {}", mask_value(&current_value));

    let new_value = Password::new("New secret value:").prompt()?;

    if new_value.is_empty() {
        println!("Secret edit cancelled.");
        return Ok(());
    }

    manager.set_secret(&selected_key, &new_value)?;
    println!("✓ Secret '{selected_key}' updated successfully");

    Ok(())
}

pub async fn execute_secret_delete(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    ensure_key_exists(&project_root)?;

    let manager = SecretManager::new(&project_root)?;
    let keys = manager.list_secrets()?;

    if keys.is_empty() {
        println!("No secrets found.");
        return Ok(());
    }

    let selected_key = Select::new("Select secret to delete:", keys).prompt()?;

    let options = vec!["Yes, delete", "No, cancel"];
    let choice = Select::new(
        &format!("Are you sure you want to delete secret '{selected_key}'?"),
        options,
    )
    .prompt()?;

    if choice == "No, cancel" {
        println!("Secret deletion cancelled.");
        return Ok(());
    }

    let removed = manager.delete_secret(&selected_key)?;
    if removed {
        println!("✓ Secret '{selected_key}' deleted successfully");
    } else {
        println!("Secret '{selected_key}' not found");
    }

    Ok(())
}

pub async fn execute_secret_gen_key(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;
    let manager = SecretManager::new(&project_root)?;

    if manager.key_exists() {
        let options = vec!["Yes, regenerate", "No, cancel"];
        let choice = Select::new(
            "Secret key already exists. Regenerating will make existing secrets inaccessible. Continue?",
            options,
        ).prompt()?;

        if choice == "No, cancel" {
            println!("Key regeneration cancelled.");
            return Ok(());
        }
    }

    manager.generate_key()?;
    println!("✓ New secret key generated successfully");

    if manager.key_exists() {
        println!("Warning: Existing encrypted secrets will no longer be accessible.");
    }

    Ok(())
}

pub async fn execute_secret_list(current_dir: &Path) -> Result<()> {
    let project_root = find_project_root(Some(current_dir))?;

    ensure_key_exists(&project_root)?;

    let manager = SecretManager::new(&project_root)?;
    let keys = manager.list_secrets()?;

    if keys.is_empty() {
        println!("No secrets found.");
        return Ok(());
    }

    println!("Available secrets:");
    for key in keys {
        let value = manager.get_secret(&key)?.unwrap_or_default();
        println!("  {} = {}", key, mask_value(&value));
    }

    Ok(())
}

fn ensure_key_exists(project_root: &Path) -> Result<()> {
    let manager = SecretManager::new(project_root)?;
    if !manager.key_exists() {
        return Err(anyhow::anyhow!(
            "Secret key not found. Run 'fbox secret gen-key' first."
        ));
    }
    Ok(())
}

fn validate_secret_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(anyhow::anyhow!("Secret key cannot be empty"));
    }

    if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(anyhow::anyhow!(
            "Secret key must contain only letters, numbers, and underscores"
        ));
    }

    if key.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(anyhow::anyhow!("Secret key cannot start with a number"));
    }

    Ok(())
}

fn mask_value(value: &str) -> String {
    if value.len() <= 4 {
        "*".repeat(value.len())
    } else {
        format!("{}***{}", &value[..2], &value[value.len() - 2..])
    }
}
