use crate::commands::{run::connect_ducklake, workspace::ensure_project_directory};
use crate::config::{Config, QueryConfig};
use anyhow::Result;
use std::fs;
use std::path::Path;

pub async fn execute_query(sql: &str, current_dir: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let config = Config::load_from_directory(&project_root)?;

    let ducklake = connect_ducklake(&config).await?;

    let results = ducklake.query(sql)?;

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    for row in results {
        let row_str = row.join("\t");
        println!("{row_str}");
    }

    Ok(())
}

pub fn list_queries(current_dir: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let config = Config::load_from_directory(&project_root)?;

    if config.queries.is_empty() {
        println!("No queries found.");
        return Ok(());
    }

    println!("Available queries:");
    for (name, query_config) in &config.queries {
        match &query_config.description {
            Some(desc) => println!("  {} - {}", name, desc),
            None => println!("  {}", name),
        }
    }

    Ok(())
}

pub fn save_query(name: &str, sql: &str, description: Option<String>, current_dir: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let queries_dir = project_root.join("queries");
    
    if !queries_dir.exists() {
        fs::create_dir_all(&queries_dir)?;
    }

    let query_config = QueryConfig {
        name: name.to_string(),
        description,
        sql: sql.to_string(),
    };

    let yaml_content = serde_yml::to_string(&query_config)?;
    let query_file = queries_dir.join(format!("{}.yml", name));
    
    if query_file.exists() {
        return Err(anyhow::anyhow!("Query '{}' already exists. Use update command to modify it.", name));
    }

    fs::write(&query_file, yaml_content)?;
    println!("Query '{}' saved successfully.", name);

    Ok(())
}

pub fn load_query(name: &str, current_dir: &Path) -> Result<String> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let config = Config::load_from_directory(&project_root)?;

    match config.queries.get(name) {
        Some(query_config) => Ok(query_config.sql.clone()),
        None => Err(anyhow::anyhow!("Query '{}' not found.", name)),
    }
}

pub async fn run_query(name: &str, current_dir: &Path) -> Result<()> {
    let sql = load_query(name, current_dir)?;
    execute_query(&sql, current_dir).await
}

pub fn delete_query(name: &str, current_dir: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let queries_dir = project_root.join("queries");
    let query_file = queries_dir.join(format!("{}.yml", name));

    if !query_file.exists() {
        return Err(anyhow::anyhow!("Query '{}' not found.", name));
    }

    fs::remove_file(&query_file)?;
    println!("Query '{}' deleted successfully.", name);

    Ok(())
}

pub fn update_query(name: &str, sql: Option<String>, description: Option<String>, current_dir: &Path) -> Result<()> {
    let project_root = ensure_project_directory(Some(current_dir))?;
    let queries_dir = project_root.join("queries");
    let query_file = queries_dir.join(format!("{}.yml", name));

    if !query_file.exists() {
        return Err(anyhow::anyhow!("Query '{}' not found.", name));
    }

    let config = Config::load_from_directory(&project_root)?;
    let mut query_config = config.queries.get(name)
        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found.", name))?
        .clone();

    if let Some(new_sql) = sql {
        query_config.sql = new_sql;
    }

    if let Some(new_description) = description {
        query_config.description = Some(new_description);
    }

    let yaml_content = serde_yml::to_string(&query_config)?;
    fs::write(&query_file, yaml_content)?;
    println!("Query '{}' updated successfully.", name);

    Ok(())
}
