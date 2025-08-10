use crate::commands::{run::connect_ducklake, workspace::ensure_project_directory};
use crate::config::Config;
use anyhow::Result;
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
