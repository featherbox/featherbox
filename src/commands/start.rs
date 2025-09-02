use anyhow::{Context, Result};
use std::path::Path;
use std::process::{Command, Stdio};
use tokio::task::JoinHandle;

pub async fn execute_start(project_name: &str, port: u16) -> Result<()> {
    let project_path = Path::new(project_name);

    if !project_path.exists() {
        return Err(anyhow::anyhow!(
            "Project '{}' not found. Use 'featherbox new {}' to create it.",
            project_name,
            project_name
        ));
    }

    if !project_path.join("project.yml").exists() {
        return Err(anyhow::anyhow!(
            "Directory '{}' is not a valid Featherbox project (missing project.yml)",
            project_name
        ));
    }

    println!("Starting Featherbox for project '{project_name}'...");

    // Change to project directory
    std::env::set_current_dir(project_path)
        .with_context(|| format!("Failed to change to project directory: {project_name}"))?;

    // Start the API server in the background
    let _api_server = Command::new("featherbox")
        .arg("server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("Failed to start API server")?;

    // Wait a moment for the API server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("✓ API server started on port {port}");

    // Start the UI server
    let ui_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { crate::ui::start_ui_server().await });

    // Wait a moment for the UI server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("✓ Opening browser at http://localhost:8015");

    // Try to open the browser
    let browser_result = if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/c", "start", "http://localhost:8015"])
            .spawn()
    } else if cfg!(target_os = "macos") {
        Command::new("open").arg("http://localhost:8015").spawn()
    } else {
        // Linux and other Unix-like systems
        Command::new("xdg-open")
            .arg("http://localhost:8015")
            .spawn()
            .or_else(|_| {
                // Fallback options for Linux
                Command::new("firefox").arg("http://localhost:8015").spawn()
            })
            .or_else(|_| {
                Command::new("chromium")
                    .arg("http://localhost:8015")
                    .spawn()
            })
    };

    match browser_result {
        Ok(_) => {
            println!("✓ Browser opened successfully");
        }
        Err(e) => {
            println!("⚠ Could not open browser automatically: {e}");
            println!("  Please manually open: http://localhost:8015");
        }
    }

    println!("\n🚀 Featherbox is running!");
    println!("   Project: {project_name}");
    println!("   API: http://localhost:{port}");
    println!("   UI: http://localhost:8015");
    println!("\nPress Ctrl+C to stop");

    // Handle shutdown
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
        }
        ui_result = ui_handle => {
            match ui_result {
                Ok(Ok(())) => println!("UI server stopped"),
                Ok(Err(e)) => println!("UI server error: {e}"),
                Err(e) => println!("UI server task error: {e}"),
            }
        }
    }

    Ok(())
}
