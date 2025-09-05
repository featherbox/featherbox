use anyhow::Result;
use std::process::Command;
use tokio::task::JoinHandle;

use crate::config::Config;

pub async fn execute_start(config: Config, port: u16) -> Result<()> {
    if !config.project_dir.join("project.yml").exists() {
        return Err(anyhow::anyhow!(
            "The directory is not a valid Featherbox project (missing project.yml)",
        ));
    }

    println!("Starting Featherbox for project ...");

    let api_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { crate::api::main(config).await });

    // Wait a moment for the API server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("✓ API server started on port {port}");

    // Start the UI server
    let ui_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { crate::ui::start_ui_server().await });

    // Wait a moment for the UI server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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
    println!("   API: http://localhost:{port}");
    println!("   UI: http://localhost:8015");
    println!("\nPress Ctrl+C to stop");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
        }
        api_result = api_handle => {
            match api_result {
                Ok(Ok(())) => println!("API server stopped"),
                Ok(Err(e)) => eprintln!("API server error: {e}"),
                Err(e) => eprintln!("API server task error: {e}"),
            }
        }
        ui_result = ui_handle => {
            match ui_result {
                Ok(Ok(())) => println!("UI server stopped"),
                Ok(Err(e)) => eprintln!("UI server error: {e}"),
                Err(e) => eprintln!("UI server task error: {e}"),
            }
        }
    }

    Ok(())
}
