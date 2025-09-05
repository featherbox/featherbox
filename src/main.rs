use anyhow::Result;
use clap::{Parser, Subcommand};
use featherbox::{
    commands::{
        new::{create_gitignore, create_secret_key},
        samples::create_samples,
    },
    config::{Config, ProjectConfig},
};

pub mod api;
pub mod commands;
pub mod config;
pub mod dependency;
pub mod metadata;
pub mod pipeline;
pub mod s3_client;
pub mod secret;
pub mod status;
pub mod test_helpers;
pub mod ui;
pub mod workspace;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    New {
        project_name: String,
    },
    Start {
        project_name: String,
        #[arg(short, long, default_value = "3015")]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::New { project_name } => {
            let mut config = Config::new();
            config
                .add_project_setting(&ProjectConfig::default())?
                .save()?;

            create_secret_key()?;
            create_gitignore()?;
            create_samples()?;

            println!("✓ Project '{project_name}' created successfully");
            println!("  Run 'featherbox start {project_name}' to open the project");
            Ok(())
        }
        Commands::Start { project_name, port } => {
            commands::start::execute_start(project_name, *port).await
        }
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
