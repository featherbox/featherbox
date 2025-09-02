use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod api;
pub mod commands;
pub mod config;
pub mod database;
pub mod dependency;
pub mod pipeline;
pub mod s3_client;
pub mod secret;
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
        #[arg(long, help = "Path to secret key file")]
        secret_key_path: Option<String>,
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
        Commands::New {
            project_name,
            secret_key_path: _,
        } => {
            let config = crate::config::ProjectConfig::new();
            config.validate()?;

            let builder = commands::init::ProjectBuilder::new(project_name.clone(), &config)?;
            builder.create_project_directory()?;
            builder.create_secret_key()?;
            builder.save_project_config()?;
            builder.create_gitignore()?;
            builder.create_sample_data()?;
            builder.create_sample_adapters()?;
            builder.create_sample_models()?;
            builder.create_sample_queries()?;
            builder.create_sample_dashboards()?;

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
