use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod commands;
pub mod config;
pub mod database;
pub mod ducklake;
pub mod entities;
pub mod graph;
pub mod impact_analysis;
pub mod metadata;
pub mod migration;
pub mod pipeline;
pub mod project;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        name: Option<String>,
    },
    Adapter {
        #[command(subcommand)]
        action: AdapterAction,
    },
    Model {
        #[command(subcommand)]
        action: ModelAction,
    },
    Run,
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    Up,
    Status,
}

#[derive(Subcommand)]
enum AdapterAction {
    New { name: String },
    Delete { name: String },
}

#[derive(Subcommand)]
enum ModelAction {
    New { name: String },
    Delete { name: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let current_dir = std::env::current_dir()?;

    let result = match &cli.command {
        Commands::Init { name } => commands::init::execute_init(name.as_deref(), &current_dir),
        Commands::Adapter { action } => match action {
            AdapterAction::New { name } => {
                commands::adapter::execute_adapter_new(name, &current_dir)
            }
            AdapterAction::Delete { name } => {
                commands::adapter::execute_adapter_delete(name, &current_dir)
            }
        },
        Commands::Model { action } => match action {
            ModelAction::New { name } => commands::model::execute_model_new(name, &current_dir),
            ModelAction::Delete { name } => {
                commands::model::execute_model_delete(name, &current_dir)
            }
        },
        Commands::Run => commands::run::execute_run(&current_dir).await,
        Commands::Migrate { action } => match action {
            MigrateAction::Up => commands::migrate::execute_migrate_up(&current_dir).await,
            MigrateAction::Status => commands::migrate::execute_migrate_status(&current_dir).await,
        },
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
