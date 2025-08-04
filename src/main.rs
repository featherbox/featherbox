use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod commands;
pub mod config;
pub mod ducklake;
pub mod graph;
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
    /// Initialize a new FeatherBox project
    Init {
        /// Project name (optional)
        name: Option<String>,
    },
    /// Manage adapters
    Adapter {
        #[command(subcommand)]
        action: AdapterAction,
    },
    /// Manage models
    Model {
        #[command(subcommand)]
        action: ModelAction,
    },
}

#[derive(Subcommand)]
enum AdapterAction {
    /// Create a new adapter
    New {
        /// Adapter name
        name: String,
    },
    /// Delete an adapter
    Delete {
        /// Adapter name
        name: String,
    },
}

#[derive(Subcommand)]
enum ModelAction {
    /// Create a new model
    New {
        /// Model name
        name: String,
    },
    /// Delete a model
    Delete {
        /// Model name
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::Init { name } => commands::init::execute_init(name.as_deref()),
        Commands::Adapter { action } => match action {
            AdapterAction::New { name } => commands::adapter::execute_adapter_new(name),
            AdapterAction::Delete { name } => commands::adapter::execute_adapter_delete(name),
        },
        Commands::Model { action } => match action {
            ModelAction::New { name } => commands::model::execute_model_new(name),
            ModelAction::Delete { name } => commands::model::execute_model_delete(name),
        },
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
