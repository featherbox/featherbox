use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod commands;
pub mod config;
pub mod database;
pub mod dependency;
pub mod pipeline;
pub mod s3_client;

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
    Migrate,
    Run,
    Query {
        sql: String,
    },
    Connection,
}

#[derive(Subcommand)]
enum AdapterAction {
    New { name: String },
    Delete { name: String },
}

#[derive(Subcommand)]
enum ModelAction {
    New,
    Delete,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

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
            ModelAction::New => commands::model::execute_model_new(&current_dir).await,
            ModelAction::Delete => commands::model::execute_model_delete(&current_dir).await,
        },
        Commands::Migrate => commands::migrate::migrate(&current_dir).await,
        Commands::Run => commands::run::run(&current_dir).await,
        Commands::Query { sql } => commands::query::execute_query(sql, &current_dir).await,
        Commands::Connection => commands::connection::execute_connection(&current_dir).await,
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
