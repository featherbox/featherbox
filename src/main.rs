use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod commands;
pub mod config;
pub mod database;
pub mod dependency;
pub mod pipeline;
pub mod s3_client;
pub mod secret;

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
    Connection {
        #[command(subcommand)]
        action: ConnectionAction,
    },
    Secret {
        #[command(subcommand)]
        action: SecretAction,
    },
}

#[derive(Subcommand)]
enum AdapterAction {
    New,
    Delete { name: String },
}

#[derive(Subcommand)]
enum ConnectionAction {
    New,
    Delete,
}

#[derive(Subcommand)]
enum ModelAction {
    New,
    Delete,
}

#[derive(Subcommand)]
enum SecretAction {
    New,
    Edit,
    Delete,
    List,
    GenKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .init();

    let cli = Cli::parse();
    let current_dir = std::env::current_dir()?;

    let result = match &cli.command {
        Commands::Init { name } => commands::init::execute_init(name.as_deref(), &current_dir),
        Commands::Adapter { action } => match action {
            AdapterAction::New => {
                commands::adapter::execute_adapter_interactive(&current_dir).await
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
        Commands::Connection { action } => match action {
            ConnectionAction::New => commands::connection::execute_connection(&current_dir).await,
            ConnectionAction::Delete => {
                commands::connection::execute_connection_delete(&current_dir).await
            }
        },
        Commands::Secret { action } => match action {
            SecretAction::New => commands::secret::execute_secret_new(&current_dir).await,
            SecretAction::Edit => commands::secret::execute_secret_edit(&current_dir).await,
            SecretAction::Delete => commands::secret::execute_secret_delete(&current_dir).await,
            SecretAction::List => commands::secret::execute_secret_list(&current_dir).await,
            SecretAction::GenKey => commands::secret::execute_secret_gen_key(&current_dir).await,
        },
    };

    if let Err(err) = result {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}
