use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod ui {
    use axum::http::{StatusCode, Uri, header};
    use axum::response::{IntoResponse, Response};
    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "src/ui/dist/"]
    #[exclude = "node_modules/*"]
    pub struct Assets;

    pub async fn static_handler(uri: Uri) -> impl IntoResponse {
        let path = uri.path().trim_start_matches('/');

        if path.is_empty() || path == "index.html" {
            return index_html().await;
        }

        match Assets::get(path) {
            Some(content) => {
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                Response::builder()
                    .header(header::CONTENT_TYPE, mime.as_ref())
                    .body(content.data.into())
                    .unwrap()
            }
            None => index_html().await,
        }
    }

    async fn index_html() -> Response {
        if let Some(content) = Assets::get("index.html") {
            Response::builder()
                .header(header::CONTENT_TYPE, "text/html")
                .body(content.data.into())
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("index.html not found".into())
                .unwrap()
        }
    }
}

pub mod api;
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
    Server,
    New {
        project_name: String,
        #[arg(long, help = "Path to secret key file")]
        secret_key_path: Option<String>,
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
    Start {
        project_name: String,
        #[arg(short, long, default_value = "3000")]
        port: u16,
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
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let cli = Cli::parse();
    let current_dir = std::env::current_dir()?;

    let result = match &cli.command {
        Commands::Server => {
            api::main().await?;
            Ok(())
        }
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

            println!("✓ Project '{project_name}' created successfully");
            println!("  Run 'fbox start {project_name}' to open the project");
            Ok(())
        }
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
