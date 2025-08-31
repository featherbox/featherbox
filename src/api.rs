use crate::ui::static_handler;
use anyhow::Result;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

mod adapter;
mod chat;
mod connection;
mod model;
mod query;
mod secret;

pub async fn main() -> Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let chat_state = chat::AppState::default();

    let api_routes = Router::new()
        .merge(adapter::routes())
        .merge(connection::routes())
        .merge(model::routes())
        .merge(query::routes())
        .merge(secret::routes())
        .merge(chat::config_routes())
        .nest("/chat", chat::routes().with_state(chat_state));

    let app = Router::new()
        .nest("/api", api_routes)
        .fallback(static_handler)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("API server listening on http://0.0.0.0:3000");
    println!("UI available on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;

    Ok(())
}
