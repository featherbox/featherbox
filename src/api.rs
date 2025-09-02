use crate::ui::static_handler;
use anyhow::Result;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

mod adapter;
mod connection;
mod model;
mod pipeline;
mod query;
mod secret;

pub async fn main() -> Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .merge(adapter::routes())
        .merge(connection::routes())
        .merge(model::routes())
        .merge(query::routes())
        .merge(secret::routes())
        .merge(pipeline::routes());

    let app = Router::new()
        .nest("/api", api_routes)
        .fallback(static_handler)
        .layer(cors);

    let port = 3015;
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to port {}: {}", port, e))?;

    println!("API server listening on http://0.0.0.0:{}", port);
    axum::serve(listener, app).await?;

    Ok(())
}
