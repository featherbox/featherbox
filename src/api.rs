use anyhow::Result;
use axum::Router;
use tower_http::cors::{Any, CorsLayer};

pub async fn main() -> Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new().layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("API server listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await?;

    Ok(())
}
