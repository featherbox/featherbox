use anyhow::Result;
use axum::{
    Router,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tower_http::cors::{Any, CorsLayer};

mod adapter;
mod connection;
mod dashboard;
mod migrate;
mod model;
mod pipeline;
mod query;
mod run;
mod secret;

pub enum AppError {
    StatusCode(StatusCode),
    Exception(anyhow::Error),
}

pub fn app_error<T>(status_code: StatusCode) -> Result<T, AppError> {
    Err(AppError::StatusCode(status_code))
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::StatusCode(status_code) => status_code.into_response(),
            AppError::Exception(error) => {
                tracing::error!("{}", error);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::Exception(err.into())
    }
}

pub async fn main() -> Result<()> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_routes = Router::new()
        .merge(adapter::routes())
        .merge(connection::routes())
        .merge(dashboard::router())
        .merge(model::routes())
        .merge(query::routes())
        .merge(secret::routes())
        .merge(pipeline::routes());

    let app = Router::new().nest("/api", api_routes).layer(cors);

    let port = 3015;
    let listener = tokio::net::TcpListener::bind(format!("localhost:{}", port))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to port {}: {}", port, e))?;

    println!("API server listening on http://localhost:{}", port);
    axum::serve(listener, app).await?;

    Ok(())
}
