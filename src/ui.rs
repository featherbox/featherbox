use anyhow::Result;

#[cfg(debug_assertions)]
mod debug_server {
    use anyhow::{Context, Result};
    use std::process::Stdio;
    use tokio::process::Command as TokioCommand;

    pub async fn start() -> Result<()> {
        let ui_dir = "src/ui";

        println!("Starting UI development server on port 8015...");

        let mut child = TokioCommand::new("pnpm")
            .args(["run", "dev"])
            .current_dir(ui_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Failed to start pnpm dev server. Make sure pnpm is installed and dependencies are installed.")?;

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        println!("✓ UI development server started on http://localhost:8015");

        let _ = child.wait().await;

        Ok(())
    }
}

#[cfg(not(debug_assertions))]
mod production_server {
    use anyhow::{Context, Result};
    use axum::{
        Router,
        http::{StatusCode, Uri, header},
        response::{IntoResponse, Response},
    };
    use rust_embed::RustEmbed;
    use tokio::net::TcpListener;

    #[derive(RustEmbed)]
    #[folder = "src/ui/build/"]
    #[exclude = "node_modules/*"]
    pub struct Assets;

    async fn static_handler(uri: Uri) -> impl IntoResponse {
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

    pub async fn start() -> Result<()> {
        println!("Starting UI production server on port 8015...");

        let app = Router::new().fallback(static_handler);

        let listener = TcpListener::bind("localhost:8015")
            .await
            .context("Failed to bind UI server to localhost:8015")?;

        println!("✓ UI production server started on http://localhost:8015");

        axum::serve(listener, app)
            .await
            .context("UI server error")?;

        Ok(())
    }
}

pub async fn start_ui_server() -> Result<()> {
    #[cfg(debug_assertions)]
    {
        debug_server::start().await
    }

    #[cfg(not(debug_assertions))]
    {
        production_server::start().await
    }
}
