use anyhow::Result;

pub mod config;
pub mod ducklake;
pub mod graph;
pub mod pipeline;

#[tokio::main]
async fn main() -> Result<()> {
    let config = config::parse_config("examples/simple".into());
    println!("{config:#?}");
    Ok(())
}
