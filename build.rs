fn main() -> anyhow::Result<()> {
    // UI is built separately in GitHub Actions workflow
    println!("cargo:warning=UI build handled by workflow, not build script");
    Ok(())
}
