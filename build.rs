use std::process::Command;

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=src/ui/src");
    println!("cargo:rerun-if-changed=src/ui/package.json");
    println!("cargo:rerun-if-changed=src/ui/pnpm-lock.yaml");
    println!("cargo:rerun-if-changed=src/ui/vite.config.ts");
    println!("cargo:rerun-if-changed=src/ui/svelte.config.js");

    if Command::new("pnpm").arg("--version").output().is_err() {
        anyhow::bail!("pnpm is not installed. Please install it to build the UI.");
    }

    let mut install_cmd = Command::new("pnpm");
    install_cmd.arg("install").current_dir("src/ui");

    // CI環境ではfrozen-lockfileを無効化
    if std::env::var("CI").is_ok() {
        install_cmd.arg("--no-frozen-lockfile");
    }

    let status = install_cmd.status()?;

    if !status.success() {
        anyhow::bail!("Failed to install UI dependencies.");
    }

    let status = Command::new("pnpm")
        .arg("run")
        .arg("build")
        .current_dir("src/ui")
        .status()?;

    if !status.success() {
        anyhow::bail!("Failed to build UI.");
    }

    Ok(())
}
