use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

/// Helper function to copy a directory recursively
fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// Setup test project with fbox init and fixtures
fn setup_test_project_with_init(temp_dir: &Path) -> Result<PathBuf> {
    let project_dir = temp_dir.join("test_project");
    fs::create_dir(&project_dir)?;

    // 1. Run fbox init to verify command functionality
    let (success, output) = run_fbox_command(&["init", "."], &project_dir)?;
    if !success {
        anyhow::bail!("fbox init failed: {}", output);
    }

    // 2. Copy complete test environment from fixtures
    let fixtures_dir = Path::new("tests/fixtures");

    // Replace the generated project.yml with the one from fixtures
    fs::copy(
        fixtures_dir.join("project.yml"),
        project_dir.join("project.yml"),
    )?;

    // Copy test data
    copy_dir_all(
        fixtures_dir.join("test_data"),
        project_dir.join("test_data"),
    )?;

    // Copy adapter configurations
    for entry in fs::read_dir(fixtures_dir.join("adapters"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        fs::copy(entry.path(), project_dir.join("adapters").join(&file_name))?;
    }

    // Copy model configurations
    for entry in fs::read_dir(fixtures_dir.join("models"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        fs::copy(entry.path(), project_dir.join("models").join(&file_name))?;
    }

    Ok(project_dir)
}

/// Get the path to the fbox binary
fn get_fbox_binary() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir).join("target/debug/fbox")
}

/// Run fbox command and return (success, output)
fn run_fbox_command(args: &[&str], project_dir: &Path) -> Result<(bool, String)> {
    let fbox_binary = get_fbox_binary();

    // Build the binary if it doesn't exist
    if !fbox_binary.exists() {
        let build_output = Command::new("cargo").arg("build").output()?;
        if !build_output.status.success() {
            anyhow::bail!("Failed to build fbox binary");
        }
    }

    let mut cmd = Command::new(&fbox_binary);
    cmd.args(args).current_dir(project_dir);

    let output = cmd.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined_output = format!("{stdout}{stderr}");

    Ok((output.status.success(), combined_output))
}

/// Verify data using fbox query command
fn verify_data_with_query(project_dir: &Path, sql: &str) -> Result<String> {
    let (success, output) = run_fbox_command(&["query", sql], project_dir)?;
    if !success {
        anyhow::bail!("Query failed: {}", output);
    }
    Ok(output)
}

#[test]
fn test_complete_e2e_workflow() -> Result<()> {
    // 1. Setup test environment with fbox init
    let temp_dir = TempDir::new()?;
    let project_dir = setup_test_project_with_init(temp_dir.path())?;

    // 2. Test fbox migrate
    let (success, output) = run_fbox_command(&["migrate"], &project_dir)?;
    assert!(success, "fbox migrate failed: {output}");

    // 3. Test fbox run
    let (success, output) = run_fbox_command(&["run"], &project_dir)?;
    if !success {
        println!("Run failed: {output}");
        // Check what tables exist before failing
        let table_check = verify_data_with_query(
            &project_dir,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        );
        if let Ok(tables) = table_check {
            println!("Available tables: {tables}");
        }
    }
    assert!(success, "fbox run failed: {output}");

    // 5. Verify tables were created
    let tables_output = verify_data_with_query(
        &project_dir,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__fbox_%' ORDER BY name",
    )?;

    // Check that expected tables exist
    assert!(
        tables_output.contains("sensor_summary"),
        "sensor_summary table not found"
    );
    assert!(
        tables_output.contains("time_series_sensors"),
        "time_series_sensors table not found"
    );

    // 6. Verify data was loaded
    let sensors_count = verify_data_with_query(
        &project_dir,
        "SELECT COUNT(*) as count FROM time_series_sensors",
    )?;
    assert!(
        sensors_count.contains("6"),
        "Expected 6 sensor entries, got: {sensors_count}"
    );

    // 7. Verify summary model executed
    let summary_output = verify_data_with_query(
        &project_dir,
        "SELECT sensor_id, reading_count FROM sensor_summary ORDER BY sensor_id",
    )?;
    assert!(
        summary_output.contains("sensor_01"),
        "sensor_01 not found in summary"
    );
    assert!(
        summary_output.contains("sensor_02"),
        "sensor_02 not found in summary"
    );
    assert!(
        summary_output.contains("3"),
        "Expected 3 readings per sensor"
    );

    Ok(())
}
