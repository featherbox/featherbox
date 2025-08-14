use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs};
use tempfile::TempDir;

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

fn setup_test_project_with_init(temp_dir: &Path) -> Result<PathBuf> {
    let project_dir = temp_dir.join("test_project");
    fs::create_dir(&project_dir)?;

    let (success, output) = run_fbox_command(&["init", "."], &project_dir)?;
    if !success {
        anyhow::bail!("fbox init failed: {}", output);
    }

    let fixtures_dir = Path::new("tests/fixtures");

    fs::copy(
        fixtures_dir.join("project.yml"),
        project_dir.join("project.yml"),
    )?;

    copy_dir_all(
        fixtures_dir.join("test_data"),
        project_dir.join("test_data"),
    )?;

    for entry in fs::read_dir(fixtures_dir.join("adapters"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if file_name_str == "sales_data.yml" && !should_run_s3_tests() {
            continue;
        }

        fs::copy(entry.path(), project_dir.join("adapters").join(&file_name))?;
    }

    for entry in fs::read_dir(fixtures_dir.join("models"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if file_name_str == "sales_summary.yml" && !should_run_s3_tests() {
            continue;
        }

        fs::copy(entry.path(), project_dir.join("models").join(&file_name))?;
    }

    Ok(project_dir)
}

fn get_fbox_binary() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir).join("target/debug/fbox")
}

fn run_fbox_command(args: &[&str], project_dir: &Path) -> Result<(bool, String)> {
    let fbox_binary = get_fbox_binary();

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

fn verify_data_with_query(project_dir: &Path, sql: &str) -> Result<String> {
    let (success, output) = run_fbox_command(&["query", sql], project_dir)?;
    if !success {
        anyhow::bail!("Query failed: {}", output);
    }
    Ok(output)
}

#[tokio::test]
async fn test_complete_e2e_workflow() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_dir = setup_test_project_with_init(temp_dir.path())?;

    let s3_cleanup = if should_run_s3_tests() {
        let s3_test = setup_s3_test_data().await?;
        update_project_config_for_s3(&project_dir, &s3_test.bucket_name)?;
        Some(s3_test)
    } else {
        println!("Skipping S3 tests - FEATHERBOX_S3_TEST not set");
        remove_s3_config_from_project(&project_dir)?;
        None
    };

    let (success, output) = run_fbox_command(&["migrate"], &project_dir)?;
    assert!(success, "fbox migrate failed: {output}");

    let (success, output) = run_fbox_command(&["run"], &project_dir)?;
    if !success {
        println!("Run failed: {output}");
        let table_check = verify_data_with_query(
            &project_dir,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        );
        if let Ok(tables) = table_check {
            println!("Available tables: {tables}");
        }
    } else {
        println!("Run succeeded: {output}");
    }
    assert!(success, "fbox run failed: {output}");

    let tables_output = verify_data_with_query(
        &project_dir,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__fbox_%' ORDER BY name",
    )?;

    assert!(
        tables_output.contains("sensor_summary"),
        "sensor_summary table not found"
    );
    assert!(
        tables_output.contains("time_series_sensors"),
        "time_series_sensors table not found"
    );

    if should_run_s3_tests() {
        assert!(
            tables_output.contains("sales_data"),
            "sales_data table not found"
        );
        assert!(
            tables_output.contains("sales_summary"),
            "sales_summary table not found"
        );
    }

    let sensors_count = verify_data_with_query(
        &project_dir,
        "SELECT COUNT(*) as count FROM time_series_sensors",
    )?;
    assert!(
        sensors_count.contains("6"),
        "Expected 6 sensor entries, got: {sensors_count}"
    );

    let summary_output = verify_data_with_query(
        &project_dir,
        "SELECT sensor_id, reading_count FROM sensor_summary ORDER BY sensor_id",
    )?;
    println!("Summary output: {summary_output}");

    let time_series_output =
        verify_data_with_query(&project_dir, "SELECT * FROM time_series_sensors LIMIT 10")?;
    println!("Time series data: {time_series_output}");

    assert!(
        summary_output.contains("sensor_01"),
        "sensor_01 not found in summary. Got: {summary_output}"
    );

    if should_run_s3_tests() {
        let sales_count =
            verify_data_with_query(&project_dir, "SELECT COUNT(*) as count FROM sales_data")?;
        assert!(
            sales_count.contains("8"),
            "Expected 8 sales records, got: {sales_count}"
        );

        let sales_summary_output = verify_data_with_query(
            &project_dir,
            "SELECT product_name, total_sales, total_revenue FROM sales_summary ORDER BY product_name",
        )?;
        assert!(
            sales_summary_output.contains("Laptop"),
            "Laptop not found in sales summary. Got: {sales_summary_output}"
        );
        assert!(
            sales_summary_output.contains("Mouse"),
            "Mouse not found in sales summary. Got: {sales_summary_output}"
        );
    }
    assert!(
        summary_output.contains("sensor_02"),
        "sensor_02 not found in summary"
    );
    assert!(
        summary_output.contains("3"),
        "Expected 3 readings per sensor"
    );

    if let Some(s3_test) = s3_cleanup {
        verify_s3_data(&project_dir)?;

        if let Err(e) = cleanup_s3_test_bucket(&s3_test).await {
            println!("Warning: Failed to cleanup S3 test bucket: {e}");
        }
    }

    Ok(())
}

fn should_run_s3_tests() -> bool {
    env::var("FEATHERBOX_S3_TEST").is_ok()
}

struct S3TestCleanup {
    bucket_name: String,
    s3_client: featherbox::s3_client::S3Client,
}

async fn setup_s3_test_data() -> Result<S3TestCleanup> {
    use featherbox::config::project::{ConnectionConfig, S3AuthMethod};

    let unique_bucket = format!(
        "featherbox-integration-test-{}",
        chrono::Utc::now().timestamp()
    );

    let connection_config = ConnectionConfig::S3 {
        bucket: unique_bucket.clone(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        auth_method: S3AuthMethod::CredentialChain,
        access_key_id: String::new(),
        secret_access_key: String::new(),
        session_token: None,
    };

    let s3_client = featherbox::s3_client::S3Client::new(&connection_config).await?;

    s3_client
        .create_bucket()
        .await
        .with_context(|| format!("Failed to create test bucket: {unique_bucket}"))?;

    let test_files = vec![
        (
            "sales/2024-01-20.json",
            include_str!("fixtures/s3_test_data/sales/2024-01-20.json"),
        ),
        (
            "sales/2024-01-21.json",
            include_str!("fixtures/s3_test_data/sales/2024-01-21.json"),
        ),
        (
            "sales/2024-01-22.json",
            include_str!("fixtures/s3_test_data/sales/2024-01-22.json"),
        ),
    ];

    for (key, content) in test_files {
        s3_client
            .put_object(key, content.as_bytes().to_vec())
            .await
            .with_context(|| format!("Failed to upload object: {key}"))?;
    }

    println!(
        "Created S3 test bucket: {} with {} objects",
        unique_bucket, 3
    );

    Ok(S3TestCleanup {
        bucket_name: unique_bucket,
        s3_client,
    })
}

async fn cleanup_s3_test_bucket(cleanup: &S3TestCleanup) -> Result<()> {
    let s3_objects = cleanup.s3_client.list_objects_matching_pattern("*").await?;

    let object_keys: Vec<String> = s3_objects
        .iter()
        .map(|s3_url| {
            let bucket_prefix = format!("s3://{}/", cleanup.bucket_name);
            s3_url
                .strip_prefix(&bucket_prefix)
                .unwrap_or(s3_url)
                .to_string()
        })
        .collect();

    if !object_keys.is_empty() {
        cleanup
            .s3_client
            .delete_objects(object_keys.clone())
            .await?;
        println!("Deleted {} objects from bucket", object_keys.len());
    }

    cleanup.s3_client.delete_bucket().await?;

    println!("Deleted S3 test bucket: {}", cleanup.bucket_name);
    Ok(())
}

fn verify_s3_data(project_dir: &Path) -> Result<()> {
    let tables_output = verify_data_with_query(
        project_dir,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__fbox_%' ORDER BY name",
    )?;

    assert!(
        tables_output.contains("sales_data"),
        "sales_data table not found in S3 test"
    );
    assert!(
        tables_output.contains("sales_summary"),
        "sales_summary table not found in S3 test"
    );

    let sales_count =
        verify_data_with_query(project_dir, "SELECT COUNT(*) as count FROM sales_data")?;
    assert!(
        sales_count.contains("8"),
        "Expected 8 sales entries from S3, got: {sales_count}"
    );

    let sales_summary_output = verify_data_with_query(
        project_dir,
        "SELECT product_name, total_sales FROM sales_summary ORDER BY product_name",
    )?;
    println!("S3 sales summary output: {sales_summary_output}");

    assert!(
        sales_summary_output.contains("Laptop"),
        "Laptop not found in S3 sales summary"
    );
    assert!(
        sales_summary_output.contains("Mouse"),
        "Mouse not found in S3 sales summary"
    );

    Ok(())
}

fn update_project_config_for_s3(project_dir: &Path, bucket_name: &str) -> Result<()> {
    let project_config_path = project_dir.join("project.yml");
    let config_content = fs::read_to_string(&project_config_path)?;

    let updated_content = config_content.replace(
        "bucket: featherbox-test-bucket",
        &format!("bucket: {bucket_name}"),
    );

    fs::write(&project_config_path, &updated_content)?;
    println!("Updated project.yml with S3 bucket: {bucket_name}");

    let verify_content = fs::read_to_string(&project_config_path)?;
    println!("Verified project.yml content:\n{verify_content}");

    Ok(())
}

fn remove_s3_config_from_project(project_dir: &Path) -> Result<()> {
    let project_config_path = project_dir.join("project.yml");
    let config_content = fs::read_to_string(&project_config_path)?;

    let lines: Vec<&str> = config_content.lines().collect();
    let mut filtered_lines = Vec::new();
    let mut skip_s3_section = false;

    for line in lines {
        if line.trim().starts_with("s3_data:") {
            skip_s3_section = true;
            continue;
        }
        if skip_s3_section && (line.starts_with("    ") || line.trim().is_empty()) {
            continue;
        }
        if skip_s3_section && !line.starts_with("    ") && !line.trim().is_empty() {
            skip_s3_section = false;
        }

        if !skip_s3_section {
            filtered_lines.push(line);
        }
    }

    let updated_content = filtered_lines.join("\n");
    fs::write(project_config_path, updated_content)?;
    println!("Removed S3 configuration from project.yml");

    Ok(())
}
