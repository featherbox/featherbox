use anyhow::Result;
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

fn create_database_config_with_name(db_type: &str, db_name: &str) -> String {
    match db_type {
        "sqlite" => r#"database:
  type: sqlite
  path: ./database.db"#
            .to_string(),
        "mysql" => {
            format!(
                r#"database:
  type: mysql
  host: localhost
  port: 3306
  database: {db_name}
  username: ${{TEST_MYSQL_USER}}
  password: ${{TEST_MYSQL_PASSWORD}}"#
            )
        }
        "postgresql" => {
            format!(
                r#"database:
  type: postgresql
  host: localhost
  port: 5432
  database: {db_name}
  username: ${{TEST_POSTGRES_USER}}
  password: ${{TEST_POSTGRES_PASSWORD}}"#
            )
        }
        _ => panic!("Unsupported database type: {db_type}"),
    }
}

fn create_project_config_for_db_with_sqlite(
    db_type: &str,
    db_name: &str,
    project_dir: &Path,
    sqlite_filename: &str,
) -> String {
    let sqlite_path = project_dir
        .join(format!("test_data/{sqlite_filename}"))
        .to_string_lossy()
        .to_string();

    format!(
        r#"storage:
  type: local
  path: ./storage

{}

deployments:
  timeout: 600

connections:
  test_data:
    type: localfile
    base_path: ./test_data
  sqlite_source:
    type: sqlite
    path: {}"#,
        create_database_config_with_name(db_type, db_name),
        sqlite_path
    )
}

fn get_table_list_query(db_type: &str) -> &'static str {
    match db_type {
        "sqlite" => {
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '__fbox_%' ORDER BY name"
        }
        "mysql" => {
            "SELECT table_name as name FROM information_schema.tables WHERE table_name NOT LIKE '__fbox_%' AND table_name NOT LIKE 'ducklake_%' ORDER BY table_name"
        }
        "postgresql" => {
            "SELECT table_name as name FROM information_schema.tables WHERE table_name NOT LIKE '__fbox_%' AND table_name NOT LIKE 'ducklake_%' ORDER BY table_name"
        }
        _ => panic!("Unsupported database type: {db_type}"),
    }
}

fn create_test_database(db_type: &str, db_name: &str) -> Result<()> {
    use std::process::Command;

    match db_type {
        "mysql" => {
            let output = Command::new("docker")
                .args([
                    "compose",
                    "exec",
                    "mysql",
                    "mysql",
                    "-u",
                    "featherbox",
                    "-ptestpass",
                    "-e",
                ])
                .arg(format!("CREATE DATABASE IF NOT EXISTS {db_name};"))
                .output()?;

            if !output.status.success() {
                anyhow::bail!(
                    "Failed to create MySQL database: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }

            // Wait for database creation to be fully committed
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Verify database exists
            let verify_output = Command::new("docker")
                .args([
                    "compose",
                    "exec",
                    "mysql",
                    "mysql",
                    "-u",
                    "featherbox",
                    "-ptestpass",
                    "-e",
                ])
                .arg(format!("SHOW DATABASES LIKE '{db_name}';"))
                .output()?;

            if !verify_output.status.success() {
                anyhow::bail!(
                    "Failed to verify MySQL database creation: {}",
                    String::from_utf8_lossy(&verify_output.stderr)
                );
            }
        }
        "postgresql" => {
            let output = Command::new("docker")
                .args([
                    "compose",
                    "exec",
                    "postgres",
                    "psql",
                    "-U",
                    "featherbox",
                    "-d",
                    "featherbox_test",
                    "-c",
                ])
                .arg(format!("CREATE DATABASE {db_name};"))
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.contains("already exists") {
                    anyhow::bail!("Failed to create PostgreSQL database: {}", stderr);
                }
            }

            // Wait for database creation to be fully committed
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Verify database exists
            let verify_output = Command::new("docker")
                .args([
                    "compose",
                    "exec",
                    "postgres",
                    "psql",
                    "-U",
                    "featherbox",
                    "-d",
                    "featherbox_test",
                    "-c",
                ])
                .arg(format!(
                    "SELECT 1 FROM pg_database WHERE datname = '{db_name}';"
                ))
                .output()?;

            if !verify_output.status.success() {
                anyhow::bail!(
                    "Failed to verify PostgreSQL database creation: {}",
                    String::from_utf8_lossy(&verify_output.stderr)
                );
            }
        }
        "sqlite" => {}
        _ => anyhow::bail!("Unsupported database type for database creation: {db_type}"),
    }

    Ok(())
}

fn setup_test_project_with_database(temp_dir: &Path, db_type: &str) -> Result<(PathBuf, String)> {
    use uuid::Uuid;

    let project_dir = temp_dir.join(format!("test_project_{db_type}"));
    fs::create_dir(&project_dir)?;

    let (success, output) = run_fbox_command(&["init", "."], &project_dir)?;
    if !success {
        anyhow::bail!("fbox init failed: {}", output);
    }

    let unique_db_name = if db_type == "sqlite" {
        String::new()
    } else {
        let db_name = format!("ducklake_test_{}", Uuid::new_v4().simple());
        create_test_database(db_type, &db_name)?;
        db_name
    };

    let fixtures_dir = Path::new("tests/fixtures");

    copy_dir_all(
        fixtures_dir.join("test_data"),
        project_dir.join("test_data"),
    )?;

    let sqlite_db_name = format!("source_{}.db", Uuid::new_v4().simple());
    let original_sqlite = project_dir.join("test_data/source.db");
    let sqlite_db_path = project_dir.join(format!("test_data/{sqlite_db_name}"));

    if original_sqlite.exists() {
        fs::copy(&original_sqlite, &sqlite_db_path)?;
        fs::remove_file(&original_sqlite)?;

        use std::process::Command;
        let verify_output = Command::new("sqlite3")
            .arg(&sqlite_db_path)
            .arg("SELECT COUNT(*) FROM users; SELECT COUNT(*) FROM products;")
            .output();

        if let Ok(output) = verify_output {
            let output_str = String::from_utf8_lossy(&output.stdout);
            let lines: Vec<&str> = output_str.trim().split('\n').collect();
            if lines.len() < 2 || lines[0] != "4" || lines[1] != "4" {
                return Err(anyhow::anyhow!(
                    "SQLite database verification failed. Expected 4 users and 4 products"
                ));
            }
        } else {
            return Err(anyhow::anyhow!("Failed to verify SQLite database"));
        }
    }

    let project_config = create_project_config_for_db_with_sqlite(
        db_type,
        &unique_db_name,
        &project_dir,
        &sqlite_db_name,
    );
    let project_yml_path = project_dir.join("project.yml");
    fs::write(&project_yml_path, &project_config)?;

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

    Ok((project_dir, unique_db_name))
}

fn run_e2e_test_for_database(db_type: &str) -> Result<()> {
    let temp_dir = TempDir::new()?;
    let project_dir = setup_test_project_with_database(temp_dir.path(), db_type)?;

    let (success, output) = run_fbox_command(&["migrate"], &project_dir.0)?;
    assert!(success, "fbox migrate failed for {db_type}: {output}");

    let (success, output) = run_fbox_command(&["run"], &project_dir.0)?;
    if !success {
        let table_check = verify_data_with_query(&project_dir.0, get_table_list_query(db_type));
        if let Ok(tables) = table_check {
            eprintln!("Available tables: {tables}");
        }
    }
    assert!(success, "fbox run failed for {db_type}: {output}");

    let tables_output = verify_data_with_query(&project_dir.0, get_table_list_query(db_type))?;

    assert!(
        tables_output.contains("sensor_summary"),
        "sensor_summary table not found for {db_type}"
    );
    assert!(
        tables_output.contains("time_series_sensors"),
        "time_series_sensors table not found for {db_type}"
    );
    assert!(
        tables_output.contains("sqlite_users"),
        "sqlite_users table not found for {db_type}"
    );
    assert!(
        tables_output.contains("sqlite_products"),
        "sqlite_products table not found for {db_type}"
    );
    assert!(
        tables_output.contains("user_stats"),
        "user_stats table not found for {db_type}"
    );
    assert!(
        tables_output.contains("product_summary"),
        "product_summary table not found for {db_type}"
    );

    let sensors_count = verify_data_with_query(
        &project_dir.0,
        "SELECT COUNT(*) as count FROM time_series_sensors",
    )?;
    assert!(
        sensors_count.contains("6"),
        "Expected 6 sensor entries for {db_type}, got: {sensors_count}"
    );

    let users_count =
        verify_data_with_query(&project_dir.0, "SELECT COUNT(*) as count FROM sqlite_users")?;
    assert!(
        users_count.contains("4"),
        "Expected 4 users for {db_type}, got: {users_count}"
    );

    let products_count = verify_data_with_query(
        &project_dir.0,
        "SELECT COUNT(*) as count FROM sqlite_products",
    )?;
    assert!(
        products_count.contains("4"),
        "Expected 4 products for {db_type}, got: {products_count}"
    );

    let summary_output = verify_data_with_query(
        &project_dir.0,
        "SELECT sensor_id, reading_count FROM sensor_summary ORDER BY sensor_id",
    )?;

    verify_data_with_query(&project_dir.0, "SELECT * FROM time_series_sensors LIMIT 10")?;

    assert!(
        summary_output.contains("sensor_01"),
        "sensor_01 not found in summary for {db_type}. Got: {summary_output}"
    );

    assert!(
        summary_output.contains("sensor_02"),
        "sensor_02 not found in summary for {db_type}"
    );
    assert!(
        summary_output.contains("3"),
        "Expected 3 readings per sensor for {db_type}"
    );

    let user_stats_output =
        verify_data_with_query(&project_dir.0, "SELECT user_count FROM user_stats")?;
    assert!(
        user_stats_output.contains("4"),
        "Expected user_count=4 in user_stats for {db_type}, got: {user_stats_output}"
    );

    let product_summary_output = verify_data_with_query(
        &project_dir.0,
        "SELECT category, product_count FROM product_summary ORDER BY category",
    )?;

    assert!(
        product_summary_output.contains("Electronics") && product_summary_output.contains("2"),
        "Expected Electronics category with 2 products for {db_type}"
    );
    assert!(
        product_summary_output.contains("Furniture") && product_summary_output.contains("2"),
        "Expected Furniture category with 2 products for {db_type}"
    );

    Ok(())
}

fn should_run_s3_tests() -> bool {
    env::var("FEATHERBOX_S3_TEST").is_ok()
}

#[test]
fn test_e2e_workflow_sqlite() -> Result<()> {
    run_e2e_test_for_database("sqlite")
}

#[test]
fn test_e2e_workflow_mysql() -> Result<()> {
    run_e2e_test_for_database("mysql")
}

#[test]
fn test_e2e_workflow_postgresql() -> Result<()> {
    run_e2e_test_for_database("postgresql")
}
