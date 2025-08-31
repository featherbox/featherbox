use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::{env, fs};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct TestUser {
    id: i32,
    name: String,
    email: String,
    age: i32,
    created_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct TestProduct {
    id: i32,
    name: String,
    price: f64,
    category: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct TestData {
    users: Option<Vec<TestUser>>,
    products: Option<Vec<TestProduct>>,
}

fn create_sqlite_from_yaml(yaml_path: &Path, db_path: &Path) -> Result<()> {
    let yaml_content = fs::read_to_string(yaml_path)?;
    let test_data: TestData = serde_yml::from_str(&yaml_content)?;

    let conn = Connection::open(db_path)?;

    if let Some(users) = test_data.users {
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, created_at TEXT)",
            [],
        )?;

        for user in users {
            conn.execute(
                "INSERT INTO users (id, name, email, age, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                [
                    &user.id as &dyn rusqlite::ToSql,
                    &user.name,
                    &user.email,
                    &user.age,
                    &user.created_at,
                ],
            )?;
        }
    }

    if let Some(products) = test_data.products {
        conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2), category TEXT)",
            [],
        )?;

        for product in products {
            conn.execute(
                "INSERT INTO products (id, name, price, category) VALUES (?1, ?2, ?3, ?4)",
                [
                    &product.id as &dyn rusqlite::ToSql,
                    &product.name,
                    &product.price,
                    &product.category,
                ],
            )?;
        }
    }

    Ok(())
}

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
    let (success, output) = run_fbox_command(&["query", "execute", sql], project_dir)?;
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

fn create_project_config_with_storage(
    storage_type: &str,
    db_type: &str,
    db_name: &str,
    project_dir: &Path,
    sqlite_filename: &str,
    minio_bucket_name: &str,
) -> String {
    let sqlite_path = project_dir
        .join(format!("test_data/{sqlite_filename}"))
        .to_string_lossy()
        .to_string();

    let storage_section = match storage_type {
        "localfile" => r#"storage:
  type: local
  path: ./storage"#
            .to_string(),
        "s3" => format!(
            r#"storage:
  type: s3
  bucket: {minio_bucket_name}
  region: us-east-1
  endpoint_url: http://localhost:9010
  auth_method: explicit
  access_key_id: user
  secret_access_key: password
  path_style_access: true"#
        ),
        _ => panic!("Unsupported storage type: {storage_type}"),
    };

    let test_data_section = match storage_type {
        "localfile" => r#"  test_data:
    type: localfile
    base_path: ./test_data"#
            .to_string(),
        "s3" => format!(
            r#"  test_data:
    type: s3
    endpoint_url: http://localhost:9010
    region: us-east-1
    auth_method: explicit
    access_key_id: user
    secret_access_key: password
    bucket: {minio_bucket_name}
    path_style_access: true"#
        ),
        _ => panic!("Unsupported storage type: {storage_type}"),
    };

    format!(
        r#"{}

{}


connections:
{}
  sqlite_source:
    type: sqlite
    path: {}
  mysql_datasource:
    type: mysql
    host: localhost
    port: 3307
    database: datasource_test
    username: datasource
    password: datasourcepass
  postgres_datasource:
    type: postgresql
    host: localhost
    port: 5433
    database: datasource_test
    username: datasource
    password: datasourcepass
  minio_datasource:
    type: s3
    endpoint_url: http://localhost:9010
    region: us-east-1
    auth_method: explicit
    access_key_id: user
    secret_access_key: password
    bucket: {}
    path_style_access: true"#,
        storage_section,
        create_database_config_with_name(db_type, db_name),
        test_data_section,
        sqlite_path,
        minio_bucket_name
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
    match db_type {
        "mysql" => {
            create_mysql_database(db_name)?;
        }
        "postgresql" => {
            create_postgresql_database(db_name)?;
        }
        "sqlite" => {}
        _ => anyhow::bail!("Unsupported database type for database creation: {db_type}"),
    }
    Ok(())
}

fn create_mysql_database(db_name: &str) -> Result<()> {
    let output = run_command_in_container_with_input(
        "catalog_db_mysql",
        &["mysql", "-u", "featherbox", "-ptestpass", "-e"],
        &format!("CREATE DATABASE IF NOT EXISTS {db_name};"),
    )?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to create MySQL database: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    let verify_output = run_command_in_container_with_input(
        "catalog_db_mysql",
        &["mysql", "-u", "featherbox", "-ptestpass", "-e"],
        &format!("SHOW DATABASES LIKE '{db_name}';"),
    )?;

    if !verify_output.status.success() {
        anyhow::bail!(
            "Failed to verify MySQL database creation: {}",
            String::from_utf8_lossy(&verify_output.stderr)
        );
    }
    Ok(())
}

fn create_postgresql_database(db_name: &str) -> Result<()> {
    let output = run_command_in_container_with_input(
        "catalog_db_postgres",
        &["psql", "-U", "featherbox", "-d", "featherbox_test", "-c"],
        &format!("CREATE DATABASE {db_name};"),
    )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("already exists") {
            anyhow::bail!("Failed to create PostgreSQL database: {}", stderr);
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    let verify_output = run_command_in_container_with_input(
        "catalog_db_postgres",
        &["psql", "-U", "featherbox", "-d", "featherbox_test", "-c"],
        &format!("SELECT 1 FROM pg_database WHERE datname = '{db_name}';"),
    )?;

    if !verify_output.status.success() {
        anyhow::bail!(
            "Failed to verify PostgreSQL database creation: {}",
            String::from_utf8_lossy(&verify_output.stderr)
        );
    }
    Ok(())
}

fn check_container_status() {
    check_mysql();
    check_postgres();
    check_minio();
}

fn run_command_in_container(service: &str, command: &[&str]) -> Result<Output> {
    Command::new("docker")
        .args(["compose", "exec", service])
        .args(command)
        .output()
        .context("Failed to execute command in container")
}

fn run_command_in_container_with_input(
    service: &str,
    command: &[&str],
    input: &str,
) -> Result<Output> {
    Command::new("docker")
        .args(["compose", "exec", "-T", service])
        .args(command)
        .arg(input)
        .output()
        .context("Failed to execute command with input in container")
}

fn check_mysql() {
    let output = run_command_in_container(
        "datasource_mysql",
        &[
            "mysql",
            "-u",
            "datasource",
            "-pdatasourcepass",
            "-e",
            "SELECT 1;",
        ],
    )
    .expect("Failed to run MySQL check command");
    assert!(
        output.status.success(),
        "MySQL check command failed. Please run 'docker compose up -d' to start the required database services."
    );
}

fn check_postgres() {
    let output = run_command_in_container(
        "datasource_postgres",
        &[
            "psql",
            "-U",
            "datasource",
            "-d",
            "datasource_test",
            "-c",
            "SELECT 1;",
        ],
    )
    .expect("Failed to run PostgreSQL check command");

    assert!(
        output.status.success(),
        "PostgreSQL check command failed. Please run 'docker compose up -d' to start the required database services."
    );
}

fn check_minio() {
    let output = run_command_in_container("minio", &["ls", "/data"])
        .expect("Failed to run MinIO check command");
    assert!(
        output.status.success(),
        "MinIO check command failed. Please run 'docker compose up -d' to start the required database services."
    );
}

fn setup_minio_test_data(bucket_name: &str) -> Result<()> {
    let _ = run_command_in_container(
        "minio",
        &[
            "mc",
            "alias",
            "set",
            "myminio",
            "http://localhost:9000",
            "user",
            "password",
        ],
    );

    let _ = run_command_in_container(
        "minio",
        &[
            "mc",
            "mb",
            "--ignore-existing",
            &format!("myminio/{bucket_name}"),
        ],
    );

    let data_dirs = ["tests/fixtures/s3_test_data", "tests/fixtures/test_data"];
    for dir_path in data_dirs {
        let dir = Path::new(dir_path);
        if dir.exists() {
            upload_files_recursively(dir, "", bucket_name)?;
        }
    }

    Ok(())
}

fn upload_files_recursively(dir: &Path, prefix: &str, bucket_name: &str) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_path = entry.path();

        let s3_key = if prefix.is_empty() {
            file_name.to_string_lossy().to_string()
        } else {
            format!("{}/{}", prefix, file_name.to_string_lossy())
        };

        if file_path.is_dir() {
            upload_files_recursively(&file_path, &s3_key, bucket_name)?;
        } else {
            let file_contents = fs::read(&file_path)
                .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

            let temp_file = format!(
                "temp-{}-{}",
                Uuid::new_v4().simple(),
                file_name.to_string_lossy().replace('/', "_")
            );

            upload_file_to_minio(&file_path, &file_contents, &temp_file, bucket_name, &s3_key)?;
        }
    }
    Ok(())
}

fn upload_file_to_minio(
    file_path: &Path,
    file_contents: &[u8],
    temp_file: &str,
    bucket_name: &str,
    s3_key: &str,
) -> Result<()> {
    use std::io::Write;

    let mut child = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "sh", "-c"])
        .arg(format!("cat > /tmp/{temp_file}"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to spawn docker command for {}", file_path.display()))?;

    if let Some(stdin) = child.stdin.take() {
        let mut stdin = stdin;
        stdin.write_all(file_contents).with_context(|| {
            format!(
                "Failed to write data to container for {}",
                file_path.display()
            )
        })?;
        stdin.flush().with_context(|| {
            format!(
                "Failed to flush data to container for {}",
                file_path.display()
            )
        })?;
    } else {
        anyhow::bail!("Failed to get stdin for container write operation");
    }

    let write_output = child.wait_with_output().with_context(|| {
        format!(
            "Failed to wait for write command for {}",
            file_path.display()
        )
    })?;

    if !write_output.status.success() {
        anyhow::bail!(
            "Failed to write file {} to container: {}",
            file_path.display(),
            String::from_utf8_lossy(&write_output.stderr)
        );
    }

    std::thread::sleep(std::time::Duration::from_millis(100));

    let upload_result = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "mc", "cp"])
        .arg(format!("/tmp/{temp_file}"))
        .arg(format!("myminio/{bucket_name}/{s3_key}"))
        .output()
        .with_context(|| {
            format!(
                "Failed to execute upload command for {}",
                file_path.display()
            )
        })?;

    if upload_result.status.success() {
        println!(
            "Successfully uploaded {} to s3://{bucket_name}/{s3_key}",
            file_path.display()
        );
    } else {
        let stderr = String::from_utf8_lossy(&upload_result.stderr);
        let stdout = String::from_utf8_lossy(&upload_result.stdout);
        anyhow::bail!(
            "Failed to upload {} to s3://{bucket_name}/{s3_key}: stderr: {} stdout: {}",
            file_path.display(),
            stderr,
            stdout
        );
    }

    let _ = Command::new("docker")
        .args(["compose", "exec", "-T", "minio", "rm", "-f"])
        .arg(format!("/tmp/{temp_file}"))
        .output();

    Ok(())
}

fn cleanup_minio_test_data(bucket_name: &str) -> Result<()> {
    let _ = run_command_in_container(
        "minio",
        &[
            "mc",
            "rm",
            "--recursive",
            "--force",
            &format!("myminio/{bucket_name}"),
        ],
    );

    let _ = run_command_in_container("minio", &["mc", "mb", &format!("myminio/{bucket_name}")]);

    Ok(())
}

const TEST_USERS_DATA: &[(&str, &str)] = &[
    ("John Doe", "john@example.com"),
    ("Jane Smith", "jane@example.com"),
    ("Bob Wilson", "bob@example.com"),
    ("Alice Brown", "alice@example.com"),
];

fn setup_database_test_data(db_type: &str) -> Result<()> {
    use std::sync::Once;

    static MYSQL_SETUP: Once = Once::new();
    static POSTGRES_SETUP: Once = Once::new();

    let setup_once = match db_type {
        "mysql" => &MYSQL_SETUP,
        "postgresql" => &POSTGRES_SETUP,
        _ => return Ok(()),
    };

    setup_once.call_once(|| {
        if let Err(e) = setup_database_test_data_impl(db_type) {
            eprintln!("Failed to setup {db_type} test data: {e}");
        }
    });

    Ok(())
}

fn setup_database_test_data_impl(db_type: &str) -> Result<()> {
    let (service, sql) = match db_type {
        "mysql" => (
            "datasource_mysql",
            format!(
                "USE datasource_test;
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                DELETE FROM users;
                INSERT INTO users (name, email) VALUES {};",
                TEST_USERS_DATA
                    .iter()
                    .map(|(name, email)| format!("('{name}', '{email}')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        "postgresql" => (
            "datasource_postgres",
            format!(
                "CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                DELETE FROM users;
                INSERT INTO users (name, email) VALUES {};",
                TEST_USERS_DATA
                    .iter()
                    .map(|(name, email)| format!("('{name}', '{email}')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        _ => return Ok(()),
    };

    let command_args = match db_type {
        "mysql" => vec!["mysql", "-u", "datasource", "-pdatasourcepass", "-e"],
        "postgresql" => vec!["psql", "-U", "datasource", "-d", "datasource_test", "-c"],
        _ => return Ok(()),
    };

    let output = run_command_in_container_with_input(service, &command_args, &sql)?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to setup {db_type} test data: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn setup_mysql_test_data() -> Result<()> {
    setup_database_test_data("mysql")
}

fn setup_postgres_test_data() -> Result<()> {
    setup_database_test_data("postgresql")
}

fn run_e2e_test(storage_type: &str, catalog_type: &str) -> Result<()> {
    setup_mysql_test_data()?;
    setup_postgres_test_data()?;

    let temp_dir = TempDir::new()?;
    let project_name = format!("test_project_{storage_type}_{catalog_type}");
    let (success, output) = run_fbox_command(&["new", &project_name], temp_dir.path())?;
    let project_dir = temp_dir.path().join(&project_name);
    if !success {
        anyhow::bail!("fbox new failed: {}", output);
    }

    let unique_db_name = if catalog_type == "sqlite" {
        String::new()
    } else {
        let db_name = format!("ducklake_test_{}", Uuid::new_v4().simple());
        create_test_database(catalog_type, &db_name)?;
        db_name
    };

    let unique_bucket_name = format!("test-bucket-{}", Uuid::new_v4().simple());

    let fixtures_dir = Path::new("tests/fixtures");

    copy_dir_all(
        fixtures_dir.join("test_data"),
        project_dir.join("test_data"),
    )?;

    let sqlite_db_name = format!("source_{}.db", Uuid::new_v4().simple());
    let sqlite_db_path = project_dir.join(format!("test_data/{sqlite_db_name}"));
    let yaml_data_path = project_dir.join("test_data/test_data.yml");

    if yaml_data_path.exists() {
        create_sqlite_from_yaml(&yaml_data_path, &sqlite_db_path)?;

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
    } else {
        return Err(anyhow::anyhow!(
            "Test data YAML file not found: {:?}",
            yaml_data_path
        ));
    }

    let project_config = create_project_config_with_storage(
        storage_type,
        catalog_type,
        &unique_db_name,
        &project_dir,
        &sqlite_db_name,
        &unique_bucket_name,
    );
    let project_yml_path = project_dir.join("project.yml");
    fs::write(&project_yml_path, &project_config)?;

    for entry in fs::read_dir(fixtures_dir.join("adapters"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if file_name_str == "mysql_users.yml" || file_name_str == "postgres_users.yml" {
            continue;
        }

        if storage_type == "localfile" && file_name_str == "sales_data.yml" {
            continue;
        }

        fs::copy(entry.path(), project_dir.join("adapters").join(&file_name))?;
    }

    for entry in fs::read_dir(fixtures_dir.join("models"))? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if file_name_str == "mysql_user_stats.yml" || file_name_str == "postgres_user_stats.yml" {
            continue;
        }

        if storage_type == "localfile" && file_name_str == "sales_summary.yml" {
            continue;
        }

        fs::copy(entry.path(), project_dir.join("models").join(&file_name))?;
    }

    if storage_type == "s3" {
        cleanup_minio_test_data(&unique_bucket_name)?;
        setup_minio_test_data(&unique_bucket_name)?;
    }

    let (success, output) = run_fbox_command(&["migrate"], &project_dir)?;
    if !success {
        anyhow::bail!("fbox migrate failed for {storage_type}-{catalog_type}: {output}");
    }

    let (success, output) = run_fbox_command(&["run"], &project_dir)?;
    if !success {
        let table_check = verify_data_with_query(&project_dir, get_table_list_query(catalog_type));
        if let Ok(tables) = table_check {
            eprintln!("Available tables: {tables}");
        }
        anyhow::bail!("fbox run failed for {storage_type}-{catalog_type}: {output}");
    }

    let tables_output = verify_data_with_query(&project_dir, get_table_list_query(catalog_type))?;

    assert!(
        tables_output.contains("sensor_summary"),
        "sensor_summary table not found for {storage_type}-{catalog_type}"
    );
    assert!(
        tables_output.contains("time_series_sensors"),
        "time_series_sensors table not found for {storage_type}-{catalog_type}"
    );

    let sensors_count = verify_data_with_query(
        &project_dir,
        "SELECT COUNT(*) as count FROM time_series_sensors",
    )?;
    assert!(
        sensors_count.contains("6"),
        "Expected 6 sensor entries for {storage_type}-{catalog_type}, got: {sensors_count}"
    );

    if let Ok(entries) = fs::read_dir(project_dir.join("test_data")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "db")
                && path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("source_"))
            {
                let _ = fs::remove_file(&path);
            }
        }
    }

    Ok(())
}

fn test_e2e_with_setup(storage_type: &str, catalog_type: &str) -> Result<()> {
    check_container_status();
    run_e2e_test(storage_type, catalog_type)
}

#[test]
fn test_e2e_localfile_sqlite() -> Result<()> {
    test_e2e_with_setup("localfile", "sqlite")
}

#[test]
fn test_e2e_localfile_mysql() -> Result<()> {
    test_e2e_with_setup("localfile", "mysql")
}

#[test]
fn test_e2e_localfile_postgresql() -> Result<()> {
    test_e2e_with_setup("localfile", "postgresql")
}

#[test]
fn test_e2e_s3_sqlite() -> Result<()> {
    test_e2e_with_setup("s3", "sqlite")
}

#[test]
fn test_e2e_s3_mysql() -> Result<()> {
    test_e2e_with_setup("s3", "mysql")
}

#[test]
fn test_e2e_s3_postgresql() -> Result<()> {
    test_e2e_with_setup("s3", "postgresql")
}
