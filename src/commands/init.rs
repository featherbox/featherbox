use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::ProjectConfig;
use crate::config::adapter::{AdapterConfig, AdapterSource, FileConfig, FormatConfig};
use crate::config::dashboard::{ChartConfig, ChartType, DashboardConfig};
use crate::config::model::ModelConfig;
use crate::config::query::QueryConfig;

pub struct ProjectBuilder {
    pub project_name: String,
    pub config: ProjectConfig,
    current_dir: PathBuf,
}

impl ProjectBuilder {
    pub fn new(project_name: String, config: &ProjectConfig) -> Result<Self> {
        let current_dir = std::env::current_dir()?;
        Ok(Self {
            project_name,
            config: config.clone(),
            current_dir,
        })
    }

    pub fn with_current_dir(
        project_name: String,
        config: &ProjectConfig,
        current_dir: PathBuf,
    ) -> Self {
        Self {
            project_name,
            config: config.clone(),
            current_dir,
        }
    }

    pub fn create_project_directory(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        if project_path.exists() {
            return Err(anyhow::anyhow!(
                "Directory '{}' already exists",
                self.project_name
            ));
        }
        self.create_directories(&project_path)
    }

    fn create_directories(&self, base_path: &Path) -> Result<()> {
        fs::create_dir_all(base_path).with_context(|| {
            format!(
                "Failed to create project directory '{}'.",
                base_path.display()
            )
        })?;

        fs::create_dir_all(base_path.join("adapters"))
            .context("Failed to create adapters directory")?;
        fs::create_dir_all(base_path.join("models"))
            .context("Failed to create models directory")?;
        fs::create_dir_all(base_path.join("models/staging"))
            .context("Failed to create models/staging directory")?;
        fs::create_dir_all(base_path.join("models/marts"))
            .context("Failed to create models/marts directory")?;
        fs::create_dir_all(base_path.join("queries"))
            .context("Failed to create queries directory")?;
        fs::create_dir_all(base_path.join("dashboards"))
            .context("Failed to create dashboards directory")?;
        fs::create_dir_all(base_path.join("sample_data"))
            .context("Failed to create sample_data directory")?;

        Ok(())
    }

    pub fn create_secret_key(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        ensure_secret_key(&project_path)
    }

    pub fn save_project_config(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let mut config = self.config.clone();

        config.add_sample_connections()?;

        let yaml_content =
            serde_yml::to_string(&config).context("Failed to serialize project config to YAML")?;

        fs::write(project_path.join("project.yml"), yaml_content)
            .context("Failed to write project.yml")?;

        Ok(())
    }

    pub fn create_gitignore(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let gitignore_content = ".secret.key\nstorage/\ndatabase.db\nsample_data/\n";

        fs::write(project_path.join(".gitignore"), gitignore_content)
            .context("Failed to write .gitignore")?;

        Ok(())
    }

    pub fn create_sample_data(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let sample_dir = project_path.join("sample_data");

        let users_csv = sample_dir.join("users.csv");
        let users_content = r#"user_id,name,email,age,created_at
1,Alice Johnson,alice@example.com,28,2024-01-15
2,Bob Smith,bob@example.com,34,2024-01-16
3,Charlie Brown,charlie@example.com,22,2024-01-17
4,Diana Prince,diana@example.com,31,2024-01-18
5,Eve Wilson,eve@example.com,27,2024-01-19
6,Frank Miller,frank@example.com,45,2024-01-20
7,Grace Lee,grace@example.com,29,2024-01-21
8,Henry Davis,henry@example.com,36,2024-01-22
9,Iris Chen,iris@example.com,33,2024-01-23
10,Jack Ryan,jack@example.com,41,2024-01-24
"#;
        fs::write(&users_csv, users_content).context("Failed to write users.csv")?;

        let app_log_1 = sample_dir.join("app_log_1.json");
        let log1_content = r#"[{"timestamp":"2024-03-01T10:15:00Z","user_id":1,"action":"login","device":"mobile","duration":120},{"timestamp":"2024-03-01T10:30:00Z","user_id":2,"action":"view_product","device":"desktop","duration":45},{"timestamp":"2024-03-01T10:45:00Z","user_id":3,"action":"add_to_cart","device":"mobile","duration":30},{"timestamp":"2024-03-01T11:00:00Z","user_id":1,"action":"purchase","device":"mobile","duration":180},{"timestamp":"2024-03-01T11:15:00Z","user_id":4,"action":"login","device":"tablet","duration":90}]
"#;
        fs::write(&app_log_1, log1_content).context("Failed to write app_log_1.json")?;

        let app_log_2 = sample_dir.join("app_log_2.json");
        let log2_content = r#"[{"timestamp":"2024-03-02T09:00:00Z","user_id":5,"action":"login","device":"desktop","duration":100},{"timestamp":"2024-03-02T09:20:00Z","user_id":6,"action":"search","device":"mobile","duration":25},{"timestamp":"2024-03-02T09:40:00Z","user_id":2,"action":"view_product","device":"desktop","duration":60},{"timestamp":"2024-03-02T10:00:00Z","user_id":7,"action":"add_to_cart","device":"mobile","duration":40},{"timestamp":"2024-03-02T10:20:00Z","user_id":5,"action":"purchase","device":"desktop","duration":200}]
"#;
        fs::write(&app_log_2, log2_content).context("Failed to write app_log_2.json")?;

        let app_log_3 = sample_dir.join("app_log_3.json");
        let log3_content = r#"[{"timestamp":"2024-03-03T14:00:00Z","user_id":8,"action":"login","device":"tablet","duration":110},{"timestamp":"2024-03-03T14:30:00Z","user_id":9,"action":"view_product","device":"mobile","duration":50},{"timestamp":"2024-03-03T15:00:00Z","user_id":10,"action":"search","device":"desktop","duration":35},{"timestamp":"2024-03-03T15:30:00Z","user_id":8,"action":"add_to_cart","device":"tablet","duration":45},{"timestamp":"2024-03-03T16:00:00Z","user_id":3,"action":"purchase","device":"mobile","duration":220}]
"#;
        fs::write(&app_log_3, log3_content).context("Failed to write app_log_3.json")?;

        let db_path = sample_dir.join("app.db");
        let conn = Connection::open(&db_path).context("Failed to create sample database")?;

        conn.execute(
            "CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )",
            [],
        )
        .context("Failed to create products table")?;

        conn.execute(
            "INSERT INTO products (product_id, name, category, price, stock, created_at) VALUES
            (1, 'Laptop Pro', 'Electronics', 1299.99, 50, '2024-01-01'),
            (2, 'Wireless Mouse', 'Electronics', 29.99, 200, '2024-01-02'),
            (3, 'Office Chair', 'Furniture', 399.99, 75, '2024-01-03'),
            (4, 'Standing Desk', 'Furniture', 599.99, 40, '2024-01-04'),
            (5, 'USB-C Hub', 'Electronics', 49.99, 150, '2024-01-05'),
            (6, 'Monitor 27inch', 'Electronics', 349.99, 80, '2024-01-06'),
            (7, 'Desk Lamp', 'Furniture', 79.99, 120, '2024-01-07'),
            (8, 'Keyboard Mechanical', 'Electronics', 149.99, 90, '2024-01-08'),
            (9, 'Webcam HD', 'Electronics', 89.99, 110, '2024-01-09'),
            (10, 'Notebook Set', 'Stationery', 19.99, 300, '2024-01-10')",
            [],
        )
        .context("Failed to insert product data")?;

        conn.execute(
            "CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                order_date TEXT NOT NULL,
                status TEXT NOT NULL
            )",
            [],
        )
        .context("Failed to create orders table")?;

        conn.execute(
            "INSERT INTO orders (order_id, user_id, product_id, quantity, total_amount, order_date, status) VALUES
            (1, 1, 1, 1, 1299.99, '2024-03-01', 'completed'),
            (2, 2, 2, 2, 59.98, '2024-03-01', 'completed'),
            (3, 3, 5, 1, 49.99, '2024-03-02', 'processing'),
            (4, 5, 4, 1, 599.99, '2024-03-02', 'completed'),
            (5, 7, 6, 1, 349.99, '2024-03-03', 'shipped'),
            (6, 8, 3, 2, 799.98, '2024-03-03', 'completed'),
            (7, 1, 8, 1, 149.99, '2024-03-04', 'processing'),
            (8, 4, 7, 3, 239.97, '2024-03-04', 'completed'),
            (9, 6, 9, 1, 89.99, '2024-03-05', 'shipped'),
            (10, 10, 10, 5, 99.95, '2024-03-05', 'completed')",
            [],
        ).context("Failed to insert order data")?;

        Ok(())
    }

    pub fn create_sample_adapters(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let adapters_dir = project_path.join("adapters");

        // Users CSV adapter
        let users_config = AdapterConfig {
            connection: "local_files".to_string(),
            description: Some("User data from CSV file".to_string()),
            source: AdapterSource::File {
                file: FileConfig {
                    path: "users.csv".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: Some(",".to_string()),
                    null_value: None,
                    has_header: Some(true),
                },
            },
            columns: vec![],
        };
        let users_yaml = serde_yml::to_string(&users_config)?;
        fs::write(adapters_dir.join("users.yml"), users_yaml)
            .context("Failed to write users adapter")?;

        // App logs JSON adapter
        let app_logs_config = AdapterConfig {
            connection: "local_files".to_string(),
            description: Some("Application logs from JSON files".to_string()),
            source: AdapterSource::File {
                file: FileConfig {
                    path: "app_log_*.json".to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "json".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: None,
                },
            },
            columns: vec![],
        };
        let app_logs_yaml = serde_yml::to_string(&app_logs_config)?;
        fs::write(adapters_dir.join("app_logs.yml"), app_logs_yaml)
            .context("Failed to write app_logs adapter")?;

        // Products database adapter
        let products_config = AdapterConfig {
            connection: "sample_db".to_string(),
            description: Some("Product data from database".to_string()),
            source: AdapterSource::Database {
                table_name: "products".to_string(),
            },
            columns: vec![],
        };
        let products_yaml = serde_yml::to_string(&products_config)?;
        fs::write(adapters_dir.join("products.yml"), products_yaml)
            .context("Failed to write products adapter")?;

        // Orders database adapter
        let orders_config = AdapterConfig {
            connection: "sample_db".to_string(),
            description: Some("Order data from database".to_string()),
            source: AdapterSource::Database {
                table_name: "orders".to_string(),
            },
            columns: vec![],
        };
        let orders_yaml = serde_yml::to_string(&orders_config)?;
        fs::write(adapters_dir.join("orders.yml"), orders_yaml)
            .context("Failed to write orders adapter")?;

        Ok(())
    }

    pub fn create_sample_models(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let staging_dir = project_path.join("models/staging");
        let marts_dir = project_path.join("models/marts");

        // Staging: app_logs model
        let app_logs_config = ModelConfig {
            description: Some("Cleaned application logs".to_string()),
            sql: "SELECT 
    timestamp::TIMESTAMP as event_time,
    user_id,
    action,
    device,
    duration,
    DATE(timestamp) as event_date
FROM app_logs
WHERE duration > 0"
                .to_string(),
        };
        let app_logs_yaml = serde_yml::to_string(&app_logs_config)?;
        fs::write(staging_dir.join("app_logs.yml"), app_logs_yaml)
            .context("Failed to write app_logs model")?;

        // Marts: user_activity_summary model
        let user_activity_config = ModelConfig {
            description: Some("User activity summary".to_string()),
            sql: "SELECT 
    u.user_id,
    u.name,
    u.email,
    COUNT(DISTINCT l.event_date) as active_days,
    COUNT(l.action) as total_actions,
    AVG(l.duration) as avg_duration,
    MAX(l.event_time) as last_activity
FROM users u
LEFT JOIN staging_app_logs l ON u.user_id = l.user_id
GROUP BY u.user_id, u.name, u.email"
                .to_string(),
        };
        let user_activity_yaml = serde_yml::to_string(&user_activity_config)?;
        fs::write(
            marts_dir.join("user_activity_summary.yml"),
            user_activity_yaml,
        )
        .context("Failed to write user_activity_summary model")?;

        // Marts: product_performance model
        let product_performance_config = ModelConfig {
            description: Some("Product performance metrics".to_string()),
            sql: "SELECT 
    p.product_id,
    p.name as product_name,
    p.category,
    p.price,
    p.stock,
    COUNT(o.order_id) as order_count,
    SUM(o.quantity) as total_quantity_sold,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.name, p.category, p.price, p.stock
ORDER BY total_revenue DESC"
                .to_string(),
        };
        let product_performance_yaml = serde_yml::to_string(&product_performance_config)?;
        fs::write(
            marts_dir.join("product_performance.yml"),
            product_performance_yaml,
        )
        .context("Failed to write product_performance model")?;

        Ok(())
    }

    pub fn create_sample_queries(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let queries_dir = project_path.join("queries");

        // Top products query
        let top_products_config = QueryConfig {
            name: "top_products".to_string(),
            description: Some("Top 5 products by revenue".to_string()),
            sql: "SELECT 
    product_name,
    category,
    total_revenue,
    order_count
FROM marts.product_performance
ORDER BY total_revenue DESC
LIMIT 5"
                .to_string(),
        };
        let top_products_yaml = serde_yml::to_string(&top_products_config)?;
        fs::write(queries_dir.join("top_products.yml"), top_products_yaml)
            .context("Failed to write top_products query")?;

        // Active users query
        let active_users_config = QueryConfig {
            name: "active_users".to_string(),
            description: Some("Most active users by action count".to_string()),
            sql: "SELECT 
    name,
    email,
    total_actions,
    active_days,
    ROUND(avg_duration, 2) as avg_duration_seconds
FROM marts.user_activity_summary
WHERE total_actions > 0
ORDER BY total_actions DESC
LIMIT 10"
                .to_string(),
        };
        let active_users_yaml = serde_yml::to_string(&active_users_config)?;
        fs::write(queries_dir.join("active_users.yml"), active_users_yaml)
            .context("Failed to write active_users query")?;

        Ok(())
    }

    pub fn create_sample_dashboards(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let dashboards_dir = project_path.join("dashboards");
        let queries_dir = project_path.join("queries");

        // First create the query files
        let revenue_query = QueryConfig {
            name: "revenue_trend".to_string(),
            description: Some("Daily revenue trend query".to_string()),
            sql: "SELECT 
    DATE(order_date) as date,
    SUM(total_amount) as daily_revenue
FROM orders
WHERE status = 'completed'
GROUP BY DATE(order_date)
ORDER BY date"
                .to_string(),
        };
        let revenue_query_yaml = serde_yml::to_string(&revenue_query)?;
        fs::write(queries_dir.join("revenue_trend.yml"), revenue_query_yaml)
            .context("Failed to write revenue_trend query")?;

        let category_query = QueryConfig {
            name: "category_distribution".to_string(),
            description: Some("Product sales by category query".to_string()),
            sql: "SELECT 
    category,
    SUM(total_quantity_sold) as units_sold
FROM marts.product_performance
GROUP BY category
ORDER BY units_sold DESC"
                .to_string(),
        };
        let category_query_yaml = serde_yml::to_string(&category_query)?;
        fs::write(
            queries_dir.join("category_distribution.yml"),
            category_query_yaml,
        )
        .context("Failed to write category_distribution query")?;

        let device_query = QueryConfig {
            name: "device_distribution".to_string(),
            description: Some("User actions by device type query".to_string()),
            sql: "SELECT 
    device,
    COUNT(*) as action_count
FROM staging_app_logs
GROUP BY device"
                .to_string(),
        };
        let device_query_yaml = serde_yml::to_string(&device_query)?;
        fs::write(
            queries_dir.join("device_distribution.yml"),
            device_query_yaml,
        )
        .context("Failed to write device_distribution query")?;

        // Now create the dashboards that reference the queries
        let revenue_config = DashboardConfig {
            name: "revenue_trend".to_string(),
            description: Some("Daily Revenue Trend".to_string()),
            query: "revenue_trend".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Line,
                x_column: "date".to_string(),
                y_column: "daily_revenue".to_string(),
            },
        };
        let revenue_yaml = serde_yml::to_string(&revenue_config)?;
        fs::write(dashboards_dir.join("revenue_trend.yml"), revenue_yaml)
            .context("Failed to write revenue_trend dashboard")?;

        let category_config = DashboardConfig {
            name: "category_distribution".to_string(),
            description: Some("Product Sales by Category".to_string()),
            query: "category_distribution".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Bar,
                x_column: "category".to_string(),
                y_column: "units_sold".to_string(),
            },
        };
        let category_yaml = serde_yml::to_string(&category_config)?;
        fs::write(
            dashboards_dir.join("category_distribution.yml"),
            category_yaml,
        )
        .context("Failed to write category_distribution dashboard")?;

        let device_config = DashboardConfig {
            name: "device_distribution".to_string(),
            description: Some("User Actions by Device Type".to_string()),
            query: "device_distribution".to_string(),
            chart: ChartConfig {
                chart_type: ChartType::Bar,
                x_column: "device".to_string(),
                y_column: "action_count".to_string(),
            },
        };
        let device_yaml = serde_yml::to_string(&device_config)?;
        fs::write(dashboards_dir.join("device_distribution.yml"), device_yaml)
            .context("Failed to write device_distribution dashboard")?;

        Ok(())
    }
}

fn ensure_secret_key(project_path: &std::path::Path) -> Result<()> {
    let key_path = project_path.join(".secret.key");

    fs::create_dir_all(project_path)
        .with_context(|| format!("Failed to create directory: {}", project_path.display()))?;

    if !key_path.exists() {
        generate_secret_key(&key_path)?;
    }
    Ok(())
}

fn generate_secret_key(key_path: &std::path::Path) -> Result<()> {
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
    use ring::rand::{SecureRandom, SystemRandom};

    let mut key_bytes = [0u8; 32];
    let rng = SystemRandom::new();
    rng.fill(&mut key_bytes)
        .map_err(|_| anyhow::anyhow!("Failed to generate random key"))?;

    let key_base64 = BASE64.encode(key_bytes);

    let key_content = format!(
        "# FeatherBox Secret Key\n# DO NOT share publicly\n# Generated: {}\n\n{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        key_base64
    );

    fs::write(key_path, key_content)
        .with_context(|| format!("Failed to write key file: {}", key_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::StorageConfig;
    use std::process::Command;
    use tempfile;

    fn get_featherbox_binary() -> PathBuf {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        PathBuf::from(manifest_dir).join("target/debug/featherbox")
    }

    fn run_featherbox_command(
        args: &[&str],
        working_dir: &std::path::Path,
    ) -> Result<(bool, String)> {
        let featherbox_binary = get_featherbox_binary();

        if !featherbox_binary.exists() {
            let build_output = Command::new("cargo").arg("build").output()?;
            if !build_output.status.success() {
                anyhow::bail!("Failed to build featherbox binary");
            }
        }

        let mut cmd = Command::new(&featherbox_binary);
        cmd.args(args).current_dir(working_dir);

        let output = cmd.output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined_output = format!("{stdout}{stderr}");

        Ok((output.status.success(), combined_output))
    }

    #[test]
    fn test_featherbox_new_command() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_new_project";

        let (success, output) = run_featherbox_command(&["new", project_name], temp_dir.path())?;

        if !success {
            anyhow::bail!("featherbox new failed: {}", output);
        }

        let project_dir = temp_dir.path().join(project_name);
        assert!(project_dir.exists(), "Project directory should be created");
        assert!(
            project_dir.join("project.yml").exists(),
            "project.yml should exist"
        );
        assert!(
            project_dir.join(".secret.key").exists(),
            "secret key should be created"
        );
        assert!(
            project_dir.join("adapters").exists(),
            "adapters directory should exist"
        );
        assert!(
            project_dir.join("models").exists(),
            "models directory should exist"
        );

        Ok(())
    }

    #[test]
    fn test_featherbox_help() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let (success, output) = run_featherbox_command(&["--help"], temp_dir.path())?;

        assert!(success, "Help command should succeed");
        assert!(output.contains("new"), "Help should mention 'new' command");
        assert!(
            output.contains("start"),
            "Help should mention 'start' command"
        );

        Ok(())
    }

    #[test]
    fn test_featherbox_version() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let (success, output) = run_featherbox_command(&["--version"], temp_dir.path())?;

        assert!(success, "Version command should succeed");
        assert!(
            output.contains("featherbox"),
            "Version should mention featherbox"
        );

        Ok(())
    }

    #[test]
    fn test_project_structure_after_new() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "structure_test_project";

        let (success, _) = run_featherbox_command(&["new", project_name], temp_dir.path())?;
        assert!(success, "Project creation should succeed");

        let project_dir = temp_dir.path().join(project_name);

        let project_yml_content = fs::read_to_string(project_dir.join("project.yml"))?;
        assert!(project_yml_content.contains("storage:"));
        assert!(project_yml_content.contains("database:"));
        assert!(project_yml_content.contains("connections:"));

        let gitignore_content = fs::read_to_string(project_dir.join(".gitignore"))?;
        assert!(gitignore_content.contains("storage/"));
        assert!(gitignore_content.contains("database.db"));

        let secret_key_path = project_dir.join(".secret.key");
        assert!(secret_key_path.exists());
        let secret_key_content = fs::read_to_string(&secret_key_path)?;
        assert!(
            !secret_key_content.trim().is_empty(),
            "Secret key should have content"
        );

        Ok(())
    }

    #[test]
    fn test_project_builder() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";
        let config = ProjectConfig::new();

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        builder.create_project_directory()?;
        builder.create_secret_key()?;
        builder.save_project_config()?;
        builder.create_gitignore()?;

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("project.yml").exists());
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        let content = fs::read_to_string(project_path.join("project.yml"))?;
        assert!(content.contains("storage:"));
        assert!(content.contains("database:"));
        assert!(content.contains("connections:"));
        assert!(content.contains("local_files:"));
        assert!(content.contains("sample_db:"));

        assert!(project_path.join(".secret.key").exists());
        assert!(project_path.join(".gitignore").exists());

        let gitignore_content = fs::read_to_string(project_path.join(".gitignore"))?;
        assert!(gitignore_content.contains(".secret.key"));
        assert!(gitignore_content.contains("storage/"));
        assert!(gitignore_content.contains("database.db"));

        Ok(())
    }

    #[test]
    fn test_project_builder_already_exists() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "existing_project";
        let config = ProjectConfig::new();

        fs::create_dir_all(temp_dir.path().join(project_name))?;

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        let result = builder.create_project_directory();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        Ok(())
    }

    #[test]
    fn test_project_builder_directories() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let project_name = "test_project";
        let config = ProjectConfig::new();

        let builder = ProjectBuilder::with_current_dir(
            project_name.to_string(),
            &config,
            temp_dir.path().to_path_buf(),
        );
        builder.create_project_directory()?;

        let project_path = temp_dir.path().join(project_name);
        assert!(project_path.join("adapters").is_dir());
        assert!(project_path.join("models").is_dir());

        Ok(())
    }

    #[test]
    fn test_project_config_validate() -> Result<()> {
        let mut config = ProjectConfig::default();
        assert!(config.validate().is_ok());

        config.storage = StorageConfig::LocalFile {
            path: "".to_string(),
        };
        assert!(config.validate().is_err());
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("Storage path cannot be empty")
        );

        config.storage = StorageConfig::LocalFile {
            path: "./storage".to_string(),
        };

        Ok(())
    }
}
