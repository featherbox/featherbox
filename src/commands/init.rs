use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::ProjectConfig;

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

        let users_adapter = adapters_dir.join("users.yml");
        let users_content = r#"name: users
connection: local_files
type: csv
source:
  path: users.csv
  options:
    header: true
    delimiter: ','
    quote: '"'
"#;
        fs::write(&users_adapter, users_content).context("Failed to write users adapter")?;

        let app_logs_adapter = adapters_dir.join("app_logs.yml");
        let app_logs_content = r#"name: app_logs
connection: local_files
type: json
source:
  path: app_log_*.json
  options:
    format: array
"#;
        fs::write(&app_logs_adapter, app_logs_content)
            .context("Failed to write app_logs adapter")?;

        let products_adapter = adapters_dir.join("products.yml");
        let products_content = r#"name: products
connection: sample_db
type: table
source:
  table_name: products
"#;
        fs::write(&products_adapter, products_content)
            .context("Failed to write products adapter")?;

        let orders_adapter = adapters_dir.join("orders.yml");
        let orders_content = r#"name: orders
connection: sample_db
type: table
source:
  table_name: orders
"#;
        fs::write(&orders_adapter, orders_content).context("Failed to write orders adapter")?;

        Ok(())
    }

    pub fn create_sample_models(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let staging_dir = project_path.join("models/staging");
        let marts_dir = project_path.join("models/marts");

        let clean_logs = staging_dir.join("app_logs.yml");
        let clean_logs_content = r#"description: 'Cleaned application logs'
sql: |
  SELECT 
    timestamp::TIMESTAMP as event_time,
    user_id,
    action,
    device,
    duration,
    DATE(timestamp) as event_date
  FROM app_logs
  WHERE duration > 0
"#;
        fs::write(&clean_logs, clean_logs_content).context("Failed to write app_logs model")?;

        let user_activity = marts_dir.join("user_activity_summary.yml");
        let user_activity_content = r#"description: 'User activity summary'
sql: |
  SELECT 
    u.user_id,
    u.name,
    u.email,
    COUNT(DISTINCT l.event_date) as active_days,
    COUNT(l.action) as total_actions,
    AVG(l.duration) as avg_duration,
    MAX(l.event_time) as last_activity
  FROM users u
  LEFT JOIN staging_app_logs l ON u.user_id = l.user_id
  GROUP BY u.user_id, u.name, u.email
"#;
        fs::write(&user_activity, user_activity_content)
            .context("Failed to write user_activity_summary model")?;

        let product_performance = marts_dir.join("product_performance.yml");
        let product_performance_content = r#"description: 'Product performance metrics'
sql: |
  SELECT 
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
  ORDER BY total_revenue DESC
"#;
        fs::write(&product_performance, product_performance_content)
            .context("Failed to write product_performance model")?;

        Ok(())
    }

    pub fn create_sample_queries(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let queries_dir = project_path.join("queries");

        let top_products = queries_dir.join("top_products.yml");
        let top_products_content = r#"name: top_products
description: Top 5 products by revenue
sql: |
  SELECT 
    product_name,
    category,
    total_revenue,
    order_count
  FROM marts.product_performance
  ORDER BY total_revenue DESC
  LIMIT 5
"#;
        fs::write(&top_products, top_products_content)
            .context("Failed to write top_products query")?;

        let active_users = queries_dir.join("active_users.yml");
        let active_users_content = r#"name: active_users
description: Most active users by action count
sql: |
  SELECT 
    name,
    email,
    total_actions,
    active_days,
    ROUND(avg_duration, 2) as avg_duration_seconds
  FROM marts.user_activity_summary
  WHERE total_actions > 0
  ORDER BY total_actions DESC
  LIMIT 10
"#;
        fs::write(&active_users, active_users_content)
            .context("Failed to write active_users query")?;

        Ok(())
    }

    pub fn create_sample_dashboards(&self) -> Result<()> {
        let project_path = self.current_dir.join(&self.project_name);
        let dashboards_dir = project_path.join("dashboards");

        let revenue_dashboard = dashboards_dir.join("revenue_trend.yml");
        let revenue_content = r#"name: revenue_trend
title: Daily Revenue Trend
type: line
query: |
  SELECT 
    DATE(order_date) as date,
    SUM(total_amount) as daily_revenue
  FROM orders
  WHERE status = 'completed'
  GROUP BY DATE(order_date)
  ORDER BY date
config:
  x_axis: date
  y_axis: daily_revenue
  x_label: Date
  y_label: Revenue ($)
"#;
        fs::write(&revenue_dashboard, revenue_content)
            .context("Failed to write revenue_trend dashboard")?;

        let category_distribution = dashboards_dir.join("category_distribution.yml");
        let category_content = r#"name: category_distribution  
title: Product Sales by Category
type: bar
query: |
  SELECT 
    category,
    SUM(total_quantity_sold) as units_sold
  FROM marts.product_performance
  GROUP BY category
  ORDER BY units_sold DESC
config:
  x_axis: category
  y_axis: units_sold
  x_label: Category
  y_label: Units Sold
  color: '#4299E1'
"#;
        fs::write(&category_distribution, category_content)
            .context("Failed to write category_distribution dashboard")?;

        let user_device_pie = dashboards_dir.join("device_distribution.yml");
        let device_content = r#"name: device_distribution
title: User Actions by Device Type
type: pie
query: |
  SELECT 
    device,
    COUNT(*) as action_count
  FROM staging_app_logs
  GROUP BY device
config:
  label: device
  value: action_count
"#;
        fs::write(&user_device_pie, device_content)
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
    use tempfile;

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
