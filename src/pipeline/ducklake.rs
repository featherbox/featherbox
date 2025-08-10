use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Sqlite { path: String },
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    LocalFile { path: String },
}

pub struct DuckLake {
    catalog_config: CatalogConfig,
    storage_config: StorageConfig,
    connection: Connection,
}

impl DuckLake {
    pub async fn new(catalog_config: CatalogConfig, storage_config: StorageConfig) -> Result<Self> {
        let connection =
            Connection::open_in_memory().context("Failed to create DuckDB connection")?;

        let instance = Self {
            catalog_config,
            storage_config,
            connection,
        };

        instance.initialize().await?;
        Ok(instance)
    }

    async fn initialize(&self) -> Result<()> {
        self.connection
            .execute_batch("INSTALL ducklake; LOAD ducklake;")
            .context("Failed to install and load extensions")?;

        match &self.catalog_config {
            CatalogConfig::Sqlite { path } => {
                let catalog_path = Path::new(path);
                if let Some(parent) = catalog_path.parent() {
                    std::fs::create_dir_all(parent)
                        .context("Failed to create catalog directory")?;
                }

                let data_path = match &self.storage_config {
                    StorageConfig::LocalFile { path } => path,
                };

                self.connection
                    .execute_batch("INSTALL sqlite; LOAD sqlite;")
                    .context("Failed to install and load SQLite extension")?;

                let attach_sql = format!(
                    "ATTACH 'ducklake:sqlite:{path}' AS db (DATA_PATH '{data_path}'); USE db;"
                );
                self.connection
                    .execute_batch(&attach_sql)
                    .context("Failed to attach DuckLake catalog")?;
            }
        }

        match &self.storage_config {
            StorageConfig::LocalFile { path } => {
                std::fs::create_dir_all(path).context("Failed to create storage directory")?;
            }
        }

        Ok(())
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.connection
            .execute_batch(sql)
            .context("Failed to execute batch SQL")?;
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let mut stmt = self.connection.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let column_count = rows.as_ref().unwrap().column_count();

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut row_data = Vec::new();
            for i in 0..column_count {
                use duckdb::types::Value;
                let value: Result<Value, _> = row.get(i);
                let string_value = match value {
                    Ok(Value::Null) => "NULL".to_string(),
                    Ok(Value::Boolean(b)) => b.to_string(),
                    Ok(Value::TinyInt(i)) => i.to_string(),
                    Ok(Value::SmallInt(i)) => i.to_string(),
                    Ok(Value::Int(i)) => i.to_string(),
                    Ok(Value::BigInt(i)) => i.to_string(),
                    Ok(Value::HugeInt(i)) => i.to_string(),
                    Ok(Value::UTinyInt(i)) => i.to_string(),
                    Ok(Value::USmallInt(i)) => i.to_string(),
                    Ok(Value::UInt(i)) => i.to_string(),
                    Ok(Value::UBigInt(i)) => i.to_string(),
                    Ok(Value::Float(f)) => f.to_string(),
                    Ok(Value::Double(f)) => f.to_string(),
                    Ok(Value::Decimal(d)) => d.to_string(),
                    Ok(Value::Text(s)) => s,
                    Ok(Value::Blob(b)) => format!("{b:?}"),
                    Ok(Value::Date32(d)) => d.to_string(),
                    Ok(Value::Time64(_, t)) => t.to_string(),
                    Ok(Value::Timestamp(_, t)) => t.to_string(),
                    Ok(Value::Interval {
                        months,
                        days,
                        nanos,
                    }) => {
                        format!("Interval({months} months, {days} days, {nanos} nanos)")
                    }
                    Ok(_) => "UNKNOWN".to_string(),
                    Err(_) => "ERROR".to_string(),
                };
                row_data.push(string_value);
            }
            results.push(row_data);
        }

        Ok(results)
    }

    pub fn create_table_from_query(&self, table_name: &str, query: &str) -> Result<()> {
        let sql = format!("CREATE OR REPLACE TABLE {table_name} AS ({query});");
        self.execute_batch(&sql)
            .with_context(|| format!("Failed to create table '{table_name}' from query"))
    }

    pub fn generate_temp_table_name(prefix: &str) -> String {
        format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
    }

    pub fn drop_temp_table(&self, table_name: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table_name};");
        self.connection
            .execute_batch(&drop_sql)
            .with_context(|| format!("Failed to drop temporary table: {table_name}"))
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        );
        let results = self.query(&sql)?;
        let exists = if let Some(row) = results.first() {
            if let Some(count_str) = row.first() {
                count_str.parse::<i64>().unwrap_or(0) > 0
            } else {
                false
            }
        } else {
            false
        };
        Ok(exists)
    }

    pub fn table_schema(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        );
        let results = self.query(&sql)?;
        let columns = results
            .into_iter()
            .filter_map(|row| {
                if row.len() >= 2 {
                    Some((row[0].clone(), row[1].clone()))
                } else {
                    None
                }
            })
            .collect();
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ducklake_new() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await;
        assert!(ducklake.is_ok());
    }

    #[tokio::test]
    async fn test_create_table_from_query() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_query";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        let query = "SELECT 1 as id, 'Alice' as name";
        ducklake
            .create_table_from_query("test_table", query)
            .unwrap();

        let results = ducklake.query("SELECT * FROM test_table").unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec!["1", "Alice"]);
    }

    #[tokio::test]
    async fn test_execute_batch_and_query() {
        use std::fs;

        let test_dir = "/tmp/ducklake_test_exec";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let catalog_config = CatalogConfig::Sqlite {
            path: format!("{test_dir}/test_catalog.sqlite"),
        };
        let storage_config = StorageConfig::LocalFile {
            path: format!("{test_dir}/test_storage"),
        };

        let ducklake = DuckLake::new(catalog_config, storage_config).await.unwrap();

        ducklake.execute_batch(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR); INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');"
        ).unwrap();

        let results = ducklake
            .query("SELECT * FROM test_table ORDER BY id")
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec!["1", "Alice"]);
        assert_eq!(results[1], vec!["2", "Bob"]);
    }
}
