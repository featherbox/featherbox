use crate::pipeline::ducklake::DuckLake;
use anyhow::{Context, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct Logger {
    ducklake: Arc<DuckLake>,
}

impl Logger {
    pub async fn new(ducklake: Arc<DuckLake>) -> Result<Self> {
        let logger = Self { ducklake };
        logger.initialize().await?;
        Ok(logger)
    }

    async fn initialize(&self) -> Result<()> {
        let create_task_logs_table = r#"
            CREATE TABLE IF NOT EXISTS db.__fbox_task_logs (
                executed_at TIMESTAMP,
                pipeline_id INTEGER,
                table_name VARCHAR,
                status VARCHAR,
                error_message TEXT,
                execution_time_ms BIGINT
            )
        "#;

        let create_pipeline_logs_table = r#"
            CREATE TABLE IF NOT EXISTS db.__fbox_pipeline_logs (
                executed_at TIMESTAMP,
                pipeline_id INTEGER,
                event_type VARCHAR,
                message TEXT,
                error_details TEXT
            )
        "#;

        self.ducklake
            .execute_batch(create_task_logs_table)
            .context("Failed to create task logs table")?;

        self.ducklake
            .execute_batch(create_pipeline_logs_table)
            .context("Failed to create pipeline logs table")?;

        Ok(())
    }

    pub fn log_task_execution(
        &self,
        pipeline_id: i32,
        table_name: &str,
        status: &str,
        error_message: Option<&str>,
        execution_time_ms: u64,
    ) -> Result<()> {
        let executed_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let error_msg = error_message.unwrap_or("");

        let escaped_error_msg = error_msg.replace("'", "''");

        let sql = format!(
            r#"INSERT INTO db.__fbox_task_logs
               (executed_at, pipeline_id, table_name, status, error_message, execution_time_ms)
               VALUES ('{executed_at}', {pipeline_id}, '{table_name}', '{status}', '{escaped_error_msg}', {execution_time_ms})"#
        );

        if let Err(e) = self.ducklake.execute_batch(&sql) {
            eprintln!("Warning: Failed to write task log: {e}");
        }

        Ok(())
    }

    pub fn log_pipeline_event(
        &self,
        pipeline_id: i32,
        event_type: &str,
        message: &str,
        error_details: Option<&str>,
    ) -> Result<()> {
        let executed_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let error_msg = error_details.unwrap_or("");

        let escaped_message = message.replace("'", "''");
        let escaped_error_msg = error_msg.replace("'", "''");

        let sql = format!(
            r#"INSERT INTO db.__fbox_pipeline_logs
               (executed_at, pipeline_id, event_type, message, error_details)
               VALUES ('{executed_at}', {pipeline_id}, '{event_type}', '{escaped_message}', '{escaped_error_msg}')"#
        );

        if let Err(e) = self.ducklake.execute_batch(&sql) {
            eprintln!("Warning: Failed to write pipeline log: {e}");
        }

        Ok(())
    }

    pub fn query_logs(&self, query: &str) -> Result<Vec<Vec<String>>> {
        self.ducklake.query(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::project::StorageConfig, pipeline::ducklake::CatalogConfig};

    #[tokio::test]
    async fn test_logger_success_cases() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("test_success_logs.db")
                .to_string_lossy()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir
                .path()
                .join("storage")
                .to_string_lossy()
                .to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let logger = Logger::new(ducklake).await.unwrap();

        logger
            .log_task_execution(456, "users_table", "SUCCESS", None, 1200)
            .unwrap();

        let results = logger
            .query_logs("SELECT pipeline_id, table_name, status, error_message, execution_time_ms FROM db.__fbox_task_logs ORDER BY executed_at")
            .unwrap();

        assert_eq!(results.len(), 1);
        let row = &results[0];

        assert_eq!(row[0], "456");
        assert_eq!(row[1], "users_table");
        assert_eq!(row[2], "SUCCESS");
        assert_eq!(row[3], "");
        assert_eq!(row[4], "1200");
    }

    #[tokio::test]
    async fn test_logger_with_error_message() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("test_error_logs.db")
                .to_string_lossy()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir
                .path()
                .join("storage")
                .to_string_lossy()
                .to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let logger = Logger::new(ducklake).await.unwrap();

        logger
            .log_task_execution(
                789,
                "failed_model",
                "FAILED",
                Some("SQL execution failed: syntax error at 'invalid'"),
                2800,
            )
            .unwrap();

        let results = logger
            .query_logs("SELECT pipeline_id, table_name, status, error_message, execution_time_ms FROM db.__fbox_task_logs ORDER BY executed_at")
            .unwrap();

        assert_eq!(results.len(), 1);
        let row = &results[0];

        assert_eq!(row[0], "789");
        assert_eq!(row[1], "failed_model");
        assert_eq!(row[2], "FAILED");
        assert_eq!(row[3], "SQL execution failed: syntax error at 'invalid'");
        assert_eq!(row[4], "2800");
    }

    #[tokio::test]
    async fn test_logger_pipeline_events() {
        use tempfile::tempdir;

        let temp_dir = tempdir().unwrap();
        let catalog_config = CatalogConfig::Sqlite {
            path: temp_dir
                .path()
                .join("test_pipeline_logs.db")
                .to_string_lossy()
                .to_string(),
        };
        let storage_config = StorageConfig::LocalFile {
            path: temp_dir
                .path()
                .join("storage")
                .to_string_lossy()
                .to_string(),
        };

        let ducklake = Arc::new(DuckLake::new(catalog_config, storage_config).await.unwrap());
        let logger = Logger::new(ducklake).await.unwrap();

        logger
            .log_pipeline_event(123, "PIPELINE_START", "Pipeline execution started", None)
            .unwrap();

        logger
            .log_pipeline_event(
                123,
                "PIPELINE_FAILED",
                "Pipeline execution failed",
                Some("Database connection timeout"),
            )
            .unwrap();

        let results = logger
            .query_logs("SELECT pipeline_id, event_type, message, error_details FROM db.__fbox_pipeline_logs ORDER BY rowid")
            .unwrap();

        assert_eq!(results.len(), 2);

        let row1 = &results[0];
        assert_eq!(row1[0], "123");
        assert_eq!(row1[1], "PIPELINE_START");
        assert_eq!(row1[2], "Pipeline execution started");
        assert_eq!(row1[3], "");

        let row2 = &results[1];
        assert_eq!(row2[0], "123");
        assert_eq!(row2[1], "PIPELINE_FAILED");
        assert_eq!(row2[2], "Pipeline execution failed");
        assert_eq!(row2[3], "Database connection timeout");
    }
}
