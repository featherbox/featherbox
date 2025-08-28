use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelConfig {
    pub description: Option<String>,
    pub sql: String,
}

impl ModelConfig {
    pub fn has_changed(&self, other: &Self) -> bool {
        self.sql != other.sql
    }
}

pub fn parse_model_config(yaml_str: &str) -> anyhow::Result<ModelConfig> {
    serde_yml::from_str(yaml_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse model config: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_model_config() {
        let yaml_str = r#"
            description: Logs for user analysis
            sql: |
              SELECT
                timestamp,
                path,
                method,
                status
              FROM
                logs
              ORDER BY
                timestamp DESC
        "#;

        let config = parse_model_config(yaml_str).unwrap();

        assert_eq!(
            config.description,
            Some("Logs for user analysis".to_string())
        );
        assert!(config.sql.contains("SELECT"));
        assert!(config.sql.contains("timestamp"));
        assert!(config.sql.contains("FROM"));
        assert!(config.sql.contains("logs"));
    }

    #[test]
    fn test_parse_model_config_minimal() {
        let yaml_str = r#"
            sql: SELECT * FROM users
        "#;

        let config = parse_model_config(yaml_str).unwrap();

        assert_eq!(config.description, None);
        assert_eq!(config.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_parse_model_config_with_complex_sql() {
        let yaml_str = r#"
            description: Daily aggregated statistics
            sql: |
              WITH daily_counts AS (
                SELECT
                  DATE(timestamp) as day,
                  COUNT(*) as request_count,
                  AVG(response_time) as avg_response_time
                FROM logs
                WHERE status = 200
                GROUP BY DATE(timestamp)
              )
              SELECT * FROM daily_counts
              ORDER BY day DESC
              LIMIT 30
        "#;

        let config = parse_model_config(yaml_str).unwrap();

        assert_eq!(
            config.description,
            Some("Daily aggregated statistics".to_string())
        );
        assert!(config.sql.contains("WITH daily_counts AS"));
        assert!(config.sql.contains("AVG(response_time)"));
        assert!(config.sql.contains("LIMIT 30"));
    }

    #[test]
    fn test_parse_model_config_missing_sql() {
        let yaml_str = r#"
            description: Model without SQL
        "#;

        let result = parse_model_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("sql"));
    }

    #[test]
    fn test_model_config_has_changed() {
        let config1 = ModelConfig {
            description: Some("Test model".to_string()),
            sql: "SELECT * FROM users".to_string(),
        };

        let config2 = config1.clone();
        assert!(!config1.has_changed(&config2));

        let mut config3 = config1.clone();
        config3.description = Some("Modified description".to_string());
        assert!(!config1.has_changed(&config3));

        let mut config4 = config1.clone();
        config4.sql = "SELECT id, name FROM users".to_string();
        assert!(config1.has_changed(&config4));

        let mut config5 = config1.clone();
        config5.sql = "SELECT * FROM users WHERE active = true".to_string();
        assert!(config1.has_changed(&config5));

        let config6 = ModelConfig {
            description: None,
            sql: "SELECT * FROM users".to_string(),
        };
        assert!(!config1.has_changed(&config6));
    }

    #[test]
    fn test_model_config_serde() {
        let config = ModelConfig {
            description: Some("Test model for analysis".to_string()),
            sql: r#"SELECT 
                id,
                name,
                email
              FROM users
              WHERE created_at > '2024-01-01'"#
                .to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ModelConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);

        assert!(json.contains("\"description\":\"Test model for analysis\""));
        assert!(json.contains("\"sql\":"));
        assert!(json.contains("WHERE created_at"));
    }
}
