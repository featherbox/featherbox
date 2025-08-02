#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelConfig {
    pub description: Option<String>,
    pub max_age: Option<u64>,
    pub sql: String,
}

pub fn parse_model_config(yaml: &yaml_rust2::Yaml) -> ModelConfig {
    let description = yaml["description"].as_str().map(|s| s.to_string());
    let max_age = yaml["max_age"].as_i64().map(|v| v as u64);

    let sql = yaml["sql"]
        .as_str()
        .expect("Model SQL is required")
        .to_string();

    ModelConfig {
        description,
        max_age,
        sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml_rust2::YamlLoader;

    #[test]
    fn test_parse_model_config() {
        let yaml_str = r#"
            description: Logs for user analysis
            max_age: 86400
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
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_model_config(yaml);

        assert_eq!(
            config.description,
            Some("Logs for user analysis".to_string())
        );
        assert_eq!(config.max_age, Some(86400));
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
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_model_config(yaml);

        assert_eq!(config.description, None);
        assert_eq!(config.max_age, None);
        assert_eq!(config.sql, "SELECT * FROM users");
    }

    #[test]
    fn test_parse_model_config_with_complex_sql() {
        let yaml_str = r#"
            description: Daily aggregated statistics
            max_age: 21600
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
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_model_config(yaml);

        assert_eq!(
            config.description,
            Some("Daily aggregated statistics".to_string())
        );
        assert_eq!(config.max_age, Some(21600));
        assert!(config.sql.contains("WITH daily_counts AS"));
        assert!(config.sql.contains("AVG(response_time)"));
        assert!(config.sql.contains("LIMIT 30"));
    }

    #[test]
    #[should_panic(expected = "Model SQL is required")]
    fn test_parse_model_config_missing_sql() {
        let yaml_str = r#"
            description: Model without SQL
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_model_config(yaml);
    }
}
