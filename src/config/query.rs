use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryConfig {
    pub name: String,
    pub description: Option<String>,
    pub sql: String,
}

impl QueryConfig {
    pub fn has_changed(&self, other: &Self) -> bool {
        self.name != other.name 
            || self.sql != other.sql
    }
}

pub fn parse_query_config(yaml_str: &str) -> anyhow::Result<QueryConfig> {
    serde_yml::from_str(yaml_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse query config: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_config_basic() {
        let yaml_str = r#"
            name: "User Analysis"
            description: "Daily active users analysis"
            sql: |
              SELECT 
                DATE(created_at) as date,
                COUNT(DISTINCT user_id) as active_users
              FROM user_events 
              WHERE created_at >= CURRENT_DATE - INTERVAL 7 DAY
              GROUP BY DATE(created_at)
              ORDER BY date DESC
        "#;

        let config = parse_query_config(yaml_str).unwrap();

        assert_eq!(config.name, "User Analysis");
        assert_eq!(config.description, Some("Daily active users analysis".to_string()));
        assert!(config.sql.contains("SELECT"));
        assert!(config.sql.contains("FROM user_events"));
    }

    #[test]
    fn test_parse_query_config_minimal() {
        let yaml_str = r#"
            name: "Simple Query"
            sql: "SELECT * FROM users LIMIT 10"
        "#;

        let config = parse_query_config(yaml_str).unwrap();

        assert_eq!(config.name, "Simple Query");
        assert_eq!(config.description, None);
        assert_eq!(config.sql, "SELECT * FROM users LIMIT 10");
    }

    #[test]
    fn test_parse_query_config_missing_name() {
        let yaml_str = r#"
            sql: "SELECT * FROM users"
        "#;

        let result = parse_query_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_parse_query_config_missing_sql() {
        let yaml_str = r#"
            name: "Test Query"
        "#;

        let result = parse_query_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("sql"));
    }

    #[test]
    fn test_query_config_has_changed() {
        let config1 = QueryConfig {
            name: "Test Query".to_string(),
            description: Some("Test description".to_string()),
            sql: "SELECT * FROM users".to_string(),
        };

        let config2 = config1.clone();
        assert!(!config1.has_changed(&config2));

        let mut config3 = config1.clone();
        config3.description = Some("Modified description".to_string());
        assert!(!config1.has_changed(&config3));

        let mut config4 = config1.clone();
        config4.name = "Modified Name".to_string();
        assert!(config1.has_changed(&config4));

        let mut config5 = config1.clone();
        config5.sql = "SELECT * FROM products".to_string();
        assert!(config1.has_changed(&config5));
    }
}