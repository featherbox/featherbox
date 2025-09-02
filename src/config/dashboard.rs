use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DashboardConfig {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    pub chart: ChartConfig,
}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChartConfig {
    #[serde(rename = "type")]
    pub chart_type: ChartType,
    pub x_column: String,
    pub y_column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChartType {
    Line,
    Bar,
}

pub fn parse_dashboard_config(yaml_str: &str) -> anyhow::Result<DashboardConfig> {
    serde_yml::from_str(yaml_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse dashboard config: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dashboard_config_line() {
        let yaml_str = r#"
            name: "sales_dashboard"
            description: "Monthly sales overview"
            query: "monthly_sales"
            chart:
              type: line
              x_column: "month"
              y_column: "revenue"
        "#;

        let config = parse_dashboard_config(yaml_str).unwrap();

        assert_eq!(config.name, "sales_dashboard");
        assert_eq!(
            config.description,
            Some("Monthly sales overview".to_string())
        );
        assert_eq!(config.query, "monthly_sales");
        assert_eq!(config.chart.chart_type, ChartType::Line);
        assert_eq!(config.chart.x_column, "month");
        assert_eq!(config.chart.y_column, "revenue");
    }

    #[test]
    fn test_parse_dashboard_config_bar() {
        let yaml_str = r#"
            name: "category_sales"
            query: "category_breakdown"
            chart:
              type: bar
              x_column: "category"
              y_column: "total_amount"
        "#;

        let config = parse_dashboard_config(yaml_str).unwrap();

        assert_eq!(config.name, "category_sales");
        assert_eq!(config.description, None);
        assert_eq!(config.query, "category_breakdown");
        assert_eq!(config.chart.chart_type, ChartType::Bar);
        assert_eq!(config.chart.x_column, "category");
        assert_eq!(config.chart.y_column, "total_amount");
    }

    #[test]
    fn test_parse_dashboard_config_missing_name() {
        let yaml_str = r#"
            query: "test_query"
            chart:
              type: line
              x_column: "x"
              y_column: "y"
        "#;

        let result = parse_dashboard_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_parse_dashboard_config_missing_query() {
        let yaml_str = r#"
            name: "test_dashboard"
            chart:
              type: line
              x_column: "x"
              y_column: "y"
        "#;

        let result = parse_dashboard_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("query"));
    }

    #[test]
    fn test_parse_dashboard_config_invalid_chart_type() {
        let yaml_str = r#"
            name: "test_dashboard"
            query: "test_query"
            chart:
              type: pie
              x_column: "x"
              y_column: "y"
        "#;

        let result = parse_dashboard_config(yaml_str);
        assert!(result.is_err());
    }


    #[test]
    fn test_chart_type_serialization() {
        let line_chart = ChartType::Line;
        let bar_chart = ChartType::Bar;

        let line_json = serde_json::to_string(&line_chart).unwrap();
        let bar_json = serde_json::to_string(&bar_chart).unwrap();

        assert_eq!(line_json, "\"line\"");
        assert_eq!(bar_json, "\"bar\"");

        let deserialized_line: ChartType = serde_json::from_str(&line_json).unwrap();
        let deserialized_bar: ChartType = serde_json::from_str(&bar_json).unwrap();

        assert_eq!(deserialized_line, ChartType::Line);
        assert_eq!(deserialized_bar, ChartType::Bar);
    }
}
