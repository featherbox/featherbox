use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdapterConfig {
    pub connection: String,
    pub description: Option<String>,
    pub source: AdapterSource,
    pub columns: Vec<ColumnConfig>,
}

impl AdapterConfig {
    pub fn has_changed(&self, other: &Self) -> bool {
        self.connection != other.connection
            || self.source != other.source
            || self.columns != other.columns
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AdapterSource {
    #[serde(rename = "file")]
    File {
        file: FileConfig,
        format: FormatConfig,
    },
    #[serde(rename = "database")]
    Database { table_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileConfig {
    pub path: String,
    pub compression: Option<String>,
    pub max_batch_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatConfig {
    #[serde(rename = "type")]
    pub ty: String,
    pub delimiter: Option<String>,
    pub null_value: Option<String>,
    pub has_header: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub description: Option<String>,
}

pub fn parse_adapter_config(yaml_str: &str) -> anyhow::Result<AdapterConfig> {
    serde_yml::from_str(yaml_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse adapter config: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_adapter_config_database_format() {
        let yaml_str = r#"
            connection: test_db
            description: 'Database source configuration'
            source:
              type: database
              table_name: users
            columns:
              - name: id
                type: INTEGER
              - name: name
                type: STRING
        "#;

        let config = parse_adapter_config(yaml_str).unwrap();

        assert_eq!(config.connection, "test_db");
        assert_eq!(
            config.description,
            Some("Database source configuration".to_string())
        );

        match &config.source {
            AdapterSource::Database { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Database source"),
        }

        assert_eq!(config.columns.len(), 2);
        assert_eq!(config.columns[0].name, "id");
        assert_eq!(config.columns[0].ty, "INTEGER");
        assert_eq!(config.columns[1].name, "name");
        assert_eq!(config.columns[1].ty, "STRING");
    }

    #[test]
    fn test_parse_adapter_config_file_format() {
        let yaml_str = r#"
            connection: test_data
            description: 'File source configuration'
            source:
              type: file
              file:
                path: data/logs.json
                compression: gzip
                max_batch_size: 100MB
              format:
                type: json
            columns:
              - name: timestamp
                type: DATETIME
              - name: message
                type: STRING
        "#;

        let config = parse_adapter_config(yaml_str).unwrap();

        assert_eq!(config.connection, "test_data");
        assert_eq!(
            config.description,
            Some("File source configuration".to_string())
        );

        match &config.source {
            AdapterSource::File { file, format } => {
                assert_eq!(file.path, "data/logs.json");
                assert_eq!(file.compression, Some("gzip".to_string()));
                assert_eq!(file.max_batch_size, Some("100MB".to_string()));
                assert_eq!(format.ty, "json");
            }
            _ => panic!("Expected File source"),
        }

        assert_eq!(config.columns.len(), 2);
        assert_eq!(config.columns[0].name, "timestamp");
        assert_eq!(config.columns[0].ty, "DATETIME");
        assert_eq!(config.columns[1].name, "message");
        assert_eq!(config.columns[1].ty, "STRING");
    }

    #[test]
    fn test_parse_adapter_config_missing_source() {
        let yaml_str = r#"
            connection: test
            columns: []
        "#;

        let result = parse_adapter_config(yaml_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source"));
    }

    #[test]
    fn test_adapter_config_has_changed() {
        let config1 = AdapterConfig {
            connection: "test_db".to_string(),
            description: Some("Test adapter".to_string()),
            source: AdapterSource::Database {
                table_name: "users".to_string(),
            },
            columns: vec![ColumnConfig {
                name: "id".to_string(),
                ty: "INTEGER".to_string(),
                description: None,
            }],
        };

        let config2 = config1.clone();
        assert!(!config1.has_changed(&config2));

        let mut config3 = config1.clone();
        config3.description = Some("Modified description".to_string());
        assert!(!config1.has_changed(&config3));

        let mut config4 = config1.clone();
        config4.connection = "other_db".to_string();
        assert!(config1.has_changed(&config4));

        let mut config5 = config1.clone();
        config5.source = AdapterSource::Database {
            table_name: "products".to_string(),
        };
        assert!(config1.has_changed(&config5));

        let mut config6 = config1.clone();
        config6.columns.push(ColumnConfig {
            name: "name".to_string(),
            ty: "STRING".to_string(),
            description: None,
        });
        assert!(config1.has_changed(&config6));

        let mut config7 = config1.clone();
        config7.columns[0].ty = "BIGINT".to_string();
        assert!(config1.has_changed(&config7));

        let mut config8 = config1.clone();
        config8.columns[0].description = Some("Primary key".to_string());
        assert!(config1.has_changed(&config8));
    }
}
