#[derive(Debug, Clone, PartialEq)]
pub struct AdapterConfig {
    pub connection: String,
    pub description: Option<String>,
    pub source: AdapterSource,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AdapterSource {
    File {
        file: FileConfig,
        format: FormatConfig,
    },
    Database {
        table_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileConfig {
    pub path: String,
    pub compression: Option<String>,
    pub max_batch_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatConfig {
    pub ty: String,
    pub delimiter: Option<String>,
    pub null_value: Option<String>,
    pub has_header: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnConfig {
    pub name: String,
    pub ty: String,
    pub description: Option<String>,
}

pub fn parse_adapter_config(yaml: &yaml_rust2::Yaml) -> anyhow::Result<AdapterConfig> {
    let connection = yaml["connection"]
        .as_str()
        .expect("Adapter connection is required")
        .to_string();

    let description = yaml["description"].as_str().map(|s| s.to_string());

    let source = if !yaml["source"].is_badvalue() {
        parse_source(&yaml["source"])?
    } else if !yaml["file"].is_badvalue() && !yaml["format"].is_badvalue() {
        let file = parse_file_config(&yaml["file"]);
        let format = parse_format_config(&yaml["format"]);
        AdapterSource::File { file, format }
    } else {
        return Err(anyhow::anyhow!(
            "Either 'source' or both 'file' and 'format' are required"
        ));
    };

    let columns = parse_columns(&yaml["columns"]);

    Ok(AdapterConfig {
        connection,
        description,
        source,
        columns,
    })
}

fn parse_source(yaml: &yaml_rust2::Yaml) -> anyhow::Result<AdapterSource> {
    let source_type = yaml["type"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Source type is required"))?;

    match source_type {
        "file" => {
            let file = parse_file_config(&yaml["file"]);
            let format = parse_format_config(&yaml["format"]);
            Ok(AdapterSource::File { file, format })
        }
        "database" => {
            let table_name = yaml["table_name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Database table_name is required"))?
                .to_string();
            Ok(AdapterSource::Database { table_name })
        }
        _ => Err(anyhow::anyhow!("Unsupported source type: {}", source_type)),
    }
}

fn parse_file_config(yaml: &yaml_rust2::Yaml) -> FileConfig {
    let path = yaml["path"]
        .as_str()
        .expect("File path is required")
        .to_string();

    let compression = yaml["compression"].as_str().map(|s| s.to_string());
    let max_batch_size = yaml["max_batch_size"].as_str().map(|s| s.to_string());

    FileConfig {
        path,
        compression,
        max_batch_size,
    }
}

fn parse_format_config(yaml: &yaml_rust2::Yaml) -> FormatConfig {
    let ty = yaml["type"]
        .as_str()
        .expect("Format type is required")
        .to_string();

    let delimiter = yaml["delimiter"].as_str().map(|s| s.to_string());
    let null_value = yaml["null_value"].as_str().map(|s| s.to_string());
    let has_header = yaml["has_header"].as_bool();

    FormatConfig {
        ty,
        delimiter,
        null_value,
        has_header,
    }
}

fn parse_columns(yaml: &yaml_rust2::Yaml) -> Vec<ColumnConfig> {
    yaml.as_vec()
        .expect("Columns must be an array")
        .iter()
        .map(|column| {
            let name = column["name"]
                .as_str()
                .expect("Column name is required")
                .to_string();

            let ty = column["type"]
                .as_str()
                .expect("Column type is required")
                .to_string();

            let description = column["description"].as_str().map(|s| s.to_string());

            ColumnConfig {
                name,
                ty,
                description,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml_rust2::YamlLoader;

    #[test]
    fn test_parse_adapter_config_legacy_format() {
        let yaml_str = r#"
            connection: test_data
            description: 'Configuration for processing web server logs'
            file:
              path: <YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz
              compression: gzip
              max_batch_size: 100MB
            format:
              type: 'json'
            columns:
              - name: timestamp
                type: DATETIME
                description: 'The timestamp of the log entry'
              - name: status
                type: INTEGER
                description: 'The HTTP status code'
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_adapter_config(yaml).unwrap();

        assert_eq!(config.connection, "test_data");
        assert_eq!(
            config.description,
            Some("Configuration for processing web server logs".to_string())
        );

        match &config.source {
            AdapterSource::File { file, format } => {
                assert_eq!(
                    file.path,
                    "<YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz"
                );
                assert_eq!(file.compression, Some("gzip".to_string()));
                assert_eq!(file.max_batch_size, Some("100MB".to_string()));
                assert_eq!(format.ty, "json");
            }
            _ => panic!("Expected File source"),
        }

        assert_eq!(config.columns.len(), 2);
        assert_eq!(config.columns[0].name, "timestamp");
        assert_eq!(config.columns[0].ty, "DATETIME");
        assert_eq!(config.columns[1].name, "status");
        assert_eq!(config.columns[1].ty, "INTEGER");
    }

    #[test]
    fn test_parse_adapter_config_new_database_format() {
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
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_adapter_config(yaml).unwrap();

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
    fn test_parse_file_config() {
        let yaml_str = r#"
            path: /logs/<YYYY>/<MM>/<DD>/app.log
            compression: gzip
            max_batch_size: 50MB
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_file_config(yaml);
        assert_eq!(config.path, "/logs/<YYYY>/<MM>/<DD>/app.log");
        assert_eq!(config.compression, Some("gzip".to_string()));
        assert_eq!(config.max_batch_size, Some("50MB".to_string()));
    }

    #[test]
    fn test_parse_format_config_json() {
        let yaml_str = r#"
            type: json
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_format_config(yaml);
        assert_eq!(config.ty, "json");
        assert_eq!(config.delimiter, None);
        assert_eq!(config.null_value, None);
        assert_eq!(config.has_header, None);
    }

    #[test]
    fn test_parse_format_config_csv() {
        let yaml_str = r#"
            type: csv
            delimiter: ','
            null_value: 'NULL'
            has_header: true
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_format_config(yaml);
        assert_eq!(config.ty, "csv");
        assert_eq!(config.delimiter, Some(",".to_string()));
        assert_eq!(config.null_value, Some("NULL".to_string()));
        assert_eq!(config.has_header, Some(true));
    }

    #[test]
    fn test_parse_columns() {
        let yaml_str = r#"
            - name: id
              type: INTEGER
              description: 'Primary key'
            - name: email
              type: STRING
            - name: created_at
              type: DATETIME
              description: 'Record creation time'
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let columns = parse_columns(yaml);
        assert_eq!(columns.len(), 3);

        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ty, "INTEGER");
        assert_eq!(columns[0].description, Some("Primary key".to_string()));

        assert_eq!(columns[1].name, "email");
        assert_eq!(columns[1].ty, "STRING");
        assert_eq!(columns[1].description, None);

        assert_eq!(columns[2].name, "created_at");
        assert_eq!(columns[2].ty, "DATETIME");
        assert_eq!(
            columns[2].description,
            Some("Record creation time".to_string())
        );
    }

    #[test]
    #[should_panic(expected = "Adapter connection is required")]
    fn test_parse_adapter_config_missing_connection() {
        let yaml_str = r#"
            file:
              path: /logs/app.log
            format:
              type: json
            columns: []
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_adapter_config(yaml).unwrap();
    }

    #[test]
    fn test_parse_adapter_config_missing_source_fields() {
        let yaml_str = r#"
            connection: test
            columns: []
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let result = parse_adapter_config(yaml);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Either 'source' or both 'file' and 'format' are required")
        );
    }

    #[test]
    #[should_panic(expected = "File path is required")]
    fn test_parse_file_config_missing_path() {
        let yaml_str = r#"
            compression: gzip
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        parse_file_config(yaml);
    }
}
