#[derive(Debug, Clone, PartialEq)]
pub struct AdapterConfig {
    pub connection: String,
    pub description: Option<String>,
    pub file: FileConfig,
    pub update_strategy: Option<UpdateStrategyConfig>,
    pub format: FormatConfig,
    pub columns: Vec<ColumnConfig>,
    pub limits: Option<LimitsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileConfig {
    pub path: String,
    pub compression: Option<String>,
    pub max_batch_size: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStrategyConfig {
    pub detection: String,
    pub timestamp_from: Option<String>,
    pub range: Option<RangeConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeConfig {
    pub since: Option<String>,
    pub until: Option<String>,
    pub since_parsed: Option<chrono::NaiveDateTime>,
    pub until_parsed: Option<chrono::NaiveDateTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitsConfig {
    pub max_files: Option<u32>,
    pub max_size_bytes: Option<u64>,
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

    let file = parse_file_config(&yaml["file"]);
    let update_strategy = parse_update_strategy(&yaml["update_strategy"]);
    let format = parse_format_config(&yaml["format"]);
    let columns = parse_columns(&yaml["columns"]);
    let limits = if !yaml["limits"].is_badvalue() {
        Some(parse_limits(&yaml["limits"]))
    } else {
        None
    };

    Ok(AdapterConfig {
        connection,
        description,
        file,
        update_strategy,
        format,
        columns,
        limits,
    })
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

fn parse_update_strategy(yaml: &yaml_rust2::Yaml) -> Option<UpdateStrategyConfig> {
    let detection = if let Some(detection) = yaml["detection"].as_str() {
        detection.to_string()
    } else {
        return None;
    };

    let timestamp_from = yaml["timestamp_from"].as_str().map(|s| s.to_string());
    let range = if !yaml["range"].is_badvalue() {
        Some(parse_range(&yaml["range"]))
    } else {
        None
    };

    Some(UpdateStrategyConfig {
        detection,
        timestamp_from,
        range,
    })
}

fn parse_range(yaml: &yaml_rust2::Yaml) -> RangeConfig {
    let since = yaml["since"].as_str().map(|s| s.to_string());
    let until = yaml["until"].as_str().map(|s| s.to_string());

    let since_parsed = since.as_ref().and_then(|s| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
            })
            .ok()
    });

    let until_parsed = until.as_ref().and_then(|s| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(23, 59, 59).unwrap())
            })
            .ok()
    });

    RangeConfig {
        since,
        until,
        since_parsed,
        until_parsed,
    }
}

fn parse_limits(yaml: &yaml_rust2::Yaml) -> LimitsConfig {
    let max_files = yaml["max_files"].as_i64().map(|i| i as u32);
    let max_size_bytes = yaml["max_size"]
        .as_str()
        .and_then(|s| parse_size_to_bytes(s).ok());

    LimitsConfig {
        max_files,
        max_size_bytes,
    }
}

fn parse_size_to_bytes(size_str: &str) -> anyhow::Result<u64> {
    let size_str = size_str.to_uppercase();
    if size_str.ends_with("GB") {
        let num: u64 = size_str[..size_str.len() - 2].parse()?;
        Ok(num * 1024 * 1024 * 1024)
    } else if size_str.ends_with("MB") {
        let num: u64 = size_str[..size_str.len() - 2].parse()?;
        Ok(num * 1024 * 1024)
    } else if size_str.ends_with("KB") {
        let num: u64 = size_str[..size_str.len() - 2].parse()?;
        Ok(num * 1024)
    } else {
        size_str
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Invalid size format: {}", e))
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
    fn test_parse_adapter_config() {
        let yaml_str = r#"
            connection: test_data
            description: 'Configuration for processing web server logs'
            file:
              path: <YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz
              compression: gzip
              max_batch_size: 100MB
            update_strategy:
              detection: filename
              timestamp_from: path
              range:
                since: 2023-01-01 00:00:00
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

        assert_eq!(
            config.file.path,
            "<YYYY>/<MM>/<DD>/*_<YYYY><MM><DD>T<HH><MM>.log.gz"
        );
        assert_eq!(config.file.compression, Some("gzip".to_string()));
        assert_eq!(config.file.max_batch_size, Some("100MB".to_string()));

        assert!(config.update_strategy.is_some());
        let update_strategy = config.update_strategy.as_ref().unwrap();
        assert_eq!(update_strategy.detection, "filename");
        assert_eq!(update_strategy.timestamp_from, Some("path".to_string()));
        assert_eq!(
            update_strategy.range.as_ref().unwrap().since,
            Some("2023-01-01 00:00:00".to_string())
        );

        assert_eq!(config.format.ty, "json");

        assert_eq!(config.columns.len(), 2);
        assert_eq!(config.columns[0].name, "timestamp");
        assert_eq!(config.columns[0].ty, "DATETIME");
        assert_eq!(config.columns[1].name, "status");
        assert_eq!(config.columns[1].ty, "INTEGER");
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
    fn test_parse_update_strategy() {
        let yaml_str = r#"
            detection: content
            timestamp_from: timestamp_column
            range:
              since: 2024-01-01
              until: 2024-12-31
        "#;
        let docs = YamlLoader::load_from_str(yaml_str).unwrap();
        let yaml = &docs[0];

        let config = parse_update_strategy(yaml);
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.detection, "content");
        assert_eq!(config.timestamp_from, Some("timestamp_column".to_string()));
        assert_eq!(
            config.range.as_ref().unwrap().since,
            Some("2024-01-01".to_string())
        );
        assert_eq!(
            config.range.as_ref().unwrap().until,
            Some("2024-12-31".to_string())
        );
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
