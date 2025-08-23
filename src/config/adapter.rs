#[derive(Debug, Clone, PartialEq)]
pub struct AdapterConfig {
    pub connection: String,
    pub description: Option<String>,
    pub source: AdapterSource,
    pub update_strategy: Option<UpdateStrategyConfig>,
    pub columns: Vec<ColumnConfig>,
    pub limits: Option<LimitsConfig>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStrategyConfig {
    pub detection: String,
    pub timestamp_from: Option<String>,
    pub range: RangeConfig,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeConfig {
    pub since: Option<chrono::NaiveDateTime>,
    pub until: Option<chrono::NaiveDateTime>,
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

    let update_strategy = parse_update_strategy(&yaml["update_strategy"]);
    let columns = parse_columns(&yaml["columns"]);
    let limits = if !yaml["limits"].is_badvalue() {
        Some(parse_limits(&yaml["limits"]))
    } else {
        None
    };

    Ok(AdapterConfig {
        connection,
        description,
        source,
        update_strategy,
        columns,
        limits,
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

fn parse_update_strategy(yaml: &yaml_rust2::Yaml) -> Option<UpdateStrategyConfig> {
    let detection = if let Some(detection) = yaml["detection"].as_str() {
        detection.to_string()
    } else {
        return None;
    };

    let timestamp_from = yaml["timestamp_from"].as_str().map(|s| s.to_string());
    let range = if !yaml["range"].is_badvalue() {
        parse_range(&yaml["range"])
    } else {
        RangeConfig {
            since: None,
            until: None,
        }
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

    let since = since.as_ref().and_then(|s| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
            })
            .ok()
    });

    let until = until.as_ref().and_then(|s| {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(|date| date.and_hms_opt(23, 59, 59).unwrap())
            })
            .ok()
    });

    RangeConfig { since, until }
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
    use chrono::{Datelike, Timelike};
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

        assert!(config.update_strategy.is_some());
        let update_strategy = config.update_strategy.as_ref().unwrap();
        assert_eq!(update_strategy.detection, "filename");
        assert_eq!(update_strategy.timestamp_from, Some("path".to_string()));
        assert!(update_strategy.range.since.is_some());

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
        let range = &config.range;
        assert!(range.since.is_some());
        assert!(range.until.is_some());

        let since = range.since.unwrap();
        let until = range.until.unwrap();

        assert_eq!(since.year(), 2024);
        assert_eq!(since.month(), 1);
        assert_eq!(since.day(), 1);
        assert_eq!(since.hour(), 0);
        assert_eq!(since.minute(), 0);
        assert_eq!(since.second(), 0);

        assert_eq!(until.year(), 2024);
        assert_eq!(until.month(), 12);
        assert_eq!(until.day(), 31);
        assert_eq!(until.hour(), 23);
        assert_eq!(until.minute(), 59);
        assert_eq!(until.second(), 59);
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

    #[test]
    fn test_parse_size_to_bytes_gigabytes() {
        assert_eq!(parse_size_to_bytes("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_to_bytes("2gb").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(
            parse_size_to_bytes("10GB").unwrap(),
            10 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_parse_size_to_bytes_megabytes() {
        assert_eq!(parse_size_to_bytes("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size_to_bytes("5mb").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_size_to_bytes("100MB").unwrap(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_to_bytes_kilobytes() {
        assert_eq!(parse_size_to_bytes("1KB").unwrap(), 1024);
        assert_eq!(parse_size_to_bytes("10kb").unwrap(), 10 * 1024);
        assert_eq!(parse_size_to_bytes("512KB").unwrap(), 512 * 1024);
    }

    #[test]
    fn test_parse_size_to_bytes_raw_bytes() {
        assert_eq!(parse_size_to_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_size_to_bytes("0").unwrap(), 0);
        assert_eq!(parse_size_to_bytes("999999").unwrap(), 999999);
    }

    #[test]
    fn test_parse_size_to_bytes_case_insensitive() {
        assert_eq!(parse_size_to_bytes("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_to_bytes("1Gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_to_bytes("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size_to_bytes("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size_to_bytes("1Mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size_to_bytes("1MB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn test_parse_size_to_bytes_invalid_format() {
        assert!(parse_size_to_bytes("invalid").is_err());
        assert!(parse_size_to_bytes("1TB").is_err());
        assert!(parse_size_to_bytes("1.5GB").is_err());
        assert!(parse_size_to_bytes("-1MB").is_err());
        assert!(parse_size_to_bytes("").is_err());
    }

    #[test]
    fn test_parse_size_to_bytes_non_numeric_prefix() {
        assert!(parse_size_to_bytes("abcGB").is_err());
        assert!(parse_size_to_bytes("GB").is_err());
        assert!(parse_size_to_bytes("xyzMB").is_err());
    }

    #[test]
    fn test_parse_size_to_bytes_edge_cases() {
        assert_eq!(parse_size_to_bytes("0GB").unwrap(), 0);
        assert_eq!(parse_size_to_bytes("0MB").unwrap(), 0);
        assert_eq!(parse_size_to_bytes("0KB").unwrap(), 0);

        let max_gb = u64::MAX / (1024 * 1024 * 1024);
        let valid_gb = format!("{max_gb}GB");
        assert!(parse_size_to_bytes(&valid_gb).is_ok());
    }

    #[test]
    fn test_parse_size_to_bytes_whitespace() {
        assert!(parse_size_to_bytes(" 1GB").is_err());
        assert!(parse_size_to_bytes("1GB ").is_err());
        assert!(parse_size_to_bytes("1 GB").is_err());
    }
}
