use crate::{
    config::{
        adapter::{AdapterConfig, LimitsConfig, RangeConfig, UpdateStrategyConfig},
        project::ConnectionConfig,
    },
    pipeline::build::TimeRange,
    s3_client,
};
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use regex::Regex;
use std::path::Path;

pub enum FileSystem {
    Local { base_path: Option<String> },
    S3 { client: s3_client::S3Client },
}

impl FileSystem {
    pub fn new_local(base_path: Option<String>) -> Self {
        Self::Local { base_path }
    }

    pub async fn new_s3(connection: &ConnectionConfig) -> Result<Self> {
        let client = s3_client::S3Client::new(connection).await?;
        Ok(Self::S3 { client })
    }

    pub async fn from_connection(connection: &ConnectionConfig) -> Result<Self> {
        match connection {
            ConnectionConfig::LocalFile { base_path } => {
                Ok(Self::new_local(Some(base_path.clone())))
            }
            ConnectionConfig::S3 { .. } => Self::new_s3(connection).await,
            ConnectionConfig::Sqlite { .. } => Err(anyhow::anyhow!(
                "SQLite connections are not supported by FileSystem. Use database adapter instead."
            )),
            ConnectionConfig::RemoteDatabase { db_type, .. } => match db_type {
                crate::config::project::DatabaseType::Mysql => Err(anyhow::anyhow!(
                    "MySQL connections are not supported by FileSystem. Use database adapter instead."
                )),
                crate::config::project::DatabaseType::Postgresql => Err(anyhow::anyhow!(
                    "PostgreSQL connections are not supported by FileSystem. Use database adapter instead."
                )),
                crate::config::project::DatabaseType::Sqlite => Err(anyhow::anyhow!(
                    "SQLite connections are not supported by FileSystem. Use database adapter instead."
                )),
            },
        }
    }

    pub async fn list_files(&self, pattern: &str) -> Result<Vec<String>> {
        match self {
            Self::Local { base_path } => {
                let resolved_pattern = if let Some(base) = base_path {
                    if pattern.starts_with('/') {
                        pattern.to_string()
                    } else {
                        format!("{base}/{pattern}")
                    }
                } else {
                    pattern.to_string()
                };

                let mut existing_paths = Vec::new();
                if resolved_pattern.contains('*') || resolved_pattern.contains('?') {
                    let glob_matches: Vec<_> = glob::glob(&resolved_pattern)
                        .context("Failed to execute glob pattern")?
                        .filter_map(Result::ok)
                        .map(|p| p.to_string_lossy().to_string())
                        .collect();
                    existing_paths.extend(glob_matches);
                } else if Path::new(&resolved_pattern).exists() {
                    existing_paths.push(resolved_pattern);
                }

                Ok(existing_paths)
            }
            Self::S3 { client } => client.list_objects_matching_pattern(pattern).await,
        }
    }
}

pub struct FileProcessor;

impl FileProcessor {
    pub async fn process_pattern_with_filesystem(
        pattern: &str,
        adapter: &AdapterConfig,
        filesystem: &FileSystem,
    ) -> Result<Vec<String>> {
        let pattern_to_expand = if Self::has_date_pattern(pattern) {
            Self::convert_date_pattern_to_wildcard(pattern)
        } else {
            pattern.to_string()
        };

        let expanded_paths = filesystem.list_files(&pattern_to_expand).await?;

        let filtered_paths = if let Some(strategy) = &adapter.update_strategy {
            Self::filter_paths_by_time_range(expanded_paths, &strategy.range, strategy)?
        } else {
            expanded_paths
        };

        if let Some(limits) = &adapter.limits {
            Self::validate_limits(&filtered_paths, limits)?;
        }

        Ok(filtered_paths)
    }

    pub async fn files_for_processing_with_filesystem(
        adapter: &AdapterConfig,
        range: Option<TimeRange>,
        filesystem: &FileSystem,
    ) -> Result<Vec<String>> {
        let Some(time_range) = range else {
            return Ok(Vec::new());
        };

        let mut adapter_with_range = adapter.clone();

        if let Some(ref mut strategy) = adapter_with_range.update_strategy {
            let adapter_range = &mut strategy.range;
            if let Some(since) = time_range.since {
                adapter_range.since = Some(since.naive_utc());
            }
            if let Some(until) = time_range.until {
                adapter_range.until = Some(until.naive_utc());
            }
        }

        let file_path = match &adapter_with_range.source {
            crate::config::adapter::AdapterSource::File { file, .. } => &file.path,
            _ => {
                return Err(anyhow::anyhow!(
                    "Only file sources are supported in file processor"
                ));
            }
        };

        let file_paths =
            Self::process_pattern_with_filesystem(file_path, &adapter_with_range, filesystem)
                .await?;

        Ok(file_paths)
    }

    fn has_date_pattern(pattern: &str) -> bool {
        pattern.contains("{YYYY}")
            || pattern.contains("{MM}")
            || pattern.contains("{DD}")
            || pattern.contains("{HH}")
            || pattern.contains("{mm}")
    }

    fn convert_date_pattern_to_wildcard(pattern: &str) -> String {
        let result = pattern
            .replace("{YYYY}", "*")
            .replace("{MM}", "*")
            .replace("{DD}", "*")
            .replace("{HH}", "*")
            .replace("{mm}", "*");

        let re = Regex::new(r"\*+").unwrap();
        re.replace_all(&result, "*").into_owned()
    }

    fn filter_paths_by_time_range(
        paths: Vec<String>,
        range: &RangeConfig,
        strategy: &UpdateStrategyConfig,
    ) -> Result<Vec<String>> {
        match strategy.detection.as_str() {
            "filename" => Self::filter_by_filename_timestamps(paths, range),
            "metadata" => Self::filter_by_file_metadata(paths, range),
            _ => Ok(paths),
        }
    }

    fn filter_by_filename_timestamps(
        paths: Vec<String>,
        range: &RangeConfig,
    ) -> Result<Vec<String>> {
        let since = range
            .since
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());
        let until = range
            .until
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());

        let mut filtered_paths = Vec::new();
        for path in paths {
            if let Some(timestamp) = Self::extract_timestamp_from_filename(&path) {
                if timestamp >= since && timestamp <= until {
                    filtered_paths.push(path);
                }
            }
        }
        Ok(filtered_paths)
    }

    fn filter_by_file_metadata(paths: Vec<String>, range: &RangeConfig) -> Result<Vec<String>> {
        let since = range
            .since
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());
        let until = range
            .until
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());

        let mut filtered_paths = Vec::new();
        for path in paths {
            if let Ok(metadata) = std::fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    let modified_time = chrono::DateTime::<chrono::Utc>::from(modified).naive_utc();
                    if modified_time >= since && modified_time <= until {
                        filtered_paths.push(path);
                    }
                }
            }
        }
        Ok(filtered_paths)
    }

    fn extract_timestamp_from_filename(filename: &str) -> Option<NaiveDateTime> {
        use regex::Regex;

        let patterns = vec![
            r"(\d{4})-(\d{2})-(\d{2})T(\d{2})(\d{2})",
            r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})",
            r"(\d{4})-(\d{2})-(\d{2})",
        ];

        for pattern_str in patterns {
            if let Ok(re) = Regex::new(pattern_str) {
                if let Some(caps) = re.captures(filename) {
                    let year: i32 = caps.get(1)?.as_str().parse().ok()?;
                    let month: u32 = caps.get(2)?.as_str().parse().ok()?;
                    let day: u32 = caps.get(3)?.as_str().parse().ok()?;
                    let hour = caps
                        .get(4)
                        .and_then(|m| m.as_str().parse().ok())
                        .unwrap_or(0);
                    let minute = caps
                        .get(5)
                        .and_then(|m| m.as_str().parse().ok())
                        .unwrap_or(0);

                    return chrono::NaiveDate::from_ymd_opt(year, month, day)?
                        .and_hms_opt(hour, minute, 0);
                }
            }
        }
        None
    }

    fn validate_limits(paths: &[String], limits: &LimitsConfig) -> Result<()> {
        if let Some(max_files) = limits.max_files {
            if paths.len() > max_files as usize {
                return Err(anyhow::anyhow!(
                    "Too many files: {} (max: {})",
                    paths.len(),
                    max_files
                ));
            }
        }

        if let Some(max_size_bytes) = limits.max_size_bytes {
            let total_size = Self::calculate_total_size(paths)?;

            if total_size > max_size_bytes {
                return Err(anyhow::anyhow!(
                    "Total file size too large: {} bytes (max: {} bytes)",
                    total_size,
                    max_size_bytes
                ));
            }
        }

        Ok(())
    }

    fn calculate_total_size(paths: &[String]) -> Result<u64> {
        let mut total_size = 0u64;

        for path in paths {
            if let Ok(metadata) = std::fs::metadata(path) {
                total_size += metadata.len();
            }
        }

        Ok(total_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig, UpdateStrategyConfig};

    fn create_test_adapter(path: &str) -> AdapterConfig {
        AdapterConfig {
            connection: "test".to_string(),
            description: None,
            source: crate::config::adapter::AdapterSource::File {
                file: FileConfig {
                    path: path.to_string(),
                    compression: None,
                    max_batch_size: None,
                },
                format: FormatConfig {
                    ty: "csv".to_string(),
                    delimiter: None,
                    null_value: None,
                    has_header: None,
                },
            },
            update_strategy: None,
            columns: vec![],
            limits: None,
        }
    }

    #[test]
    fn test_convert_date_pattern_to_wildcard() {
        assert_eq!(
            FileProcessor::convert_date_pattern_to_wildcard("logs/{YYYY}-{MM}-{DD}T{HH}{mm}.json"),
            "logs/*-*-*T*.json"
        );
        assert_eq!(
            FileProcessor::convert_date_pattern_to_wildcard("data/{YYYY}/{MM}/{DD}/file.csv"),
            "data/*/*/*/file.csv"
        );
        assert_eq!(
            FileProcessor::convert_date_pattern_to_wildcard("static.csv"),
            "static.csv"
        );
    }

    #[tokio::test]
    async fn test_process_pattern_without_update_strategy() {
        let adapter = create_test_adapter("data/*.csv");
        let filesystem = FileSystem::new_local(None);
        let result =
            FileProcessor::process_pattern_with_filesystem("data/*.csv", &adapter, &filesystem)
                .await
                .unwrap();
        assert_eq!(result, vec![] as Vec<String>);
    }

    #[tokio::test]
    async fn test_process_pattern_with_date_pattern() {
        let adapter = create_test_adapter("logs/{YYYY}-{MM}-{DD}.json");
        let filesystem = FileSystem::new_local(None);
        let result = FileProcessor::process_pattern_with_filesystem(
            "logs/{YYYY}-{MM}-{DD}.json",
            &adapter,
            &filesystem,
        )
        .await;
        assert!(result.is_ok());
        let paths = result.unwrap();
        assert_eq!(paths, Vec::<String>::new());
    }

    #[test]
    fn test_has_date_pattern() {
        assert!(FileProcessor::has_date_pattern(
            "logs/{YYYY}-{MM}-{DD}.json"
        ));
        assert!(FileProcessor::has_date_pattern("{HH}{mm}.csv"));
        assert!(!FileProcessor::has_date_pattern("logs/*.json"));
        assert!(!FileProcessor::has_date_pattern("static.csv"));
    }

    #[test]
    fn test_extract_timestamp_from_filename() {
        use chrono::{Datelike, Timelike};

        let result = FileProcessor::extract_timestamp_from_filename("logs/2024-01-15T1030.json");
        assert!(result.is_some());
        let timestamp = result.unwrap();
        assert_eq!(timestamp.year(), 2024);
        assert_eq!(timestamp.month(), 1);
        assert_eq!(timestamp.day(), 15);
        assert_eq!(timestamp.hour(), 10);
        assert_eq!(timestamp.minute(), 30);
    }

    #[tokio::test]
    async fn test_process_pattern_with_non_filename_detection() {
        let mut adapter = create_test_adapter("data/*.csv");
        adapter.update_strategy = Some(UpdateStrategyConfig {
            detection: "content".to_string(),
            timestamp_from: None,
            range: RangeConfig {
                since: None,
                until: None,
            },
        });

        let filesystem = FileSystem::new_local(None);
        let result =
            FileProcessor::process_pattern_with_filesystem("data/*.csv", &adapter, &filesystem)
                .await
                .unwrap();
        assert_eq!(result, vec![] as Vec<String>);
    }

    #[tokio::test]
    async fn test_filename_detection_integration() {
        use chrono::NaiveDate;
        use std::fs;

        let test_dir = "/tmp/file_pattern_integration_test";
        fs::remove_dir_all(test_dir).ok();
        fs::create_dir_all(test_dir).unwrap();

        fs::write(format!("{test_dir}/users_2024-01-01.csv"), "test data").unwrap();
        fs::write(format!("{test_dir}/users_2024-01-15.csv"), "test data").unwrap();
        fs::write(format!("{test_dir}/users_2024-02-01.csv"), "test data").unwrap();
        fs::write(format!("{test_dir}/users_2024-03-01.csv"), "test data").unwrap();

        let range_config = RangeConfig {
            since: Some(
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            until: Some(
                NaiveDate::from_ymd_opt(2024, 1, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
        };

        let mut adapter_date_pattern =
            create_test_adapter(&format!("{test_dir}/users_{{YYYY}}-{{MM}}-{{DD}}.csv"));
        adapter_date_pattern.update_strategy = Some(UpdateStrategyConfig {
            detection: "filename".to_string(),
            timestamp_from: None,
            range: range_config.clone(),
        });

        let filesystem = FileSystem::new_local(None);
        let result_date_pattern = FileProcessor::process_pattern_with_filesystem(
            &format!("{test_dir}/users_{{YYYY}}-{{MM}}-{{DD}}.csv"),
            &adapter_date_pattern,
            &filesystem,
        )
        .await
        .unwrap();

        assert_eq!(result_date_pattern.len(), 2);
        assert!(
            result_date_pattern
                .iter()
                .any(|path| path.contains("2024-01-01"))
        );
        assert!(
            result_date_pattern
                .iter()
                .any(|path| path.contains("2024-01-15"))
        );
        assert!(
            !result_date_pattern
                .iter()
                .any(|path| path.contains("2024-02-01"))
        );
        assert!(
            !result_date_pattern
                .iter()
                .any(|path| path.contains("2024-03-01"))
        );

        let mut adapter_wildcard = create_test_adapter(&format!("{test_dir}/users_*.csv"));
        adapter_wildcard.update_strategy = Some(UpdateStrategyConfig {
            detection: "filename".to_string(),
            timestamp_from: None,
            range: range_config,
        });

        let result_wildcard = FileProcessor::process_pattern_with_filesystem(
            &format!("{test_dir}/users_*.csv"),
            &adapter_wildcard,
            &filesystem,
        )
        .await
        .unwrap();

        assert_eq!(result_date_pattern.len(), result_wildcard.len());
        assert_eq!(result_date_pattern.len(), 2);

        for file in &result_date_pattern {
            assert!(
                result_wildcard.contains(file),
                "Wildcard result missing file: {file}"
            );
        }

        fs::remove_dir_all(test_dir).unwrap();
    }
}
