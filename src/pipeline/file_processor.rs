use crate::config::adapter::{AdapterConfig, LimitsConfig, RangeConfig, UpdateStrategyConfig};
use crate::pipeline::build::TimeRange;
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use regex::Regex;
use std::path::Path;

pub struct FileProcessor;

impl FileProcessor {
    pub fn process_pattern(pattern: &str, adapter: &AdapterConfig) -> Result<Vec<String>> {
        let expanded_paths = if Self::has_date_pattern(pattern) {
            let wildcard_pattern = Self::convert_date_pattern_to_wildcard(pattern);
            Self::filter_existing_files(vec![wildcard_pattern])?
        } else {
            Self::filter_existing_files(vec![pattern.to_string()])?
        };

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

    pub fn files_for_processing(
        adapter: &AdapterConfig,
        range: Option<TimeRange>,
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

        let file_paths = Self::process_pattern(&adapter_with_range.file.path, &adapter_with_range)?;

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

    fn filter_existing_files(paths: Vec<String>) -> Result<Vec<String>> {
        let mut existing_paths = Vec::new();

        for path in paths {
            if path.contains('*') || path.contains('?') {
                let glob_matches: Vec<_> = glob::glob(&path)
                    .context("Failed to execute glob pattern")?
                    .filter_map(Result::ok)
                    .map(|p| p.to_string_lossy().to_string())
                    .collect();
                existing_paths.extend(glob_matches);
            } else if Path::new(&path).exists() {
                existing_paths.push(path);
            }
        }

        Ok(existing_paths)
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
            file: FileConfig {
                path: path.to_string(),
                compression: None,
                max_batch_size: None,
            },
            update_strategy: None,
            format: FormatConfig {
                ty: "csv".to_string(),
                delimiter: None,
                null_value: None,
                has_header: None,
            },
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

    #[test]
    fn test_process_pattern_without_update_strategy() {
        let adapter = create_test_adapter("data/*.csv");
        let result = FileProcessor::process_pattern("data/*.csv", &adapter).unwrap();
        assert_eq!(result, vec![] as Vec<String>);
    }

    #[test]
    fn test_process_pattern_with_date_pattern() {
        let adapter = create_test_adapter("logs/{YYYY}-{MM}-{DD}.json");
        let result = FileProcessor::process_pattern("logs/{YYYY}-{MM}-{DD}.json", &adapter);
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

    #[test]
    fn test_process_pattern_with_non_filename_detection() {
        let mut adapter = create_test_adapter("data/*.csv");
        adapter.update_strategy = Some(UpdateStrategyConfig {
            detection: "content".to_string(),
            timestamp_from: None,
            range: RangeConfig {
                since: None,
                until: None,
            },
        });

        let result = FileProcessor::process_pattern("data/*.csv", &adapter).unwrap();
        assert_eq!(result, vec![] as Vec<String>);
    }

    #[test]
    fn test_filename_detection_integration() {
        use chrono::NaiveDate;
        use std::fs;

        let test_dir = "/tmp/file_pattern_integration_test";
        let _ = fs::remove_dir_all(test_dir);
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

        let result_date_pattern = FileProcessor::process_pattern(
            &format!("{test_dir}/users_{{YYYY}}-{{MM}}-{{DD}}.csv"),
            &adapter_date_pattern,
        )
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

        let result_wildcard =
            FileProcessor::process_pattern(&format!("{test_dir}/users_*.csv"), &adapter_wildcard)
                .unwrap();

        assert_eq!(result_date_pattern.len(), result_wildcard.len());
        assert_eq!(result_date_pattern.len(), 2);

        for file in &result_date_pattern {
            assert!(
                result_wildcard.contains(file),
                "Wildcard result missing file: {file}"
            );
        }

        println!("Date pattern result: {result_date_pattern:?}");
        println!("Wildcard result: {result_wildcard:?}");

        let _ = fs::remove_dir_all(test_dir);
    }
}
