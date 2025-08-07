use crate::config::adapter::{AdapterConfig, LimitsConfig, RangeConfig, UpdateStrategyConfig};
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use std::path::Path;

pub struct FilePatternProcessor;

impl FilePatternProcessor {
    pub fn process_pattern(pattern: &str, adapter: &AdapterConfig) -> Result<Vec<String>> {
        let expanded_paths = if Self::has_date_pattern(pattern) {
            let wildcard_pattern = Self::convert_date_pattern_to_wildcard(pattern);
            Self::filter_existing_files(vec![wildcard_pattern])?
        } else {
            vec![pattern.to_string()]
        };

        let filtered_paths = if let Some(strategy) = &adapter.update_strategy {
            if let Some(range) = &strategy.range {
                Self::filter_paths_by_time_range(expanded_paths, range, strategy)?
            } else {
                expanded_paths
            }
        } else {
            expanded_paths
        };

        if let Some(limits) = &adapter.limits {
            Self::validate_limits(&filtered_paths, limits)?;
        }

        Ok(filtered_paths)
    }

    fn has_date_pattern(pattern: &str) -> bool {
        pattern.contains("{YYYY}")
            || pattern.contains("{MM}")
            || pattern.contains("{DD}")
            || pattern.contains("{HH}")
            || pattern.contains("{mm}")
    }

    fn convert_date_pattern_to_wildcard(pattern: &str) -> String {
        pattern
            .replace("{YYYY}", "*")
            .replace("{MM}", "*")
            .replace("{DD}", "*")
            .replace("{HH}", "*")
            .replace("{mm}", "*")
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
            .since_parsed
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());
        let until = range
            .until_parsed
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
            .since_parsed
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());
        let until = range
            .until_parsed
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
            FilePatternProcessor::convert_date_pattern_to_wildcard(
                "logs/{YYYY}-{MM}-{DD}T{HH}{mm}.json"
            ),
            "logs/*-*-*T**.json"
        );
        assert_eq!(
            FilePatternProcessor::convert_date_pattern_to_wildcard(
                "data/{YYYY}/{MM}/{DD}/file.csv"
            ),
            "data/*/*/*/file.csv"
        );
        assert_eq!(
            FilePatternProcessor::convert_date_pattern_to_wildcard("static.csv"),
            "static.csv"
        );
    }

    #[test]
    fn test_process_pattern_without_update_strategy() {
        let adapter = create_test_adapter("data/*.csv");
        let result = FilePatternProcessor::process_pattern("data/*.csv", &adapter).unwrap();
        assert_eq!(result, vec!["data/*.csv"]);
    }

    #[test]
    fn test_process_pattern_with_date_pattern() {
        let adapter = create_test_adapter("logs/{YYYY}-{MM}-{DD}.json");
        let result = FilePatternProcessor::process_pattern("logs/{YYYY}-{MM}-{DD}.json", &adapter);
        assert!(result.is_ok());
        let paths = result.unwrap();
        assert_eq!(paths, Vec::<String>::new());
    }

    #[test]
    fn test_has_date_pattern() {
        assert!(FilePatternProcessor::has_date_pattern(
            "logs/{YYYY}-{MM}-{DD}.json"
        ));
        assert!(FilePatternProcessor::has_date_pattern("{HH}{mm}.csv"));
        assert!(!FilePatternProcessor::has_date_pattern("logs/*.json"));
        assert!(!FilePatternProcessor::has_date_pattern("static.csv"));
    }

    #[test]
    fn test_extract_timestamp_from_filename() {
        use chrono::{Datelike, Timelike};

        let result =
            FilePatternProcessor::extract_timestamp_from_filename("logs/2024-01-15T1030.json");
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
            range: None,
        });

        let result = FilePatternProcessor::process_pattern("data/*.csv", &adapter).unwrap();
        assert_eq!(result, vec!["data/*.csv"]);
    }
}
