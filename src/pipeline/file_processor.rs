use crate::{
    config::{adapter::AdapterConfig, project::ConnectionConfig},
    s3_client,
};
use anyhow::{Context, Result};
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
            ConnectionConfig::S3(_) => Self::new_s3(connection).await,
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
        filesystem: &FileSystem,
    ) -> Result<Vec<String>> {
        let pattern_to_expand = if Self::has_date_pattern(pattern) {
            Self::convert_date_pattern_to_wildcard(pattern)
        } else {
            pattern.to_string()
        };

        let expanded_paths = filesystem.list_files(&pattern_to_expand).await?;

        Ok(expanded_paths)
    }

    pub async fn find_matching_files(
        pattern: &str,
        filesystem: &FileSystem,
    ) -> Result<Vec<String>> {
        let pattern = if Self::has_date_pattern(pattern) {
            Self::convert_date_pattern_to_wildcard(pattern)
        } else {
            pattern.to_string()
        };

        filesystem.list_files(&pattern).await
    }

    pub async fn files_for_processing(
        adapter: &AdapterConfig,
        filesystem: &FileSystem,
    ) -> Result<Vec<String>> {
        let pattern = match &adapter.source {
            crate::config::adapter::AdapterSource::File { file, .. } => &file.path,
            _ => {
                return Err(anyhow::anyhow!(
                    "Only file sources are supported in file processor"
                ));
            }
        };

        Self::find_matching_files(pattern, filesystem).await
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
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::config::adapter::{FileConfig, FormatConfig};

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
            columns: vec![],
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
    async fn test_process_pattern_with_nonexistent_files() {
        let filesystem = FileSystem::new_local(None);
        let result = FileProcessor::process_pattern_with_filesystem("data/*.csv", &filesystem)
            .await
            .unwrap();
        assert_eq!(result, vec![] as Vec<String>);
    }

    #[tokio::test]
    async fn test_process_pattern_with_date_pattern() {
        let filesystem = FileSystem::new_local(None);
        let result = FileProcessor::process_pattern_with_filesystem(
            "logs/{YYYY}-{MM}-{DD}.json",
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

    #[tokio::test]
    async fn test_files_for_processing() {
        use std::fs;
        let tmpdir = TempDir::new().unwrap().path().to_path_buf();
        let tmppath = tmpdir.to_str().unwrap();
        fs::remove_dir_all(tmppath).ok();
        fs::create_dir_all(tmppath).unwrap();
        fs::write(format!("{tmppath}/users.csv"), "id,name\n1,Alice\n2,Bob").unwrap();

        let adapter = create_test_adapter(&format!("{tmppath}/users.csv"));

        let result = FileProcessor::files_for_processing(
            &adapter,
            &FileSystem::new_local(Some(tmppath.to_string())),
        )
        .await;

        assert_eq!(result.unwrap(), vec![format!("{tmppath}/users.csv")]);
    }
}
