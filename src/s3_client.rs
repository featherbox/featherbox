use crate::config::project::{ConnectionConfig, S3AuthMethod};
use anyhow::{Context, Result};
use aws_config::Region;
use aws_sdk_s3::Client;
use regex::Regex;

pub struct S3Client {
    client: Client,
    bucket: String,
}

impl S3Client {
    pub async fn new(connection: &ConnectionConfig) -> Result<Self> {
        let (bucket, config) = match connection {
            ConnectionConfig::S3 {
                bucket,
                region,
                endpoint_url,
                auth_method,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(region.clone()));

                if let Some(endpoint) = endpoint_url {
                    config_loader = config_loader.endpoint_url(endpoint);
                }

                match auth_method {
                    S3AuthMethod::CredentialChain => {}
                    S3AuthMethod::Explicit => {
                        config_loader = config_loader.credentials_provider(
                            aws_sdk_s3::config::Credentials::new(
                                access_key_id,
                                secret_access_key,
                                session_token.clone(),
                                None,
                                "featherbox-explicit",
                            ),
                        );
                    }
                }

                let config = config_loader.load().await;
                (bucket.clone(), config)
            }
            _ => return Err(anyhow::anyhow!("Expected S3 connection")),
        };

        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket: bucket.clone(),
        })
    }

    pub async fn list_objects_matching_pattern(&self, pattern: &str) -> Result<Vec<String>> {
        let prefix = extract_prefix_from_pattern(pattern);

        let all_keys = self.list_all_objects_with_prefix(&prefix).await?;

        let matching_objects: Vec<String> = all_keys
            .into_iter()
            .filter(|key| matches_pattern(pattern, key))
            .map(|key| format!("s3://{}/{}", self.bucket, key))
            .collect();

        Ok(matching_objects)
    }

    async fn list_all_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let result = request.send().await.context("Failed to list S3 objects")?;

            if let Some(contents) = result.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        all_keys.push(key);
                    }
                }
            }

            if !result.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = result.next_continuation_token;
        }

        Ok(all_keys)
    }
}

fn extract_prefix_from_pattern(pattern: &str) -> String {
    let mut prefix = String::new();
    for part in pattern.split('/') {
        if part.contains('*') || part.contains('?') {
            break;
        }
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(part);
    }
    prefix
}

fn matches_pattern(pattern: &str, key: &str) -> bool {
    let pattern_regex = pattern
        .replace(".", "\\.")
        .replace("*", ".*")
        .replace("?", ".");

    if let Ok(regex) = Regex::new(&format!("^{pattern_regex}$")) {
        regex.is_match(key)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::project::S3AuthMethod;
    use std::{env, panic, sync::Arc, sync::Mutex};

    #[test]
    fn test_extract_prefix_from_pattern() {
        assert_eq!(
            extract_prefix_from_pattern("data/2024/01/*"),
            "data/2024/01"
        );
        assert_eq!(extract_prefix_from_pattern("data/*/01/file.txt"), "data");
        assert_eq!(extract_prefix_from_pattern("data/file?.txt"), "data");
        assert_eq!(
            extract_prefix_from_pattern("data/subfolder/file.txt"),
            "data/subfolder/file.txt"
        );
        assert_eq!(extract_prefix_from_pattern("*"), "");
        assert_eq!(extract_prefix_from_pattern(""), "");
    }

    #[test]
    fn test_matches_pattern() {
        assert!(matches_pattern(
            "data/2024/01/*.csv",
            "data/2024/01/sales.csv"
        ));
        assert!(matches_pattern(
            "data/2024/01/*.csv",
            "data/2024/01/inventory.csv"
        ));
        assert!(!matches_pattern(
            "data/2024/01/*.csv",
            "data/2024/01/sales.txt"
        ));
        assert!(!matches_pattern(
            "data/2024/01/*.csv",
            "data/2024/02/sales.csv"
        ));

        assert!(matches_pattern("data/file?.txt", "data/file1.txt"));
        assert!(matches_pattern("data/file?.txt", "data/fileA.txt"));
        assert!(!matches_pattern("data/file?.txt", "data/file12.txt"));

        assert!(matches_pattern("*.json", "config.json"));
        assert!(matches_pattern("*.json", "data.json"));
        assert!(!matches_pattern("*.json", "config.txt"));

        assert!(matches_pattern(
            "data/2024/*/sales.csv",
            "data/2024/01/sales.csv"
        ));
        assert!(matches_pattern(
            "data/2024/*/sales.csv",
            "data/2024/december/sales.csv"
        ));
        assert!(!matches_pattern(
            "data/2024/*/sales.csv",
            "data/2023/01/sales.csv"
        ));

        assert!(matches_pattern("exact_match.txt", "exact_match.txt"));
        assert!(!matches_pattern("exact_match.txt", "exact_match_not.txt"));
    }

    fn should_run_s3_tests() -> bool {
        env::var("AWS_PROFILE").is_ok()
            || (env::var("AWS_ACCESS_KEY_ID").is_ok() && env::var("AWS_SECRET_ACCESS_KEY").is_ok())
    }

    fn create_s3_connection_config() -> ConnectionConfig {
        if env::var("AWS_PROFILE").is_ok() {
            ConnectionConfig::S3 {
                bucket: env::var("S3_TEST_BUCKET").unwrap_or_else(|_| "test-bucket".to_string()),
                region: env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                endpoint_url: env::var("S3_ENDPOINT_URL").ok(),
                auth_method: S3AuthMethod::CredentialChain,
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: None,
            }
        } else {
            ConnectionConfig::S3 {
                bucket: env::var("S3_TEST_BUCKET").unwrap_or_else(|_| "test-bucket".to_string()),
                region: env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
                endpoint_url: env::var("S3_ENDPOINT_URL").ok(),
                auth_method: S3AuthMethod::Explicit,
                access_key_id: env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
                secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                session_token: env::var("AWS_SESSION_TOKEN").ok(),
            }
        }
    }

    async fn setup_test_bucket(s3_client: &S3Client) -> Result<()> {
        s3_client
            .client
            .create_bucket()
            .bucket(&s3_client.bucket)
            .send()
            .await
            .context("Failed to create test bucket")?;

        println!("Created test bucket: {}", s3_client.bucket);

        let test_objects = vec![
            (
                "data/2024/01/sales.csv",
                "id,product,amount\n1,laptop,1000\n2,mouse,20",
            ),
            (
                "data/2024/01/inventory.csv",
                "id,item,stock\n1,laptop,50\n2,mouse,100",
            ),
            ("data/2024/02/sales.csv", "id,product,amount\n3,keyboard,50"),
            ("data/2024/sales.txt", "Different format file"),
            ("logs/error.log", "Error log content"),
            ("config.json", "{\"setting\": \"value\"}"),
            ("readme.txt", "README content"),
        ];

        for (key, content) in test_objects {
            s3_client
                .client
                .put_object()
                .bucket(&s3_client.bucket)
                .key(key)
                .body(content.as_bytes().to_vec().into())
                .send()
                .await
                .with_context(|| format!("Failed to upload object: {key}"))?;
        }

        println!("Uploaded {} test objects", 7);
        Ok(())
    }

    async fn cleanup_test_bucket(s3_client: &S3Client) -> Result<()> {
        let objects = s3_client.list_all_objects_with_prefix("").await?;

        if !objects.is_empty() {
            let delete_objects: Vec<_> = objects
                .iter()
                .map(|key| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .unwrap()
                })
                .collect();

            s3_client
                .client
                .delete_objects()
                .bucket(&s3_client.bucket)
                .delete(
                    aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(delete_objects))
                        .build()
                        .unwrap(),
                )
                .send()
                .await
                .context("Failed to delete objects")?;

            println!("Deleted {} objects", objects.len());
        }

        s3_client
            .client
            .delete_bucket()
            .bucket(&s3_client.bucket)
            .send()
            .await
            .context("Failed to delete test bucket")?;

        println!("Deleted test bucket: {}", s3_client.bucket);
        Ok(())
    }

    async fn setup_s3_for_test() -> Result<S3Client> {
        let mut connection_config = create_s3_connection_config();
        let unique_bucket = format!("featherbox-test-{}", chrono::Utc::now().timestamp());

        match &mut connection_config {
            ConnectionConfig::S3 { bucket, .. } => {
                *bucket = unique_bucket;
            }
            _ => unreachable!(),
        }

        let s3_client = S3Client::new(&connection_config).await?;
        setup_test_bucket(&s3_client).await?;

        let panic_hook = setup_panic_cleanup(&s3_client);
        panic::set_hook(panic_hook);

        Ok(s3_client)
    }

    fn setup_panic_cleanup(
        s3_client: &S3Client,
    ) -> Box<dyn Fn(&panic::PanicHookInfo<'_>) + Sync + Send + 'static> {
        let bucket_name = s3_client.bucket.clone();
        let cleanup_client = Arc::new(Mutex::new(Some(S3Client {
            client: s3_client.client.clone(),
            bucket: s3_client.bucket.clone(),
        })));

        let original_hook = panic::take_hook();

        Box::new(move |info| {
            if let Ok(mut client_guard) = cleanup_client.lock() {
                if let Some(client) = client_guard.take() {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    if let Err(e) = rt.block_on(cleanup_test_bucket(&client)) {
                        eprintln!("Panic cleanup failed for bucket {bucket_name}: {e}");
                    } else {
                        eprintln!("Emergency cleanup completed for bucket: {bucket_name}");
                    }
                }
            }
            original_hook(info);
        })
    }

    async fn run_pattern_matching_tests(s3_client: &S3Client) {
        let csv_files = s3_client
            .list_objects_matching_pattern("*.csv")
            .await
            .unwrap();
        assert_eq!(csv_files.len(), 3, "Expected 3 CSV files");

        for file in &csv_files {
            assert!(file.ends_with(".csv"));
            assert!(file.starts_with(&format!("s3://{}/", s3_client.bucket)));
        }

        let jan_csv_files = s3_client
            .list_objects_matching_pattern("data/2024/01/*.csv")
            .await
            .unwrap();
        assert_eq!(
            jan_csv_files.len(),
            2,
            "Expected 2 CSV files in data/2024/01/"
        );

        let data_files = s3_client
            .list_objects_matching_pattern("data/*")
            .await
            .unwrap();
        assert_eq!(data_files.len(), 4, "Expected 4 files in data/ directory");

        let txt_files = s3_client
            .list_objects_matching_pattern("*.txt")
            .await
            .unwrap();
        assert_eq!(txt_files.len(), 2, "Expected 2 TXT files");

        let config_files = s3_client
            .list_objects_matching_pattern("config.json")
            .await
            .unwrap();
        assert_eq!(config_files.len(), 1, "Expected exactly 1 config.json");
        assert!(config_files[0].ends_with("config.json"));

        println!("All pattern matching tests passed!");
    }

    #[tokio::test]
    async fn test_list_objects_matching_pattern_integration() {
        if !should_run_s3_tests() {
            println!("Skipping S3 integration test - no AWS credentials configured");
            return;
        }

        let s3_client = match setup_s3_for_test().await {
            Ok(client) => client,
            Err(e) => {
                println!("Failed to setup S3 for test: {e}");
                return;
            }
        };

        run_pattern_matching_tests(&s3_client).await;

        if let Err(e) = cleanup_test_bucket(&s3_client).await {
            println!("Warning: Failed to cleanup test bucket: {e}");
        }
    }
}
