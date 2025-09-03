use crate::secret::SecretManager;
use anyhow::Result;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::Json;
use axum::{
    Router,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SecretSummary {
    pub key: String,
    pub masked_value: String,
}

#[derive(Deserialize)]
pub struct CreateSecretRequest {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize)]
pub struct UpdateSecretRequest {
    pub value: String,
}

#[derive(Deserialize)]
pub struct GenerateSecretKeyRequest {
    pub connection_name: String,
    pub connection_type: String,
    pub field_type: String,
}

#[derive(Serialize)]
pub struct GenerateSecretKeyResponse {
    pub key: String,
}

pub fn routes() -> Router {
    Router::new()
        .route("/secrets", get(list_secrets).post(create_secret))
        .route(
            "/secrets/{key}",
            get(get_secret_info)
                .put(update_secret)
                .delete(delete_secret),
        )
        .route("/secrets/generate-key", post(generate_unique_secret_key))
}

async fn list_secrets() -> Result<Json<Vec<SecretSummary>>, StatusCode> {
    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let secrets = manager
        .get_all_secrets()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut summaries = Vec::new();
    for (key, value) in secrets {
        let masked_value = mask_secret_value(&value);
        summaries.push(SecretSummary { key, masked_value });
    }

    summaries.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(Json(summaries))
}

async fn get_secret_info(Path(key): Path<String>) -> Result<Json<SecretSummary>, StatusCode> {
    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match manager
        .get_secret(&key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(value) => {
            let masked_value = mask_secret_value(&value);
            Ok(Json(SecretSummary { key, masked_value }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn create_secret(Json(req): Json<CreateSecretRequest>) -> Result<StatusCode, StatusCode> {
    if !is_valid_secret_key(&req.key) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if manager
        .get_secret(&req.key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_some()
    {
        return Err(StatusCode::CONFLICT);
    }

    manager
        .set_secret(&req.key, &req.value)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::CREATED)
}

async fn update_secret(
    Path(key): Path<String>,
    Json(req): Json<UpdateSecretRequest>,
) -> Result<StatusCode, StatusCode> {
    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if manager
        .get_secret(&key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .is_none()
    {
        return Err(StatusCode::NOT_FOUND);
    }

    manager
        .set_secret(&key, &req.value)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::OK)
}

async fn delete_secret(Path(key): Path<String>) -> Result<StatusCode, StatusCode> {
    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let removed = manager
        .delete_secret(&key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn generate_unique_secret_key(
    Json(req): Json<GenerateSecretKeyRequest>,
) -> Result<Json<GenerateSecretKeyResponse>, StatusCode> {
    let manager = SecretManager::new().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let base_key = generate_secret_key_for_connection(
        &req.connection_name,
        &req.connection_type,
        &req.field_type,
    );

    let unique_key = find_unique_secret_key(&manager, &base_key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(GenerateSecretKeyResponse { key: unique_key }))
}

fn mask_secret_value(value: &str) -> String {
    if value.len() <= 2 {
        "*".repeat(value.len())
    } else {
        let visible_chars = 2;
        let masked_part = "*".repeat(value.len() - visible_chars);
        format!("{}{}", masked_part, &value[value.len() - visible_chars..])
    }
}

fn is_valid_secret_key(key: &str) -> bool {
    if key.is_empty() || key.len() > 64 {
        return false;
    }

    let first_char = key.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() {
        return false;
    }

    key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn generate_secret_key_for_connection(
    connection_name: &str,
    connection_type: &str,
    field_type: &str,
) -> String {
    let sanitized_name = sanitize_connection_name(connection_name);
    let upper_type = connection_type.to_uppercase();
    let upper_field = field_type.to_uppercase();
    format!("{}_{}_{}", sanitized_name, upper_type, upper_field)
}

pub fn find_unique_secret_key(
    manager: &SecretManager,
    base_key: &str,
) -> Result<String, anyhow::Error> {
    if manager.get_secret(base_key)?.is_none() {
        return Ok(base_key.to_string());
    }

    for i in 2..=999 {
        let candidate = format!("{}_{}", base_key, i);
        if manager.get_secret(&candidate)?.is_none() {
            return Ok(candidate);
        }
    }

    Err(anyhow::anyhow!(
        "Unable to find unique secret key for: {}",
        base_key
    ))
}

fn sanitize_connection_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_secret_value() {
        assert_eq!(mask_secret_value(""), "");
        assert_eq!(mask_secret_value("a"), "*");
        assert_eq!(mask_secret_value("ab"), "**");
        assert_eq!(mask_secret_value("abc"), "*bc");
        assert_eq!(mask_secret_value("password123"), "*********23");
        assert_eq!(mask_secret_value("verylongpassword"), "**************rd");
    }

    #[test]
    fn test_is_valid_secret_key() {
        assert!(is_valid_secret_key("VALID_KEY"));
        assert!(is_valid_secret_key("test123"));
        assert!(is_valid_secret_key("a"));
        assert!(is_valid_secret_key("My_Secret_123"));

        assert!(!is_valid_secret_key(""));
        assert!(!is_valid_secret_key("123invalid"));
        assert!(!is_valid_secret_key("_invalid"));
        assert!(!is_valid_secret_key("invalid-key"));
        assert!(!is_valid_secret_key("invalid key"));
        assert!(!is_valid_secret_key(&"a".repeat(65)));
    }

    #[test]
    fn test_generate_secret_key_for_connection() {
        let key = generate_secret_key_for_connection("my_db", "mysql", "password");
        assert_eq!(key, "MY_DB_MYSQL_PASSWORD");

        let key = generate_secret_key_for_connection("test-connection", "postgresql", "password");
        assert_eq!(key, "TEST_CONNECTION_POSTGRESQL_PASSWORD");

        let key = generate_secret_key_for_connection("s3 storage", "s3", "secret_access_key");
        assert_eq!(key, "S3_STORAGE_S3_SECRET_ACCESS_KEY");
    }

    #[test]
    fn test_sanitize_connection_name() {
        assert_eq!(sanitize_connection_name("my_db"), "MY_DB");
        assert_eq!(
            sanitize_connection_name("test-connection"),
            "TEST_CONNECTION"
        );
        assert_eq!(sanitize_connection_name("s3 storage"), "S3_STORAGE");
        assert_eq!(sanitize_connection_name("123abc"), "123ABC");
        assert_eq!(
            sanitize_connection_name("special!@#chars"),
            "SPECIAL___CHARS"
        );
    }
}
