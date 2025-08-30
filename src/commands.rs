pub mod adapter;
pub mod connection;
pub mod init;
pub mod migrate;
pub mod model;
pub mod query;
pub mod run;
pub mod secret;
pub mod start;
pub mod workspace;

pub const ADAPTER_TEMPLATE: &str = include_str!("commands/templates/adapter.yml");
pub const MODEL_TEMPLATE: &str = include_str!("commands/templates/model.yml");

pub fn render_adapter_template(name: &str) -> String {
    ADAPTER_TEMPLATE.replace("{name}", name)
}

pub fn render_model_template(name: &str) -> String {
    MODEL_TEMPLATE.replace("{name}", name)
}

pub fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty() {
        return Err(anyhow::anyhow!("Name cannot be empty"));
    }

    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(anyhow::anyhow!(
            "Name can only contain alphanumeric characters, underscores, and hyphens"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name("test_adapter").is_ok());
        assert!(validate_name("test-model").is_ok());
        assert!(validate_name("TestAdapter123").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        assert!(validate_name("").is_err());
        assert!(validate_name("test adapter").is_err());
        assert!(validate_name("test@adapter").is_err());
        assert!(validate_name("test/adapter").is_err());
    }

    #[test]
    fn test_render_adapter_template_replaces_name() {
        let result = render_adapter_template("test_adapter");
        assert!(result.contains("Generated adapter for test_adapter"));
        assert!(!result.contains("{name}"));
    }

    #[test]
    fn test_render_adapter_template_preserves_other_placeholders() {
        let result = render_adapter_template("my_adapter");
        assert!(result.contains("<CONNECTION_NAME>"));
        assert!(result.contains("<PATH_TO_DATA_FILE>"));
        assert!(result.contains("Generated adapter for my_adapter"));
    }

    #[test]
    fn test_render_adapter_template_with_empty_name() {
        let result = render_adapter_template("");
        assert!(result.contains("Generated adapter for "));
        assert!(!result.contains("{name}"));
    }

    #[test]
    fn test_render_adapter_template_with_special_characters() {
        let result = render_adapter_template("test-adapter_123");
        assert!(result.contains("Generated adapter for test-adapter_123"));
    }

    #[test]
    fn test_render_model_template_replaces_name() {
        let result = render_model_template("test_model");
        assert!(result.contains("Generated model for test_model"));
        assert!(!result.contains("{name}"));
    }

    #[test]
    fn test_render_model_template_preserves_sql_structure() {
        let result = render_model_template("analytics");
        assert!(result.contains("sql: |"));
        assert!(result.contains("SELECT COUNT(*) FROM source_table"));
        assert!(result.contains("Generated model for analytics"));
    }

    #[test]
    fn test_render_model_template_with_empty_name() {
        let result = render_model_template("");
        assert!(result.contains("Generated model for "));
        assert!(!result.contains("{name}"));
    }

    #[test]
    fn test_render_model_template_with_special_characters() {
        let result = render_model_template("user-profile_v2");
        assert!(result.contains("Generated model for user-profile_v2"));
    }
}
