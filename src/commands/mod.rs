pub mod adapter;
pub mod init;
pub mod model;
pub mod run;

pub const ADAPTER_TEMPLATE: &str = include_str!("templates/adapter.yml");
pub const MODEL_TEMPLATE: &str = include_str!("templates/model.yml");

pub fn render_adapter_template(name: &str) -> String {
    ADAPTER_TEMPLATE.replace("{name}", name)
}

pub fn render_model_template(name: &str) -> String {
    MODEL_TEMPLATE.replace("{name}", name)
}

// 共通のバリデーション関数
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
        assert!(validate_name("test adapter").is_err()); // スペース
        assert!(validate_name("test@adapter").is_err()); // 特殊文字
        assert!(validate_name("test/adapter").is_err()); // スラッシュ
    }
}
