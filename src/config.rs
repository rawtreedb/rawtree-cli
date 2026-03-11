use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub default_project: Option<String>,
    #[serde(default)]
    pub default_organization: Option<String>,
    #[serde(default)]
    pub last_claim_url: Option<String>,
    #[serde(default)]
    pub last_claim_token: Option<String>,
    #[serde(default)]
    pub last_project_temporary: Option<bool>,
    #[serde(default)]
    pub last_project_expires_in_seconds: Option<u64>,
}

fn config_path() -> Result<PathBuf> {
    let dir = dirs_fallback().context("cannot determine config directory")?;
    Ok(dir.join("config.json"))
}

/// Returns ~/.config/rtree on Unix, or an equivalent on other platforms.
fn dirs_fallback() -> Option<PathBuf> {
    if let Ok(home) = std::env::var("HOME") {
        Some(PathBuf::from(home).join(".config").join("rtree"))
    } else {
        None
    }
}

pub fn load() -> Result<Config> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(Config::default());
    }
    let data = fs::read_to_string(&path).context("failed to read config file")?;
    let cfg: Config = serde_json::from_str(&data).context("invalid config JSON")?;
    Ok(cfg)
}

pub fn save(cfg: &Config) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("failed to create config directory")?;
    }
    let data = serde_json::to_string_pretty(cfg)?;
    fs::write(&path, &data).context("failed to write config file")?;

    // Set file permissions to 0600 on Unix (contains JWT token)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn old_config_without_default_organization_still_deserializes() {
        let old = r#"{
  "token": "t",
  "email": "e@example.com",
  "url": "https://api.rawtree.dev",
  "default_project": "analytics"
}"#;
        let cfg: Config = serde_json::from_str(old).expect("old config should parse");
        assert_eq!(cfg.default_project.as_deref(), Some("analytics"));
        assert_eq!(cfg.default_organization, None);
    }

    #[test]
    fn new_config_with_default_organization_deserializes() {
        let new_cfg = r#"{
  "default_organization": "team_alpha"
}"#;
        let cfg: Config = serde_json::from_str(new_cfg).expect("new config should parse");
        assert_eq!(cfg.default_organization.as_deref(), Some("team_alpha"));
    }

    #[test]
    fn new_config_with_claim_metadata_deserializes() {
        let new_cfg = r#"{
  "last_claim_url": "https://app.rawtree.dev/claim/project?token=abc",
  "last_claim_token": "abc",
  "last_project_temporary": true,
  "last_project_expires_in_seconds": 86400
}"#;
        let cfg: Config = serde_json::from_str(new_cfg).expect("new config should parse");
        assert_eq!(
            cfg.last_claim_url.as_deref(),
            Some("https://app.rawtree.dev/claim/project?token=abc")
        );
        assert_eq!(cfg.last_claim_token.as_deref(), Some("abc"));
        assert_eq!(cfg.last_project_temporary, Some(true));
        assert_eq!(cfg.last_project_expires_in_seconds, Some(86400));
    }
}
