use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth: Option<crate::oauth::Credentials>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default, rename = "database", alias = "default_database")]
    pub default_database: Option<String>,
    #[serde(default)]
    pub default_organization: Option<String>,
    #[serde(default, rename = "cluster", alias = "default_cluster")]
    pub default_cluster: Option<String>,
}

fn config_path() -> Result<PathBuf> {
    let dir = dirs_fallback().context("cannot determine config directory")?;
    Ok(dir.join("config.json"))
}

pub fn path() -> Result<PathBuf> {
    config_path()
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

/// The stable sidecar lock survives atomic replacement of config.json.
/// Hold it across every read-modify-write, especially refresh-token rotation.
pub fn lock() -> Result<fs::File> {
    let path = config_path()?.with_extension("lock");
    fs::create_dir_all(path.parent().context("config path has no parent")?)
        .context("failed to create config directory")?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock().context("failed to lock CLI config")?;
    Ok(file)
}

pub fn save(cfg: &Config) -> Result<()> {
    let path = config_path()?;
    let parent = path.parent().context("config path has no parent")?;
    fs::create_dir_all(parent).context("failed to create config directory")?;
    use std::io::Write;
    // Tempfiles start private and rename atomically, including during refresh rotation.
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(serde_json::to_string_pretty(cfg)?.as_bytes())?;
    file.as_file().sync_all()?;
    file.persist(&path)
        .context("failed to replace config file")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn old_config_with_default_database_still_deserializes() {
        let old = r#"{
  "default_database": "analytics"
}"#;
        let cfg: Config = serde_json::from_str(old).expect("old config should parse");
        assert_eq!(cfg.default_database.as_deref(), Some("analytics"));
    }

    #[test]
    fn config_serializes_database_key() {
        let cfg = Config {
            default_database: Some("analytics".to_string()),
            ..Config::default()
        };
        let json = serde_json::to_value(&cfg).expect("config should serialize");

        assert_eq!(json["database"], "analytics");
        assert!(json.get("default_database").is_none());
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
    fn config_serializes_cluster_key() {
        let cfg = Config {
            default_cluster: Some("production".to_string()),
            ..Config::default()
        };
        let json = serde_json::to_value(&cfg).expect("config should serialize");

        assert_eq!(json["cluster"], "production");
        assert!(json.get("default_cluster").is_none());
    }

    #[test]
    fn old_default_cluster_key_still_deserializes() {
        let cfg: Config = serde_json::from_str(r#"{"default_cluster":"production"}"#)
            .expect("old default_cluster key should deserialize");
        assert_eq!(cfg.default_cluster.as_deref(), Some("production"));
    }
}
