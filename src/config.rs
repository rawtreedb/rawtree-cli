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
