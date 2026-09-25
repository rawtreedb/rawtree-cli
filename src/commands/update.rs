use std::fs;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axoupdater::{AxoUpdater, AxoupdateError, Version};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{config, output};

const APP_NAME: &str = "rawtree-cli";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
const LATEST_RELEASE_URL: &str =
    "https://api.github.com/repos/rawtreedb/rawtree-cli/releases/latest";
const UPDATE_CHECK_INTERVAL_SECS: u64 = 24 * 60 * 60;
const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
const DISABLE_UPDATE_CHECK_ENV: &str = "RAWTREE_NO_UPDATE_CHECK";
const INSTALL_COMMAND: &str = "curl --proto '=https' --tlsv1.2 -LsSf https://github.com/rawtreedb/rawtree-cli/releases/latest/download/rawtree-cli-installer.sh | sh";

pub fn update(json_mode: bool) -> Result<()> {
    let mut updater = AxoUpdater::new_for(APP_NAME);
    match updater.load_receipt() {
        Ok(_) => {}
        Err(AxoupdateError::NoReceipt { .. }) => return Err(unmanaged_install_error()),
        Err(err) => return Err(err).context("failed to read the rtree install receipt"),
    }
    // A receipt from a release install doesn't mean this binary is that install
    // (e.g. a `cargo install` or local build run alongside it).
    if !updater.check_receipt_is_for_this_executable()? {
        return Err(unmanaged_install_error());
    }
    let current_version: Version = CURRENT_VERSION.parse().context("invalid rtree version")?;
    updater.set_current_version(current_version)?;
    if json_mode {
        updater.disable_installer_output();
    } else {
        updater.enable_installer_output();
    }

    let result = updater.run_sync().context("failed to update rtree")?;
    match result {
        Some(result) => output::print_result(
            &json!({
                "updated": true,
                "previous_version": CURRENT_VERSION,
                "version": result.new_version.to_string(),
            }),
            json_mode,
            |_| {
                println!(
                    "Updated rtree from {} to {}.",
                    CURRENT_VERSION, result.new_version
                )
            },
        ),
        None => output::print_result(
            &json!({"updated": false, "version": CURRENT_VERSION}),
            json_mode,
            |_| println!("rtree {CURRENT_VERSION} is already up to date."),
        ),
    }
    Ok(())
}

fn unmanaged_install_error() -> anyhow::Error {
    output::coded_error(
        "update_unsupported",
        format!(
            "This rtree was not installed with the release installer, so it can't update itself.\n\
             Install the latest release with:\n  {INSTALL_COMMAND}\n\
             For a source install, run `git pull && cargo install --path .` instead."
        ),
        1,
    )
}

#[derive(Default, Serialize, Deserialize)]
struct UpdateCheckCache {
    checked_at: u64,
    latest_version: Option<String>,
}

/// Prints a one-line notice to stderr when a newer release exists. Never fails the command.
pub fn notify_if_outdated(json_mode: bool) {
    if !update_check_enabled(
        json_mode,
        io::stderr().is_terminal(),
        std::env::var_os("CI").is_some(),
        std::env::var_os(DISABLE_UPDATE_CHECK_ENV).is_some(),
    ) {
        return;
    }
    let Some(latest) = cached_latest_version() else {
        return;
    };
    if let Some(latest) = newer_version(CURRENT_VERSION, &latest) {
        eprintln!(
            "\nA new rtree version is available ({CURRENT_VERSION} -> {latest}). Run `rtree update` to upgrade."
        );
    }
}

fn update_check_enabled(
    json_mode: bool,
    stderr_is_terminal: bool,
    is_ci: bool,
    disabled_by_env: bool,
) -> bool {
    !json_mode && stderr_is_terminal && !is_ci && !disabled_by_env
}

fn cached_latest_version() -> Option<String> {
    let path = update_check_cache_path()?;
    let mut cache: UpdateCheckCache = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if check_is_due(cache.checked_at, now) {
        // Record the attempt even when the fetch fails so offline use doesn't retry on every command.
        cache.checked_at = now;
        if let Some(latest) = fetch_latest_version() {
            cache.latest_version = Some(latest);
        }
        if let (Some(dir), Ok(bytes)) = (path.parent(), serde_json::to_vec(&cache)) {
            let _ = fs::create_dir_all(dir).and_then(|_| fs::write(&path, bytes));
        }
    }
    cache.latest_version
}

fn update_check_cache_path() -> Option<PathBuf> {
    let config_path = config::path().ok()?;
    Some(config_path.parent()?.join("update-check.json"))
}

fn check_is_due(checked_at: u64, now: u64) -> bool {
    now.saturating_sub(checked_at) >= UPDATE_CHECK_INTERVAL_SECS
}

fn fetch_latest_version() -> Option<String> {
    #[derive(Deserialize)]
    struct LatestRelease {
        tag_name: String,
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(UPDATE_CHECK_TIMEOUT)
        .user_agent(concat!("rawtree-cli/", env!("CARGO_PKG_VERSION")))
        .build()
        .ok()?;
    let release: LatestRelease = client
        .get(LATEST_RELEASE_URL)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .ok()?;
    Some(release.tag_name.trim_start_matches('v').to_string())
}

fn newer_version(current: &str, latest: &str) -> Option<Version> {
    let current: Version = current.parse().ok()?;
    let latest: Version = latest.parse().ok()?;
    (latest > current).then_some(latest)
}

#[cfg(test)]
mod tests {
    use super::{check_is_due, newer_version, update_check_enabled, UPDATE_CHECK_INTERVAL_SECS};

    #[test]
    fn newer_version_only_reports_strictly_newer_releases() {
        assert_eq!(
            newer_version("0.6.11", "0.6.12").map(|v| v.to_string()),
            Some("0.6.12".to_string())
        );
        assert_eq!(
            newer_version("0.6.9", "0.6.10").map(|v| v.to_string()),
            Some("0.6.10".to_string())
        );
        assert!(newer_version("0.6.12", "0.6.12").is_none());
        assert!(newer_version("0.6.12", "0.6.11").is_none());
        assert!(newer_version("0.6.12", "not-a-version").is_none());
    }

    #[test]
    fn update_check_runs_at_most_once_per_interval() {
        assert!(check_is_due(0, UPDATE_CHECK_INTERVAL_SECS));
        assert!(!check_is_due(1_000, 1_000 + UPDATE_CHECK_INTERVAL_SECS - 1));
        assert!(check_is_due(1_000, 1_000 + UPDATE_CHECK_INTERVAL_SECS));
    }

    #[test]
    fn update_check_is_skipped_for_scripts_ci_and_opt_out() {
        assert!(update_check_enabled(false, true, false, false));
        assert!(!update_check_enabled(true, true, false, false));
        assert!(!update_check_enabled(false, false, false, false));
        assert!(!update_check_enabled(false, true, true, false));
        assert!(!update_check_enabled(false, true, false, true));
    }
}
