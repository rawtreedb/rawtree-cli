use anyhow::{Context, Result};
use axoupdater::{AxoUpdater, AxoupdateError, Version};
use serde_json::json;

use crate::output;

const APP_NAME: &str = "rawtree-cli";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");
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
