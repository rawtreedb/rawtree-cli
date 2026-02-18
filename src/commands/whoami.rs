use anyhow::Result;
use serde_json::json;

use crate::config;
use crate::output;

pub fn whoami(resolved_url: &str, json_mode: bool) -> Result<()> {
    let cfg = config::load()?;
    match &cfg.email {
        Some(email) => {
            output::print_result(
                &json!({
                    "email": email,
                    "url": resolved_url,
                    "default_project": cfg.default_project,
                }),
                json_mode,
                |_| println!("{}", email),
            );
        }
        None => {
            anyhow::bail!("Not logged in. Run `rtree login` to authenticate.");
        }
    }
    Ok(())
}
