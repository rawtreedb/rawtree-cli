use anyhow::Result;
use serde_json::json;

use crate::config;
use crate::output;

pub fn status(resolved_url: &str, json_mode: bool) -> Result<()> {
    let cfg = config::load()?;
    output::print_result(
        &json!({
            "url": resolved_url,
            "email": cfg.email,
            "authenticated": cfg.token.is_some(),
            "default_project": cfg.default_project,
        }),
        json_mode,
        |_| {
            println!("Server URL: {}", resolved_url);
            match &cfg.email {
                Some(email) => println!("Logged in as: {}", email),
                None => println!("Not logged in. Run `rtree login` to authenticate."),
            }
            if let Some(ref project) = cfg.default_project {
                println!("Default project: {}", project);
            }
        },
    );
    Ok(())
}
