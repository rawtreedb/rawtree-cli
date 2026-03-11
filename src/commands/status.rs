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
            "default_organization": cfg.default_organization,
            "last_claim_url": cfg.last_claim_url,
            "last_claim_token": cfg.last_claim_token,
            "last_project_temporary": cfg.last_project_temporary,
            "last_project_expires_in_seconds": cfg.last_project_expires_in_seconds,
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
            if let Some(ref organization) = cfg.default_organization {
                println!("Default organization: {}", organization);
            }
            if let Some(ref claim_url) = cfg.last_claim_url {
                println!("Last claim URL: {}", claim_url);
            }
            if let Some(ref claim_token) = cfg.last_claim_token {
                println!("Last claim token: {}", claim_token);
            }
            if let Some(temporary) = cfg.last_project_temporary {
                println!("Last created project temporary: {}", temporary);
            }
            if let Some(expires_in_seconds) = cfg.last_project_expires_in_seconds {
                println!("Last temporary project TTL (s): {}", expires_in_seconds);
            }
        },
    );
    Ok(())
}
