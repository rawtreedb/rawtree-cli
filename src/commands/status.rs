use anyhow::Result;
use serde_json::json;

use crate::config;
use crate::commands::open;
use crate::output;

fn build_claim_dashboard_url(base_url: &str, claim_token: &str) -> String {
    format!(
        "{}/claim/{}/dashboard",
        base_url.trim_end_matches('/'),
        urlencoding::encode(claim_token)
    )
}

fn resolve_dashboard_url(claim_token: Option<&str>) -> Option<String> {
    claim_token.map(|token| {
        let ui_base_url = open::resolve_ui_base_url();
        build_claim_dashboard_url(&ui_base_url, token)
    })
}

pub fn status(resolved_url: &str, json_mode: bool) -> Result<()> {
    let cfg = config::load()?;
    let authenticated = cfg.token.is_some();
    let user = cfg.email.clone();
    let project = cfg.default_project.clone();
    let organization = cfg.default_organization.clone();
    let dashboard_url = resolve_dashboard_url(cfg.last_claim_token.as_deref());

    output::print_result(
        &json!({
            "authenticated": authenticated,
            "user": user.as_deref(),
            "project": project.as_deref(),
            "organization": organization.as_deref(),
            "api_url": resolved_url,
            "dashboard_url": dashboard_url.as_deref(),
        }),
        json_mode,
        |_| {
            println!("API URL: {}", resolved_url);
            println!("Authenticated: {}", authenticated);
            println!("User: {}", user.as_deref().unwrap_or("-"));
            println!("Project: {}", project.as_deref().unwrap_or("-"));
            println!("Organization: {}", organization.as_deref().unwrap_or("-"));
            println!(
                "Dashboard URL: {}",
                dashboard_url.as_deref().unwrap_or("-")
            );
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::build_claim_dashboard_url;

    #[test]
    fn build_claim_dashboard_url_appends_dashboard_route() {
        let url = build_claim_dashboard_url("https://rawtree.com/", "a/b");
        assert_eq!(url, "https://rawtree.com/claim/a%2Fb/dashboard");
    }
}
