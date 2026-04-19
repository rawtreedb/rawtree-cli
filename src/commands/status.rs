use anyhow::Result;
use serde_json::json;

use crate::config;
use crate::commands::open;
use crate::output;

fn build_login_url(base_url: &str) -> String {
    format!("{}/login", base_url.trim_end_matches('/'))
}

fn resolve_dashboard_url(
    base_url: &str,
    authenticated: bool,
    organization: Option<&str>,
    project: Option<&str>,
    claim_token: Option<&str>,
) -> Option<String> {
    if authenticated {
        return Some(open::build_open_url(base_url, organization, project));
    }
    match claim_token {
        Some(token) => Some(open::build_claim_dashboard_url(base_url, token)),
        None => Some(build_login_url(base_url)),
    }
}

pub fn status(resolved_url: &str, json_mode: bool) -> Result<()> {
    let cfg = config::load()?;
    let authenticated = cfg
        .token
        .as_deref()
        .map(crate::token_looks_like_jwt)
        .unwrap_or(false);
    let user = cfg.email.clone();
    let project = cfg.default_project.clone();
    let organization = cfg.default_organization.clone();
    let ui_base_url = open::resolve_ui_base_url();
    let dashboard_url = resolve_dashboard_url(
        &ui_base_url,
        authenticated,
        organization.as_deref(),
        project.as_deref(),
        cfg.last_claim_token.as_deref(),
    );

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
    use super::{build_login_url, resolve_dashboard_url};

    #[test]
    fn resolve_dashboard_url_prefers_normal_dashboard_when_authenticated() {
        let url = resolve_dashboard_url(
            "https://rawtree.com",
            true,
            Some("team"),
            Some("analytics"),
            Some("claim_abc"),
        );
        assert_eq!(url.as_deref(), Some("https://rawtree.com/team/analytics"));
    }

    #[test]
    fn resolve_dashboard_url_uses_claim_dashboard_when_not_authenticated() {
        let url = resolve_dashboard_url(
            "https://rawtree.com",
            false,
            Some("team"),
            Some("analytics"),
            Some("claim_abc"),
        );
        assert_eq!(
            url.as_deref(),
            Some("https://rawtree.com/claim/claim_abc/dashboard")
        );
    }

    #[test]
    fn resolve_dashboard_url_uses_login_when_not_authenticated_and_no_claim_token() {
        let url = resolve_dashboard_url("https://rawtree.com", false, None, None, None);
        assert_eq!(url.as_deref(), Some("https://rawtree.com/login"));
    }

    #[test]
    fn build_login_url_appends_login_path() {
        let url = build_login_url("https://rawtree.com/");
        assert_eq!(url, "https://rawtree.com/login");
    }

    #[test]
    fn temporary_token_is_treated_as_not_authenticated() {
        assert!(!crate::token_looks_like_jwt("rw_temporary"));
    }
}
