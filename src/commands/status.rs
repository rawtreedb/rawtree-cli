use anyhow::Result;
use serde_json::json;

use crate::config;
use crate::commands::open;
use crate::output;

fn build_dashboard_url(base_url: &str, organization: Option<&str>, project: Option<&str>) -> String {
    let trimmed_base = base_url.trim_end_matches('/');
    match (organization, project) {
        (Some(org), Some(project_name)) => format!(
            "{}/{}/{}",
            trimmed_base,
            urlencoding::encode(org),
            urlencoding::encode(project_name)
        ),
        _ => trimmed_base.to_string(),
    }
}

fn build_claim_dashboard_url(base_url: &str, claim_token: &str) -> String {
    format!(
        "{}/claim/{}/dashboard",
        base_url.trim_end_matches('/'),
        urlencoding::encode(claim_token)
    )
}

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
        return Some(build_dashboard_url(base_url, organization, project));
    }
    match claim_token {
        Some(token) => Some(build_claim_dashboard_url(base_url, token)),
        None => Some(build_login_url(base_url)),
    }
}

pub fn status(resolved_url: &str, json_mode: bool) -> Result<()> {
    let cfg = config::load()?;
    let authenticated = cfg.token.is_some();
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
    use super::{build_claim_dashboard_url, build_dashboard_url, build_login_url, resolve_dashboard_url};

    #[test]
    fn build_claim_dashboard_url_appends_dashboard_route() {
        let url = build_claim_dashboard_url("https://rawtree.com/", "a/b");
        assert_eq!(url, "https://rawtree.com/claim/a%2Fb/dashboard");
    }

    #[test]
    fn build_dashboard_url_appends_org_and_project_path() {
        let url = build_dashboard_url("https://rawtree.com/", Some("team alpha"), Some("p/1"));
        assert_eq!(url, "https://rawtree.com/team%20alpha/p%2F1");
    }

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
}
