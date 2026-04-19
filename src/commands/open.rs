use anyhow::{Context, Result};
use serde_json::json;

use crate::output;

const DEFAULT_UI_BASE_URL: &str = "https://rawtree.com";

pub fn resolve_ui_base_url() -> String {
    std::env::var("RAWTREE_UI_URL").unwrap_or_else(|_| DEFAULT_UI_BASE_URL.to_string())
}

pub(crate) fn build_claim_dashboard_url(base_url: &str, claim_token: &str) -> String {
    format!(
        "{}/claim/{}/dashboard",
        base_url.trim_end_matches('/'),
        urlencoding::encode(claim_token)
    )
}

pub(crate) fn build_open_url(
    base_url: &str,
    organization: Option<&str>,
    project: Option<&str>,
) -> String {
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

pub fn open_url(target_url: &str, json_mode: bool) -> Result<()> {
    webbrowser::open(target_url).with_context(|| format!("failed to open '{}'", target_url))?;
    output::print_result(&json!({ "url": target_url }), json_mode, |_| {
        println!("Opened {}", target_url);
    });
    Ok(())
}

pub fn open(
    base_url: &str,
    organization: Option<&str>,
    project: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let target_url = build_open_url(base_url, organization, project);
    open_url(&target_url, json_mode)
}

#[cfg(test)]
mod tests {
    use super::{build_claim_dashboard_url, build_open_url};

    #[test]
    fn build_open_url_uses_base_url_when_project_context_missing() {
        let url = build_open_url("https://rawtree.com/", Some("team_alpha"), None);
        assert_eq!(url, "https://rawtree.com");
    }

    #[test]
    fn build_open_url_appends_org_and_project_path() {
        let url = build_open_url("https://rawtree.com", Some("team_alpha"), Some("analytics"));
        assert_eq!(url, "https://rawtree.com/team_alpha/analytics");
    }

    #[test]
    fn build_open_url_encodes_path_segments() {
        let url = build_open_url("https://rawtree.com", Some("team alpha"), Some("p/1"));
        assert_eq!(url, "https://rawtree.com/team%20alpha/p%2F1");
    }

    #[test]
    fn build_claim_dashboard_url_appends_dashboard_route() {
        let url = build_claim_dashboard_url("https://rawtree.com/", "a/b");
        assert_eq!(url, "https://rawtree.com/claim/a%2Fb/dashboard");
    }
}
