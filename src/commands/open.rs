use anyhow::{Context, Result};
use serde_json::json;

use crate::output;

const DEFAULT_UI_BASE_URL: &str = "https://rawtree.com";

pub fn resolve_ui_base_url(api_url: &str) -> String {
    std::env::var("RAWTREE_UI_URL").unwrap_or_else(|_| ui_base_url_for_api(api_url))
}

const HOSTED_DOMAINS: [&str; 2] = ["rawtree.com", "rawtree.dev"];

/// Every hosted API, regional ones included, serves its UI from the root of its
/// domain: rawtree.com in production, rawtree.dev in development. Other APIs,
/// such as a local gateway, serve the UI from their own origin.
fn ui_base_url_for_api(api_url: &str) -> String {
    let Ok(url) = reqwest::Url::parse(api_url) else {
        return DEFAULT_UI_BASE_URL.to_string();
    };
    let Some(host) = url.host_str() else {
        return DEFAULT_UI_BASE_URL.to_string();
    };
    HOSTED_DOMAINS
        .into_iter()
        .find(|domain| {
            host == *domain
                || host
                    .strip_suffix(domain)
                    .is_some_and(|prefix| prefix.ends_with('.'))
        })
        .map_or_else(
            || url.origin().ascii_serialization(),
            |domain| format!("https://{domain}"),
        )
}

pub(crate) fn build_open_url(
    base_url: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    database: Option<&str>,
) -> String {
    let trimmed_base = base_url.trim_end_matches('/');
    match (organization, cluster, database) {
        (Some(org), Some(cluster_name), Some(database_name)) => format!(
            "{}/{}/{}?database={}",
            trimmed_base,
            urlencoding::encode(org),
            urlencoding::encode(cluster_name),
            urlencoding::encode(database_name),
        ),
        (Some(org), Some(cluster_name), None) => format!(
            "{}/{}/{}",
            trimmed_base,
            urlencoding::encode(org),
            urlencoding::encode(cluster_name),
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
    cluster: Option<&str>,
    database: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let target_url = build_open_url(base_url, organization, cluster, database);
    open_url(&target_url, json_mode)
}

#[cfg(test)]
mod tests {
    use super::{build_open_url, ui_base_url_for_api};

    #[test]
    fn ui_base_url_follows_api_url() {
        assert_eq!(
            ui_base_url_for_api("https://api.rawtree.com"),
            "https://rawtree.com"
        );
        assert_eq!(
            ui_base_url_for_api("https://api.us-east-1.aws.rawtree.com/"),
            "https://rawtree.com"
        );
        assert_eq!(
            ui_base_url_for_api("https://api.rawtree.dev"),
            "https://rawtree.dev"
        );
        assert_eq!(
            ui_base_url_for_api("https://api.us-east-1.aws.rawtree.dev/"),
            "https://rawtree.dev"
        );
        assert_eq!(
            ui_base_url_for_api("https://notrawtree.dev"),
            "https://notrawtree.dev"
        );
        assert_eq!(
            ui_base_url_for_api("http://localhost:8082/"),
            "http://localhost:8082"
        );
        assert_eq!(
            ui_base_url_for_api("https://api.example.com/rawtree"),
            "https://api.example.com"
        );
        assert_eq!(ui_base_url_for_api("not a url"), "https://rawtree.com");
    }

    #[test]
    fn build_open_url_uses_base_url_when_database_context_missing() {
        let url = build_open_url("https://rawtree.com/", Some("team_alpha"), None, None);
        assert_eq!(url, "https://rawtree.com");
    }

    #[test]
    fn build_open_url_appends_org_cluster_and_database() {
        let url = build_open_url(
            "https://rawtree.com",
            Some("team_alpha"),
            Some("production"),
            Some("analytics"),
        );
        assert_eq!(
            url,
            "https://rawtree.com/team_alpha/production?database=analytics"
        );
    }

    #[test]
    fn build_open_url_encodes_path_segments() {
        let url = build_open_url(
            "https://rawtree.com",
            Some("team alpha"),
            Some("prod/eu"),
            Some("p/1"),
        );
        assert_eq!(
            url,
            "https://rawtree.com/team%20alpha/prod%2Feu?database=p%2F1"
        );
    }
}
