use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::client::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationItem {
    pub organization_id: String,
    pub organization_name: String,
    pub role: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
struct ListOrganizationsResponse {
    organizations: Vec<OrganizationItem>,
}

pub fn list_organizations(client: &ApiClient) -> Result<Vec<OrganizationItem>> {
    let resp: ListOrganizationsResponse = client.get("/v1/organizations")?;
    Ok(resp.organizations)
}

pub fn first_organization_name(client: &ApiClient) -> Option<String> {
    list_organizations(client)
        .ok()?
        .into_iter()
        .next()
        .map(|org| org.organization_name)
}

pub fn resolve_organization_id(client: &ApiClient, organization_name: &str) -> Result<String> {
    let organizations = list_organizations(client)?;
    organizations
        .into_iter()
        .find(|org| org.organization_name == organization_name)
        .map(|org| org.organization_id)
        .ok_or_else(|| {
            anyhow!(
                "Organization '{}' not found for current user.",
                organization_name
            )
        })
}

pub fn project_scoped_path(project: &str, suffix: &str, organization: Option<&str>) -> String {
    let normalized_suffix = if suffix.is_empty() {
        String::new()
    } else if suffix.starts_with('/') {
        suffix.to_string()
    } else {
        format!("/{suffix}")
    };

    match organization {
        Some(org) => format!("/v1/{org}/{project}{normalized_suffix}"),
        None => format!("/v1/{project}{normalized_suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use super::project_scoped_path;

    #[test]
    fn project_scoped_path_builds_unscoped_route() {
        let path = project_scoped_path("analytics", "/query", None);
        assert_eq!(path, "/v1/analytics/query");
    }

    #[test]
    fn project_scoped_path_builds_scoped_route() {
        let path = project_scoped_path("analytics", "/query", Some("team_alpha"));
        assert_eq!(path, "/v1/team_alpha/analytics/query");
    }

    #[test]
    fn project_scoped_path_accepts_suffix_without_leading_slash() {
        let path = project_scoped_path("analytics", "tables", Some("team_alpha"));
        assert_eq!(path, "/v1/team_alpha/analytics/tables");
    }

    #[test]
    fn project_scoped_path_handles_empty_suffix() {
        let path = project_scoped_path("analytics", "", Some("team_alpha"));
        assert_eq!(path, "/v1/team_alpha/analytics");
    }
}
