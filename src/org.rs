use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::client::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationItem {
    #[serde(default, skip_serializing)]
    pub organization_id: String,
    pub name: String,
    pub role: Option<String>,
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
        .map(|org| org.name)
}

pub fn database_path(database: &str, suffix: &str) -> String {
    let normalized_suffix = if suffix.is_empty() {
        String::new()
    } else if suffix.starts_with('/') {
        suffix.to_string()
    } else {
        format!("/{suffix}")
    };

    let sep = if normalized_suffix.contains('?') {
        '&'
    } else {
        '?'
    };
    format!(
        "/v1{normalized_suffix}{sep}database={}",
        urlencoding::encode(database)
    )
}

#[cfg(test)]
mod tests {
    use super::database_path;

    #[test]
    fn database_path_adds_database_query_param() {
        assert_eq!(
            database_path("analytics", "/query"),
            "/v1/query?database=analytics"
        );
    }

    #[test]
    fn database_path_accepts_suffix_without_leading_slash() {
        assert_eq!(
            database_path("analytics", "tables"),
            "/v1/tables?database=analytics"
        );
    }

    #[test]
    fn database_path_appends_to_existing_query() {
        assert_eq!(
            database_path("analytics db", "/tables/events?url=x"),
            "/v1/tables/events?url=x&database=analytics%20db"
        );
    }
}

#[derive(Deserialize)]
struct Organization {
    organization_id: String,
    name: String,
}

#[derive(Deserialize)]
struct OrganizationsResponse {
    organizations: Vec<Organization>,
}

pub fn organization_path(client: &ApiClient, organization: Option<&str>) -> Result<String> {
    if let Some(id) = organization.and_then(|name| client.saved_organization_id(name)) {
        return Ok(format!("/v1/organizations/{}", urlencoding::encode(&id)));
    }
    let response: OrganizationsResponse = client.get("/v1/organizations")?;
    let selected = match organization {
        Some(name) => response
            .organizations
            .into_iter()
            .find(|item| item.name == name)
            .with_context(|| {
                format!("Organization '{name}' was not found. Run `rtree organization list`.")
            })?,
        None => response.organizations.into_iter().next().context(
            "No organization available. Run `rtree organization list` or select one with --org.",
        )?,
    };
    Ok(format!(
        "/v1/organizations/{}",
        urlencoding::encode(&selected.organization_id)
    ))
}
