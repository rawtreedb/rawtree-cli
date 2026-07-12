use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::client::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationItem {
    pub name: String,
    pub role: String,
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

pub fn append_scope_params(
    path: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
) -> String {
    let mut path = path.to_string();
    for (name, value) in [("organization", organization), ("cluster", cluster)] {
        if let Some(value) = value {
            path.push(if path.contains('?') { '&' } else { '?' });
            path.push_str(name);
            path.push('=');
            path.push_str(&urlencoding::encode(value));
        }
    }
    path
}

pub fn databases_collection_path(organization: Option<&str>, cluster: Option<&str>) -> String {
    append_scope_params("/v1/databases", organization, cluster)
}

pub fn database_scoped_path(
    database: &str,
    suffix: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
) -> String {
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
    let mut path = format!(
        "/v1{normalized_suffix}{sep}database={}",
        urlencoding::encode(database)
    );
    if let Some(org) = organization {
        path.push_str("&organization=");
        path.push_str(&urlencoding::encode(org));
    }
    if let Some(cluster) = cluster {
        path.push_str("&cluster=");
        path.push_str(&urlencoding::encode(cluster));
    }
    path
}

#[cfg(test)]
mod tests {
    use super::{database_scoped_path, databases_collection_path};

    #[test]
    fn databases_collection_path_uses_organization_filter() {
        assert_eq!(
            databases_collection_path(Some("team alpha"), None),
            "/v1/databases?organization=team%20alpha"
        );
        assert_eq!(databases_collection_path(None, None), "/v1/databases");
        assert_eq!(
            databases_collection_path(Some("team alpha"), Some("prod eu")),
            "/v1/databases?organization=team%20alpha&cluster=prod%20eu"
        );
    }

    #[test]
    fn database_scoped_path_adds_database_query_param() {
        let path = database_scoped_path("analytics", "/query", None, None);
        assert_eq!(path, "/v1/query?database=analytics");
    }

    #[test]
    fn database_scoped_path_adds_organization_query_param() {
        let path = database_scoped_path("analytics", "/query", Some("team_alpha"), None);
        assert_eq!(path, "/v1/query?database=analytics&organization=team_alpha");
    }

    #[test]
    fn database_scoped_path_adds_cluster_query_param() {
        let path = database_scoped_path(
            "analytics",
            "/query",
            Some("team_alpha"),
            Some("production"),
        );
        assert_eq!(
            path,
            "/v1/query?database=analytics&organization=team_alpha&cluster=production"
        );
    }

    #[test]
    fn database_scoped_path_accepts_suffix_without_leading_slash() {
        let path = database_scoped_path("analytics", "tables", Some("team_alpha"), None);
        assert_eq!(
            path,
            "/v1/tables?database=analytics&organization=team_alpha"
        );
    }

    #[test]
    fn database_scoped_path_appends_to_existing_query() {
        let path =
            database_scoped_path("analytics db", "/tables/events?url=x", Some("team a"), None);
        assert_eq!(
            path,
            "/v1/tables/events?url=x&database=analytics%20db&organization=team%20a"
        );
    }
}
