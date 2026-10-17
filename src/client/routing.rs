use super::{handle_response, ApiClient};
use anyhow::{bail, Context, Result};
use reqwest::{blocking::RequestBuilder, Method, Url};
use serde::Deserialize;
use std::{
    cell::RefCell,
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Clone, Deserialize)]
struct Organization {
    organization_id: String,
    name: String,
}
#[derive(Deserialize)]
struct Organizations {
    organizations: Vec<Organization>,
}
#[derive(Clone, Deserialize)]
struct Cluster {
    id: String,
    name: String,
    #[serde(default)]
    deployment_id: Option<String>,
    #[serde(default)]
    cloud_region: Option<String>,
}
#[derive(Deserialize)]
struct Clusters {
    clusters: Vec<Cluster>,
}

#[derive(Default)]
pub(super) struct RoutingCache {
    organizations: RefCell<Option<Vec<Organization>>>,
    clusters: RefCell<HashMap<String, (Instant, Vec<Cluster>)>>,
}

impl ApiClient {
    pub(super) fn remember_routing_metadata(&self, path: &str, value: &serde_json::Value) {
        if path == "/v1/organizations" {
            if let Ok(response) = serde_json::from_value::<Organizations>(value.clone()) {
                *self.routing.organizations.borrow_mut() = Some(response.organizations);
            }
        } else if path.starts_with("/v1/organizations/") && path.ends_with("/clusters") {
            if let Ok(response) = serde_json::from_value::<Clusters>(value.clone()) {
                self.routing
                    .clusters
                    .borrow_mut()
                    .insert(path.to_owned(), (Instant::now(), response.clusters));
            }
        }
    }

    pub fn clusters_path(&self, organization: Option<&str>) -> Result<String> {
        self.clusters_path_before(organization, None)
    }

    fn clusters_path_before(
        &self,
        organization: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<String> {
        if organization.is_none()
            && !self
                .token
                .as_deref()
                .is_some_and(|token| token.starts_with("rt_"))
        {
            bail!("No organization selected. Pass --org or select an organization first.");
        }
        // IDs need no discovery. Names remain supported by CLI flags/configuration.
        if let Some(id) = organization.filter(|id| is_uuid(id)) {
            return Ok(format!("/v1/organizations/{id}/clusters"));
        }
        if self.routing.organizations.borrow().is_none() {
            let response: Organizations = handle_response(
                before_deadline(
                    self.unrouted_request(Method::GET, "/v1/organizations"),
                    deadline,
                )?
                .send()?,
            )?;
            *self.routing.organizations.borrow_mut() = Some(response.organizations);
        }
        let organizations = self.routing.organizations.borrow();
        let matched = organizations.as_ref().and_then(|items| {
            items
                .iter()
                .find(|item| organization.is_none_or(|name| item.name == name))
        });
        let Some(matched) = matched else {
            bail!("Organization not found. Pass --org with an organization you can access.")
        };
        if !is_uuid(&matched.organization_id) {
            bail!("Invalid organization ID returned by the API.");
        }
        Ok(format!(
            "/v1/organizations/{}/clusters",
            matched.organization_id
        ))
    }

    pub(super) fn routed_request(&self, method: Method, path: &str) -> Result<RequestBuilder> {
        self.request_before(method, path, None)
    }

    pub(super) fn routed_request_with_timeout(
        &self,
        method: Method,
        path: &str,
        timeout: Duration,
    ) -> Result<RequestBuilder> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("invalid request timeout")?;
        self.request_before(method, path, Some(deadline))
    }

    fn request_before(
        &self,
        method: Method,
        path: &str,
        deadline: Option<Instant>,
    ) -> Result<RequestBuilder> {
        let pair = self.deployment_for(path, deadline)?;
        if method != Method::GET && path.starts_with("/v1/organizations") {
            self.routing.clusters.borrow_mut().clear();
            *self.routing.organizations.borrow_mut() = None;
        }
        let mut request = self.unrouted_request(method, path);
        if let Some((deployment, region)) = pair {
            request = request
                .header("X-RawTree-Deployment-Id", deployment)
                .header("X-RawTree-Region", region);
        }
        before_deadline(request, deadline)
    }

    fn deployment_for(
        &self,
        path: &str,
        deadline: Option<Instant>,
    ) -> Result<Option<(String, String)>> {
        if self
            .token
            .as_deref()
            .is_none_or(|token| token.starts_with("rt_"))
        {
            return Ok(None);
        }
        let url = Url::parse(&format!("{}{}", self.base_url, path)).context("invalid API URL")?;
        let Some((organization, cluster)) = scope(&url) else {
            return Ok(None);
        };
        let collection = self.clusters_path_before(Some(&organization), deadline)?;
        let cached = self
            .routing
            .clusters
            .borrow()
            .get(&collection)
            .filter(|(time, _)| time.elapsed() < Duration::from_secs(30))
            .map(|(_, clusters)| clusters.clone());
        let clusters = match cached {
            Some(clusters) => clusters,
            None => {
                let response: Clusters = handle_response(
                    before_deadline(self.unrouted_request(Method::GET, &collection), deadline)?
                        .send()?,
                )?;
                self.routing
                    .clusters
                    .borrow_mut()
                    .insert(collection, (Instant::now(), response.clusters.clone()));
                response.clusters
            }
        };
        let target = clusters.iter().find(|item| match &cluster {
            ClusterSelector::Id(id) => item.id == *id,
            ClusterSelector::Name(name) => item.name == *name,
        });
        let pair = target.and_then(|item| {
            item.deployment_id
                .as_deref()
                .zip(item.cloud_region.as_deref())
        });
        Ok(pair
            .filter(|(deployment, region)| valid_routing(deployment, region))
            .map(|(deployment, region)| (deployment.to_owned(), region.to_owned())))
    }
}

fn before_deadline(request: RequestBuilder, deadline: Option<Instant>) -> Result<RequestBuilder> {
    match deadline {
        Some(deadline) => {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .context("request timed out during routing discovery")?;
            Ok(request.timeout(remaining))
        }
        None => Ok(request),
    }
}

enum ClusterSelector {
    Id(String),
    Name(String),
}

fn scope(url: &Url) -> Option<(String, ClusterSelector)> {
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    let parts: Vec<_> = url.path_segments()?.collect();
    let nested = parts.len() >= 4
        && parts[0] == "v1"
        && parts[1] == "organizations"
        && parts[3] == "clusters";
    let organization = if nested {
        urlencoding::decode(parts[2]).ok()?.into_owned()
    } else {
        query
            .get("organization_id")
            .or_else(|| query.get("organization"))?
            .clone()
    };
    let cluster =
        if nested && parts.len() >= 5 && !matches!(parts[4], "metrics" | "verify-s3-access") {
            ClusterSelector::Id(urlencoding::decode(parts[4]).ok()?.into_owned())
        } else if let Some(id) = query.get("cluster_id") {
            ClusterSelector::Id(id.clone())
        } else {
            ClusterSelector::Name(query.get("cluster")?.clone())
        };
    Some((organization, cluster))
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

// Match the backend/worker syntax; do not maintain a catalog of AWS regions.
fn valid_routing(deployment: &str, region: &str) -> bool {
    let mut parts = region.rsplit('-');
    let number = parts.next().unwrap_or_default();
    let names: Vec<_> = parts.collect();
    deployment.len() == 8
        && deployment
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && region.len() <= 63
        && !number.starts_with('0')
        && !number.is_empty()
        && number.bytes().all(|byte| byte.is_ascii_digit())
        && names.len() >= 2
        && names
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_lowercase()))
}
