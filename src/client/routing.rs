use super::{handle_response, ApiClient};
use anyhow::{Context, Result};
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
    organizations: RefCell<Option<(Instant, Vec<Organization>)>>,
    clusters: RefCell<HashMap<String, (Instant, Vec<Cluster>)>>,
}

impl ApiClient {
    pub(super) fn remember_routing_metadata(&self, path: &str, value: &serde_json::Value) {
        let Ok(url) = Url::parse(&format!("{}{}", self.base_url, path)) else {
            return;
        };
        if url.path() == "/v1/organizations" {
            if let Ok(response) = serde_json::from_value::<Organizations>(value.clone()) {
                *self.routing.organizations.borrow_mut() =
                    Some((Instant::now(), response.organizations));
            }
        } else if url.path() == "/v1/clusters" {
            if let Some((_, organization)) =
                url.query_pairs().find(|(key, _)| key == "organization")
            {
                if let Ok(response) = serde_json::from_value::<Clusters>(value.clone()) {
                    self.routing.clusters.borrow_mut().insert(
                        clusters_path(&organization),
                        (Instant::now(), response.clusters),
                    );
                }
            }
        }
    }

    pub(super) fn organization_id(&self, name: &str) -> Result<String> {
        let response: Organizations = self.get("/v1/organizations")?;
        response
            .organizations
            .into_iter()
            .find(|organization| organization.name == name)
            .map(|organization| organization.organization_id)
            .context("Organization not found.")
    }

    fn organization_name(&self, id: &str, deadline: Option<Instant>) -> Result<String> {
        if let Some((time, organizations)) = self.routing.organizations.borrow().as_ref() {
            if time.elapsed() < Duration::from_secs(30) {
                if let Some(organization) =
                    organizations.iter().find(|item| item.organization_id == id)
                {
                    return Ok(organization.name.clone());
                }
            }
        }
        let response: Organizations = handle_response(
            before_deadline(
                self.unrouted_request(Method::GET, "/v1/organizations"),
                deadline,
            )?
            .send()?,
        )?;
        let name = response
            .organizations
            .iter()
            .find(|item| item.organization_id == id)
            .map(|item| item.name.clone());
        *self.routing.organizations.borrow_mut() = Some((Instant::now(), response.organizations));
        name.context("Organization not found.")
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
        if method != Method::GET
            && (path.starts_with("/v1/organizations") || path.starts_with("/v1/clusters"))
        {
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
        let organization = match organization {
            OrganizationSelector::Id(id) => self.organization_name(&id, deadline)?,
            OrganizationSelector::Name(name) => name,
        };
        let collection = clusters_path(&organization);
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

enum OrganizationSelector {
    Id(String),
    Name(String),
}

fn clusters_path(organization: &str) -> String {
    format!(
        "/v1/clusters?organization={}",
        urlencoding::encode(organization)
    )
}

fn scope(url: &Url) -> Option<(OrganizationSelector, ClusterSelector)> {
    let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
    let organization = if let Some(id) = query.get("organization_id") {
        OrganizationSelector::Id(id.clone())
    } else {
        OrganizationSelector::Name(query.get("organization")?.clone())
    };
    let path_cluster = url
        .path()
        .strip_prefix("/v1/clusters/")
        .and_then(|path| path.split('/').next())
        .filter(|part| {
            !part.is_empty() && !matches!(*part, "sizes" | "metrics" | "verify-s3-access")
        });
    let cluster = if let Some(id) = path_cluster {
        ClusterSelector::Id(urlencoding::decode(id).ok()?.into_owned())
    } else if let Some(id) = query.get("cluster_id") {
        ClusterSelector::Id(id.clone())
    } else {
        ClusterSelector::Name(query.get("cluster")?.clone())
    };
    Some((organization, cluster))
}

// Match the backend/worker syntax; do not maintain a catalog of AWS regions.
pub(super) fn valid_routing(deployment: &str, region: &str) -> bool {
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
