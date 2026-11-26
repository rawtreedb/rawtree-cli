use super::{
    discovery::UnavailableRegion, handle_response, ApiClient, Selector, CLUSTER_ID_HEADER,
    ORGANIZATION_ID_HEADER,
};
use anyhow::{Context, Result};
use reqwest::{blocking::RequestBuilder, Method, Url};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Clone, Deserialize)]
pub(super) struct Organization {
    pub(super) organization_id: String,
    pub(super) name: String,
}
#[derive(Deserialize)]
pub(super) struct Organizations {
    pub(super) organizations: Vec<Organization>,
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
#[derive(Clone, Deserialize)]
struct Clusters {
    clusters: Vec<Cluster>,
    #[serde(default)]
    unavailable_regions: Vec<UnavailableRegion>,
}

#[derive(Default)]
pub(super) struct RoutingCache {
    organizations: Mutex<Option<(Instant, Vec<Organization>)>>,
    clusters: Mutex<HashMap<String, (Instant, Clusters)>>,
    bound_cluster: Mutex<Option<Cluster>>,
}

impl RoutingCache {
    pub(super) fn store_organizations(&self, organizations: Vec<Organization>) {
        *self.organizations.lock().unwrap() = Some((Instant::now(), organizations));
    }

    pub(super) fn with_organizations_of(&self, other: &RoutingCache) -> RoutingCache {
        let cache = RoutingCache::default();
        *cache.organizations.lock().unwrap() = other.organizations.lock().unwrap().clone();
        cache
    }
}

struct Selectors {
    organization: Option<Selector>,
    cluster: Option<Selector>,
    /// The cluster is addressed by the URL path, so no cluster header is sent.
    cluster_in_path: bool,
}

#[derive(Default)]
pub(super) struct ResolvedScope {
    organization_id: Option<String>,
    cluster_id: Option<String>,
    deployment: Option<(String, String)>,
}

pub(super) fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

pub(super) fn with_scope_headers(
    request: RequestBuilder,
    organization_id: Option<&str>,
    cluster_id: Option<&str>,
) -> RequestBuilder {
    let request = match organization_id {
        Some(id) => request.header(ORGANIZATION_ID_HEADER, id),
        None => request,
    };
    match cluster_id {
        Some(id) => request.header(CLUSTER_ID_HEADER, id),
        None => request,
    }
}

impl ApiClient {
    pub(super) fn remember_routing_metadata(&self, path: &str, value: &serde_json::Value) {
        if path == "/v1/organizations" {
            if let Ok(response) = serde_json::from_value::<Organizations>(value.clone()) {
                self.routing.store_organizations(response.organizations);
            }
        } else if path == "/v1/clusters" {
            let selected = match &self.scope.organization {
                Some(Selector::Id(id)) => Some(id.clone()),
                Some(Selector::Name(name)) => self
                    .cached_organization(name)
                    .map(|item| item.organization_id),
                None => None,
            };
            if let Some(organization_id) = selected {
                if let Ok(response) = serde_json::from_value::<Clusters>(value.clone()) {
                    self.routing
                        .clusters
                        .lock()
                        .unwrap()
                        .insert(organization_id, (Instant::now(), response));
                }
            }
        }
    }

    fn cached_organization(&self, name: &str) -> Option<Organization> {
        let cache = self.routing.organizations.lock().unwrap();
        let (time, organizations) = cache.as_ref()?;
        if time.elapsed() >= Duration::from_secs(30) {
            return None;
        }
        organizations.iter().find(|item| item.name == name).cloned()
    }

    pub fn organization_id(&self, organization: &Selector) -> Result<String> {
        self.find_organization(organization, None)
            .map(|organization| organization.organization_id)
    }

    pub(super) fn find_organization(
        &self,
        selector: &Selector,
        deadline: Option<Instant>,
    ) -> Result<Organization> {
        match selector {
            Selector::Id(id) => Ok(Organization {
                organization_id: id.clone(),
                name: id.clone(),
            }),
            Selector::Name(name) => self
                .organizations(deadline)?
                .into_iter()
                .find(|item| item.name == *name)
                .context("Organization not found."),
        }
    }

    pub(super) fn organizations(&self, deadline: Option<Instant>) -> Result<Vec<Organization>> {
        if let Some((time, organizations)) = self.routing.organizations.lock().unwrap().as_ref() {
            if time.elapsed() < Duration::from_secs(30) {
                return Ok(organizations.clone());
            }
        }
        let response: Organizations = handle_response(
            before_deadline(
                self.unrouted_request(Method::GET, "/v1/organizations")?,
                deadline,
            )?
            .send()?,
        )?;
        self.routing
            .store_organizations(response.organizations.clone());
        Ok(response.organizations)
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
        let scope = self.resolve_scope(path, deadline)?;
        if method != Method::GET
            && (path.starts_with("/v1/organizations") || path.starts_with("/v1/clusters"))
        {
            self.routing.clusters.lock().unwrap().clear();
            *self.routing.organizations.lock().unwrap() = None;
        }
        let request = self.deployment_request(
            method,
            path,
            scope.deployment.as_ref().or(self.deployment.as_ref()),
        )?;
        let request = with_scope_headers(
            request,
            scope.organization_id.as_deref(),
            scope.cluster_id.as_deref(),
        );
        before_deadline(request, deadline)
    }

    fn resolve_scope(&self, path: &str, deadline: Option<Instant>) -> Result<ResolvedScope> {
        let selectors = self.selectors(path);
        if selectors.organization.is_none() && selectors.cluster.is_none() {
            return Ok(ResolvedScope::default());
        }
        if self.has_user_credential()? {
            self.user_scope(selectors, deadline)
        } else {
            self.bound_scope(selectors, deadline)
        }
    }

    /// API keys are bound to one organization and cluster. Names are matched
    /// against the key's own organization (the only entry `GET /v1/organizations`
    /// returns for a key) and bound cluster.
    fn bound_scope(
        &self,
        selectors: Selectors,
        deadline: Option<Instant>,
    ) -> Result<ResolvedScope> {
        let organization_id = match selectors.organization {
            Some(Selector::Id(id)) => Some(id),
            Some(Selector::Name(name)) if self.token()?.is_some() => {
                let organization = self
                    .organizations(deadline)?
                    .into_iter()
                    .find(|item| item.name == name)
                    .with_context(|| {
                        format!("Organization '{name}' is not the API key's organization.")
                    })?;
                Some(organization.organization_id)
            }
            Some(Selector::Name(_)) | None => None,
        };
        let mut scope = ResolvedScope {
            organization_id,
            ..ResolvedScope::default()
        };
        if selectors.cluster_in_path {
            return Ok(scope);
        }
        scope.cluster_id = match selectors.cluster {
            Some(Selector::Id(id)) => Some(id),
            Some(Selector::Name(_)) if self.token()?.is_none() => None,
            Some(Selector::Name(name)) => {
                let bound = self.bound_cluster(deadline)?;
                anyhow::ensure!(
                    bound.name == name,
                    "The API key is not bound to cluster '{name}'."
                );
                Some(bound.id)
            }
            None => None,
        };
        Ok(scope)
    }

    fn bound_cluster(&self, deadline: Option<Instant>) -> Result<Cluster> {
        if let Some(cluster) = self.routing.bound_cluster.lock().unwrap().as_ref() {
            return Ok(cluster.clone());
        }
        let bound: Clusters = handle_response(
            before_deadline(
                self.unrouted_request(Method::GET, "/v1/clusters")?,
                deadline,
            )?
            .send()?,
        )?;
        let cluster = bound
            .clusters
            .into_iter()
            .next()
            .context("The API key is not bound to a cluster.")?;
        *self.routing.bound_cluster.lock().unwrap() = Some(cluster.clone());
        Ok(cluster)
    }

    fn user_scope(&self, selectors: Selectors, deadline: Option<Instant>) -> Result<ResolvedScope> {
        let organization = selectors
            .organization
            .as_ref()
            .map(|selector| self.find_organization(selector, deadline))
            .transpose()?;
        let mut scope = ResolvedScope {
            organization_id: organization
                .as_ref()
                .map(|organization| organization.organization_id.clone()),
            ..ResolvedScope::default()
        };
        let Some(cluster) = selectors.cluster else {
            return Ok(scope);
        };
        let Some(organization) = organization else {
            match cluster {
                Selector::Id(id) => scope.cluster_id = Some(id),
                Selector::Name(name) => anyhow::bail!(
                    "Choose an organization with --org to select cluster '{name}' by name."
                ),
            }
            return Ok(scope);
        };
        let clusters = self.organization_clusters(&organization, deadline)?;
        let target = clusters.clusters.iter().find(|item| match &cluster {
            Selector::Id(id) => item.id == *id,
            Selector::Name(name) => item.name == *name,
        });
        if target.is_none() && !clusters.unavailable_regions.is_empty() {
            // Keep known routes usable, but retry missing routes after a region recovers.
            self.routing
                .clusters
                .lock()
                .unwrap()
                .remove(&organization.organization_id);
            anyhow::bail!("Some regions are unavailable. Cannot resolve the selected cluster; retry when regional discovery recovers.");
        }
        if !selectors.cluster_in_path {
            scope.cluster_id = match (&cluster, target) {
                (_, Some(target)) => Some(target.id.clone()),
                (Selector::Id(id), None) => Some(id.clone()),
                (Selector::Name(name), None) => {
                    anyhow::bail!(
                        "Cluster '{name}' not found in organization '{}'.",
                        organization.name
                    )
                }
            };
        }
        let pair = target.and_then(|item| {
            item.deployment_id
                .as_deref()
                .zip(item.cloud_region.as_deref())
        });
        scope.deployment = pair
            .filter(|(deployment, region)| valid_routing(deployment, region))
            .map(|(deployment, region)| (deployment.to_owned(), region.to_owned()));
        Ok(scope)
    }

    fn organization_clusters(
        &self,
        organization: &Organization,
        deadline: Option<Instant>,
    ) -> Result<Clusters> {
        let cached = self
            .routing
            .clusters
            .lock()
            .unwrap()
            .get(&organization.organization_id)
            .filter(|(time, _)| time.elapsed() < Duration::from_secs(30))
            .map(|(_, clusters)| clusters.clone());
        if let Some(clusters) = cached {
            return Ok(clusters);
        }
        let selector = Selector::Id(organization.organization_id.clone());
        let response: Clusters =
            serde_json::from_value(self.discover_clusters(Some(&selector), deadline)?)?;
        self.routing.clusters.lock().unwrap().insert(
            organization.organization_id.clone(),
            (Instant::now(), response.clone()),
        );
        Ok(response)
    }
}

pub(super) fn before_deadline(
    request: RequestBuilder,
    deadline: Option<Instant>,
) -> Result<RequestBuilder> {
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

impl ApiClient {
    fn selectors(&self, path: &str) -> Selectors {
        let path_cluster = path
            .split('?')
            .next()
            .and_then(|path| path.strip_prefix("/v1/clusters/"))
            .and_then(|path| path.split('/').next())
            .filter(|part| {
                !part.is_empty() && !matches!(*part, "sizes" | "metrics" | "verify-s3-access")
            })
            .and_then(|id| urlencoding::decode(id).ok())
            .map(|id| Selector::Id(id.into_owned()));
        Selectors {
            organization: self.scope.organization.clone(),
            cluster_in_path: path_cluster.is_some(),
            cluster: path_cluster.or_else(|| self.scope.cluster.clone()),
        }
    }

    /// The organization and cluster IDs this client's scope resolves to, for
    /// saving in the config.
    pub fn resolve_scope_ids(&self) -> Result<(Option<String>, Option<String>)> {
        let resolved = self.resolve_scope("/v1/databases", None)?;
        Ok((resolved.organization_id, resolved.cluster_id))
    }
}

// Match the backend/worker syntax; do not maintain a catalog of AWS regions.
pub(super) fn valid_routing(deployment: &str, region: &str) -> bool {
    let mut parts = region.rsplit('-');
    let number = parts.next().unwrap_or_default();
    let names: Vec<_> = parts.collect();
    matches!(deployment.len(), 8 | 32)
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

/// Only public RawTree API origins use deployment hosts; custom endpoints keep their URL.
pub(super) fn deployment_url(url: &str, pair: Option<&(String, String)>) -> String {
    let Some((deployment, region)) =
        pair.filter(|(deployment, region)| valid_routing(deployment, region))
    else {
        return url.to_owned();
    };
    let Ok(mut target) = Url::parse(url) else {
        return url.to_owned();
    };
    if !matches!(
        target.origin().ascii_serialization().as_str(),
        "https://api.rawtree.com" | "https://api.rawtree.dev"
    ) {
        return url.to_owned();
    }
    let host = target.host_str().unwrap_or_default().replacen(
        "api.",
        &format!("api.{deployment}.{region}.aws."),
        1,
    );
    if target.set_host(Some(&host)).is_err() {
        return url.to_owned();
    }
    target.to_string()
}
