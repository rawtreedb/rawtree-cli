use super::{
    handle_response,
    regions::{Region, Regions},
    routing::{before_deadline, valid_routing},
    ApiClient,
};
use anyhow::{Context, Result};
use reqwest::{blocking::RequestBuilder, Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const MAX_CATALOG_WORKERS: usize = 8;
const REGIONAL_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Deserialize, serde::Serialize)]
pub(super) struct UnavailableRegion {
    pub id: String,
    pub name: String,
}

struct DeploymentTarget {
    routing: Option<(String, String)>,
    regions: Vec<UnavailableRegion>,
}

struct OrganizationCatalog {
    name: String,
    regions: HashMap<String, Region>,
    deployments: Vec<DeploymentTarget>,
}

// Build requests on the caller thread: the routing cache is intentionally not shared.
// Bound native threads, and include queueing and response bodies in the lookup deadline.
fn load_catalogs(requests: Vec<RequestBuilder>, deadline: Instant) -> Result<Vec<Result<Value>>> {
    let worker_count = requests.len().min(MAX_CATALOG_WORKERS);
    let requests = Mutex::new(requests.into_iter().enumerate());
    thread::scope(|scope| {
        let workers: Vec<_> = (0..worker_count)
            .map(|_| {
                thread::Builder::new()
                    .spawn_scoped(scope, || {
                        let mut responses = Vec::new();
                        loop {
                            let next = requests
                                .lock()
                                .map_err(|_| anyhow::anyhow!("Regional discovery queue failed"))?
                                .next();
                            let Some((index, request)) = next else {
                                break;
                            };
                            let response = before_deadline(request, Some(deadline))
                                .and_then(|request| Ok(request.send()?));
                            let result = match response {
                                Ok(response) => {
                                    let status = response.status();
                                    let result = handle_response(response);
                                    if matches!(
                                        status,
                                        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                                    ) {
                                        return Err(result.err().unwrap_or_else(|| {
                                            anyhow::anyhow!("Access denied ({status})")
                                        }));
                                    }
                                    result
                                }
                                Err(error) => Err(error),
                            };
                            responses.push((index, result));
                        }
                        Ok(responses)
                    })
                    .context("Failed to start regional discovery worker")
            })
            .collect::<Result<Vec<_>>>()?;
        let mut responses = Vec::new();
        for worker in workers {
            responses.extend(
                worker
                    .join()
                    .map_err(|_| anyhow::anyhow!("Regional discovery worker failed"))??,
            );
        }
        responses.sort_by_key(|(index, _)| *index);
        Ok(responses
            .into_iter()
            .map(|(_, response)| response)
            .collect())
    })
}

// Metadata identifies placement; only the owning catalog supplies authoritative fields.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CatalogAuthority {
    Fallback,
    RegionMetadata,
    OwningDeployment,
}

#[derive(Deserialize)]
struct Organization {
    organization_id: String,
    name: String,
}

#[derive(Deserialize)]
struct Organizations {
    organizations: Vec<Organization>,
}

impl ApiClient {
    pub(super) fn has_user_credential(&self) -> bool {
        self.token
            .as_deref()
            .is_some_and(|token| !token.starts_with("rt_"))
    }

    /// Discover enabled deployments concurrently, retaining explicit partial-catalog metadata.
    pub(super) fn discover_clusters(
        &self,
        organization: Option<&str>,
        deadline: Option<Instant>,
    ) -> Result<Value> {
        let organizations: Organizations = handle_response(
            before_deadline(
                self.unrouted_request(Method::GET, "/v1/organizations"),
                deadline,
            )?
            .send()?,
        )?;
        let selected: Vec<_> = organizations
            .organizations
            .into_iter()
            .filter(|item| organization.is_none_or(|name| item.name == name))
            .collect();
        anyhow::ensure!(
            organization.is_none() || !selected.is_empty(),
            "Organization not found."
        );
        let mut catalogs = Vec::new();
        for owner in selected {
            let regions: Regions = handle_response(
                before_deadline(
                    self.unrouted_request(
                        Method::GET,
                        &format!(
                            "/v1/organizations/{}/regions",
                            urlencoding::encode(&owner.organization_id)
                        ),
                    ),
                    deadline,
                )?
                .send()?,
            )?;
            let mut deployments: Vec<DeploymentTarget> = Vec::new();
            for region in regions.regions.iter().filter(|region| region.enabled) {
                let pair = region
                    .deployment_id
                    .as_ref()
                    .filter(|id| valid_routing(id, &region.cloud_region))
                    .map(|id| (id.clone(), region.cloud_region.clone()));
                let unavailable = UnavailableRegion {
                    id: region.id.clone(),
                    name: region.name.clone(),
                };
                if let Some(target) = deployments.iter_mut().find(|target| target.routing == pair) {
                    target.regions.push(unavailable);
                } else {
                    deployments.push(DeploymentTarget {
                        routing: pair,
                        regions: vec![unavailable],
                    });
                }
            }
            catalogs.push(OrganizationCatalog {
                name: owner.name,
                regions: regions
                    .regions
                    .into_iter()
                    .map(|region| (region.id.clone(), region))
                    .collect(),
                deployments,
            });
        }
        // All organizations share one concurrent regional lookup window.
        let targets: Vec<_> = catalogs
            .iter()
            .flat_map(|catalog| {
                catalog
                    .deployments
                    .iter()
                    .map(move |target| (catalog, target))
            })
            .collect();
        let mut result = json!({"clusters": []});
        let mut clusters = Vec::new();
        let mut positions = HashMap::new();
        let mut unavailable_regions = Vec::new();
        let lookup_deadline = Instant::now() + REGIONAL_LOOKUP_TIMEOUT;
        let lookup_deadline =
            deadline.map_or(lookup_deadline, |deadline| deadline.min(lookup_deadline));
        let requests = targets
            .iter()
            .map(|(catalog, target)| {
                self.deployment_request(
                    Method::GET,
                    &format!(
                        "/v1/clusters?organization={}",
                        urlencoding::encode(&catalog.name)
                    ),
                    target.routing.as_ref(),
                )
            })
            .collect();
        let responses = load_catalogs(requests, lookup_deadline)?;
        if !responses.is_empty() && responses.iter().all(Result::is_err) {
            return Err(responses
                .into_iter()
                .find_map(Result::err)
                .context("Missing regional failure")?);
        }
        for ((catalog, target), response) in targets.into_iter().zip(responses) {
            let response = match response {
                Ok(response) => response,
                Err(_) => {
                    unavailable_regions.extend(target.regions.iter().cloned());
                    continue;
                }
            };
            let deployment = &target.routing;
            let items = response
                .get("clusters")
                .and_then(Value::as_array)
                .context("Invalid cluster catalog response")?;
            if organization.is_some() {
                result = response.clone();
            }
            for item in items {
                let mut cluster = item.clone();
                let id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .context("Cluster is missing its ID")?;
                let region_id = item
                    .get("region_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty());
                let mut authority = CatalogAuthority::Fallback;
                if let Some(region_id) = region_id {
                    let region = catalog.regions.get(region_id).with_context(|| {
                        format!("Region '{region_id}' for cluster '{id}' was not found.")
                    })?;
                    let owns_cluster = match deployment.as_ref() {
                        Some((id, cloud_region)) => {
                            region.deployment_id.as_ref() == Some(id)
                                && region.cloud_region == *cloud_region
                        }
                        None => region.deployment_id.is_none(),
                    };
                    authority = if owns_cluster {
                        CatalogAuthority::OwningDeployment
                    } else {
                        CatalogAuthority::RegionMetadata
                    };
                    cluster["deployment_id"] = json!(region.deployment_id);
                    cluster["cloud_region"] = json!(region.cloud_region);
                }
                match positions.get_mut(id) {
                    Some((index, previous)) if authority > *previous => {
                        clusters[*index] = cluster;
                        *previous = authority;
                    }
                    Some(_) => {}
                    None => {
                        positions.insert(id.to_owned(), (clusters.len(), authority));
                        clusters.push(cluster);
                    }
                }
            }
        }
        result["clusters"] = json!(clusters);
        if !unavailable_regions.is_empty() {
            result["unavailable_regions"] = json!(unavailable_regions);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn queued_catalogs_share_the_deadline_and_bound_active_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut sockets = Vec::new();
            while stopped.try_recv().is_err() {
                match listener.accept() {
                    Ok((socket, _)) => sockets.push(socket),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            }
            sockets.len()
        });
        let client = reqwest::blocking::Client::new();
        let requests = (0..24).map(|_| client.get(&url)).collect();
        let started = Instant::now();
        let responses = load_catalogs(requests, started + Duration::from_millis(300)).unwrap();
        stop.send(()).unwrap();
        assert_eq!(server.join().unwrap(), 8);
        assert_eq!(responses.len(), 24);
        assert!(responses.iter().all(Result::is_err));
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "queued requests must not receive a fresh timeout"
        );
    }
}
