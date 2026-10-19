use super::{
    handle_response,
    regions::Regions,
    routing::{before_deadline, valid_routing},
    ApiClient,
};
use anyhow::{Context, Result};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, time::Instant};

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

    /// Discover each enabled deployment before publishing or caching a complete list.
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
        let mut result = json!({"clusters": []});
        let mut clusters = Vec::new();
        let mut positions = HashMap::new();
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
            let mut deployments = Vec::new();
            for region in regions.regions.iter().filter(|region| region.enabled) {
                let pair = region
                    .deployment_id
                    .as_ref()
                    .filter(|id| valid_routing(id, &region.cloud_region))
                    .map(|id| (id.clone(), region.cloud_region.clone()));
                if !deployments.contains(&pair) {
                    deployments.push(pair);
                }
            }
            let regions: HashMap<_, _> = regions
                .regions
                .iter()
                .map(|region| (region.id.as_str(), region))
                .collect();
            for deployment in deployments {
                let response: Value = handle_response(
                    before_deadline(
                        self.deployment_request(
                            Method::GET,
                            &format!(
                                "/v1/clusters?organization={}",
                                urlencoding::encode(&owner.name)
                            ),
                            deployment.as_ref(),
                        ),
                        deadline,
                    )?
                    .send()?,
                )?;
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
                    if let Some(region_id) = region_id {
                        let region = regions.get(region_id).with_context(|| {
                            format!("Region '{region_id}' for cluster '{id}' was not found.")
                        })?;
                        cluster["deployment_id"] = json!(region.deployment_id);
                        cluster["cloud_region"] = json!(region.cloud_region);
                    }
                    // A gateway fallback must never replace an authoritative regional record.
                    match positions.get(id) {
                        Some(&index) if region_id.is_some() => clusters[index] = cluster,
                        Some(_) => {}
                        None => {
                            positions.insert(id.to_owned(), clusters.len());
                            clusters.push(cluster);
                        }
                    }
                }
            }
        }
        result["clusters"] = json!(clusters);
        Ok(result)
    }
}
