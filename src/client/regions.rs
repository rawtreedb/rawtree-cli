use super::{routing, ApiClient};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Region {
    pub id: String,
    pub name: String,
    pub cloud_region: String,
    pub organization_id: Option<String>,
    pub deployment_id: Option<String>,
    pub enabled: bool,
}

#[derive(Deserialize, Serialize)]
pub struct Regions {
    pub regions: Vec<Region>,
}

impl ApiClient {
    pub fn list_regions(&self, organization: &str) -> Result<Regions> {
        let id = self.organization_id(organization)?;
        self.get(&format!(
            "/v1/organizations/{}/regions",
            urlencoding::encode(&id)
        ))
    }

    /// Scope creation and size discovery to the same validated region, without mutating this client.
    pub fn for_region(&self, organization: Option<&str>, region_id: Option<&str>) -> Result<Self> {
        let deployment = match region_id {
            None => None,
            Some(id) => {
                anyhow::ensure!(
                    !self
                        .token
                        .as_deref()
                        .is_some_and(|token| token.starts_with("rt_")),
                    "Region selection requires a user credential. Run `rtree login`."
                );
                let organization = organization
                    .context("Choose an organization with --org when selecting a region.")?;
                let region = self
                    .list_regions(organization)?
                    .regions
                    .into_iter()
                    .find(|region| region.id == id)
                    .context("Region not found in this organization.")?;
                anyhow::ensure!(region.enabled, "The selected region is disabled.");
                match region.deployment_id {
                    Some(deployment) if routing::valid_routing(&deployment, &region.cloud_region) => Some((deployment, region.cloud_region)),
                    None if region.id == "0c0f0a56-7f03-4d39-8e5d-000000000001" && region.organization_id.is_none() && region.cloud_region == "us-east-1" => None,
                    _ => anyhow::bail!("The selected region is not ready for cluster creation. Choose another region."),
                }
            }
        };
        Ok(Self {
            base_url: self.base_url.clone(),
            token: self.token.clone(),
            client: self.client.clone(),
            routing: routing::RoutingCache::default(),
            deployment,
        })
    }
}
