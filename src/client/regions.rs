use super::{handle_response, routing, ApiClient, Scope, Selector};
use anyhow::{Context, Result};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

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
    pub fn list_regions(&self, organization: &Selector) -> Result<Regions> {
        let id = self.organization_id(organization)?;
        self.regions_for_organization(&id, None)
    }

    pub(super) fn regions_for_organization(
        &self,
        organization_id: &str,
        deadline: Option<Instant>,
    ) -> Result<Regions> {
        let request = self
            .unrouted_request(Method::GET, "/v1/regions")?
            .query(&[("organization_id", organization_id)]);
        handle_response(routing::before_deadline(request, deadline)?.send()?)
    }

    /// An unscoped client for pre-creation size discovery in a region of this
    /// client's organization.
    pub fn for_region(&self, region_id: &str) -> Result<Self> {
        anyhow::ensure!(
            !self
                .token()?
                .as_deref()
                .is_some_and(|token| token.starts_with("rt_")),
            "Region selection requires a user credential. Run `rtree login`."
        );
        let organization = self
            .scope
            .organization
            .as_ref()
            .context("Choose an organization with --org when selecting a region.")?;
        let region = self
            .list_regions(organization)?
            .regions
            .into_iter()
            .find(|region| region.id == region_id)
            .context("Region not found in this organization.")?;
        anyhow::ensure!(region.enabled, "The selected region is disabled.");
        let deployment = region
            .deployment_id
            .filter(|id| routing::valid_routing(id, &region.cloud_region))
            .context(
                "The selected region is not ready for cluster creation. Choose another region.",
            )?;
        let mut client = self.fork()?;
        client.routing =
            Arc::new(routing::RoutingCache::default().with_organizations_of(&self.routing));
        client.deployment = Some((deployment, region.cloud_region));
        client.scope = Scope::default();
        Ok(client)
    }
}
