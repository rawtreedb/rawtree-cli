use std::io::{self, IsTerminal, Write};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::ApiClient;
use crate::config;
use crate::org;
use crate::output;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterStatus {
    pub phase: String,
    pub ready: bool,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterResources {
    pub shards: u32,
    pub replicas: u32,
    pub cpu_cores_per_replica: Option<f64>,
    pub memory_bytes_per_replica: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterItem {
    pub id: String,
    pub name: String,
    pub shared: bool,
    pub created_at: String,
    pub status: ClusterStatus,
    pub resources: Option<ClusterResources>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClusterOrganizationRef {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListClustersResponse {
    pub organization: ClusterOrganizationRef,
    pub clusters: Vec<ClusterItem>,
}

#[derive(Deserialize)]
struct DeleteClusterResponse {
    deleted: bool,
}

fn clusters_path(organization: Option<&str>) -> String {
    org::append_scope_params("/v1/clusters", organization, None)
}

fn cluster_path(cluster_id: &str, organization: Option<&str>) -> String {
    org::append_scope_params(
        &format!("/v1/clusters/{}", urlencoding::encode(cluster_id)),
        organization,
        None,
    )
}

fn load_clusters(client: &ApiClient, organization: Option<&str>) -> Result<ListClustersResponse> {
    client.get(&clusters_path(organization))
}

fn resolve_cluster<'a>(clusters: &'a [ClusterItem], name_or_id: &str) -> Result<&'a ClusterItem> {
    clusters
        .iter()
        .find(|cluster| cluster.name == name_or_id || cluster.id == name_or_id)
        .ok_or_else(|| {
            output::coded_error(
                "cluster_not_found",
                format!("Cluster '{name_or_id}' not found."),
                4,
            )
        })
}

fn resolve_cluster_name<'a>(clusters: &'a [ClusterItem], name: &str) -> Result<&'a ClusterItem> {
    clusters
        .iter()
        .find(|cluster| cluster.name == name)
        .ok_or_else(|| {
            output::coded_error(
                "cluster_not_found",
                format!("Cluster '{name}' not found."),
                4,
            )
        })
}

fn format_cpu(value: Option<f64>) -> String {
    value
        .map(|value| {
            if value.fract() == 0.0 {
                format!("{value:.0}")
            } else {
                format!("{value:.1}")
            }
        })
        .unwrap_or_else(|| "-".to_string())
}

fn format_memory(value: Option<u64>) -> String {
    value
        .map(|bytes| format!("{:.0} GiB", bytes as f64 / 1024_f64.powi(3)))
        .unwrap_or_else(|| "-".to_string())
}

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let resp = load_clusters(client, organization)?;
    let cfg = config::load()?;
    let saved_cluster = cfg.default_cluster;
    let saved_organization = cfg.default_organization;

    output::print_result(&resp, json_mode, |resp| {
        if resp.clusters.is_empty() {
            println!(
                "No clusters found in organization '{}'.",
                resp.organization.name
            );
            return;
        }

        println!(
            "{:<20} {:<16} {:>8} {:>12} {:>16}  DEFAULT",
            "NAME", "STATUS", "REPLICAS", "CPU/REPLICA", "MEMORY/REPLICA"
        );
        for cluster in &resp.clusters {
            let resources = cluster.resources.as_ref();
            let replicas = resources
                .map(|resources| resources.replicas.to_string())
                .unwrap_or_else(|| "-".to_string());
            let cpu = format_cpu(resources.and_then(|resources| resources.cpu_cores_per_replica));
            let memory =
                format_memory(resources.and_then(|resources| resources.memory_bytes_per_replica));
            let default = if saved_organization.as_deref() == Some(resp.organization.name.as_str())
                && saved_cluster.as_deref() == Some(cluster.name.as_str())
            {
                "yes"
            } else {
                ""
            };
            println!(
                "{:<20} {:<16} {:>8} {:>12} {:>16}  {}",
                cluster.name, cluster.status.phase, replicas, cpu, memory, default
            );
        }
    });
    Ok(())
}

pub fn use_cluster(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let resp = load_clusters(client, organization)?;
    let cluster = resolve_cluster_name(&resp.clusters, name)?;
    let cluster_name = cluster.name.clone();
    let organization_name = resp.organization.name;

    let mut cfg = config::load()?;
    if cfg.default_organization.as_deref() != Some(organization_name.as_str())
        || cfg.default_cluster.as_deref() != Some(cluster_name.as_str())
    {
        cfg.default_database = None;
    }
    cfg.default_cluster = Some(cluster_name.clone());
    cfg.default_organization = Some(organization_name.clone());
    config::save(&cfg)?;

    output::print_result(
        &json!({
            "default_cluster": cluster_name,
            "default_organization": organization_name,
        }),
        json_mode,
        |_| {
            println!(
                "Default cluster set to '{}' in organization '{}'.",
                cluster_name, organization_name
            );
        },
    );
    Ok(())
}

pub fn create(
    client: &ApiClient,
    name: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let cluster: ClusterItem =
        client.post(&clusters_path(organization), &json!({ "name": name }))?;
    output::print_result(&cluster, json_mode, |cluster| {
        println!("Cluster '{}' created.", cluster.name);
        println!("Status: {}", cluster.status.phase);
    });
    Ok(())
}

fn confirm_delete(
    cluster: &ClusterItem,
    organization: &str,
    yes: bool,
    json_mode: bool,
) -> Result<()> {
    if yes {
        return Ok(());
    }
    if json_mode || !io::stdin().is_terminal() {
        return Err(output::coded_error(
            "confirmation_required",
            "Cluster deletion requires --yes in JSON or non-interactive mode.",
            2,
        ));
    }

    print!(
        "Delete cluster '{}' in organization '{}'? This deletes its databases and cluster-scoped API keys. [y/N] ",
        cluster.name, organization
    );
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        return Err(output::coded_error(
            "cancelled",
            "Cluster deletion cancelled.",
            2,
        ));
    }
    Ok(())
}

pub fn delete(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    yes: bool,
    json_mode: bool,
) -> Result<()> {
    let listed = load_clusters(client, organization)?;
    let cluster = resolve_cluster(&listed.clusters, name_or_id)?.clone();
    confirm_delete(&cluster, &listed.organization.name, yes, json_mode)?;

    let resp: DeleteClusterResponse =
        client.delete(&cluster_path(&cluster.id, Some(&listed.organization.name)))?;
    if resp.deleted {
        let mut cfg = config::load()?;
        if cfg.default_organization.as_deref() == Some(listed.organization.name.as_str())
            && cfg.default_cluster.as_deref() == Some(cluster.name.as_str())
        {
            cfg.default_cluster = None;
            cfg.default_database = None;
            config::save(&cfg)?;
        }
    }

    output::print_result(
        &json!({
            "deleted": resp.deleted,
            "cluster": cluster.name,
            "organization": listed.organization.name,
        }),
        json_mode,
        |_| {
            if resp.deleted {
                println!("Cluster '{}' deleted.", cluster.name);
            }
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        format_cpu, format_memory, resolve_cluster, resolve_cluster_name, ClusterItem,
        ClusterStatus,
    };

    fn cluster() -> ClusterItem {
        ClusterItem {
            id: "11111111-1111-1111-1111-111111111111".to_string(),
            name: "production".to_string(),
            shared: false,
            created_at: "2026-07-13T10:00:00Z".to_string(),
            status: ClusterStatus {
                phase: "ready".to_string(),
                ready: true,
                message: None,
            },
            resources: None,
        }
    }

    #[test]
    fn resolves_cluster_by_name_or_id() {
        let clusters = vec![cluster()];
        assert_eq!(
            resolve_cluster(&clusters, "production").unwrap().name,
            "production"
        );
        assert_eq!(
            resolve_cluster(&clusters, "11111111-1111-1111-1111-111111111111")
                .unwrap()
                .name,
            "production"
        );
    }

    #[test]
    fn use_resolution_accepts_only_cluster_names() {
        let clusters = vec![cluster()];
        assert!(resolve_cluster_name(&clusters, "production").is_ok());
        assert!(resolve_cluster_name(&clusters, "11111111-1111-1111-1111-111111111111").is_err());
    }

    #[test]
    fn formats_cluster_resources() {
        assert_eq!(format_cpu(Some(4.0)), "4");
        assert_eq!(format_cpu(Some(1.5)), "1.5");
        assert_eq!(format_memory(Some(16 * 1024 * 1024 * 1024)), "16 GiB");
    }

    #[test]
    fn deserializes_platform_cluster_payload() {
        let payload = r#"{
            "organization":{"name":"acme"},
            "clusters":[{
                "id":"11111111-1111-1111-1111-111111111111",
                "name":"production",
                "shared":false,
                "created_at":"2026-07-13T10:00:00Z",
                "status":{"phase":"ready","ready":true,"message":null},
                "resources":{
                    "shards":1,
                    "replicas":2,
                    "cpu_cores_per_replica":4.0,
                    "memory_bytes_per_replica":17179869184
                }
            }]
        }"#;
        let response: super::ListClustersResponse =
            serde_json::from_str(payload).expect("valid platform cluster response");
        assert_eq!(response.organization.name, "acme");
        assert_eq!(response.clusters[0].name, "production");
        assert_eq!(response.clusters[0].resources.as_ref().unwrap().replicas, 2);
    }
}
