use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use comfy_table::{Cell, CellAlignment};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::table_output::new_cli_table;
use crate::client::ApiClient;
use crate::output;

#[derive(Deserialize)]
struct ListClustersResponse {
    organization: OrganizationRef,
    clusters: Vec<ClusterItem>,
}

#[derive(Deserialize)]
struct OrganizationRef {
    name: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct ClusterItem {
    id: String,
    name: String,
    shared: bool,
    created_at: String,
    status: ClusterStatus,
    resources: Option<ClusterResources>,
    can_pause: bool,
    can_resume: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct ClusterStatus {
    phase: String,
    ready: bool,
    message: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct ClusterResources {
    shards: u32,
    replicas: u32,
    cpu_cores_per_replica: Option<f64>,
    memory_bytes_per_replica: Option<u64>,
}

#[derive(Clone, Copy)]
enum LifecycleAction {
    Stop,
    Resume,
}

impl LifecycleAction {
    fn path_segment(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Resume => "resume",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Stop => "Stop",
            Self::Resume => "Resume",
        }
    }
}

#[derive(Deserialize, Serialize)]
struct DeleteClusterResponse {
    deleted: bool,
}

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let (value, resp) = load_dedicated_clusters(client, organization)?;

    output::print_result(&value, json_mode, |_| {
        if resp.clusters.is_empty() {
            println!(
                "No dedicated clusters found for organization '{}'.",
                resp.organization.name
            );
            return;
        }

        let mut table = new_cli_table();
        table.set_header(vec![
            "cluster",
            "status",
            "replicas",
            "size / replica",
            "created",
            "id",
        ]);
        for cluster in &resp.clusters {
            let replicas = cluster
                .resources
                .as_ref()
                .map(|resources| resources.replicas.to_string())
                .unwrap_or_else(|| "—".to_string());
            table.add_row(vec![
                Cell::new(&cluster.name),
                Cell::new(format_phase(&cluster.status.phase)),
                Cell::new(replicas).set_alignment(CellAlignment::Right),
                Cell::new(format_size_per_replica(cluster.resources.as_ref())),
                Cell::new(format_created_at(&cluster.created_at)),
                Cell::new(&cluster.id),
            ]);
        }
        println!("{table}");
    });

    Ok(())
}

pub fn status(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let (_, resp) = load_dedicated_clusters(client, organization)?;
    let cluster = resolve_cluster(&resp.clusters, name_or_id)?;

    output::print_result(cluster, json_mode, |cluster| {
        println!("Cluster: {}", cluster.name);
        println!("ID: {}", cluster.id);
        println!("Status: {}", format_phase(&cluster.status.phase));
        println!("Ready: {}", if cluster.status.ready { "yes" } else { "no" });
        if let Some(message) = cluster.status.message.as_deref() {
            println!("Message: {message}");
        }
    });
    Ok(())
}

pub fn stop(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    request_lifecycle(
        client,
        name_or_id,
        organization,
        LifecycleAction::Stop,
        json_mode,
    )
}

pub fn resume(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    request_lifecycle(
        client,
        name_or_id,
        organization,
        LifecycleAction::Resume,
        json_mode,
    )
}

pub fn delete(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let (_, resp) = load_dedicated_clusters(client, organization)?;
    let cluster = resolve_cluster(&resp.clusters, name_or_id)?.clone();
    let path = cluster_path(&cluster.id, None, organization);
    let result: DeleteClusterResponse = client.delete(&path)?;

    output::print_result(&result, json_mode, |result| {
        if result.deleted {
            println!("Delete request accepted for cluster '{}'.", cluster.name);
            println!(
                "The cluster is removed from status listings while infrastructure cleanup continues asynchronously."
            );
        }
    });
    Ok(())
}

fn load_dedicated_clusters(
    client: &ApiClient,
    organization: Option<&str>,
) -> Result<(Value, ListClustersResponse)> {
    let path = clusters_collection_path(organization);
    let mut value: Value = client.get(&path)?;
    filter_dedicated_clusters(&mut value)?;
    let resp =
        serde_json::from_value(value.clone()).context("invalid clusters response from server")?;
    Ok((value, resp))
}

fn resolve_cluster<'a>(clusters: &'a [ClusterItem], name_or_id: &str) -> Result<&'a ClusterItem> {
    clusters
        .iter()
        .find(|cluster| cluster.name == name_or_id || cluster.id == name_or_id)
        .ok_or_else(|| {
            output::coded_error(
                "cluster_not_found",
                format!("Dedicated cluster '{name_or_id}' not found."),
                4,
            )
        })
}

fn request_lifecycle(
    client: &ApiClient,
    name_or_id: &str,
    organization: Option<&str>,
    action: LifecycleAction,
    json_mode: bool,
) -> Result<()> {
    let (_, resp) = load_dedicated_clusters(client, organization)?;
    let cluster = resolve_cluster(&resp.clusters, name_or_id)?;
    let path = cluster_path(&cluster.id, Some(action.path_segment()), organization);
    let value: Value = client.post_empty(&path)?;
    let updated: ClusterItem =
        serde_json::from_value(value.clone()).context("invalid cluster response from server")?;

    output::print_result(&value, json_mode, |_| {
        println!(
            "{} request accepted for cluster '{}' (status: {}).",
            action.label(),
            updated.name,
            format_phase(&updated.status.phase),
        );
        println!(
            "Run `rtree cluster status {}` to check progress.",
            updated.name
        );
    });
    Ok(())
}

fn clusters_collection_path(organization: Option<&str>) -> String {
    match organization {
        Some(name) => format!("/v1/clusters?organization={}", urlencoding::encode(name)),
        None => "/v1/clusters".to_string(),
    }
}

fn cluster_path(cluster_id: &str, action: Option<&str>, organization: Option<&str>) -> String {
    let mut path = format!("/v1/clusters/{}", urlencoding::encode(cluster_id));
    if let Some(action) = action {
        path.push('/');
        path.push_str(action);
    }
    if let Some(name) = organization {
        path.push_str("?organization=");
        path.push_str(&urlencoding::encode(name));
    }
    path
}

fn filter_dedicated_clusters(value: &mut Value) -> Result<()> {
    let clusters = value
        .get_mut("clusters")
        .and_then(Value::as_array_mut)
        .context("invalid clusters response from server: missing clusters array")?;
    for (index, cluster) in clusters.iter().enumerate() {
        if cluster.get("shared").and_then(Value::as_bool).is_none() {
            anyhow::bail!(
                "invalid clusters response from server: cluster at index {index} has a missing or invalid boolean shared field"
            );
        }
    }
    clusters.retain(|cluster| cluster.get("shared").and_then(Value::as_bool) == Some(false));
    Ok(())
}

fn format_phase(phase: &str) -> String {
    phase.replace('_', " ")
}

fn format_created_at(created_at: &str) -> String {
    DateTime::parse_from_str(created_at, "%Y-%m-%d %H:%M:%S%.f%#z")
        .map(|timestamp| {
            timestamp
                .with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        })
        .unwrap_or_else(|_| created_at.to_string())
}

fn format_size_per_replica(resources: Option<&ClusterResources>) -> String {
    let Some((cpu, memory_bytes)) = resources.and_then(|resources| {
        Some((
            resources.cpu_cores_per_replica?,
            resources.memory_bytes_per_replica?,
        ))
    }) else {
        return "—".to_string();
    };

    let memory_gib = memory_bytes as f64 / 1024_f64.powi(3);
    format!(
        "{} CPU / {} GiB",
        compact_number(cpu),
        compact_number(memory_gib)
    )
}

fn compact_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        cluster_path, clusters_collection_path, filter_dedicated_clusters, format_created_at,
        format_phase, format_size_per_replica, resolve_cluster, ClusterItem, ClusterResources,
        ClusterStatus,
    };

    #[test]
    fn collection_path_encodes_organization() {
        assert_eq!(
            clusters_collection_path(Some("team alpha")),
            "/v1/clusters?organization=team%20alpha"
        );
        assert_eq!(clusters_collection_path(None), "/v1/clusters");
    }

    #[test]
    fn lifecycle_paths_encode_cluster_and_organization() {
        assert_eq!(
            cluster_path("cluster/id", Some("stop"), Some("team alpha")),
            "/v1/clusters/cluster%2Fid/stop?organization=team%20alpha"
        );
        assert_eq!(
            cluster_path("cluster-id", Some("resume"), None),
            "/v1/clusters/cluster-id/resume"
        );
        assert_eq!(
            cluster_path("cluster-id", None, Some("team alpha")),
            "/v1/clusters/cluster-id?organization=team%20alpha"
        );
    }

    fn cluster() -> ClusterItem {
        ClusterItem {
            id: "11111111-1111-1111-1111-111111111111".to_string(),
            name: "production".to_string(),
            shared: false,
            created_at: "2026-07-14 20:38:33.004347+00".to_string(),
            status: ClusterStatus {
                phase: "ready".to_string(),
                ready: true,
                message: None,
            },
            resources: None,
            can_pause: true,
            can_resume: false,
        }
    }

    #[test]
    fn resolves_dedicated_cluster_by_name_or_id() {
        let clusters = vec![cluster()];
        assert_eq!(
            resolve_cluster(&clusters, "production")
                .expect("cluster by name")
                .id,
            "11111111-1111-1111-1111-111111111111"
        );
        assert_eq!(
            resolve_cluster(&clusters, "11111111-1111-1111-1111-111111111111")
                .expect("cluster by id")
                .name,
            "production"
        );
        let error = match resolve_cluster(&clusters, "missing") {
            Ok(_) => panic!("missing cluster should fail"),
            Err(error) => error,
        };
        let cli_error = error
            .downcast_ref::<crate::output::CliError>()
            .expect("coded CLI error");
        assert_eq!(cli_error.code(), "cluster_not_found");
        assert_eq!(cli_error.exit_code(), 4);
    }

    #[test]
    fn lifecycle_response_fields_deserialize_for_status_output() {
        let cluster: ClusterItem = serde_json::from_value(json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "name": "production",
            "shared": false,
            "created_at": "2026-07-14 20:38:33.004347+00",
            "status": {
                "phase": "pausing",
                "ready": false,
                "message": "Waiting for active queries to finish."
            },
            "resources": {
                "shards": 1,
                "replicas": 3,
                "cpu_cores_per_replica": 2.0,
                "memory_bytes_per_replica": 8589934592_u64
            },
            "can_pause": false,
            "can_resume": false
        }))
        .expect("valid cluster lifecycle response");

        assert_eq!(cluster.status.phase, "pausing");
        assert!(!cluster.status.ready);
        assert_eq!(
            cluster.status.message.as_deref(),
            Some("Waiting for active queries to finish.")
        );
        assert!(!cluster.can_pause);
        assert!(!cluster.can_resume);
    }

    #[test]
    fn shared_clusters_are_removed_without_changing_dedicated_cluster_fields() {
        let dedicated = json!({
            "id": "dedicated-id",
            "name": "production",
            "shared": false,
            "created_at": "2026-07-14 20:38:33.004347+00",
            "future_field": {"preserved": true}
        });
        let mut response = json!({
            "organization": {"name": "acme"},
            "clusters": [
                {"id": "shared-id", "shared": true},
                dedicated.clone()
            ],
            "future_top_level_field": true
        });

        filter_dedicated_clusters(&mut response).expect("valid response");

        assert_eq!(response["clusters"], json!([dedicated]));
        assert_eq!(response["future_top_level_field"], true);
    }

    #[test]
    fn missing_or_invalid_shared_field_is_rejected() {
        for shared_field in [None, Some(json!(null)), Some(json!("false"))] {
            let mut cluster = json!({"id": "cluster-id"});
            if let Some(shared) = shared_field {
                cluster["shared"] = shared;
            }
            let mut response = json!({
                "organization": {"name": "acme"},
                "clusters": [cluster]
            });

            let error = filter_dedicated_clusters(&mut response)
                .expect_err("missing or invalid shared field should fail");

            assert!(error
                .to_string()
                .contains("missing or invalid boolean shared field"));
        }
    }

    #[test]
    fn timestamp_is_rendered_to_seconds_in_utc() {
        assert_eq!(
            format_created_at("2026-07-14 20:38:33.004347+00"),
            "2026-07-14 20:38:33 UTC"
        );
        assert_eq!(
            format_created_at("2026-07-14 22:38:33+02"),
            "2026-07-14 20:38:33 UTC"
        );
    }

    #[test]
    fn resources_are_formatted_per_replica() {
        let resources = ClusterResources {
            shards: 1,
            replicas: 3,
            cpu_cores_per_replica: Some(2.0),
            memory_bytes_per_replica: Some(8 * 1024 * 1024 * 1024),
        };
        assert_eq!(format_size_per_replica(Some(&resources)), "2 CPU / 8 GiB");
        assert_eq!(format_size_per_replica(None), "—");
    }

    #[test]
    fn lifecycle_phase_is_human_readable() {
        assert_eq!(format_phase("rolling_rawtree"), "rolling rawtree");
    }
}
