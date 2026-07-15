use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use comfy_table::{Cell, CellAlignment};
use serde::Deserialize;
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

#[derive(Deserialize)]
struct ClusterItem {
    id: String,
    name: String,
    created_at: String,
    status: ClusterStatus,
    resources: Option<ClusterResources>,
}

#[derive(Deserialize)]
struct ClusterStatus {
    phase: String,
}

#[derive(Deserialize)]
struct ClusterResources {
    replicas: u32,
    cpu_cores_per_replica: Option<f64>,
    memory_bytes_per_replica: Option<u64>,
}

pub fn list(client: &ApiClient, organization: Option<&str>, json_mode: bool) -> Result<()> {
    let path = clusters_collection_path(organization);
    let mut value: Value = client.get(&path)?;
    filter_dedicated_clusters(&mut value)?;
    let resp: ListClustersResponse =
        serde_json::from_value(value.clone()).context("invalid clusters response from server")?;

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

fn clusters_collection_path(organization: Option<&str>) -> String {
    match organization {
        Some(name) => format!("/v1/clusters?organization={}", urlencoding::encode(name)),
        None => "/v1/clusters".to_string(),
    }
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
        clusters_collection_path, filter_dedicated_clusters, format_created_at, format_phase,
        format_size_per_replica, ClusterResources,
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
