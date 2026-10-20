use super::ClusterItem;
use serde_json::json;

#[test]
fn cluster_metadata_exposes_identity_and_nested_storage() {
    let cluster: ClusterItem = serde_json::from_value(json!({
        "id": "cluster-id", "name": "production", "created_at": "2026-10-17T00:00:00Z",
        "status": {"phase": "ready", "ready": true, "message": null},
        "resources": null, "can_pause": true, "can_resume": false,
        "s3_storage_access": {"external_id": "rawtree-example", "resource_tag": "customer-buckets"},
        "s3_storage": {"data": { "bucket": "customer-data", "path": "events" }, "backups": { "bucket": "customer-backups", "path": "snapshots" }},
        "idle_timeout_minutes": 15
    })).expect("cluster metadata");
    assert_eq!(
        cluster.s3_storage_access.as_ref().unwrap().external_id,
        "rawtree-example"
    );
    assert_eq!(
        cluster.s3_storage_access.as_ref().unwrap().resource_tag,
        "customer-buckets"
    );
    let storage = cluster.s3_storage.expect("storage metadata");
    assert_eq!(storage.data.bucket, "customer-data");
    assert_eq!(storage.backups.path, "snapshots");
}
