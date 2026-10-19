use super::ClusterItem;
use serde_json::json;

#[test]
fn cluster_metadata_exposes_identity_and_flat_storage() {
    let cluster: ClusterItem = serde_json::from_value(json!({
        "id": "cluster-id", "name": "production", "created_at": "2026-10-17T00:00:00Z",
        "status": {"phase": "ready", "ready": true, "message": null},
        "resources": null, "can_pause": true, "can_resume": false,
        "s3_storage_access": {"external_id": "rawtree-example", "resource_tag": "customer-buckets"},
        "s3_storage": {"data_bucket": "customer-data", "data_path": "events", "backup_bucket": "customer-backups", "backup_path": "snapshots"},
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
    assert_eq!(storage.data_bucket, "customer-data");
    assert_eq!(storage.backup_path, "snapshots");
}
