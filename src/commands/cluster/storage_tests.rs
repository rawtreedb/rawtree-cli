use super::{create_request_body, format_database_s3_access, format_storage, ClusterItem};
use crate::cli::ClusterSizeArg;
use crate::s3_storage::{validate_matching_s3_external_ids, DatabaseS3AccessArgs, S3StorageArgs};
use serde_json::json;

#[test]
fn cluster_storage_response_deserializes_with_the_new_field_name() {
    let cluster: ClusterItem = serde_json::from_value(json!({
        "id": "cluster-id",
        "name": "production",
        "created_at": "2026-07-14 20:38:33.004347+00",
        "status": {"phase": "ready", "ready": true, "message": null},
        "resources": null,
        "can_pause": true,
        "can_resume": false,
        "s3_storage": {
            "data": {"bucket": "customer-data", "path": "rawtree/data"},
            "backups": {"bucket": "customer-backups", "path": "rawtree/backups"}
        },
        "idle_timeout_minutes": 15
    }))
    .expect("cluster storage response should deserialize");

    assert_eq!(
        format_storage(cluster.s3_storage.as_ref()),
        "customer-owned S3"
    );
    assert_eq!(
        cluster.s3_storage.expect("storage metadata").data.bucket,
        "customer-data"
    );
}

#[test]
fn cluster_database_s3_access_response_deserializes_metadata() {
    let cluster: ClusterItem = serde_json::from_value(json!({
        "id": "cluster-id",
        "name": "production",
        "created_at": "2026-07-14 20:38:33.004347+00",
        "status": {"phase": "ready", "ready": true, "message": null},
        "resources": null,
        "can_pause": true,
        "can_resume": false,
        "s3_storage": null,
        "database_s3_access": {
            "external_id": "rawtree-database-access",
            "database_bucket_tag": "rawtree-customer-database"
        },
        "idle_timeout_minutes": 15
    }))
    .expect("cluster database S3 access metadata should deserialize");

    let access = cluster
        .database_s3_access
        .expect("database S3 access metadata");
    assert_eq!(access.external_id, "rawtree-database-access");
    assert_eq!(access.database_bucket_tag, "rawtree-customer-database");
    assert_eq!(
        format_database_s3_access(Some(&access)),
        "configured (rawtree-customer-database)"
    );
    assert_eq!(format_database_s3_access(None), "not configured");
}

#[test]
fn cluster_create_body_uses_s3_storage() {
    let storage = S3StorageArgs {
        s3_data_bucket: Some("customer-data".to_string()),
        s3_data_path: Some("rawtree/data".to_string()),
        s3_backups_bucket: Some("customer-backups".to_string()),
        s3_backups_path: Some("rawtree/backups".to_string()),
        s3_role_arn: Some("arn:aws:iam::123456789012:role/RawTreeS3Access".to_string()),
        s3_external_id: Some("rawtree-example".to_string()),
    };
    let body = create_request_body(
        "production",
        1,
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        None,
        storage.to_json().expect("valid storage"),
        None,
    );

    assert_eq!(
        body["s3_storage"],
        json!({
            "data": {"bucket": "customer-data", "path": "rawtree/data"},
            "backups": {"bucket": "customer-backups", "path": "rawtree/backups"},
            "role_arn": "arn:aws:iam::123456789012:role/RawTreeS3Access",
            "external_id": "rawtree-example"
        })
    );
}

#[test]
fn cluster_create_body_supports_database_s3_access_without_cluster_storage() {
    let body = create_request_body(
        "production",
        1,
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        None,
        None,
        Some(json!({
            "external_id": "rawtree-database-access",
            "database_bucket_tag": "rawtree-customer-database"
        })),
    );

    assert!(body.get("s3_storage").is_none());
    assert_eq!(
        body["database_s3_access"],
        json!({
            "external_id": "rawtree-database-access",
            "database_bucket_tag": "rawtree-customer-database"
        })
    );
}

#[test]
fn cluster_create_body_supports_matching_storage_and_database_access() {
    let storage = S3StorageArgs {
        s3_data_bucket: Some("customer-data".to_string()),
        s3_backups_bucket: Some("customer-backups".to_string()),
        s3_role_arn: Some("arn:aws:iam::123456789012:role/RawTreeS3Access".to_string()),
        s3_external_id: Some("rawtree-example".to_string()),
        ..S3StorageArgs::default()
    };
    let access = DatabaseS3AccessArgs {
        database_s3_external_id: Some("rawtree-example".to_string()),
        database_bucket_tag: Some("rawtree-customer-database".to_string()),
    };

    validate_matching_s3_external_ids(&storage, &access)
        .expect("matching external IDs should be accepted");
    let body = create_request_body(
        "production",
        1,
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        ClusterSizeArg {
            cpu_cores: 2,
            memory_gib: 8,
        },
        None,
        storage.to_json().expect("valid storage"),
        access.to_json().expect("valid database access"),
    );

    assert!(body["s3_storage"].is_object());
    assert!(body["database_s3_access"].is_object());
}

#[test]
fn cluster_create_rejects_mismatched_storage_and_database_external_ids() {
    let storage = S3StorageArgs {
        s3_external_id: Some("rawtree-cluster".to_string()),
        ..S3StorageArgs::default()
    };
    let access = DatabaseS3AccessArgs {
        database_s3_external_id: Some("rawtree-database".to_string()),
        ..DatabaseS3AccessArgs::default()
    };

    let error = validate_matching_s3_external_ids(&storage, &access)
        .expect_err("mismatched external IDs should be rejected");
    assert!(error.to_string().contains("S3 External IDs do not match"));
}
