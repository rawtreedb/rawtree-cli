mod common;

use serde_json::{json, Value};

const S3_ARGS: &[&str] = &[
    "--s3-data-bucket",
    "customer-data",
    "--s3-backup-bucket",
    "customer-backups",
    "--s3-role-arn",
    "arn:aws:iam::123456789012:role/RawTreeS3Access",
];

fn storage() -> Value {
    json!({
        "data_bucket": "customer-data", "data_path": "",
        "backup_bucket": "customer-backups", "backup_path": "",
        "role_arn": "arn:aws:iam::123456789012:role/RawTreeS3Access"
    })
}

fn cluster() -> Value {
    json!({
        "id": "cluster-id", "name": "production", "created_at": "2026-09-24T00:00:00Z",
        "status": {"phase": "creating", "ready": false, "message": null},
        "resources": null, "can_pause": false, "can_resume": false,
        "idle_timeout_minutes": 15,
        "s3_storage": {
            "data_bucket": "customer-data", "data_path": "",
            "backup_bucket": "customer-backups", "backup_path": ""
        },
        "s3_storage_access": {"external_id": "rawtree-example", "resource_tag": "customer-buckets"}
    })
}

#[test]
fn database_storage_create_uses_current_response_and_persists_only_on_success() {
    let original = json!({"token": "fixture-session", "default_organization": "old-org", "database": "old-db"});
    for success in [true, false] {
        let response = if success {
            json!({"database": {"name": "analytics"}})
        } else {
            json!({"message": "S3 access verification failed"})
        };
        let mut args = vec![
            "--org",
            "team alpha",
            "--cluster",
            "prod/eu",
            "--json",
            "database",
            "create",
            "analytics",
        ];
        args.extend_from_slice(S3_ARGS);
        args.extend_from_slice(&[
            "--s3-data-path",
            "events/",
            "--s3-backup-path",
            "snapshots/",
        ]);
        let (output, saved, bodies) = common::run_cli(
            &[
                (
                    "GET /v1/clusters?organization=team%20alpha".into(),
                    "200 OK",
                    json!({"clusters": []}),
                ),
                (
                    "POST /v1/databases?organization=team%20alpha&cluster=prod%2Feu".into(),
                    if success { "200 OK" } else { "400 Bad Request" },
                    response.clone(),
                ),
            ],
            &args,
            &original,
        );
        assert_eq!(
            output.status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut expected = storage();
        expected["data_path"] = json!("events/");
        expected["backup_path"] = json!("snapshots/");
        assert_eq!(
            bodies,
            vec![
                Value::Null,
                json!({"name": "analytics", "s3_storage": expected})
            ]
        );
        if success {
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                response
            );
            assert_eq!(saved["database"], "analytics");
            assert_eq!(saved["default_organization"], "team alpha");
            assert_eq!(saved["cluster"], "prod/eu");
            assert_eq!(saved["token"], original["token"]);
        } else {
            assert_eq!(saved, original);
        }
    }
}

#[test]
fn cluster_creation_forwards_optional_identity_and_flat_storage() {
    for (with_storage, with_identity) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let mut args = vec![
            "--org",
            "team",
            "--json",
            "cluster",
            "create",
            "--name",
            "production",
            "--replicas",
            "1",
            "--min-size",
            "2:8",
        ];
        if with_storage {
            args.extend_from_slice(S3_ARGS);
        }
        if with_identity {
            args.extend_from_slice(&[
                "--s3-storage-external-id",
                "rawtree-example",
                "--s3-storage-resource-tag",
                "customer-buckets",
            ]);
        }
        if with_storage && !with_identity {
            args.extend_from_slice(&["--s3-storage-external-id", "rawtree-example"]);
        }
        let mut response = cluster();
        if !with_storage {
            response["s3_storage"] = Value::Null;
        }
        let (output, _, bodies) = common::run_cli(
            &[
                (
                    "GET /v1/clusters/sizes".into(),
                    "200 OK",
                    json!({"sizes": [{"cpu_cores": 2, "memory_gib": 8}]}),
                ),
                (
                    "POST /v1/clusters?organization=team".into(),
                    "200 OK",
                    response.clone(),
                ),
            ],
            &args,
            &json!({}),
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            response
        );
        let mut expected = json!({"name": "production", "replicas": 1, "size": {"cpu_cores": 2, "memory_gib": 8}, "autoscaling": {"min_size": {"cpu_cores": 2, "memory_gib": 8}, "max_size": {"cpu_cores": 2, "memory_gib": 8}}});
        if with_storage {
            expected["s3_storage"] = storage();
        }
        if with_identity || with_storage {
            expected["s3_storage_access"]["external_id"] = json!("rawtree-example");
        }
        if with_identity {
            expected["s3_storage_access"]["resource_tag"] = json!("customer-buckets");
        }
        assert_eq!(bodies[1], expected);
    }
}

#[test]
fn invalid_storage_flags_fail_before_any_api_request() {
    for (command, flags, error) in [(
        vec!["database", "create", "analytics"],
        vec!["--s3-data-bucket", "customer-data"],
        "--s3-backup-bucket",
    )] {
        let mut args = vec!["--org", "team", "--cluster", "prod"];
        args.extend(command);
        args.extend(flags);
        let original = json!({"database": "existing"});
        let (output, saved, _) = common::run_cli(&[], &args, &original);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(error),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(saved, original);
    }
}

#[test]
fn cluster_list_and_status_show_storage_without_an_organization_envelope() {
    for action in [
        vec!["cluster", "list"],
        vec!["cluster", "status", "production"],
    ] {
        for json_mode in [true, false] {
            let mut args = vec!["--org", "team"];
            if json_mode {
                args.push("--json");
            }
            args.extend_from_slice(&action);
            let (output, _, _) = common::run_cli(
                &[(
                    "GET /v1/clusters?organization=team".into(),
                    "200 OK",
                    json!({"clusters": [cluster()]}),
                )],
                &args,
                &json!({}),
            );
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            if json_mode {
                let response: Value = serde_json::from_str(&stdout).unwrap();
                let item = if action[1] == "list" {
                    &response["clusters"][0]
                } else {
                    &response
                };
                assert_eq!(item["s3_storage"], cluster()["s3_storage"]);
                assert_eq!(item["s3_storage_access"]["external_id"], "rawtree-example");
                assert_eq!(
                    item["s3_storage_access"]["resource_tag"],
                    "customer-buckets"
                );
                assert!(item["s3_storage"].get("role_arn").is_none());
            } else {
                assert!(stdout.contains("customer-owned S3"), "{stdout}");
                assert!(stdout.contains("customer-buckets"), "{stdout}");
            }
        }
    }
}

#[test]
fn obsolete_identity_and_plural_backup_flags_are_rejected() {
    for flag in [
        "--external-id",
        "--resource-tag",
        "--s3-external-id",
        "--database-s3-external-id",
        "--database-bucket-tag",
        "--s3-backups-bucket",
        "--s3-storage-external-id",
        "--s3-storage-resource-tag",
    ] {
        let (output, _, bodies) = common::run_cli(
            &[],
            &["database", "create", "analytics", flag, "obsolete"],
            &json!({}),
        );
        assert!(!output.status.success(), "{flag}");
        assert!(bodies.is_empty());
    }
}

#[test]
fn managed_cluster_storage_and_inherited_database_storage_have_distinct_labels() {
    let mut managed = cluster();
    managed["s3_storage"] = Value::Null;
    managed["s3_storage_access"] = Value::Null;
    for action in [
        vec!["cluster", "list"],
        vec!["cluster", "status", "production"],
    ] {
        let mut args = vec!["--org", "team"];
        args.extend_from_slice(&action);
        let (output, _, _) = common::run_cli(
            &[(
                "GET /v1/clusters?organization=team".into(),
                "200 OK",
                json!({"clusters": [managed]}),
            )],
            &args,
            &json!({}),
        );
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("RawTree-managed"), "{text}");
        assert!(!text.contains("cluster default"), "{text}");
        if action[1] == "status" {
            assert!(text.contains("External ID: not configured"));
            assert!(text.contains("Resource tag: not configured"));
        }
    }
    let (output, _, _) = common::run_cli(
        &[(
            "GET /v1/databases?organization=team&cluster=production".into(),
            "200 OK",
            json!({"databases": [{"name": "analytics", "s3_storage": null}]}),
        )],
        &[
            "--org",
            "team",
            "--cluster",
            "production",
            "database",
            "list",
        ],
        &json!({}),
    );
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("storage=cluster default"));
}

#[test]
fn cluster_identity_validation_errors_come_from_the_api() {
    let (output, _, bodies) = common::run_cli(
        &[
            (
                "GET /v1/clusters/sizes".into(),
                "200 OK",
                json!({"sizes": [{"cpu_cores": 2, "memory_gib": 8}]}),
            ),
            (
                "POST /v1/clusters?organization=team".into(),
                "400 Bad Request",
                json!({"message": "Custom S3 storage requires s3_storage_access.external_id."}),
            ),
        ],
        &[
            &[
                "--org",
                "team",
                "cluster",
                "create",
                "--name",
                "production",
                "--replicas",
                "1",
                "--min-size",
                "2:8",
            ][..],
            S3_ARGS,
        ]
        .concat(),
        &json!({}),
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires s3_storage_access.external_id")
    );
    assert!(bodies[1].get("external_id").is_none());
    assert!(bodies[1].get("resource_tag").is_none());
}
