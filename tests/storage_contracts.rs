mod common;

use serde_json::{json, Value};

const S3_ARGS: &[&str] = &[
    "--s3-data-bucket",
    "customer-data",
    "--s3-backups-bucket",
    "customer-backups",
    "--s3-role-arn",
    "arn:aws:iam::123456789012:role/RawTreeS3Access",
    "--s3-external-id",
    "rawtree-example",
];

fn storage() -> Value {
    json!({
        "data": {"bucket": "customer-data", "path": ""},
        "backups": {"bucket": "customer-backups", "path": ""},
        "role_arn": "arn:aws:iam::123456789012:role/RawTreeS3Access",
        "external_id": "rawtree-example"
    })
}

fn cluster() -> Value {
    json!({
        "id": "cluster-id", "name": "production", "created_at": "2026-09-24T00:00:00Z",
        "status": {"phase": "creating", "ready": false, "message": null},
        "resources": null, "can_pause": false, "can_resume": false,
        "idle_timeout_minutes": 15,
        "s3_storage": {
            "data": {"bucket": "customer-data", "path": ""},
            "backups": {"bucket": "customer-backups", "path": ""}
        },
        "database_s3_access": {"external_id": "rawtree-example", "database_bucket_tag": "customer-buckets"}
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
            "--s3-backups-path",
            "snapshots/",
        ]);
        let (output, saved, bodies) = common::run_cli(
            &[(
                "POST /v1/databases?organization=team%20alpha&cluster=prod%2Feu".into(),
                if success { "200 OK" } else { "400 Bad Request" },
                response.clone(),
            )],
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
        expected["data"]["path"] = json!("events/");
        expected["backups"]["path"] = json!("snapshots/");
        assert_eq!(
            bodies,
            vec![json!({"name": "analytics", "s3_storage": expected})]
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
fn cluster_creation_supports_default_storage_s3_and_independent_database_access() {
    for (with_storage, with_access) in [(false, false), (true, false), (false, true), (true, true)]
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
        if with_access {
            args.extend_from_slice(&[
                "--database-s3-external-id",
                "rawtree-example",
                "--database-bucket-tag",
                "customer-buckets",
            ]);
        }
        let mut response = cluster();
        if !with_storage {
            response["s3_storage"] = Value::Null;
        }
        if !with_access {
            response["database_s3_access"] = Value::Null;
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
        if with_access {
            expected["database_s3_access"] = cluster()["database_s3_access"].clone();
        }
        assert_eq!(bodies[1], expected);
    }
}

#[test]
fn invalid_storage_flags_fail_before_any_api_request() {
    for (command, flags, error) in [
        (
            vec!["database", "create", "analytics"],
            vec!["--s3-data-bucket", "customer-data"],
            "--s3-backups-bucket",
        ),
        (
            vec![
                "cluster",
                "create",
                "--name",
                "production",
                "--replicas",
                "1",
                "--min-size",
                "2:8",
            ],
            vec!["--database-s3-external-id", "rawtree-example"],
            "--database-bucket-tag",
        ),
        (
            vec![
                "cluster",
                "create",
                "--name",
                "production",
                "--replicas",
                "1",
                "--min-size",
                "2:8",
            ],
            [
                S3_ARGS,
                &[
                    "--database-s3-external-id",
                    "different-id",
                    "--database-bucket-tag",
                    "customer-buckets",
                ],
            ]
            .concat(),
            "External IDs do not match",
        ),
    ] {
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
                assert_eq!(item["database_s3_access"], cluster()["database_s3_access"]);
                assert!(item["s3_storage"].get("role_arn").is_none());
            } else {
                assert!(stdout.contains("customer-owned S3"), "{stdout}");
                assert!(stdout.contains("customer-buckets"), "{stdout}");
            }
        }
    }
}
