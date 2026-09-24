mod common;

use serde_json::{json, Value};

fn run(
    args: &[&str],
    method: &str,
    path: &str,
    response: Value,
    config: Value,
) -> (Value, Value, Value) {
    let mut command = vec!["--org", "team alpha", "--cluster", "prod/eu", "--json"];
    command.extend_from_slice(args);
    let (output, saved, bodies) = common::run_cli(
        &[(format!("{method} {path}"), "200 OK", response)],
        &command,
        &config,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        serde_json::from_slice(&output.stdout).unwrap(),
        saved,
        bodies[0].clone(),
    )
}

#[test]
fn database_create_reads_nested_response_and_saves_requested_context() {
    let response = json!({"database": {"name": "analytics"}});
    let (result, saved, body) = run(
        &["database", "create", "analytics"],
        "POST",
        "/v1/databases?organization=team%20alpha&cluster=prod%2Feu",
        response.clone(),
        json!({"token": "fixture-session", "default_organization": "old-org", "database": "old-db"}),
    );
    assert_eq!(result, response);
    assert_eq!(body, json!({"name": "analytics"}));
    assert_eq!(saved["token"], "fixture-session");
    assert_eq!(saved["default_organization"], "team alpha");
    assert_eq!(saved["cluster"], "prod/eu");
    assert_eq!(saved["database"], "analytics");
}

#[test]
fn database_create_failure_does_not_overwrite_config() {
    let config = json!({"token": "fixture-session", "default_organization": "team", "cluster": "prod", "database": "old-db"});
    let (output, saved, _) = common::run_cli(
        &[(
            "POST /v1/databases?organization=team&cluster=prod".to_string(),
            "400 Bad Request",
            json!({"message": "Invalid database name"}),
        )],
        &["--json", "database", "create", "invalid-name"],
        &config,
    );
    assert!(!output.status.success());
    assert_eq!(saved, config);
}

#[test]
fn database_list_preserves_current_metadata_without_obsolete_organization() {
    let response = json!({"databases": [
        {"name": "default", "s3_storage": null},
        {"name": "analytics", "s3_storage": {"data": {"bucket": "data-bucket", "path": "prefix/"}, "backups": {"bucket": "backups", "path": ""}}}
    ]});
    let (result, _, _) = run(
        &["database", "list"],
        "GET",
        "/v1/databases?organization=team%20alpha&cluster=prod%2Feu",
        response.clone(),
        json!({}),
    );
    assert_eq!(result, response);

    let (output, _, _) = common::run_cli(
        &[(
            "GET /v1/databases?organization=team&cluster=prod".to_string(),
            "200 OK",
            response,
        )],
        &["--org", "team", "--cluster", "prod", "database", "list"],
        &json!({}),
    );
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "default              storage=cluster default\nanalytics            storage=customer-owned S3\n"
    );
}

#[test]
fn key_list_and_delete_are_cluster_scoped_even_with_a_saved_database() {
    let config = json!({"database": "irrelevant"});
    let response = json!({"keys": [
        {"id": "key-1", "token": "rt_***hint", "name": "ci", "permission": "admin", "database": {"name": "default"}, "created_at": "2026-09-24"},
        {"id": "key-2", "token": "rt_***hint", "name": "analytics", "permission": "read_only", "database": {"name": "analytics"}, "created_at": "2026-09-24"}
    ]});
    for config in [json!({}), config] {
        let (result, _, _) = run(
            &["key", "list"],
            "GET",
            "/v1/keys?organization=team%20alpha&cluster=prod%2Feu",
            response.clone(),
            config.clone(),
        );
        assert_eq!(result, response);
        let (result, _, _) = run(
            &["key", "delete", "key-1"],
            "DELETE",
            "/v1/keys/key-1?organization=team%20alpha&cluster=prod%2Feu",
            json!({"deleted": true}),
            config,
        );
        assert_eq!(result["deleted"], true);
    }
}

#[test]
fn key_create_uses_server_default_or_an_explicit_or_saved_database() {
    for (selectors, config, query, database) in [
        (
            vec![],
            json!({}),
            "organization=team%20alpha&cluster=prod%2Feu",
            "default",
        ),
        (
            vec!["--database", "analytics"],
            json!({"database": "old-db"}),
            "database=analytics&organization=team%20alpha&cluster=prod%2Feu",
            "analytics",
        ),
        (
            vec![],
            json!({"database": "saved-db"}),
            "database=saved-db&organization=team%20alpha&cluster=prod%2Feu",
            "saved-db",
        ),
    ] {
        let response = json!({"id": "key-1", "token": "rt_fixture", "name": "ci", "permission": "read_write", "database": {"name": database}});
        let mut args = vec![
            "key",
            "create",
            "--name",
            "ci",
            "--permission",
            "read_write",
        ];
        args.extend(selectors);
        let (result, _, body) = run(
            &args,
            "POST",
            &format!("/v1/keys?{query}"),
            response.clone(),
            config,
        );
        assert_eq!(result, response);
        assert_eq!(body, json!({"name": "ci", "permission": "read_write"}));
    }
}

#[test]
fn logs_use_cluster_scope_without_a_database_or_with_a_stale_saved_database() {
    for config in [json!({}), json!({"database": "old-db"})] {
        let response = json!({"logs": [], "has_more": false, "next_offset": null});
        let (result, _, _) = run(
            &["logs", "--start-time", "2026-09-24T00:00:00Z", "--end-time", "2026-09-24T01:00:00Z", "--status-codes", "500"],
            "GET",
            "/v1/logs?start_time=2026-09-24T00%3A00%3A00Z&end_time=2026-09-24T01%3A00%3A00Z&status_codes=500&limit=50&offset=0&organization=team%20alpha&cluster=prod%2Feu",
            response.clone(), config,
        );
        assert_eq!(result, response);
    }
}
