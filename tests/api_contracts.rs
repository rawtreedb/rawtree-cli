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
        "default\nanalytics\n"
    );
}

#[test]
fn key_list_and_delete_are_cluster_scoped_even_with_a_saved_database() {
    let config = json!({"database": "irrelevant"});
    let response = json!({"keys": [
        {"id": "key-1", "token": "rt_***hint", "name": "ci", "permission": "admin", "expires_at": null, "database": {"name": "default"}, "created_at": "2026-09-24"},
        {"id": "key-2", "token": "rt_***hint", "name": "analytics", "permission": "read_only", "expires_at": null, "database": {"name": "analytics"}, "created_at": "2026-09-24"}
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
        let response = json!({"id": "key-1", "token": "rt_fixture", "name": "ci", "permission": "read_write", "expires_at": null, "database": {"name": database}});
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

const WORKFLOW_SCOPE: &str = "organization=team%20alpha&cluster=prod%2Feu";

fn workflow_response() -> Value {
    json!({"id": "wf-1", "name": "alerts", "query": {"database": "analytics", "sql": "SELECT 1"},
        "enabled": true, "revision": 1, "interval_seconds": 60,
        "next_run_at": "2026-10-07T10:01:00Z",
        "created_at": "2026-10-07 10:00:00+00", "updated_at": "2026-10-07 10:00:00+00",
        "sinks": [{"type": "table", "id": "sink-1", "settings": {"database": "analytics", "table": "out"}}]})
}

#[test]
fn workflow_create_uses_saved_database_and_sends_sinks() {
    let (result, _, body) = run(
        &[
            "workflow",
            "create",
            "--name",
            "alerts",
            "--sql",
            "SELECT 1",
            "--interval-seconds",
            "60",
            "--sink",
            r#"{"type":"table","settings":{"database":"analytics","table":"out"}}"#,
        ],
        "POST",
        &format!("/v1/workflows?{WORKFLOW_SCOPE}"),
        workflow_response(),
        json!({"database": "analytics"}),
    );
    assert_eq!(result, workflow_response());
    assert_eq!(
        body,
        json!({"name": "alerts", "query": {"database": "analytics", "sql": "SELECT 1"}, "enabled": true,
            "interval_seconds": 60,
            "sinks": [{"type": "table", "settings": {"database": "analytics", "table": "out"}}]})
    );
}

fn run_workflow_human(args: &[&str], method: &str, path: &str, response: Value) -> String {
    let mut command = vec!["--org", "team alpha", "--cluster", "prod/eu"];
    command.extend_from_slice(args);
    let (output, _, _) = common::run_cli(
        &[(format!("{method} {path}"), "200 OK", response)],
        &command,
        &json!({}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn workflow_list_renders_empty_and_populated_responses() {
    let path = format!("/v1/workflows?{WORKFLOW_SCOPE}");
    let stdout = run_workflow_human(
        &["workflow", "list"],
        "GET",
        &path,
        json!({"workflows": []}),
    );
    assert!(stdout.contains("No workflows yet"), "{stdout}");

    let mut manual = workflow_response();
    manual["id"] = json!("wf-2");
    manual["name"] = json!("backfill");
    manual["interval_seconds"] = Value::Null;
    manual["next_run_at"] = Value::Null;
    let stdout = run_workflow_human(
        &["workflow", "list"],
        "GET",
        &path,
        json!({"workflows": [workflow_response(), manual]}),
    );
    for expected in [
        "alerts",
        "analytics",
        "1m",
        "wf-1",
        "backfill",
        "manual",
        "wf-2",
        "2026-10-07T10:01:00Z",
        "—",
    ] {
        assert!(stdout.contains(expected), "missing {expected}: {stdout}");
    }
}

#[test]
fn workflow_human_output_reads_query_and_optional_metadata() {
    for (interval, enabled, status, next_run) in [
        (Value::Null, true, "manual", Value::Null),
        (Value::Null, false, "manual", Value::Null),
        (json!(60), false, "paused", Value::Null),
        (json!(60), true, "enabled", json!("2026-10-07T10:01:00Z")),
    ] {
        let mut response = workflow_response();
        response["interval_seconds"] = interval;
        response["enabled"] = json!(enabled);
        response["next_run_at"] = next_run.clone();
        response["sinks"] = json!([{"id": "sink-http", "type": "http", "settings": {
            "url": "https://example.com/hook", "url_configured": true, "header_names": ["Authorization"]
        }}]);
        for (args, method, suffix) in [
            (vec!["workflow", "get", "wf-1"], "GET", "/wf-1"),
            (
                vec![
                    "workflow",
                    "create",
                    "--name",
                    "alerts",
                    "--database",
                    "analytics",
                    "--sql",
                    "SELECT 1",
                ],
                "POST",
                "",
            ),
            (
                vec!["workflow", "update", "wf-1", "--name", "alerts"],
                "PATCH",
                "/wf-1",
            ),
        ] {
            let text = run_workflow_human(
                &args,
                method,
                &format!("/v1/workflows{suffix}?{WORKFLOW_SCOPE}"),
                response.clone(),
            );
            assert!(text.contains("analytics"), "{text}");
            assert!(text.contains(status), "{text}");
            assert!(text.contains(next_run.as_str().unwrap_or("—")), "{text}");
            assert!(text.contains("SELECT 1"), "{text}");
            assert!(text.contains("https://example.com/hook"), "{text}");
            assert!(text.contains("headers=Authorization"), "{text}");
        }
    }
}

#[test]
fn workflow_query_updates_preserve_unspecified_fields() {
    for (flags, expected) in [
        (
            vec!["--sql", "SELECT 2"],
            json!({"query": {"sql": "SELECT 2"}}),
        ),
        (
            vec!["--database", "other"],
            json!({"query": {"database": "other"}}),
        ),
        (
            vec!["--database", "other", "--sql", "SELECT 2"],
            json!({"query": {"database": "other", "sql": "SELECT 2"}}),
        ),
        (vec!["--name", "renamed"], json!({"name": "renamed"})),
    ] {
        let mut args = vec!["workflow", "update", "wf-1"];
        args.extend(flags);
        let (_, _, body) = run(
            &args,
            "PATCH",
            &format!("/v1/workflows/wf-1?{WORKFLOW_SCOPE}"),
            workflow_response(),
            json!({"database": "saved-db"}),
        );
        assert_eq!(body, expected);
    }
}

#[test]
fn workflow_scheduling_distinguishes_omitted_null_and_numeric_intervals() {
    for (flags, interval) in [
        (vec![], None),
        (vec!["--interval-seconds", "null"], Some(Value::Null)),
        (vec!["--interval-seconds", "300"], Some(json!(300))),
    ] {
        let mut create = vec![
            "workflow", "create", "--name", "alerts", "--sql", "SELECT 1",
        ];
        create.extend_from_slice(&flags);
        let (_, _, body) = run(
            &create,
            "POST",
            &format!("/v1/workflows?{WORKFLOW_SCOPE}"),
            workflow_response(),
            json!({"database": "analytics"}),
        );
        assert_eq!(
            body.get("interval_seconds"),
            Some(interval.as_ref().unwrap_or(&Value::Null))
        );
        assert_eq!(body["enabled"], true);

        let mut update = vec!["workflow", "update", "wf-1", "--name", "renamed"];
        update.extend_from_slice(&flags);
        let (_, _, body) = run(
            &update,
            "PATCH",
            &format!("/v1/workflows/wf-1?{WORKFLOW_SCOPE}"),
            workflow_response(),
            json!({}),
        );
        assert_eq!(body.get("interval_seconds"), interval.as_ref());
        assert!(body.get("enabled").is_none());
        assert!(body.get("query").is_none());
    }
}

#[test]
fn workflow_update_sends_only_changed_fields() {
    let (_, _, body) = run(
        &["workflow", "update", "wf-1", "--disable", "--clear-sinks"],
        "PATCH",
        &format!("/v1/workflows/wf-1?{WORKFLOW_SCOPE}"),
        workflow_response(),
        json!({}),
    );
    assert_eq!(body, json!({"enabled": false, "sinks": []}));
}

#[test]
fn workflow_delete_and_cancel_accept_empty_responses() {
    let (result, _, _) = run(
        &["workflow", "delete", "wf-1"],
        "DELETE",
        &format!("/v1/workflows/wf-1?{WORKFLOW_SCOPE}"),
        Value::Null,
        json!({}),
    );
    assert_eq!(result, json!({"id": "wf-1", "deleted": true}));

    let (result, _, body) = run(
        &["workflow", "cancel", "wf-1", "run-1"],
        "POST",
        &format!("/v1/workflows/wf-1/runs/run-1/cancel?{WORKFLOW_SCOPE}"),
        Value::Null,
        json!({}),
    );
    assert_eq!(body, Value::Null);
    assert_eq!(
        result,
        json!({"workflow_id": "wf-1", "run_id": "run-1", "cancel_requested": true})
    );
}

#[test]
fn workflow_runs_and_logs_pass_pagination_and_time_window() {
    let response = json!({"runs": [], "next_cursor": null});
    let (result, _, _) = run(
        &[
            "workflow", "runs", "wf-1", "--limit", "5", "--cursor", "a+b",
        ],
        "GET",
        &format!("/v1/workflows/wf-1/runs?{WORKFLOW_SCOPE}&limit=5&cursor=a%2Bb"),
        response.clone(),
        json!({}),
    );
    assert_eq!(result, response);

    let response =
        json!({"logs": [], "next_cursor": null, "from": 1790812800000i64, "to": 1790816400000i64});
    let (result, _, _) = run(
        &[
            "workflow",
            "logs",
            "wf-1",
            "--start-time",
            "2026-10-01T00:00:00Z",
            "--end-time",
            "2026-10-01T01:00:00Z",
        ],
        "GET",
        &format!("/v1/workflows/wf-1/logs?{WORKFLOW_SCOPE}&from=1790812800000&to=1790816400000"),
        response.clone(),
        json!({}),
    );
    assert_eq!(result, response);
}

#[test]
fn workflow_commands_require_a_cluster() {
    let (output, _, _) = common::run_cli(
        &[],
        &["--json", "--org", "team", "workflow", "list"],
        &json!({}),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("No cluster specified"));
}

#[test]
fn workflow_invalid_intervals_are_rejected_before_sending_requests() {
    for args in [
        vec![
            "workflow", "create", "--name", "alerts", "--sql", "SELECT 1",
        ],
        vec!["workflow", "update", "wf-1"],
    ] {
        for value in ["0", "86401", "1.5", "manual", ""] {
            let mut command = args.clone();
            command.extend(["--interval-seconds", value]);
            let (output, _, _) = common::run_cli(&[], &command, &json!({}));
            assert_eq!(output.status.code(), Some(2));
            assert!(String::from_utf8_lossy(&output.stderr).contains("expected a whole number"));
        }
    }
}

#[test]
fn workflow_manual_flag_is_rejected() {
    for mut args in [
        vec![
            "workflow", "create", "--name", "alerts", "--sql", "SELECT 1",
        ],
        vec!["workflow", "update", "wf-1"],
    ] {
        args.push("--manual");
        let (output, _, _) = common::run_cli(&[], &args, &json!({}));
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--manual'"));
    }
}
