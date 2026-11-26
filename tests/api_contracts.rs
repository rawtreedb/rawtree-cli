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
    let bound_lookup = config["token"]
        .as_str()
        .is_some_and(|token| token.starts_with("rt_"))
        .then(|| {
            [
                common::key_organization("team alpha"),
                common::bound_cluster("prod/eu"),
            ]
        });
    let responses: Vec<_> = bound_lookup
        .into_iter()
        .flatten()
        .chain([(format!("{method} {path}"), "200 OK", response)])
        .collect();
    let (output, saved, bodies) = common::run_cli(&responses, &command, &config);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        serde_json::from_slice(&output.stdout).unwrap(),
        saved,
        bodies.last().unwrap().clone(),
    )
}

#[test]
fn database_create_reads_nested_response_and_saves_requested_context() {
    let response = json!({"database": {"name": "analytics"}});
    let (result, saved, body) = run(
        &["database", "create", "analytics"],
        "POST",
        "/v1/databases",
        response.clone(),
        json!({"token": "rt_fixture", "default_organization": "old-org", "database": "old-db"}),
    );
    assert_eq!(result, response);
    assert_eq!(body, json!({"name": "analytics"}));
    assert_eq!(saved["token"], "rt_fixture");
    assert_eq!(saved["default_organization"], "team alpha");
    assert_eq!(saved["cluster"], "prod/eu");
    assert_eq!(saved["default_organization_id"], common::ORG_ID);
    assert_eq!(saved["cluster_id"], common::CLUSTER_ID);
    assert_eq!(saved["database"], "analytics");
}

#[test]
fn database_create_failure_does_not_overwrite_config() {
    let config = json!({"token": "rt_fixture", "default_organization": "team", "cluster": "prod", "database": "old-db"});
    let (output, saved, _) = common::run_cli(
        &[
            common::key_organization("team"),
            common::bound_cluster("prod"),
            (
                "POST /v1/databases".to_string(),
                "400 Bad Request",
                json!({"message": "Invalid database name"}),
            ),
        ],
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
        {"name": "analytics", "s3_storage": {"data": { "bucket": "data-bucket", "path": "prefix/" }, "backups": { "bucket": "backups", "path": "" }}}
    ]});
    let (result, _, _) = run(
        &["database", "list"],
        "GET",
        "/v1/databases",
        response.clone(),
        json!({}),
    );
    assert_eq!(result, response);

    let (output, _, _) = common::run_cli(
        &[("GET /v1/databases".to_string(), "200 OK", response)],
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
        {"id": "key-1", "token": "rt_***hint", "name": "ci", "permission": "admin", "expires_at": null, "database": {"name": "default"}, "created_at": "2026-09-24"},
        {"id": "key-2", "token": "rt_***hint", "name": "analytics", "permission": "read_only", "expires_at": null, "database": {"name": "analytics"}, "created_at": "2026-09-24"}
    ]});
    for config in [json!({}), config] {
        let (result, _, _) = run(
            &["api-key", "list"],
            "GET",
            "/v1/api-keys",
            response.clone(),
            config.clone(),
        );
        assert_eq!(result, response);
        let (result, _, _) = run(
            &["api-key", "delete", "11111111-1111-4111-8111-111111111111"],
            "DELETE",
            "/v1/api-keys/11111111-1111-4111-8111-111111111111",
            json!({"deleted": true}),
            config,
        );
        assert_eq!(
            result,
            json!({"deleted": true, "id": "11111111-1111-4111-8111-111111111111"})
        );
    }
}

#[test]
fn key_create_uses_server_default_or_an_explicit_or_saved_database() {
    for (selectors, config, query, database) in [
        (vec![], json!({}), "", "default"),
        (
            vec!["--database", "analytics"],
            json!({"database": "old-db"}),
            "database=analytics",
            "analytics",
        ),
        (
            vec![],
            json!({"database": "saved-db"}),
            "database=saved-db",
            "saved-db",
        ),
    ] {
        let response = json!({"id": "key-1", "token": "rt_fixture", "name": "ci", "permission": "read_write", "expires_at": null, "database": {"name": database}});
        let mut args = vec![
            "api-key",
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
            &format!(
                "/v1/api-keys{}",
                if query.is_empty() {
                    String::new()
                } else {
                    format!("?{query}")
                }
            ),
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
            "/v1/logs?start_time=2026-09-24T00%3A00%3A00Z&end_time=2026-09-24T01%3A00%3A00Z&status_codes=500&limit=50&offset=0",
            response.clone(), config,
        );
        assert_eq!(result, response);
    }
}

fn workflow_response() -> Value {
    json!({"id": "wf-1", "name": "alerts", "query": {"database": "analytics", "sql": "SELECT 1"},
        "revision": 1, "schedule": {"cron": "* * * * *", "enabled": true,
        "next_run_at": "2026-10-07T10:01:00Z"},
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
            "--schedule-cron",
            "* * * * *",
            "--sink",
            r#"{"type":"table","settings":{"database":"analytics","table":"out"}}"#,
        ],
        "POST",
        "/v1/workflows",
        workflow_response(),
        json!({"database": "analytics"}),
    );
    assert_eq!(result, workflow_response());
    assert_eq!(
        body,
        json!({"name": "alerts", "query": {"database": "analytics", "sql": "SELECT 1"},
            "schedule": {"cron": "* * * * *", "enabled": true},
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
    let path = "/v1/workflows".to_string();
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
    manual["schedule"] = Value::Null;
    let stdout = run_workflow_human(
        &["workflow", "list"],
        "GET",
        &path,
        json!({"workflows": [workflow_response(), manual]}),
    );
    for expected in [
        "alerts",
        "analytics",
        "* * * * *",
        "wf-1",
        "backfill",
        "active",
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
    for (cron, enabled, status, next_run) in [
        (Value::Null, true, "manual", Value::Null),
        (Value::Null, false, "manual", Value::Null),
        (json!("* * * * *"), false, "paused", Value::Null),
        (
            json!("* * * * *"),
            true,
            "active",
            json!("2026-10-07T10:01:00Z"),
        ),
    ] {
        let mut response = workflow_response();
        response["schedule"] = if cron.is_null() {
            Value::Null
        } else {
            json!({"cron": cron, "enabled": enabled, "next_run_at": next_run})
        };
        response["sinks"] = json!([{"id": "sink-http", "type": "http", "settings": {
            "url": "https://example.com/hook", "url_configured": true, "header_names": ["Authorization"]
        }}]);
        let list = run_workflow_human(
            &["workflow", "list"],
            "GET",
            "/v1/workflows",
            json!({"workflows": [response.clone()]}),
        );
        let row = list.lines().find(|line| line.contains("alerts")).unwrap();
        let cells: Vec<_> = row.split(['│', '┆']).map(str::trim).collect();
        assert_eq!(cells[3], status, "{list}");
        let expected_cron = if cron.is_null() { "—" } else { "* * * * *" };
        assert_eq!(cells[4], expected_cron, "{list}");
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
                &format!("/v1/workflows{suffix}"),
                response.clone(),
            );
            assert!(text.contains("analytics"), "{text}");
            assert!(text.contains(&format!("  status:   {status}\n")), "{text}");
            assert!(
                text.contains(&format!("  cron:     {expected_cron}\n")),
                "{text}"
            );
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
            "/v1/workflows/wf-1",
            workflow_response(),
            json!({"database": "saved-db"}),
        );
        assert_eq!(body, expected);
    }
}

#[test]
fn workflow_scheduling_distinguishes_omitted_null_and_cron() {
    for (flags, schedule) in [
        (vec![], None),
        (vec!["--schedule-cron", "null"], Some(Value::Null)),
        (
            vec!["--schedule-cron", "*/5 * * * *"],
            Some(json!({"cron": "*/5 * * * *"})),
        ),
    ] {
        let mut create = vec![
            "workflow", "create", "--name", "alerts", "--sql", "SELECT 1",
        ];
        create.extend_from_slice(&flags);
        let (_, _, body) = run(
            &create,
            "POST",
            "/v1/workflows",
            workflow_response(),
            json!({"database": "analytics"}),
        );
        let mut expected = schedule.clone().unwrap_or(Value::Null);
        if !expected.is_null() {
            expected["enabled"] = json!(true);
        }
        assert_eq!(body["schedule"], expected);
        assert!(body.get("enabled").is_none());
        assert!(body.get("interval_seconds").is_none());
        let mut update = vec!["workflow", "update", "wf-1", "--name", "renamed"];
        update.extend_from_slice(&flags);
        let (_, _, body) = run(
            &update,
            "PATCH",
            "/v1/workflows/wf-1",
            workflow_response(),
            json!({}),
        );
        assert_eq!(body.get("schedule"), schedule.as_ref());
        assert!(body.get("enabled").is_none());
        assert!(body.get("query").is_none());
    }
}

#[test]
fn workflow_update_sends_only_changed_fields() {
    let (_, _, body) = run(
        &[
            "workflow",
            "update",
            "wf-1",
            "--schedule-enabled",
            "false",
            "--clear-sinks",
        ],
        "PATCH",
        "/v1/workflows/wf-1",
        workflow_response(),
        json!({}),
    );
    assert_eq!(body, json!({"schedule": {"enabled": false}, "sinks": []}));
}

#[test]
fn workflow_create_reads_sink_arrays_from_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sinks.json");
    let sinks = json!([
        {"type": "table", "settings": {"database": "analytics", "table": "alerts"}},
        {"type": "http", "settings": {"url": "https://example.com/hook",
            "headers": {"Authorization": "Bearer token"}}},
    ]);
    std::fs::write(&path, sinks.to_string()).unwrap();
    let file_arg = format!("@{}", path.display());
    let (_, _, body) = run(
        &[
            "workflow",
            "create",
            "--name",
            "alerts",
            "--database",
            "analytics",
            "--sql",
            "SELECT 1",
            "--sink",
            &file_arg,
        ],
        "POST",
        "/v1/workflows",
        workflow_response(),
        json!({}),
    );
    assert_eq!(body["sinks"], sinks);
}

#[test]
fn rejected_sinks_point_to_help() {
    let (output, _, _) = common::run_cli(
        &[(
            "POST /v1/workflows".to_string(),
            "422 Unprocessable Entity",
            json!({"message": "Invalid workflow.",
                "hint": "sinks[0].settings: unknown field `uri`, expected `url` or `headers`"}),
        )],
        &[
            "--org",
            "team alpha",
            "--cluster",
            "prod/eu",
            "workflow",
            "create",
            "--name",
            "alerts",
            "--database",
            "analytics",
            "--sql",
            "SELECT 1",
            "--sink",
            r#"{"type":"http","settings":{"uri":"https://example.com/hook"}}"#,
        ],
        &json!({}),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown field `uri`"), "{stderr}");
    assert!(stderr.contains("rtree workflow sink --help"), "{stderr}");
}

#[test]
fn workflow_sink_commands_change_one_sink() {
    let sink = json!({"type": "table", "id": "sink-2",
        "settings": {"database": "analytics", "table": "alerts"}});
    let (result, _, body) = run(
        &[
            "workflow",
            "sink",
            "create",
            "wf-1",
            r#"{"type":"table","settings":{"database":"analytics","table":"alerts"}}"#,
        ],
        "POST",
        "/v1/workflows/wf-1/sinks",
        sink.clone(),
        json!({}),
    );
    assert_eq!(result, sink);
    assert_eq!(
        body,
        json!({"type": "table", "settings": {"database": "analytics", "table": "alerts"}})
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("changes.json");
    let changes = json!({"settings": {"headers": {"Authorization": null, "X-Region": "eu"}}});
    std::fs::write(&path, changes.to_string()).unwrap();
    let file_arg = format!("@{}", path.display());
    let (_, _, body) = run(
        &["workflow", "sink", "update", "wf-1", "sink 1", &file_arg],
        "PATCH",
        "/v1/workflows/wf-1/sinks/sink%201",
        sink,
        json!({}),
    );
    assert_eq!(body, changes);

    let (result, _, _) = run(
        &["workflow", "sink", "delete", "wf-1", "sink-2"],
        "DELETE",
        "/v1/workflows/wf-1/sinks/sink-2",
        Value::Null,
        json!({}),
    );
    assert_eq!(
        result,
        json!({"workflow_id": "wf-1", "sink_id": "sink-2", "deleted": true})
    );
}

#[test]
fn workflow_sink_human_output_describes_the_sink() {
    let stdout = run_workflow_human(
        &[
            "workflow",
            "sink",
            "update",
            "wf-1",
            "sink-2",
            r#"{"settings":{"table":"alerts_v2"}}"#,
        ],
        "PATCH",
        "/v1/workflows/wf-1/sinks/sink-2",
        json!({"type": "table", "id": "sink-2",
            "settings": {"database": "analytics", "table": "alerts_v2"}}),
    );
    assert_eq!(
        stdout,
        "Sink 'sink-2' updated:\n  table analytics.alerts_v2  id=sink-2\n"
    );
}

#[test]
fn rejected_sink_changes_point_to_help() {
    let (output, _, _) = common::run_cli(
        &[(
            "PATCH /v1/workflows/wf-1/sinks/sink-2".to_string(),
            "400 Bad Request",
            json!({"message": "Invalid workflow sink.",
                "hint": "A sink's type cannot change. Delete the sink and add a new one."}),
        )],
        &[
            "--org",
            "team alpha",
            "--cluster",
            "prod/eu",
            "workflow",
            "sink",
            "update",
            "wf-1",
            "sink-2",
            r#"{"type":"http","settings":{"url":"https://example.com"}}"#,
        ],
        &json!({}),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("type cannot change"), "{stderr}");
    assert!(stderr.contains("rtree workflow sink --help"), "{stderr}");
}

#[test]
fn workflow_delete_and_cancel_accept_empty_responses() {
    let (result, _, _) = run(
        &["workflow", "delete", "wf-1"],
        "DELETE",
        "/v1/workflows/wf-1",
        Value::Null,
        json!({}),
    );
    assert_eq!(result, json!({"id": "wf-1", "deleted": true}));

    let (result, _, body) = run(
        &["workflow", "cancel", "wf-1", "run-1"],
        "POST",
        "/v1/workflows/wf-1/runs/run-1/cancel",
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
        "/v1/workflows/wf-1/runs?limit=5&cursor=a%2Bb",
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
        "/v1/workflows/wf-1/logs?from=1790812800000&to=1790816400000",
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
fn workflow_commands_require_a_cluster_for_user_credentials() {
    let (output, _, _) = common::run_cli(
        &[],
        &["--json", "--org", "team", "workflow", "list"],
        &json!({"token": "session.fixture"}),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("No cluster specified"));
}

#[test]
fn workflow_commands_let_api_keys_omit_organization_and_cluster() {
    let (output, _, _) = common::run_cli(
        &[(
            "GET /v1/workflows".to_string(),
            "200 OK",
            json!({"workflows": []}),
        )],
        &["--json", "workflow", "list"],
        &json!({"token": "rt_fixture"}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn workflow_obsolete_intervals_are_rejected_before_sending_requests() {
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
            assert!(String::from_utf8_lossy(&output.stderr)
                .contains("unexpected argument '--interval-seconds'"));
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

#[test]
fn workflow_schedule_pause_and_resume_fields_are_nested() {
    let (_, _, body) = run(
        &[
            "workflow",
            "create",
            "--name",
            "alerts",
            "--sql",
            "SELECT 1",
            "--schedule-cron",
            "*/30 * * * * *",
            "--schedule-enabled",
            "false",
        ],
        "POST",
        "/v1/workflows",
        workflow_response(),
        json!({"database":"analytics"}),
    );
    assert_eq!(
        body["schedule"],
        json!({"cron":"*/30 * * * * *","enabled":false})
    );
    assert!(body.get("enabled").is_none());
    for (flags, schedule) in [
        (vec!["--schedule-enabled", "true"], json!({"enabled":true})),
        (
            vec!["--schedule-cron", "0 * * * *", "--schedule-enabled", "true"],
            json!({"cron":"0 * * * *","enabled":true}),
        ),
        (
            vec![
                "--schedule-cron",
                "0 * * * *",
                "--schedule-enabled",
                "false",
            ],
            json!({"cron":"0 * * * *","enabled":false}),
        ),
    ] {
        let mut args = vec!["workflow", "update", "wf-1"];
        args.extend(flags);
        let (_, _, body) = run(
            &args,
            "PATCH",
            "/v1/workflows/wf-1",
            workflow_response(),
            json!({}),
        );
        assert_eq!(body, json!({"schedule":schedule}));
    }
}

#[test]
fn workflow_manual_schedule_rejects_pause_flags_before_http() {
    for args in [
        vec![
            "workflow",
            "create",
            "--name",
            "alerts",
            "--sql",
            "SELECT 1",
            "--schedule-enabled",
            "false",
        ],
        vec![
            "workflow",
            "create",
            "--name",
            "alerts",
            "--sql",
            "SELECT 1",
            "--schedule-cron",
            "null",
            "--schedule-enabled",
            "false",
        ],
        vec![
            "workflow",
            "update",
            "wf-1",
            "--schedule-cron",
            "null",
            "--schedule-enabled",
            "true",
        ],
        vec![
            "workflow",
            "update",
            "wf-1",
            "--schedule-cron",
            "null",
            "--schedule-enabled",
            "false",
        ],
    ] {
        let mut command = vec!["--json", "--org", "team", "--cluster", "prod"];
        command.extend(args);
        let (output, _, bodies) = common::run_cli(&[], &command, &json!({"database":"analytics"}));
        assert!(!output.status.success());
        assert!(bodies.is_empty());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("requires --schedule-cron") || error.contains("cannot be combined"),
            "{error}"
        );
    }
}

#[test]
fn workflow_cron_validation_errors_are_returned_by_the_api() {
    let (output, _, bodies) = common::run_cli(
        &[(
            "POST /v1/workflows".into(),
            "400 Bad Request",
            json!({"message":"Invalid workflow cron", "hint":"Cron must have 5 or 6 fields."}),
        )],
        &[
            "--json",
            "workflow",
            "create",
            "--name",
            "alerts",
            "--sql",
            "SELECT 1",
            "--schedule-cron",
            "invalid-cron",
        ],
        &json!({"default_organization":"team","cluster":"prod","database":"analytics"}),
    );
    assert!(!output.status.success());
    assert_eq!(bodies[0]["schedule"]["cron"], "invalid-cron");
    assert!(String::from_utf8_lossy(&output.stderr).contains("Cron must have 5 or 6 fields"));
}

const ORGANIZATION_ID: &str = "00000000-0000-4000-8000-000000000001";
const INTEGRATIONS_PATH: &str =
    "/v1/organizations/00000000-0000-4000-8000-000000000001/integrations";

fn organizations() -> Value {
    json!({"organizations": [
        {"organization_id": "00000000-0000-4000-8000-000000000002", "name": "other"},
        {"organization_id": ORGANIZATION_ID, "name": "team alpha"}
    ]})
}

fn integrations_response() -> Value {
    json!({"integrations": [{"id": "int-1", "organization_id": "org-1", "type": "slack",
        "settings": {"team_id": "T01", "team_name": "Acme", "bot_user_id": "U01", "scopes": ["chat:write"]},
        "created_at": "2026-10-26T15:59:44Z"}], "slack_available": true})
}

fn run_integration(args: &[&str], path: &str, response: Value) -> String {
    let mut command = vec!["--org", "team alpha"];
    command.extend_from_slice(args);
    let (output, _, _) = common::run_cli(
        &[
            ("GET /v1/organizations".into(), "200 OK", organizations()),
            (format!("GET {path}"), "200 OK", response),
        ],
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
fn integration_list_preserves_json_and_renders_workspaces() {
    let result = run_integration(
        &["--json", "integration", "list"],
        INTEGRATIONS_PATH,
        integrations_response(),
    );
    assert_eq!(
        serde_json::from_str::<Value>(&result).unwrap(),
        integrations_response()
    );

    let text = run_integration(
        &["integration", "list"],
        INTEGRATIONS_PATH,
        integrations_response(),
    );
    let row = text.lines().find(|line| line.contains("int-1")).unwrap();
    assert!(
        row.contains("slack") && row.contains("Acme (T01)"),
        "{text}"
    );

    let empty = run_integration(
        &["integration", "list"],
        INTEGRATIONS_PATH,
        json!({"integrations": [], "slack_available": true}),
    );
    assert!(empty.contains("rtree integration add slack"), "{empty}");
}

fn config_with_saved_ids() -> Value {
    json!({
        "token": "rt_fixture",
        "default_organization": "team", "default_organization_id": common::ORG_ID,
        "cluster": "prod", "cluster_id": common::CLUSTER_ID,
    })
}

#[test]
fn saved_ids_need_no_lookup_requests() {
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/databases".to_string(),
            "200 OK",
            json!({"databases": []}),
        )],
        &["--json", "database", "list"],
        &config_with_saved_ids(),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(saved, config_with_saved_ids());
}

#[test]
fn an_org_flag_ignores_both_saved_ids_and_resolves_by_name() {
    let (output, _, _) = common::run_cli(
        &[
            common::key_organization("other"),
            common::bound_cluster("prod"),
            (
                "GET /v1/databases".to_string(),
                "200 OK",
                json!({"databases": []}),
            ),
        ],
        &["--json", "--org", "other", "database", "list"],
        &config_with_saved_ids(),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

const REFRESH_HINT: &str = "saved organization or cluster ID was rejected";

fn cluster_not_found() -> (String, &'static str, Value) {
    (
        "GET /v1/databases".to_string(),
        "404 Not Found",
        json!({"message": "Cluster not found."}),
    )
}

#[test]
fn a_saved_id_the_credential_can_no_longer_see_fails_with_a_refresh_hint() {
    let other_organization = (
        "GET /v1/organizations".to_string(),
        "200 OK",
        json!({"organizations": [{"organization_id": "00000000-0000-4000-8000-000000000002", "name": "other", "role": null}]}),
    );
    let other_cluster = (
        "GET /v1/clusters".to_string(),
        "200 OK",
        json!({"clusters": [{"id": "00000000-0000-4000-8000-0000000000c2", "name": "other"}]}),
    );
    for responses in [
        vec![cluster_not_found(), other_organization],
        vec![
            cluster_not_found(),
            common::key_organization("team"),
            other_cluster,
        ],
    ] {
        let (output, saved, _) = common::run_cli(
            &responses,
            &["--json", "database", "list"],
            &config_with_saved_ids(),
        );
        assert_eq!(output.status.code(), Some(4));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(REFRESH_HINT), "{stderr}");
        assert!(stderr.contains("rtree organization use"));
        assert_eq!(saved, config_with_saved_ids());
    }
}

#[test]
fn other_refusals_with_valid_saved_ids_get_no_refresh_hint() {
    for (args, refusal) in [
        (
            vec!["--json", "table", "describe", "--database", "db", "missing"],
            (
                "GET /v1/tables/missing?database=db".to_string(),
                "404 Not Found",
                json!({"message": "Table not found."}),
            ),
        ),
        (
            vec!["--json", "workflow", "list"],
            (
                "GET /v1/workflows".to_string(),
                "403 Forbidden",
                json!({"message": "Forbidden", "hint": "Workflow API access requires an admin API key."}),
            ),
        ),
    ] {
        let (output, _, _) = common::run_cli(
            &[
                refusal,
                common::key_organization("team"),
                common::bound_cluster("prod"),
            ],
            &args,
            &config_with_saved_ids(),
        );
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.contains(REFRESH_HINT), "{stderr}");
    }
}

#[test]
fn configs_with_only_names_keep_resolving_by_name() {
    let mut config = config_with_saved_ids();
    let fields = config.as_object_mut().unwrap();
    fields.remove("default_organization_id");
    fields.remove("cluster_id");
    let (output, _, _) = common::run_cli(
        &[
            common::key_organization("team"),
            common::bound_cluster("prod"),
            (
                "GET /v1/databases".to_string(),
                "200 OK",
                json!({"databases": []}),
            ),
        ],
        &["--json", "database", "list"],
        &config,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn organization_use_saves_the_id_and_drops_the_previous_clusters_id() {
    let (output, saved, _) = common::run_cli(
        &[common::key_organization("team")],
        &["--json", "organization", "use", "team"],
        &json!({"token": "rt_fixture", "default_organization": "old", "default_organization_id": "old-id", "cluster": "prod", "cluster_id": common::CLUSTER_ID}),
    );
    assert!(output.status.success());
    assert_eq!(saved["default_organization"], "team");
    assert_eq!(saved["default_organization_id"], common::ORG_ID);
    assert_eq!(saved["cluster"], "prod");
    assert!(saved.get("cluster_id").is_none());
}

#[test]
fn organization_use_without_a_resolvable_id_saves_only_the_name() {
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/organizations".to_string(),
            "503 Service Unavailable",
            json!({"message": "down"}),
        )],
        &["--json", "organization", "use", "team"],
        &json!({"token": "rt_fixture", "default_organization": "old", "default_organization_id": "old-id"}),
    );
    assert!(output.status.success());
    assert_eq!(saved["default_organization"], "team");
    assert!(saved.get("default_organization_id").is_none());
}

#[test]
fn cluster_list_uses_the_saved_organization_id_without_a_name_lookup() {
    let (output, _, _) = common::run_cli(
        &[(
            format!("GET /v1/regions?organization_id={}", common::ORG_ID),
            "200 OK",
            json!({"regions": []}),
        )],
        &["--json", "cluster", "list"],
        &json!({
            "token": "user_fixture",
            "default_organization": "team", "default_organization_id": common::ORG_ID,
        }),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
