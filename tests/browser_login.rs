mod common;

use serde_json::{json, Value};
use std::process::Output;

type Response = (String, &'static str, Value);

fn original_config() -> Value {
    json!({
        "token": "old-test-credential",
        "email": "old@example.test",
        "default_organization": "old-org",
        "cluster": "old-cluster",
        "database": "old-db"
    })
}

fn device_start(expires_in: u64) -> Response {
    (
        "POST /v1/auth/cli/device/start".into(),
        "200 OK",
        json!({
            "device_code": "private-device-fixture",
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://example.test/device",
            "verification_uri_complete": "https://example.test/device?code=ABCD-EFGH",
            "expires_in": expires_in,
            "interval": 1
        }),
    )
}

fn approved_login() -> Vec<Response> {
    vec![
        device_start(600),
        (
            "POST /v1/auth/cli/device/token".into(),
            "200 OK",
            json!({"token": "new-session-fixture", "user_id": "user-1", "email": "user@example.test"}),
        ),
    ]
}

fn organizations(names: &[&str]) -> Response {
    (
        "GET /v1/organizations".into(),
        "200 OK",
        json!({"organizations": names.iter().map(|name| json!({"name": name, "role": "owner"})).collect::<Vec<_>>()}),
    )
}

fn clusters(names: &[&str]) -> Response {
    (
        "GET /v1/clusters?organization=team".into(),
        "200 OK",
        json!({"clusters": names.iter().map(|name| json!({"name": name})).collect::<Vec<_>>()}),
    )
}

fn databases(names: &[&str]) -> Response {
    (
        "GET /v1/databases?organization=team&cluster=production".into(),
        "200 OK",
        json!({"databases": names.iter().map(|name| json!({"name": name})).collect::<Vec<_>>()}),
    )
}

fn approval_event(expires_in: u64) -> Value {
    json!({
        "event": "device_approval_required",
        "verification_uri": "https://example.test/device",
        "verification_uri_complete": "https://example.test/device?code=ABCD-EFGH",
        "user_code": "ABCD-EFGH",
        "expires_in": expires_in
    })
}

fn stderr_records(output: &Output) -> Vec<Value> {
    for stream in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(stream);
        for secret in [
            "private-device-fixture",
            "new-session-fixture",
            "old-test-credential",
        ] {
            assert!(!text.contains(secret), "credential leaked in CLI output");
        }
    }
    String::from_utf8(output.stderr.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("stderr must contain only JSON records"))
        .collect()
}

fn assert_saved_authentication(saved: &Value) {
    assert_eq!(saved["token"], "new-session-fixture");
    assert_eq!(saved["email"], "user@example.test");
    assert!(saved["default_organization"].is_null());
    assert!(saved["cluster"].is_null());
    assert!(saved["database"].is_null());
}

#[test]
fn bare_login_saves_authentication_without_resource_discovery() {
    let (output, saved, _) = common::run_cli(&approved_login(), &["login"], &original_config());
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    for field in [
        "selected_organization",
        "selected_cluster",
        "selected_database",
    ] {
        assert!(result[field].is_null());
    }
    assert_eq!(result["status"], "logged_in");
    assert_eq!(stderr_records(&output), vec![approval_event(600)]);
    assert_saved_authentication(&saved);
}

#[test]
fn login_stops_at_the_last_requested_selector() {
    for with_cluster in [false, true] {
        let mut responses = approved_login();
        responses.push(organizations(&["team", "other"]));
        let mut args = vec!["login", "--org", "team"];
        if with_cluster {
            responses.push(clusters(&["production", "staging"]));
            args.extend(["--cluster", "production"]);
        }
        let (output, saved, _) = common::run_cli(&responses, &args, &original_config());
        assert!(output.status.success());
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["selected_organization"], "team");
        assert_eq!(
            result["selected_cluster"],
            if with_cluster {
                json!("production")
            } else {
                Value::Null
            }
        );
        assert!(result["selected_database"].is_null());
        assert_eq!(saved["default_organization"], "team");
        assert_eq!(saved["cluster"], result["selected_cluster"]);
        assert!(saved["database"].is_null());
    }
}

#[test]
fn missing_parent_returns_choices_and_retains_authentication() {
    for names in [vec![], vec!["team", "other"]] {
        let mut responses = approved_login();
        responses.push(organizations(&names));
        let (output, saved, _) = common::run_cli(
            &responses,
            &["login", "--cluster", "production"],
            &original_config(),
        );
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let records = stderr_records(&output);
        assert_eq!(records[1]["needs"], "org");
        assert_eq!(records[1]["orgs"], json!(names));
        assert_eq!(records[1]["authentication_saved"], true);
        assert_saved_authentication(&saved);
    }
    for names in [vec![], vec!["production", "staging"]] {
        let mut responses = approved_login();
        responses.extend([organizations(&["team"]), clusters(&names)]);
        let (output, saved, _) = common::run_cli(
            &responses,
            &["login", "--database", "analytics"],
            &original_config(),
        );
        assert_eq!(output.status.code(), Some(2));
        let records = stderr_records(&output);
        assert_eq!(records[1]["needs"], "cluster");
        assert_eq!(records[1]["clusters"], json!(names));
        assert_eq!(records[1]["authentication_saved"], true);
        assert_saved_authentication(&saved);
    }
}

#[test]
fn approval_is_reported_before_the_server_completes_login() {
    let mut responses = approved_login();
    responses.extend([
        organizations(&["team"]),
        clusters(&["production"]),
        databases(&["analytics"]),
    ]);
    let (output, saved, bodies) = common::run_cli_with_progress(
        &responses,
        &[
            "login",
            "--org",
            "team",
            "--cluster",
            "production",
            "--database",
            "analytics",
        ],
        &original_config(),
        Some(|line| {
            assert_eq!(
                serde_json::from_str::<Value>(line).unwrap(),
                approval_event(600)
            );
        }),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(stderr_records(&output), vec![approval_event(600)]);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({
            "email": "user@example.test", "status": "logged_in", "method": "browser",
            "selected_organization": "team", "selected_cluster": "production", "selected_database": "analytics"
        })
    );
    assert_eq!(bodies[1], json!({"device_code": "private-device-fixture"}));
    assert_eq!(saved["token"], "new-session-fixture");
    assert_eq!(saved["default_organization"], "team");
    assert_eq!(saved["cluster"], "production");
    assert_eq!(saved["database"], "analytics");
}

#[test]
fn explicit_json_and_selectors_complete_login_with_no_browser() {
    let mut responses = approved_login();
    responses.extend([
        organizations(&["other", "team"]),
        clusters(&["staging", "production"]),
        databases(&["billing", "analytics"]),
    ]);
    let (output, saved, _) = common::run_cli(
        &responses,
        &[
            "login",
            "--json",
            "--no-browser",
            "--org",
            "team",
            "--cluster",
            "production",
            "--database",
            "analytics",
        ],
        &original_config(),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "logged_in");
    assert_eq!(saved["token"], "new-session-fixture");
    assert_eq!(saved["database"], "analytics");
    assert_eq!(stderr_records(&output), vec![approval_event(600)]);
}

#[test]
fn invalid_selectors_retain_authentication_without_saving_defaults() {
    for flag in ["--org", "--cluster", "--database"] {
        let mut responses = approved_login();
        responses.push(organizations(&["team"]));
        if flag != "--org" {
            responses.push(clusters(&["production"]));
        }
        if flag == "--database" {
            responses.push(databases(&["analytics"]));
        }
        let original = original_config();
        let (output, saved, _) =
            common::run_cli(&responses, &["login", flag, "missing"], &original);
        assert_eq!(output.status.code(), Some(4));
        assert!(output.stdout.is_empty());
        let records = stderr_records(&output);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], approval_event(600));
        assert_eq!(records[1]["error"]["code"], "not_found");
        assert_eq!(records[1]["authentication_saved"], true);
        assert_saved_authentication(&saved);
    }
}

#[test]
fn discovery_errors_are_not_treated_as_empty_choices_or_success() {
    for stage in 0..3 {
        let mut responses = approved_login();
        let discovery = [
            organizations(&["team"]),
            clusters(&["production"]),
            databases(&["analytics"]),
        ];
        responses.extend(discovery[..stage].iter().cloned());
        responses.push((
            discovery[stage].0.clone(),
            "503 Service Unavailable",
            json!({"message": "Unavailable"}),
        ));
        let original = original_config();
        let (output, saved, _) = common::run_cli(
            &responses,
            &[
                "login",
                "--org",
                "team",
                "--cluster",
                "production",
                "--database",
                "analytics",
            ],
            &original,
        );
        assert_eq!(output.status.code(), Some(3));
        assert!(output.stdout.is_empty());
        let records = stderr_records(&output);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["error"]["code"], "server_error");
        assert_eq!(records[1]["authentication_saved"], true);
        assert_saved_authentication(&saved);
    }
}

#[test]
fn approval_timeout_returns_json_without_saving_authentication() {
    let responses = [
        device_start(0),
        (
            "POST /v1/auth/cli/device/token".into(),
            "428 Precondition Required",
            json!({"error": "authorization_pending"}),
        ),
    ];
    let original = original_config();
    let (output, saved, _) = common::run_cli(&responses, &["login"], &original);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let records = stderr_records(&output);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], approval_event(0));
    assert!(records[1]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("timed out"));
    assert_eq!(saved, original);
}

#[test]
fn device_authentication_errors_preserve_config() {
    for failure_at_start in [true, false] {
        let mut responses = if failure_at_start {
            vec![]
        } else {
            vec![device_start(600)]
        };
        responses.push((
            if failure_at_start {
                "POST /v1/auth/cli/device/start".into()
            } else {
                "POST /v1/auth/cli/device/token".into()
            },
            "403 Forbidden",
            json!({"error": "access_denied", "message": "Approval denied"}),
        ));
        let original = original_config();
        let (output, saved, _) = common::run_cli(&responses, &["login"], &original);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let records = stderr_records(&output);
        if failure_at_start {
            assert_eq!(records.len(), 1);
        } else {
            assert_eq!(records.len(), 2);
            assert_eq!(records[0], approval_event(600));
        }
        assert_eq!(records.last().unwrap()["error"]["code"], "auth_error");
        assert_eq!(saved, original);
    }
}

#[test]
fn api_key_login_defaults_to_json_without_device_approval() {
    let responses = [("GET /v1/keys".into(), "200 OK", json!({"keys": []}))];
    let (output, saved, _) = common::run_cli(
        &responses,
        &["login", "--api-key", "rt_test_fixture"],
        &original_config(),
    );
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["success"],
        true
    );
    assert!(output.stderr.is_empty());
    assert_eq!(saved["token"], "rt_test_fixture");
}

#[test]
fn one_approval_supports_discovery_and_defaults_across_processes() {
    for org_names in [vec![], vec!["team", "other"]] {
        let mut responses = approved_login();
        responses.push(organizations(&org_names));
        let mut commands: Vec<&[&str]> = vec![&["login"], &["organization", "list", "--json"]];
        if !org_names.is_empty() {
            commands.extend([
                &["organization", "use", "team", "--json"][..],
                &["cluster", "list", "--json"],
                &["cluster", "use", "production", "--json"],
                &["database", "list", "--json"],
                &["database", "use", "analytics", "--json"],
            ]);
            let mut cluster_list = clusters(&["production", "staging"]);
            cluster_list.2["default_organization"] = json!("team");
            // The public cluster list contract also carries cluster metadata.
            cluster_list.2["clusters"] = json!([
                {"id":"cluster-1", "name":"production", "status":{"phase":"ready", "ready":true}, "created_at":"2026-10-04", "can_pause":true, "can_resume":false, "idle_timeout_minutes":30},
                {"id":"cluster-2", "name":"staging", "status":{"phase":"ready", "ready":true}, "created_at":"2026-10-04", "can_pause":true, "can_resume":false, "idle_timeout_minutes":30}
            ]);
            responses.extend([cluster_list, databases(&["analytics", "billing"])]);
        }
        let (outputs, configs, _) =
            common::run_cli_sequence(&responses, &commands, &original_config());
        for output in &outputs {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let _: Value = serde_json::from_slice(&output.stdout).unwrap();
        }
        assert_saved_authentication(&configs[0]);
        assert_eq!(stderr_records(&outputs[0]), vec![approval_event(600)]);
        for output in &outputs[1..] {
            assert!(output.stderr.is_empty());
        }
        for config in &configs {
            assert_eq!(config["token"], "new-session-fixture");
        }
        if !org_names.is_empty() {
            assert_eq!(configs[6]["default_organization"], "team");
            assert_eq!(configs[6]["cluster"], "production");
            assert_eq!(configs[6]["database"], "analytics");
        }
    }
}

#[test]
fn password_login_also_saves_authentication_without_defaults() {
    let responses = [(
        "POST /v1/auth/login".into(),
        "200 OK",
        json!({"token":"new-session-fixture","email":"user@example.test"}),
    )];
    let (output, saved, _) = common::run_cli(
        &responses,
        &[
            "login",
            "--email",
            "user@example.test",
            "--password",
            "test-password",
        ],
        &original_config(),
    );
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_saved_authentication(&saved);
}

#[test]
fn a_selection_failure_does_not_require_another_login_for_discovery() {
    let mut responses = approved_login();
    responses.extend([
        (
            "GET /v1/organizations".into(),
            "503 Service Unavailable",
            json!({"message":"Unavailable"}),
        ),
        organizations(&["team", "other"]),
    ]);
    let commands: Vec<&[&str]> = vec![
        &["login", "--org", "team"],
        &["organization", "list", "--json"],
    ];
    let (outputs, configs, _) = common::run_cli_sequence(&responses, &commands, &original_config());
    assert_eq!(outputs[0].status.code(), Some(3));
    assert_eq!(stderr_records(&outputs[0])[1]["authentication_saved"], true);
    assert!(outputs[1].status.success());
    assert_saved_authentication(&configs[0]);
    assert_saved_authentication(&configs[1]);
}

#[test]
fn a_requested_database_can_resolve_single_parents() {
    let mut responses = approved_login();
    responses.extend([
        organizations(&["team"]),
        clusters(&["production"]),
        databases(&["analytics", "billing"]),
    ]);
    let (output, saved, _) = common::run_cli(
        &responses,
        &["login", "--database", "analytics"],
        &original_config(),
    );
    assert!(output.status.success());
    assert_eq!(saved["default_organization"], "team");
    assert_eq!(saved["cluster"], "production");
    assert_eq!(saved["database"], "analytics");
}
