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

fn assert_selection(responses: &[Response], args: &[&str], expected: Value) {
    let original = original_config();
    let (output, saved, _) = common::run_cli(responses, args, &original);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        expected
    );
    assert_eq!(stderr_records(&output), vec![approval_event(600)]);
    assert_eq!(saved, original);
}

#[test]
fn missing_organization_returns_available_choices_without_saving_authentication() {
    for names in [vec![], vec!["team", "other"]] {
        let mut responses = approved_login();
        responses.push(organizations(&names));
        assert_selection(
            &responses,
            &["login"],
            json!({"needs": "org", "orgs": names}),
        );
    }
}

#[test]
fn missing_cluster_returns_choices_in_the_selected_organization() {
    for names in [vec![], vec!["production", "staging"]] {
        let mut responses = approved_login();
        responses.extend([organizations(&["team", "other"]), clusters(&names)]);
        assert_selection(
            &responses,
            &["login", "--org", "team"],
            json!({"needs": "cluster", "organization": "team", "clusters": names}),
        );
    }
}

#[test]
fn missing_database_returns_choices_in_the_selected_cluster() {
    for names in [vec![], vec!["analytics", "billing"]] {
        let mut responses = approved_login();
        responses.extend([
            organizations(&["team"]),
            clusters(&["staging", "production"]),
            databases(&names),
        ]);
        assert_selection(
            &responses,
            &["login", "--org", "team", "--cluster", "production"],
            json!({"needs": "database", "organization": "team", "cluster": "production", "databases": names}),
        );
    }
}

#[test]
fn approval_is_reported_before_polling_completes_and_only_once() {
    let mut responses = approved_login();
    responses.insert(
        1,
        (
            "POST /v1/auth/cli/device/token".into(),
            "428 Precondition Required",
            json!({"error": "authorization_pending"}),
        ),
    );
    responses.extend([
        organizations(&["team"]),
        clusters(&["production"]),
        databases(&["analytics"]),
    ]);
    let (output, saved, bodies) = common::run_cli_with_progress(
        &responses,
        &["login"],
        &original_config(),
        Some(common::ProgressExpectation {
            before_response: "POST /v1/auth/cli/device/token",
            observe: |line| {
                assert_eq!(
                    serde_json::from_str::<Value>(line).unwrap(),
                    approval_event(600)
                );
            },
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
fn invalid_selectors_return_json_errors_and_preserve_config() {
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
        assert_eq!(saved, original);
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
        let (output, saved, _) = common::run_cli(&responses, &["login"], &original);
        assert_eq!(output.status.code(), Some(3));
        assert!(output.stdout.is_empty());
        let records = stderr_records(&output);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["error"]["code"], "server_error");
        assert_eq!(saved, original);
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
