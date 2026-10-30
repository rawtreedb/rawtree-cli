mod common;

use serde_json::{json, Value};

const ORGANIZATION_ID: &str = "00000000-0000-4000-8000-000000000001";

fn organizations() -> Value {
    json!({"organizations": [
        {"organization_id": "00000000-0000-4000-8000-000000000002", "name": "other"},
        {"organization_id": ORGANIZATION_ID, "name": "team alpha"}
    ]})
}

fn account() -> Value {
    json!({"id": "00000000-0000-4000-8000-000000000003", "organization_id": ORGANIZATION_ID,
        "name": "build-jobs", "description": null, "role": "member"})
}

#[test]
fn create_resolves_selected_organization_and_sends_default_or_explicit_fields() {
    for explicit in [false, true] {
        let mut args = vec![
            "--json",
            "--org",
            "team alpha",
            "service-account",
            "create",
            "build-jobs",
        ];
        let mut expected = json!({"name": "build-jobs", "role": "member"});
        let mut response = account();
        if explicit {
            args.extend(["--role", "admin", "--description", "CI workloads"]);
            expected["role"] = json!("admin");
            expected["description"] = json!("CI workloads");
            response["role"] = json!("admin");
            response["description"] = json!("CI workloads");
        }
        let config = json!({"token": "session_fixture", "default_organization": "other", "cluster": "ignored", "database": "ignored"});
        let (output, saved, bodies) = common::run_cli(
            &[
                ("GET /v1/organizations".into(), "200 OK", organizations()),
                (
                    format!("POST /v1/organizations/{ORGANIZATION_ID}/service-accounts"),
                    "200 OK",
                    response.clone(),
                ),
            ],
            &args,
            &config,
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
        assert_eq!(bodies[1], expected);
        assert_eq!(saved, config);
    }
}

#[test]
fn list_uses_saved_organization_and_preserves_all_account_fields() {
    for accounts in [json!([]), json!([account()])] {
        let config = json!({"token": "session_fixture", "default_organization": "team alpha", "cluster": "ignored"});
        let (output, saved, _) = common::run_cli(
            &[
                ("GET /v1/organizations".into(), "200 OK", organizations()),
                (
                    format!("GET /v1/organizations/{ORGANIZATION_ID}/service-accounts"),
                    "200 OK",
                    accounts.clone(),
                ),
            ],
            &["--json", "service-account", "list"],
            &config,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({"service_accounts": accounts})
        );
        assert_eq!(saved, config);
    }
}

#[test]
fn missing_organization_never_creates_in_another_organization() {
    let (output, _, bodies) = common::run_cli(
        &[("GET /v1/organizations".into(), "200 OK", organizations())],
        &[
            "--json",
            "--org",
            "missing",
            "service-account",
            "create",
            "build-jobs",
        ],
        &json!({}),
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Organization 'missing' was not found")
    );
    assert_eq!(bodies.len(), 1);
}

#[test]
fn service_account_errors_are_reported_without_retrying_creation() {
    for (status, message) in [
        ("401 Unauthorized", "User credential required"),
        ("403 Forbidden", "Organization administrator required"),
        ("409 Conflict", "Service account already exists"),
    ] {
        let (output, _, bodies) = common::run_cli(
            &[
                ("GET /v1/organizations".into(), "200 OK", organizations()),
                (
                    format!("POST /v1/organizations/{ORGANIZATION_ID}/service-accounts"),
                    status,
                    json!({"message": message}),
                ),
            ],
            &[
                "--json",
                "--org",
                "team alpha",
                "service-account",
                "create",
                "build-jobs",
            ],
            &json!({}),
        );
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(message));
        assert_eq!(bodies.len(), 2);
    }
}

#[test]
fn invalid_input_is_rejected_before_network_requests() {
    let long_name = "a".repeat(129);
    let long_description = "é".repeat(1001);
    for input in [
        vec![""],
        vec!["Build"],
        vec!["a--b"],
        vec!["a\r"],
        vec![&long_name],
        vec!["build", "--role", "owner"],
        vec!["build", "--description", &long_description],
    ] {
        let mut args = vec!["service-account", "create"];
        args.extend(input);
        let (output, _, bodies) = common::run_cli(&[], &args, &json!({}));
        assert!(!output.status.success());
        assert!(bodies.is_empty());
    }
}
