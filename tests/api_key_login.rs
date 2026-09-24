mod common;

use serde_json::{json, Value};
use std::process::Output;

fn login(responses: &[(&str, &str, Value)], selectors: &[&str]) -> (Output, Value, Value) {
    let original = json!({
        "token": "old-test-credential", "email": "old@example.test",
        "default_organization": "old-org", "cluster": "old-cluster", "database": "old-db"
    });
    let responses = responses
        .iter()
        .map(|(path, status, body)| (format!("GET {path}"), *status, body.clone()))
        .collect::<Vec<_>>();
    let mut args = vec!["--api-key", "rt_login_fixture", "--json", "login"];
    args.extend_from_slice(selectors);
    let (output, saved, _) = common::run_cli(&responses, &args, &original);
    (output, saved, original)
}

#[test]
fn api_key_login_accepts_current_responses_and_replaces_stale_defaults() {
    for fallback in [false, true] {
        for explicit in [false, true] {
            let query = if explicit {
                "?database=analytics&organization=team%20alpha&cluster=prod%2Feu"
            } else {
                ""
            };
            let keys = format!("/v1/keys{query}");
            let tables = format!("/v1/tables{query}");
            let responses = if fallback {
                vec![
                    (
                        keys.as_str(),
                        "403 Forbidden",
                        json!({"message": "Admin required"}),
                    ),
                    (tables.as_str(), "200 OK", json!({"tables": []})),
                ]
            } else {
                vec![(keys.as_str(), "200 OK", json!({"keys": []}))]
            };
            let selectors = if explicit {
                vec![
                    "--org",
                    "team alpha",
                    "--cluster",
                    "prod/eu",
                    "--database",
                    "analytics",
                ]
            } else {
                vec![]
            };
            let (output, saved, _) = login(&responses, &selectors);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["success"], true);
            assert_eq!(saved["token"], "rt_login_fixture");
            assert_eq!(saved["email"], Value::Null);
            for (field, config_field, selected) in [
                ("organization", "default_organization", "team alpha"),
                ("cluster", "cluster", "prod/eu"),
                ("database", "database", "analytics"),
            ] {
                let expected = if explicit {
                    json!(selected)
                } else {
                    Value::Null
                };
                assert_eq!(result[field], expected);
                assert_eq!(saved[config_field], expected);
            }
        }
    }
}

#[test]
fn api_key_login_failures_preserve_saved_credentials() {
    for (status, body) in [
        ("401 Unauthorized", json!({"message": "Invalid API key"})),
        (
            "403 Forbidden",
            json!({"message": "Key is not scoped to the requested cluster"}),
        ),
        ("200 OK", json!({})),
    ] {
        let (output, saved, original) = login(
            &[
                ("/v1/keys?cluster=other", status, body.clone()),
                ("/v1/tables?cluster=other", status, body),
            ],
            &["--cluster", "other"],
        );
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("validation_failed"));
        assert_eq!(saved, original);
    }
}

#[test]
fn api_key_login_preserves_legacy_response_metadata() {
    let (output, saved, _) = login(
        &[(
            "/v1/keys",
            "200 OK",
            json!({
                "keys": [], "organization": {"name": "team"}, "database": {"name": "analytics"}
            }),
        )],
        &[],
    );
    assert!(output.status.success());
    assert_eq!(saved["default_organization"], "team");
    assert_eq!(saved["database"], "analytics");
}
