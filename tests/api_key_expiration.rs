mod common;

use serde_json::{json, Value};

fn key(expiration: Option<Value>) -> Value {
    let mut key = json!({"id": "key-1", "token": "rt_test", "name": "ci", "permission": "read_only", "database": {"name": "default"}});
    if let Some(value) = expiration {
        key["expires_at"] = value;
    }
    key
}

#[test]
fn create_sends_expiration_and_preserves_normalized_response() {
    let input = "2027-01-01T02:00:00+02:00";
    let expected = key(Some(json!("2027-01-01T00:00:00Z")));
    let (output, _, bodies) = common::run_cli(
        &[(
            "POST /v1/keys?organization=team&cluster=prod".into(),
            "200 OK",
            expected.clone(),
        )],
        &[
            "--json",
            "key",
            "create",
            "--name",
            "ci",
            "--permission",
            "read_only",
            "--expires-at",
            input,
        ],
        &json!({"default_organization": "team", "cluster": "prod"}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        bodies,
        [json!({"name": "ci", "permission": "read_only", "expires_at": input})]
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        expected
    );
}

#[test]
fn create_and_list_display_expiration_or_never_in_text_and_json() {
    for expiration in [None, Some(Value::Null), Some(json!("2027-01-01T00:00:00Z"))] {
        for list in [false, true] {
            for json_mode in [false, true] {
                let mut response_key = key(expiration.clone());
                let (method, mut args, response) = if list {
                    response_key["created_at"] = json!("2026-10-01T00:00:00Z");
                    (
                        "GET",
                        vec!["key", "list"],
                        json!({"keys": [response_key.clone()]}),
                    )
                } else {
                    (
                        "POST",
                        vec!["key", "create", "--name", "ci", "--permission", "read_only"],
                        response_key.clone(),
                    )
                };
                if json_mode {
                    args.insert(0, "--json");
                }
                let (output, _, bodies) = common::run_cli(
                    &[(
                        format!("{method} /v1/keys?organization=team&cluster=prod"),
                        "200 OK",
                        response,
                    )],
                    &args,
                    &json!({"default_organization": "team", "cluster": "prod"}),
                );
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                if !list {
                    assert_eq!(bodies, [json!({"name": "ci", "permission": "read_only"})]);
                }
                if json_mode {
                    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
                    let actual_key = if list { &actual["keys"][0] } else { &actual };
                    assert!(actual_key.get("expires_at").is_some());
                    assert_eq!(
                        actual_key["expires_at"],
                        expiration.clone().unwrap_or(Value::Null)
                    );
                } else {
                    let text = String::from_utf8(output.stdout).unwrap();
                    assert!(text.contains("expires"));
                    assert!(
                        text.contains(
                            expiration
                                .as_ref()
                                .and_then(Value::as_str)
                                .unwrap_or("never")
                        ),
                        "{text}"
                    );
                }
            }
        }
    }
}

#[test]
fn create_surfaces_backend_expiration_validation_errors() {
    let (output, _, bodies) = common::run_cli(
        &[(
            "POST /v1/keys?organization=team&cluster=prod".into(),
            "400 Bad Request",
            json!({"error":"bad_request", "message":"Invalid expiration date.", "hint":"expires_at must be in the future."}),
        )],
        &[
            "key",
            "create",
            "--name",
            "ci",
            "--permission",
            "read_only",
            "--expires-at",
            "2000-01-01T00:00:00Z",
        ],
        &json!({"default_organization": "team", "cluster": "prod"}),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Invalid expiration date"));
    assert_eq!(bodies[0]["expires_at"], "2000-01-01T00:00:00Z");
}

#[test]
fn list_preserves_database_role_keys_and_expiration() {
    let response = json!({"keys": [{
        "id": "role-key", "token": "rt_hint", "name": "reporting",
        "database_roles": ["reader", "writer"], "database": {"name": "default"},
        "created_at": "2026-10-01T00:00:00Z", "expires_at": "2027-01-01T00:00:00Z"
    }]});
    for args in [vec!["--json", "key", "list"], vec!["key", "list"]] {
        let (output, _, _) = common::run_cli(
            &[(
                "GET /v1/keys?organization=team&cluster=prod".into(),
                "200 OK",
                response.clone(),
            )],
            &args,
            &json!({"default_organization": "team", "cluster": "prod"}),
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if args[0] == "--json" {
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                response
            );
        } else {
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains("reader, writer"));
            assert!(text.contains("expires=2027-01-01T00:00:00Z"));
        }
    }
}
