mod common;

use serde_json::{json, Value};

fn configured() -> Value {
    json!({"token":"session-fixture", "default_organization":"team", "cluster":"production", "database":"analytics"})
}

fn clusters(names: &[&str]) -> Value {
    json!({"clusters":names.iter().map(|name| json!({
        "id":format!("id-{name}"), "name":name, "created_at":"2026-10-04",
        "status":{"phase":"provisioning", "ready":false},
        "can_pause":false, "can_resume":false, "idle_timeout_minutes":30
    })).collect::<Vec<_>>()})
}

#[test]
fn status_resolves_flags_environment_defaults_and_positional_selectors_without_saving() {
    let cases: &[(&[&str], &[(&str, &str)], &str)] = &[
        (&["cluster", "status"], &[], "production"),
        (
            &["cluster", "status"],
            &[("RAWTREE_CLUSTER", "staging")],
            "staging",
        ),
        (
            &["cluster", "status", "--cluster", "staging"],
            &[("RAWTREE_CLUSTER", "production")],
            "staging",
        ),
        (
            &["cluster", "status", "staging"],
            &[("RAWTREE_CLUSTER", "production")],
            "staging",
        ),
        (
            &["cluster", "status", "staging", "--cluster", "staging"],
            &[],
            "staging",
        ),
        (&["cluster", "status", "id-staging"], &[], "staging"),
    ];
    for (args, environment, expected) in cases {
        let original = configured();
        let mut args = args.to_vec();
        args.push("--json");
        let (output, saved, _) = common::run_cli_with_env(
            &[(
                "GET /v1/clusters?organization=team".into(),
                "200 OK",
                clusters(&["production", "staging"]),
            )],
            &args,
            &original,
            environment,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["name"], *expected);
        assert_eq!(result["status"]["phase"], "provisioning");
        assert_eq!(saved, original);
    }
}

#[test]
fn status_rejects_conflicting_explicit_selectors_before_any_api_request() {
    for json_mode in [false, true] {
        let mut args = vec!["cluster", "status", "production", "--cluster", "staging"];
        if json_mode {
            args.push("--json");
        }
        let original = json!({"token":"session-fixture"});
        let (output, saved, _) = common::run_cli(&[], &args, &original);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        if json_mode {
            let error: Value = serde_json::from_slice(&output.stderr).unwrap();
            assert_eq!(error["error"]["code"], "context_conflict");
        } else {
            assert!(String::from_utf8_lossy(&output.stderr)
                .contains("positional cluster and --cluster values differ"));
        }
        assert_eq!(saved, original);
    }
}

#[test]
fn status_parent_overrides_drop_saved_child_and_encode_the_organization() {
    for environment in [vec![], vec![("RAWTREE_ORG", "team alpha")]] {
        let args = if environment.is_empty() {
            vec!["cluster", "status", "--org", "team alpha", "--json"]
        } else {
            vec!["cluster", "status", "--json"]
        };
        let original = configured();
        let (output, saved, _) = common::run_cli_with_env(
            &[(
                "GET /v1/clusters?organization=team%20alpha".into(),
                "200 OK",
                clusters(&["prod/eu"]),
            )],
            &args,
            &original,
            &environment,
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["name"], "prod/eu");
        assert_eq!(saved, original);
    }
}

#[test]
fn status_can_discover_single_parents_but_returns_choices_when_ambiguous() {
    let original = json!({"token":"session-fixture"});
    let organizations = json!({"organizations":[{"name":"team", "role":"owner"}]});
    for names in [vec!["production"], vec!["production", "staging"]] {
        let (output, saved, _) = common::run_cli(
            &[
                (
                    "GET /v1/organizations".into(),
                    "200 OK",
                    organizations.clone(),
                ),
                (
                    "GET /v1/clusters?organization=team".into(),
                    "200 OK",
                    clusters(&names),
                ),
            ],
            &["cluster", "status", "--json"],
            &original,
        );
        if names.len() == 1 {
            assert!(output.status.success());
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["name"], "production");
        } else {
            assert_eq!(output.status.code(), Some(2));
            let error: Value = serde_json::from_slice(&output.stderr).unwrap();
            assert_eq!(error["needs"], "cluster");
            assert_eq!(error["organization"], "team");
            assert_eq!(error["clusters"], json!(names));
        }
        assert_eq!(saved, original);
    }
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/organizations".into(),
            "200 OK",
            json!({"organizations":[
                {"name":"team", "role":"owner"}, {"name":"other", "role":"owner"}
            ]}),
        )],
        &["cluster", "status", "production", "--json"],
        &original,
    );
    assert_eq!(output.status.code(), Some(2));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["needs"], "org");
    assert_eq!(error["orgs"], json!(["team", "other"]));
    assert_eq!(saved, original);
}

#[test]
fn empty_cluster_context_names_the_organization_and_gives_a_creation_command() {
    for command in [
        vec!["database", "list"],
        vec!["cluster", "status"],
        vec!["cluster", "list"],
    ] {
        for json_mode in [false, true] {
            let mut args = command.clone();
            if json_mode {
                args.push("--json");
            }
            let original = json!({"token":"session-fixture", "default_organization":"team alpha"});
            let (output, saved, _) = common::run_cli(
                &[(
                    "GET /v1/clusters?organization=team%20alpha".into(),
                    "200 OK",
                    clusters(&[]),
                )],
                &args,
                &original,
            );
            if command[1] == "list" && command[0] == "cluster" {
                assert!(output.status.success());
                if json_mode {
                    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
                    assert_eq!(result["clusters"], json!([]));
                } else {
                    assert!(String::from_utf8_lossy(&output.stdout)
                        .contains("rtree cluster create --org 'team alpha'"));
                }
            } else {
                assert_eq!(output.status.code(), Some(2));
                assert!(output.stdout.is_empty());
                let message = if json_mode {
                    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
                    assert_eq!(error["needs"], "cluster");
                    assert_eq!(error["organization"], "team alpha");
                    assert_eq!(error["clusters"], json!([]));
                    assert_eq!(error["error"]["code"], "selection_required");
                    error["error"]["message"].as_str().unwrap().to_string()
                } else {
                    String::from_utf8(output.stderr).unwrap()
                };
                assert!(message.contains("Organization 'team alpha' has no clusters."));
                assert!(message.contains("rtree cluster create --org 'team alpha'"));
            }
            assert_eq!(saved, original);
        }
    }
}

#[test]
fn named_missing_cluster_is_not_an_empty_or_ambiguous_selection() {
    let original = configured();
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/clusters?organization=team".into(),
            "200 OK",
            clusters(&[]),
        )],
        &["cluster", "status", "--cluster", "missing", "--json"],
        &original,
    );
    assert_eq!(output.status.code(), Some(4));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "cluster_not_found");
    assert!(error.get("needs").is_none());
    assert_eq!(saved, original);

    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/databases?organization=team&cluster=missing".into(),
            "404 Not Found",
            json!({"error":"cluster_not_found", "message":"Cluster 'missing' not found."}),
        )],
        &["database", "list", "--cluster", "missing", "--json"],
        &original,
    );
    assert_eq!(output.status.code(), Some(4));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "not_found");
    assert!(error.get("needs").is_none());
    assert_eq!(saved, original);
}

#[test]
fn unready_database_errors_keep_domain_code_hint_and_saved_defaults() {
    let original = configured();
    for command in [
        vec!["database", "list"],
        vec!["database", "create", "new-db"],
    ] {
        for json_mode in [false, true] {
            let mut args = vec!["--org", "team alpha", "--cluster", "prod/eu"];
            args.extend_from_slice(&command);
            if json_mode {
                args.push("--json");
            }
            let method = if command[1] == "list" { "GET" } else { "POST" };
            let (output, saved, bodies) = common::run_cli(
                &[(
                    format!("{method} /v1/databases?organization=team%20alpha&cluster=prod%2Feu"),
                    "503 Service Unavailable",
                    json!({
                        "error":"cluster_not_ready",
                        "message":"Cluster 'prod/eu' is not ready. Phase: provisioning.",
                        "hint":"Wait for the cluster to become ready, then try again."
                    }),
                )],
                &args,
                &original,
            );
            assert_eq!(output.status.code(), Some(3));
            assert!(output.stdout.is_empty());
            let message = if json_mode {
                let error: Value = serde_json::from_slice(&output.stderr).unwrap();
                assert_eq!(error["error"]["code"], "cluster_not_ready");
                assert_eq!(error["exit_code"], 3);
                assert_eq!(
                    error["error"]["message"],
                    "Cluster 'prod/eu' is not ready. Phase: provisioning."
                );
                error["hint"].as_str().unwrap().to_string()
            } else {
                String::from_utf8(output.stderr).unwrap()
            };
            assert!(message.contains("Wait for the cluster to become ready, then try again."));
            assert!(message.contains("rtree cluster status --org 'team alpha' --cluster 'prod/eu'"));
            assert_eq!(
                bodies[0],
                if method == "POST" {
                    json!({"name":"new-db"})
                } else {
                    Value::Null
                }
            );
            assert_eq!(saved, original);
        }
    }
}

#[test]
fn unready_api_key_errors_work_without_scope_or_a_server_hint() {
    let original = json!({"token":"rt_fixture"});
    let (output, saved, _) = common::run_cli(
        &[(
            "GET /v1/databases".into(),
            "503 Service Unavailable",
            json!({
                "error":"cluster_not_ready", "message":"Cluster 'production' is not ready. Phase: provisioning."
            }),
        )],
        &["database", "list", "--json"],
        &original,
    );
    assert_eq!(output.status.code(), Some(3));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "cluster_not_ready");
    assert!(error.get("hint").is_none());
    assert_eq!(saved, original);
}

#[test]
fn old_and_unrelated_server_errors_keep_their_existing_output() {
    for response in [
        json!({"message":"Request failed", "hint":"Try again or contact support."}),
        json!({"error":"internal_server_error", "message":"Request failed", "hint":"Try again or contact support."}),
    ] {
        let original = configured();
        let (output, saved, _) = common::run_cli(
            &[(
                "GET /v1/databases?organization=team&cluster=production".into(),
                "503 Service Unavailable",
                response,
            )],
            &["database", "list", "--json"],
            &original,
        );
        assert_eq!(output.status.code(), Some(3));
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(
            error,
            json!({"error":{
            "message":"Server error (503): Request failed\nHint: Try again or contact support.", "code":"server_error"
        }, "exit_code":3})
        );
        assert_eq!(saved, original);
    }
}

#[test]
fn status_transport_errors_keep_the_generic_server_contract() {
    for (status, server_code, message, hint) in [
        (
            "503 Service Unavailable",
            "cluster_status_unavailable",
            "Cluster status is temporarily unavailable.",
            "Try again shortly.",
        ),
        (
            "500 Internal Server Error",
            "internal_error",
            "Internal server error",
            "Try again or contact support.",
        ),
    ] {
        let original = configured();
        // The CLI reads status from the cluster list.
        // This fixture checks transport errors, not the explicit status route.
        let (output, saved, _) = common::run_cli(
            &[(
                "GET /v1/clusters?organization=team".into(),
                status,
                json!({"error": server_code, "message": message, "hint": hint}),
            )],
            &["cluster", "status", "production", "--json"],
            &original,
        );
        assert_eq!(output.status.code(), Some(3));
        assert!(output.stdout.is_empty());
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        let status_code = status.split_whitespace().next().unwrap();
        assert_eq!(
            error,
            json!({
                "error": {
                    "code": "server_error",
                    "message": format!("Server error ({status_code}): {message}\nHint: {hint}"),
                },
                "exit_code": 3,
            })
        );
        assert_ne!(error["error"]["code"], "cluster_not_ready");
        assert_eq!(saved, original);
    }
}

#[test]
fn organization_creation_keeps_defaults_and_gives_a_safe_selection_hint() {
    for json_mode in [false, true] {
        let original = configured();
        let mut args = vec![
            "organization",
            "create",
            "team's sandbox",
            "--org",
            "other",
            "--cluster",
            "staging",
        ];
        if json_mode {
            args.push("--json");
        }
        let (output, saved, bodies) = common::run_cli(
            &[(
                "POST /v1/organizations".into(),
                "200 OK",
                json!({"name":"team's sandbox"}),
            )],
            &args,
            &original,
        );
        assert!(output.status.success());
        let message = if json_mode {
            let result: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result["name"], "team's sandbox");
            result["hint"].as_str().unwrap().to_string()
        } else {
            String::from_utf8(output.stdout).unwrap()
        };
        assert!(message.contains("Saved defaults did not change."));
        assert!(message.contains("rtree organization use 'team'\"'\"'s sandbox'"));
        assert_eq!(bodies[0], json!({"organization_name":"team's sandbox"}));
        assert_eq!(saved, original);
    }
}
