mod common;
use serde_json::{json, Value};
const ORG: &str = "00000000-0000-4000-8000-000000000001";
fn catalog() -> Value {
    json!({"organizations":[{"name":"other","organization_id":"other"},{"name":"team alpha","organization_id":ORG}]})
}
fn issuer_args() -> Vec<&'static str> {
    vec![
        "--name",
        "ci",
        "--issuer-url",
        "https://EXAMPLE.com/tenant/",
    ]
}
fn rule_args() -> Vec<&'static str> {
    vec![
        "--name",
        "ci",
        "--issuer-id",
        ORG,
        "--allowed-audiences",
        "rawtree",
        "--allowed-audiences",
        "second",
        "--claim-conditions",
        r#"{"sub":"repo:acme/app:ref:refs/heads/main","label":"a=b"}"#,
        "--principal-type",
        "service_account",
        "--principal-id",
        ORG,
        "--database",
        "warehouse",
        "--enabled",
        "false",
    ]
}
#[test]
fn issuer_crud_uses_org_uuid_and_preserves_complete_json() {
    for action in ["list", "get", "create", "update", "delete"] {
        let mut args = vec!["--json", "--org", "team alpha", "federation-issuer", action];
        let suffix = if matches!(action, "get" | "update" | "delete") {
            args.push("id/with space");
            "/id%2Fwith%20space"
        } else {
            ""
        };
        if matches!(action, "create" | "update") {
            args.extend(issuer_args());
        }
        let method = match action {
            "create" => "POST",
            "update" => "PUT",
            "delete" => "DELETE",
            _ => "GET",
        };
        let response = match action {
            "list" => json!([]),
            "delete" => Value::Null,
            _ => {
                json!({"id":"saved","organization_id":ORG,"name":"ci","created_at":"date","jwks_url":null})
            }
        };
        let config =
            json!({"default_organization":"other","cluster":"ignored","database":"ignored"});
        let (out, saved, bodies) = common::run_cli(
            &[
                ("GET /v1/organizations".into(), "200 OK", catalog()),
                (
                    format!("{method} /v1/organizations/{ORG}/federation-issuers{suffix}"),
                    "200 OK",
                    response.clone(),
                ),
            ],
            &args,
            &config,
        );
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&out.stdout).unwrap(),
            response
        );
        assert_eq!(saved, config);
        if matches!(action, "create" | "update") {
            assert_eq!(
                bodies[1],
                json!({"name":"ci","description":null,"issuer_url":"https://EXAMPLE.com/tenant/","jwks_source":"discovery","jwks_url":null})
            );
        }
    }
}
#[test]
fn rules_use_full_put_and_exact_scope_for_both_access_modes() {
    for roles in [false, true] {
        for action in ["list", "get", "create", "update", "delete"] {
            let mut args = vec![
                "--json",
                "--org",
                "team alpha",
                "--cluster",
                "prod/eu",
                "federation-rule",
                action,
            ];
            let suffix = if matches!(action, "get" | "update" | "delete") {
                args.push("rule/id");
                "/rule%2Fid"
            } else {
                ""
            };
            if matches!(action, "create" | "update") {
                args.extend(rule_args());
                args.extend(if roles {
                    vec!["--database-roles", "reader", "--database-roles", "writer"]
                } else {
                    vec!["--permission", "read_only"]
                });
            }
            let method = match action {
                "create" => "POST",
                "update" => "PUT",
                "delete" => "DELETE",
                _ => "GET",
            };
            let response = if action == "list" {
                json!([])
            } else if action == "delete" {
                Value::Null
            } else {
                json!({"id":"rule","principal_type":"service_account","principal_id":ORG})
            };
            let config = json!({"token":"rt_fixture","default_organization":"wrong","cluster":"wrong","database":"ignored"});
            let (out,saved,bodies)=common::run_cli(&[(format!("{method} /v1/federation-rules{suffix}?organization=team%20alpha&cluster=prod%2Feu"),"200 OK",response.clone())],&args,&config);
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            assert_eq!(saved, config);
            assert_eq!(
                serde_json::from_slice::<Value>(&out.stdout).unwrap(),
                response
            );
            if matches!(action, "create" | "update") {
                let body = &bodies[0];
                assert_eq!(body["database"], "warehouse");
                assert_eq!(body["enabled"], false);
                assert_eq!(body["claim_conditions"]["label"], "a=b");
                assert_eq!(body["allowed_audiences"], json!(["rawtree", "second"]));
                if roles {
                    assert_eq!(body["database_roles"], json!(["reader", "writer"]));
                    assert!(body.get("permission").is_none());
                } else {
                    assert_eq!(body["permission"], "read_only");
                    assert!(body.get("database_roles").is_none());
                }
            }
        }
    }
}
#[test]
fn invalid_or_incomplete_inputs_do_not_send_requests() {
    let mut missing_access = vec![
        "--org",
        "team",
        "--cluster",
        "prod",
        "federation-rule",
        "create",
    ];
    missing_access.extend(rule_args());
    let mut conflicting = missing_access.clone();
    conflicting.extend(["--permission", "admin", "--database-roles", "reader"]);
    for args in [
        missing_access,
        conflicting,
        vec!["federation-rule", "list"],
        vec!["--org", "team", "federation-rule", "list"],
        vec![
            "federation-issuer",
            "create",
            "--name",
            "ci",
            "--issuer-url",
            "https://example.com",
            "--jwks-source",
            "explicit_url",
        ],
        vec!["federation-rule", "update", "id", "--name", "ci"],
    ] {
        let (out, _, bodies) = common::run_cli(&[], &args, &json!({}));
        assert!(!out.status.success());
        assert!(bodies.is_empty());
    }
}
#[test]
fn rule_writes_require_an_explicit_enabled_state() {
    for action in ["create", "update"] {
        let mut args = vec![
            "--org",
            "team",
            "--cluster",
            "prod",
            "federation-rule",
            action,
        ];
        if action == "update" {
            args.push("rule-id");
        }
        let fields = rule_args();
        let enabled = fields
            .iter()
            .position(|field| *field == "--enabled")
            .unwrap();
        args.extend(&fields[..enabled]);
        args.extend(&fields[enabled + 2..]);
        args.extend(["--permission", "read_only"]);
        let (out, _, bodies) = common::run_cli(&[], &args, &json!({}));
        assert!(!out.status.success());
        let error = String::from_utf8_lossy(&out.stderr);
        assert!(
            error.contains("required arguments were not provided"),
            "{error}"
        );
        assert!(error.contains("--enabled <ENABLED>"), "{error}");
        assert!(bodies.is_empty());
    }
}

#[test]
fn unknown_org_and_api_errors_are_not_retried_or_retargeted() {
    let (out, _, bodies) = common::run_cli(
        &[("GET /v1/organizations".into(), "200 OK", catalog())],
        &["--org", "missing", "federation-issuer", "list"],
        &json!({}),
    );
    assert!(!out.status.success());
    assert_eq!(bodies.len(), 1);
    for status in [
        "401 Unauthorized",
        "403 Forbidden",
        "404 Not Found",
        "409 Conflict",
    ] {
        let (out, _, bodies) = common::run_cli(
            &[(
                "DELETE /v1/federation-rules/id?organization=team&cluster=prod".into(),
                status,
                json!({"message":"Denied"}),
            )],
            &[
                "--org",
                "team",
                "--cluster",
                "prod",
                "federation-rule",
                "delete",
                "id",
            ],
            &json!({"token":"rt_fixture"}),
        );
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("Denied"));
        assert_eq!(bodies.len(), 1);
    }
}
