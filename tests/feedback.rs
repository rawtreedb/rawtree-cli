mod common;

use serde_json::{json, Value};

fn receipt() -> Value {
    json!({"id":"b2b27b86-f2eb-4059-86f9-b99c29f05980","created_at":"2026-11-08T12:34:56Z"})
}

#[test]
fn submits_both_authors_and_all_categories_without_discovery_or_defaults() {
    for actor in ["human", "agent"] {
        for category in ["bug", "feature_request", "question", "other"] {
            let config = json!({"token":"user_session", "default_organization":"saved", "cluster":"saved", "database":"saved"});
            let extra = json!({"tool":"query", "nested":{"array":[1,true,null,"café"]}, "user_id":"untrusted", "actor_type":"human"});
            let (output, saved, bodies) = common::run_cli(
                &[("POST /v1/feedback".into(), "201 Created", receipt())],
                &[
                    "feedback",
                    "--message",
                    "Confusing error",
                    "--category",
                    category,
                    "--actor-type",
                    actor,
                    "--extra",
                    &extra.to_string(),
                    "--json",
                ],
                &config,
            );
            assert!(output.status.success(), "{:?}", output);
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                receipt()
            );
            assert_eq!(saved, config);
            assert_eq!(
                bodies,
                vec![
                    json!({"message":"Confusing error","category":category,"actor_type":actor,"extra":extra})
                ]
            );
        }
    }
}

#[test]
fn optional_scope_and_extra_are_preserved_and_human_output_acknowledges_receipt() {
    let org = "47ce36ed-fada-442a-b69d-0ecc9bbc5af4";
    let cluster = "98e74d8b-d7f3-4ae2-a6ad-b21f10640ad7";
    for (fields, query) in [
        (vec![], String::new()),
        (vec!["--organization-id", org], String::new()),
        (vec!["--cluster-id", cluster], String::new()),
        (
            vec!["--organization-id", org, "--cluster-id", cluster],
            format!("?organization_id={org}&cluster_id={cluster}"),
        ),
    ] {
        let mut args = vec![
            "feedback",
            "--message",
            "Report",
            "--category",
            "bug",
            "--actor-type",
            "human",
        ];
        args.extend_from_slice(&fields);
        let (output, _, bodies) = common::run_cli(
            &[(
                format!("POST /v1/feedback{query}"),
                "201 Created",
                receipt(),
            )],
            &args,
            &json!({"token":"rt_fixture"}),
        );
        assert!(output.status.success(), "{:?}", output);
        assert!(String::from_utf8(output.stdout)
            .unwrap()
            .contains(receipt()["id"].as_str().unwrap()));
        let mut expected = json!({"message":"Report", "category":"bug", "actor_type":"human"});
        for pair in fields.chunks_exact(2) {
            expected[pair[0].trim_start_matches("--").replace('-', "_")] = json!(pair[1]);
        }
        assert_eq!(bodies, vec![expected]);
    }
}

#[test]
fn requires_authorship_and_valid_json_object_before_network_requests() {
    let base = [
        "feedback",
        "--message",
        "Report",
        "--category",
        "bug",
        "--actor-type",
        "agent",
    ];
    for extra in ["not-json", "null", "[]", "1", "true", "\"text\""] {
        let mut args = base.to_vec();
        args.extend(["--extra", extra]);
        let (output, _, bodies) = common::run_cli(&[], &args, &json!({}));
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("JSON object"));
        assert!(bodies.is_empty());
    }
    for args in [
        vec!["feedback", "--message", "Report", "--category", "bug"],
        vec!["feedback", "--message", "Report", "--actor-type", "human"],
        vec![
            "feedback",
            "--message",
            "Report",
            "--category",
            "bug",
            "--actor-type",
            "user",
        ],
        vec![
            "feedback",
            "--message",
            "Report",
            "--category",
            "invalid",
            "--actor-type",
            "agent",
        ],
    ] {
        let (output, _, bodies) = common::run_cli(&[], &args, &json!({}));
        assert!(!output.status.success());
        assert!(bodies.is_empty());
    }
}

#[test]
fn enforces_utf8_message_and_total_body_limits() {
    for message in [String::new(), " \n\t".into(), "é".repeat(8193)] {
        let (output, _, bodies) = common::run_cli(
            &[],
            &[
                "feedback",
                "--message",
                &message,
                "--category",
                "bug",
                "--actor-type",
                "agent",
            ],
            &json!({}),
        );
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("message must be nonblank"));
        assert!(bodies.is_empty());
    }
    let (output, _, _) = common::run_cli(
        &[("POST /v1/feedback".into(), "201 Created", receipt())],
        &[
            "feedback",
            "--message",
            &"é".repeat(8192),
            "--category",
            "bug",
            "--actor-type",
            "agent",
        ],
        &json!({"token":"rt_fixture"}),
    );
    assert!(output.status.success(), "{:?}", output);
    let (output, _, bodies) = common::run_cli(
        &[],
        &[
            "feedback",
            "--message",
            "Report",
            "--category",
            "bug",
            "--actor-type",
            "agent",
            "--extra",
            &json!({"large":"x".repeat(32768)}).to_string(),
        ],
        &json!({}),
    );
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr).unwrap().contains("32 KiB"));
    assert!(bodies.is_empty());
}

#[test]
fn api_failures_are_not_retried_or_reported_as_success() {
    for status in [
        "400 Bad Request",
        "401 Unauthorized",
        "403 Forbidden",
        "413 Payload Too Large",
        "429 Too Many Requests",
        "503 Service Unavailable",
    ] {
        let (output, _, bodies) = common::run_cli(
            &[(
                "POST /v1/feedback".into(),
                status,
                json!({"message":"Feedback rejected"}),
            )],
            &[
                "feedback",
                "--message",
                "Report",
                "--category",
                "bug",
                "--actor-type",
                "agent",
            ],
            &json!({"token":"rt_fixture"}),
        );
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("Feedback rejected"));
        assert_eq!(bodies.len(), 1);
    }
}

#[test]
fn help_explains_agent_observations_and_human_requests() {
    let (output, _, bodies) = common::run_cli(&[], &["feedback", "--help"], &json!({}));
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--actor-type agent"));
    assert!(help.contains("--actor-type human"));
    assert!(help.contains("inconsistencies"));
    assert!(help.contains("JSON object"));
    assert!(bodies.is_empty());
}
