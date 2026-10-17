mod common;

use serde_json::{json, Value};

const PATH: &str = "POST /v1/query-format?organization=team&cluster=prod";

fn config() -> Value {
    json!({"token": "rt_fixture", "default_organization": "team", "cluster": "prod"})
}

#[test]
fn formats_complete_positional_sql_with_optional_database() {
    let sql = "  -- café\nselect 'a\\b'; INSERT INTO t VALUES (1), (  \n";
    let formatted = "-- café\nSELECT 'a\\b';\n\nINSERT INTO t VALUES (1), (  \n";
    let (output, saved, bodies) = common::run_cli(
        &[(PATH.to_string(), "200 OK", json!({"formatted": formatted}))],
        &["format", sql],
        &config(),
    );
    assert!(output.status.success(), "{:?}", output);
    assert_eq!(output.stdout, formatted.as_bytes());
    assert_eq!(bodies, vec![json!({"sql": sql})]);
    assert_eq!(saved, config());
}

#[test]
fn json_output_and_scope_overrides_preserve_the_document() {
    let formatted = "SELECT 1;\n\n";
    let (output, _, bodies) = common::run_cli(
        &[(
            "POST /v1/query-format?database=analytics%20db&organization=team%20alpha&cluster=prod%2Feu".to_string(),
            "200 OK",
            json!({"formatted": formatted}),
        )],
        &["format", "--sql", "select 1", "--database", "analytics db", "--org", "team alpha", "--cluster", "prod/eu", "--json"],
        &config(),
    );
    assert!(output.status.success(), "{:?}", output);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"formatted": formatted})
    );
    assert_eq!(bodies, vec![json!({"sql": "select 1"})]);
}

#[test]
fn preserves_large_sql_from_stdin_and_complete_output() {
    let sql = format!(
        "  INSERT INTO t VALUES ('{}'), (  \n",
        "café\\\"".repeat(200_000)
    );
    let formatted = format!(
        "INSERT INTO t VALUES ('{}'), (  \n",
        "café\\\"".repeat(200_000)
    );
    for args in [
        vec!["format", "-"],
        vec!["format"],
        vec!["format", "--sql", "-"],
    ] {
        let responses = [(PATH.to_string(), "200 OK", json!({"formatted": formatted}))];
        let (output, _, bodies) = common::run_cli_with_stdin(&responses, &args, &config(), &sql);
        assert!(output.status.success(), "{:?}", output.status);
        assert_eq!(output.stdout, formatted.as_bytes());
        assert_eq!(bodies, vec![json!({"sql": sql})]);
    }
}

#[test]
fn uses_saved_database_scope() {
    let mut configuration = config();
    configuration["database"] = json!("saved db");
    let (output, _, _) = common::run_cli(
        &[(
            "POST /v1/query-format?database=saved%20db&organization=team&cluster=prod".to_string(),
            "200 OK",
            json!({"formatted": "SELECT 1"}),
        )],
        &["format", "select 1"],
        &configuration,
    );
    assert!(output.status.success());
    assert_eq!(output.stdout, b"SELECT 1");
}

#[test]
fn surfaces_api_errors_without_partial_output_or_query_fallback() {
    for status in [
        "400 Bad Request",
        "401 Unauthorized",
        "403 Forbidden",
        "408 Request Timeout",
        "413 Payload Too Large",
        "503 Service Unavailable",
    ] {
        let (output, _, bodies) = common::run_cli(
            &[(
                PATH.to_string(),
                status,
                json!({"message": "Formatting failed", "hint": "Try a smaller selection"}),
            )],
            &["format", "select 1"],
            &config(),
        );
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(&status[..3]), "{stderr}");
        assert!(stderr.contains("Formatting failed"), "{stderr}");
        assert!(stderr.contains("Try a smaller selection"), "{stderr}");
        assert_eq!(bodies.len(), 1);
    }
}

#[test]
fn rejects_invalid_response_conflicting_inputs_and_removed_options() {
    let (output, _, _) = common::run_cli(
        &[(PATH.to_string(), "200 OK", json!({"formatted": null}))],
        &["format", "select 1"],
        &config(),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());

    for args in [
        vec!["format", "select 1", "--sql", "select 2"],
        vec!["format", "--file", "query.sql"],
        vec!["query-format", "select 1"],
    ] {
        let (output, _, bodies) = common::run_cli(&[], &args, &config());
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(bodies.is_empty());
    }
}
