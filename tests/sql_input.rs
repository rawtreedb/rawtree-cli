mod common;

use serde_json::{json, Value};

fn config() -> Value {
    json!({"token": "rt_fixture", "database": "analytics", "default_organization": "team", "cluster": "prod"})
}

fn response(command: &str) -> (String, &'static str, Value) {
    let (endpoint, body) = match command {
        "query" => ("query", json!({"data": [{"n": 1}]})),
        "format" => ("query-format", json!({"formatted": "SELECT 1\n"})),
        _ => unreachable!(),
    };
    (
        format!("POST /v1/{endpoint}?database=analytics&organization=team&cluster=prod"),
        "200 OK",
        body,
    )
}

#[test]
fn both_commands_accept_positional_sql_and_all_named_forms() {
    let sql = "  SELECT 'café', 'a\\b';\nSELECT 2;\n";
    for command in ["query", "format"] {
        for input in [
            vec![sql],
            vec!["--sql", sql],
            vec!["--query", sql],
            vec!["-q", sql],
        ] {
            let mut args = vec![command, "--json"];
            args.extend(input);
            let response = response(command);
            let expected_output = response.2.clone();
            let (output, _, bodies) = common::run_cli(&[response], &args, &config());
            assert!(output.status.success(), "{args:?}: {output:?}");
            assert_eq!(bodies, vec![json!({"sql": sql})]);
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout).unwrap(),
                expected_output
            );
        }
    }
}

#[test]
fn both_commands_read_complete_sql_from_implicit_and_explicit_stdin() {
    let sql = "  -- Preserve whitespace and Unicode\nSELECT 'café';\n\n";
    for command in ["query", "format"] {
        for input in [
            vec![],
            vec!["-"],
            vec!["--sql", "-"],
            vec!["--query", "-"],
            vec!["-q", "-"],
        ] {
            let mut args = vec![command];
            args.extend(input);
            let (output, _, bodies) =
                common::run_cli_with_stdin(&[response(command)], &args, &config(), sql);
            assert!(output.status.success(), "{args:?}: {output:?}");
            assert_eq!(bodies, vec![json!({"sql": sql})]);
        }
    }
}

#[test]
fn both_commands_reject_multiple_sql_sources() {
    for command in ["query", "format"] {
        for input in [
            vec!["SELECT 1", "--sql", "SELECT 2"],
            vec!["SELECT 1", "--query", "SELECT 2"],
            vec!["SELECT 1", "-q", "SELECT 2"],
            vec!["--sql", "SELECT 1", "--query", "SELECT 2"],
            vec!["--sql", "SELECT 1", "-q", "SELECT 2"],
            vec!["--query", "SELECT 1", "-q", "SELECT 2"],
        ] {
            let mut args = vec![command];
            args.extend(input);
            let (output, _, bodies) = common::run_cli(&[], &args, &config());
            assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
            assert!(output.stdout.is_empty());
            assert!(bodies.is_empty());
        }
    }
}
