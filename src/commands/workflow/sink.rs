use std::io::Read;

use anyhow::{Context, Result};
use serde_json::{json, Value};

use super::{describe_sink, WorkflowScope};
use crate::client::ApiClient;
use crate::output;

const HELP_HINT: &str = "Run `rtree workflow sink --help` for sink commands and their help.";

pub fn create(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sink: &str,
    json_mode: bool,
) -> Result<()> {
    let body = read_object(sink, "sink")?;
    let value: Value = client
        .post(&scope.workflow_path(workflow_id, "/sinks"), &body)
        .map_err(with_help_hint)?;
    print_sink(
        &value,
        json_mode,
        &format!("Sink added to workflow '{workflow_id}':"),
    );
    Ok(())
}

pub fn update(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sink_id: &str,
    changes: &str,
    json_mode: bool,
) -> Result<()> {
    let body = read_object(changes, "changes")?;
    let value: Value = client
        .patch(&sink_path(scope, workflow_id, sink_id), &body)
        .map_err(with_help_hint)?;
    print_sink(&value, json_mode, &format!("Sink '{sink_id}' updated:"));
    Ok(())
}

pub fn delete(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sink_id: &str,
    json_mode: bool,
) -> Result<()> {
    client.delete_no_content(&sink_path(scope, workflow_id, sink_id))?;
    output::print_result(
        &json!({"workflow_id": workflow_id, "sink_id": sink_id, "deleted": true}),
        json_mode,
        |_| println!("Sink '{sink_id}' removed from workflow '{workflow_id}'."),
    );
    Ok(())
}

fn sink_path(scope: &WorkflowScope, workflow_id: &str, sink_id: &str) -> String {
    scope.workflow_path(
        workflow_id,
        &format!("/sinks/{}", urlencoding::encode(sink_id)),
    )
}

fn print_sink(sink: &Value, json_mode: bool, heading: &str) {
    output::print_result(sink, json_mode, |sink| {
        println!("{heading}");
        println!("  {}", describe_sink(sink));
    });
}

/// Accepts inline JSON, `@path`, or `-` for stdin; each holds a sink object or
/// an array of them.
pub(super) fn read_json_args(args: &[String]) -> Result<Vec<Value>> {
    let mut sinks = Vec::new();
    for (index, arg) in args.iter().enumerate() {
        match read_json(arg, "--sink", &format!("--sink #{}", index + 1))? {
            (_, sink @ Value::Object(_)) => sinks.push(sink),
            (_, Value::Array(items)) if items.iter().all(Value::is_object) => sinks.extend(items),
            (label, _) => {
                return Err(invalid(format!(
                    "invalid {label}: expected a JSON object or an array of objects. {HELP_HINT}"
                )))
            }
        }
    }
    Ok(sinks)
}

fn read_object(arg: &str, name: &str) -> Result<Value> {
    match read_json(arg, name, name)? {
        (_, value @ Value::Object(_)) => Ok(value),
        (label, _) => Err(invalid(format!(
            "invalid {label}: expected a JSON object. {HELP_HINT}"
        ))),
    }
}

/// Reads inline JSON, `@path`, or `-` for stdin, and returns it with the label
/// errors use for it.
fn read_json(arg: &str, name: &str, inline_label: &str) -> Result<(String, Value)> {
    let (label, text) = if arg == "-" {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .with_context(|| format!("failed to read {name} from stdin"))?;
        (format!("{name} from stdin"), text)
    } else if let Some(path) = arg.strip_prefix('@') {
        let text = std::fs::read_to_string(path)
            .map_err(|error| invalid(format!("failed to read {name} file '{path}': {error}")))?;
        (format!("{name} in '{path}'"), text)
    } else {
        // Inline JSON often carries header credentials, so errors name it
        // instead of echoing it.
        (inline_label.to_string(), arg.to_string())
    };
    match serde_json::from_str(&text) {
        Ok(value) => Ok((label, value)),
        Err(error) => Err(invalid(format!(
            "invalid {label}: not valid JSON ({error}). {HELP_HINT}"
        ))),
    }
}

fn invalid(message: String) -> anyhow::Error {
    output::coded_error("validation_error", message, 2)
}

/// The API names the field it rejects, so a rejected request that carried
/// sinks only needs a pointer to the shapes.
pub(super) fn with_help_hint(error: anyhow::Error) -> anyhow::Error {
    let message = format!("{error:#}");
    if message.contains("(400)") || message.contains("(422)") {
        anyhow::anyhow!("{message}\n{HELP_HINT}")
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_files_and_inline_json_accept_objects_and_arrays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sinks.json");
        std::fs::write(
            &path,
            r#"[{"type":"http","settings":{"url":"https://example.com"}}]"#,
        )
        .unwrap();
        let sinks = read_json_args(&[
            format!("@{}", path.display()),
            r#"{"type":"slack","settings":{"channel_id":"C0123456789"}}"#.into(),
        ])
        .unwrap();
        assert_eq!(sinks.len(), 2);

        let secret =
            r#"{"type":"http","settings":{"headers":{"Authorization":"Bearer secret-token"}"#;
        let err = read_json_args(&["{}".into(), secret.into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("--sink #2: not valid JSON"), "{err}");
        assert!(err.contains("rtree workflow sink --help"), "{err}");
        assert!(!err.contains("secret-token"), "{err}");
        for value in ["42", r#"["table"]"#] {
            let err = read_json_args(&[value.into()]).unwrap_err().to_string();
            assert!(err.contains("object or an array"), "{err}");
        }
        let err = read_json_args(&["@missing.json".into()]).unwrap_err();
        assert!(err.to_string().contains("missing.json"), "{err}");
        let coded = err.downcast_ref::<output::CliError>().unwrap();
        assert_eq!(coded.exit_code(), 2);
    }

    #[test]
    fn rejected_requests_point_to_help() {
        let rejected = anyhow::anyhow!("Server error (422): Invalid workflow.");
        assert!(with_help_hint(rejected).to_string().ends_with(HELP_HINT));
        let unavailable = anyhow::anyhow!("Server error (503): Workflow sinks are unavailable.");
        assert!(!with_help_hint(unavailable).to_string().contains(HELP_HINT));
    }
}
