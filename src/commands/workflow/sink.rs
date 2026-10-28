use std::io::Read;

use anyhow::{Context, Result};
use comfy_table::Cell;
use serde_json::{json, Value};

use super::super::table_output::new_cli_table;
use crate::cli::SinkTypeArg;
use crate::output;

/// Documents the platform's `MAX_SINKS`; the API enforces it.
const MAX_SINKS: usize = 5;

struct SinkField {
    name: &'static str,
    kind: &'static str,
    required: &'static str,
    description: &'static str,
}

struct SinkType {
    name: &'static str,
    description: &'static str,
    fields: &'static [SinkField],
    rules: &'static [&'static str],
    example: &'static str,
    existing_example: &'static str,
}

const SCHEMA_HINT: &str = "Run `rtree workflow sink-schema` for each sink type's fields.";

const IDENTIFIER_RULE: &str =
    "Letters, digits, and '_', starting with a letter; at most 64 characters";

const SINK_TYPES: [SinkType; 2] = [
    SinkType {
        name: "table",
        description: "Inserts the rows each run returns into a RawTree table.",
        fields: &[
            SinkField {
                name: "settings.database",
                kind: "string",
                required: "always",
                description: IDENTIFIER_RULE,
            },
            SinkField {
                name: "settings.table",
                kind: "string",
                required: "always",
                description: IDENTIFIER_RULE,
            },
        ],
        rules: &[
            "Each table sink in a workflow must target a different database and table.",
            "Existing table sinks are sent with their id and full settings.",
        ],
        example: r#"{"type":"table","settings":{"database":"analytics","table":"alerts"}}"#,
        existing_example: r#"{"type":"table","id":"<sink-id>","settings":{"database":"analytics","table":"alerts"}}"#,
    },
    SinkType {
        name: "http",
        description: "Sends the rows each run returns to a webhook, as one JSON array.",
        fields: &[
            SinkField {
                name: "settings.url",
                kind: "string",
                required: "when adding",
                description: "HTTP or HTTPS URL without user info or a fragment; at most 2048 bytes. The server rejects destinations it does not allow.",
            },
            SinkField {
                name: "settings.headers",
                kind: "object",
                required: "no",
                description: "Header names to values, at most 20. Values are write-only: responses list header_names only.",
            },
        ],
        rules: &[
            "An existing HTTP sink (sent with its id) may omit settings, settings.url, or settings.headers to keep them.",
            "In a supplied headers map, null keeps the stored value and omitted names are removed.",
            "URLs and header values cannot contain {{, %, or ${; percent-encoded URLs are not supported.",
        ],
        example: r#"{"type":"http","settings":{"url":"https://example.com/hook","headers":{"Authorization":"Bearer ..."}}}"#,
        existing_example: r#"{"type":"http","id":"<sink-id>"}"#,
    },
];

pub fn schema(sink_type_arg: Option<SinkTypeArg>, json_mode: bool) -> Result<()> {
    let types: Vec<&SinkType> = match sink_type_arg {
        Some(SinkTypeArg::Table) => vec![&SINK_TYPES[0]],
        Some(SinkTypeArg::Http) => vec![&SINK_TYPES[1]],
        None => SINK_TYPES.iter().collect(),
    };
    let value = json!({
        "max_sinks": MAX_SINKS,
        "common_fields": [
            {"name": "type", "type": "string", "required": "always", "description": "Sink type: table or http"},
            {"name": "id", "type": "string", "required": "no", "description": "Existing sink ID. Omit when adding a sink; include it to keep a sink when replacing the list."},
        ],
        "types": types.iter().map(|sink_type| json!({
            "type": sink_type.name,
            "description": sink_type.description,
            "fields": sink_type.fields.iter().map(|field| json!({
                "name": field.name,
                "type": field.kind,
                "required": field.required,
                "description": field.description,
            })).collect::<Vec<_>>(),
            "rules": sink_type.rules,
            "example": serde_json::from_str::<Value>(sink_type.example).expect("valid example"),
            "existing_example": serde_json::from_str::<Value>(sink_type.existing_example).expect("valid example"),
        })).collect::<Vec<_>>(),
    });
    output::print_result(&value, json_mode, |_| {
        println!(
            "Each sink is a JSON object with a \"type\", its \"settings\", and an \"id\" when it already exists."
        );
        println!("A workflow has at most {MAX_SINKS} sinks.");
        for sink_type in &types {
            println!();
            println!("{}: {}", sink_type.name, sink_type.description);
            let mut table = new_cli_table();
            table.set_header(vec!["field", "type", "required", "description"]);
            for field in sink_type.fields {
                table.add_row(vec![
                    Cell::new(field.name),
                    Cell::new(field.kind),
                    Cell::new(field.required),
                    Cell::new(field.description),
                ]);
            }
            println!("{table}");
            for rule in sink_type.rules {
                println!("  - {rule}");
            }
            println!("  New sink:");
            println!("    {}", sink_type.example);
            println!("  Keep an existing sink on `rtree workflow update`:");
            println!("    {}", sink_type.existing_example);
        }
    });
    Ok(())
}

/// Accepts inline JSON, `@path`, or `-` for stdin; each holds a sink object or
/// an array of them.
pub(super) fn read_json_args(args: &[String]) -> Result<Vec<Value>> {
    let mut sinks = Vec::new();
    for (index, arg) in args.iter().enumerate() {
        let (label, text) = if arg == "-" {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .context("failed to read sinks from stdin")?;
            ("from stdin".to_string(), text)
        } else if let Some(path) = arg.strip_prefix('@') {
            let text = std::fs::read_to_string(path)
                .map_err(|error| invalid(format!("failed to read sink file '{path}': {error}")))?;
            (format!("in '{path}'"), text)
        } else {
            // Inline JSON often carries header credentials, so errors name its
            // position instead of echoing it.
            (format!("#{}", index + 1), arg.clone())
        };
        match serde_json::from_str::<Value>(&text) {
            Ok(sink @ Value::Object(_)) => sinks.push(sink),
            Ok(Value::Array(items)) if items.iter().all(Value::is_object) => sinks.extend(items),
            Ok(_) => {
                return Err(invalid(format!(
                    "invalid --sink {label}: expected a JSON object or an array of objects, e.g. {}. {SCHEMA_HINT}",
                    SINK_TYPES[0].example
                )))
            }
            Err(error) => {
                return Err(invalid(format!(
                    "invalid --sink {label}: not valid JSON ({error}). {SCHEMA_HINT}"
                )))
            }
        }
    }
    Ok(sinks)
}

fn invalid(message: String) -> anyhow::Error {
    output::coded_error("validation_error", message, 2)
}

/// The API names the field it rejects, so a rejected request that carried
/// sinks only needs a pointer to the shapes.
pub(super) fn with_schema_hint(error: anyhow::Error) -> anyhow::Error {
    let message = format!("{error:#}");
    if message.contains("(400)") || message.contains("(422)") {
        anyhow::anyhow!("{message}\n{SCHEMA_HINT}")
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{SINK_HELP, SINK_UPDATE_HELP};

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
        assert!(err.contains("rtree workflow sink-schema"), "{err}");
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
    fn rejected_requests_point_to_the_schema() {
        let rejected = anyhow::anyhow!("Server error (422): Invalid workflow.");
        assert!(with_schema_hint(rejected)
            .to_string()
            .ends_with(SCHEMA_HINT));
        let unavailable = anyhow::anyhow!("Server error (503): Workflow sinks are unavailable.");
        assert!(!with_schema_hint(unavailable)
            .to_string()
            .contains(SCHEMA_HINT));
    }

    #[test]
    fn help_shows_each_sink_type_example() {
        for sink_type in &SINK_TYPES {
            serde_json::from_str::<Value>(sink_type.example).unwrap();
            serde_json::from_str::<Value>(sink_type.existing_example).unwrap();
            assert!(SINK_HELP.contains(sink_type.example), "{}", sink_type.name);
            assert!(
                SINK_UPDATE_HELP.contains(sink_type.existing_example),
                "{}",
                sink_type.name
            );
        }
    }
}
