use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use anyhow::{Context, Result};
use comfy_table::Cell;
use serde_json::{json, Map, Value};

use super::super::table_output::new_cli_table;
use super::{describe_sink, Workflow, WorkflowScope};
use crate::cli::SinkTypeArg;
use crate::client::ApiClient;
use crate::output;

/// Sink input errors exit as validation errors, whatever their wording.
macro_rules! invalid {
    ($($arg:tt)*) => {
        return Err(output::coded_error("validation_error", format!($($arg)*), 2))
    };
}

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
    flags: &'static [&'static str],
}

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
        flags: &[
            "rtree workflow create ... --sink-table analytics.alerts",
            "rtree workflow sink create <workflow-id> --table analytics.alerts",
        ],
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
        flags: &[
            "rtree workflow create ... --sink-http https://example.com/hook --sink-header 'Authorization: Bearer ...'",
            "rtree workflow sink create <workflow-id> --http https://example.com/hook --header 'Authorization: Bearer ...'",
        ],
    },
];

fn sink_type(name: &str) -> Option<&'static SinkType> {
    SINK_TYPES.iter().find(|sink_type| sink_type.name == name)
}

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
            "flags": sink_type.flags,
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
            println!("  JSON:");
            println!("    {}", sink_type.example);
            println!("  Flags:");
            for flag in sink_type.flags {
                println!("    {flag}");
            }
        }
    });
    Ok(())
}

/// Sink flags of `rtree workflow create`, resolved once the workflow's
/// database is known.
pub struct SinkArgs {
    pub tables: Vec<String>,
    pub urls: Vec<String>,
    pub headers: Vec<String>,
    pub json: Vec<String>,
}

pub(super) fn build(args: &SinkArgs, default_database: &str) -> Result<Vec<Value>> {
    if !args.headers.is_empty() && args.urls.len() != 1 {
        invalid!("--sink-header applies to a single --sink-http; pass several HTTP sinks with headers as --sink JSON");
    }
    let mut sinks = args
        .tables
        .iter()
        .map(|target| Ok(table_sink(target, default_database)))
        .collect::<Result<Vec<_>>>()?;
    for url in &args.urls {
        sinks.push(http_sink(url, &args.headers)?);
    }
    sinks.extend(read_json_args(&args.json)?);
    validate(&sinks)?;
    Ok(sinks)
}

fn table_sink(target: &str, default_database: &str) -> Value {
    let (database, table) = target.split_once('.').unwrap_or((default_database, target));
    json!({"type": "table", "settings": {"database": database, "table": table}})
}

fn http_sink(url: &str, headers: &[String]) -> Result<Value> {
    let mut settings = Map::new();
    settings.insert("url".into(), json!(url));
    if !headers.is_empty() {
        let mut map = Map::new();
        for raw in headers {
            let (name, value) = parse_header(raw)?;
            set_header(&mut map, name, json!(value))?;
        }
        settings.insert("headers".into(), Value::Object(map));
    }
    Ok(json!({"type": "http", "settings": settings}))
}

/// Errors never echo the argument because it usually carries a credential.
fn parse_header(raw: &str) -> Result<(String, String)> {
    match raw.split_once(':') {
        Some((name, value)) if !name.trim().is_empty() => {
            Ok((name.trim().to_string(), value.trim().to_string()))
        }
        _ => {
            invalid!("invalid header: expected 'Name: value', e.g. 'Authorization: Bearer <token>'")
        }
    }
}

fn set_header(headers: &mut Map<String, Value>, name: String, value: Value) -> Result<()> {
    if headers
        .keys()
        .any(|existing| existing.eq_ignore_ascii_case(&name))
    {
        invalid!("header '{name}' is passed more than once");
    }
    headers.insert(name, value);
    Ok(())
}

/// Accepts inline JSON, `@path`, or `-` for stdin; each holds a sink object or
/// an array of them.
pub(super) fn read_json_args(args: &[String]) -> Result<Vec<Value>> {
    let mut sinks = Vec::new();
    for arg in args {
        let (label, text) = if arg == "-" {
            let mut text = String::new();
            std::io::stdin()
                .read_to_string(&mut text)
                .context("failed to read sinks from stdin")?;
            ("from stdin".to_string(), text)
        } else if let Some(path) = arg.strip_prefix('@') {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read sink file '{path}'"))?;
            (format!("in '{path}'"), text)
        } else {
            (format!("'{arg}'"), arg.clone())
        };
        match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(sink)) => sinks.push(Value::Object(sink)),
            Ok(Value::Array(items)) => sinks.extend(items),
            Ok(_) => invalid!(
                "invalid --sink {label}: expected a JSON object or an array of objects, e.g. {}. Run `rtree workflow sink schema` for every field.",
                SINK_TYPES[0].example
            ),
            Err(error) => invalid!(
                "invalid --sink {label}: not valid JSON ({error}). Run `rtree workflow sink schema` for the expected shape."
            ),
        }
    }
    Ok(sinks)
}

/// Checks only the shape of each sink so malformed input fails before a
/// request; values and limits are left to the API. Types this CLI does not
/// know are passed through.
pub(super) fn validate(sinks: &[Value]) -> Result<()> {
    for (index, sink) in sinks.iter().enumerate() {
        let kind = sink["type"].as_str().and_then(sink_type);
        if let Err(reason) = validate_shape(sink, kind) {
            let position = index + 1;
            match kind {
                Some(kind) => invalid!(
                    "invalid {} sink #{position}: {reason}.\nExpected, e.g.: {}\nRun `rtree workflow sink schema --type {}` for every field.",
                    kind.name, kind.example, kind.name
                ),
                None => invalid!(
                    "invalid sink #{position}: {reason}.\nRun `rtree workflow sink schema` for every sink type."
                ),
            }
        }
    }
    Ok(())
}

fn validate_shape(sink: &Value, kind: Option<&SinkType>) -> Result<(), String> {
    let Value::Object(fields) = sink else {
        return Err("expected a JSON object".into());
    };
    let Some(kind) = kind else {
        return match fields.get("type") {
            Some(Value::String(_)) => Ok(()),
            _ => Err(r#"missing "type"; expected "table" or "http""#.into()),
        };
    };
    reject_unknown(fields, &["type", "id", "settings"], "")?;
    let existing = match fields.get("id") {
        None => false,
        Some(Value::String(_)) => true,
        Some(_) => return Err(r#""id" must be a string"#.into()),
    };
    match (kind.name, fields.get("settings")) {
        ("table", Some(Value::Object(settings))) => {
            reject_unknown(settings, &["database", "table"], "settings.")?;
            for name in ["database", "table"] {
                if !settings.get(name).is_some_and(Value::is_string) {
                    return Err(format!("settings.{name} is required and must be a string"));
                }
            }
            Ok(())
        }
        ("table", _) => Err("settings must be an object with database and table".into()),
        ("http", None) if existing => Ok(()),
        ("http", Some(Value::Object(settings))) => {
            reject_unknown(settings, &["url", "headers"], "settings.")?;
            match settings.get("url") {
                Some(Value::String(_)) => {}
                Some(_) => return Err("settings.url must be a string".into()),
                None if existing => {}
                None => return Err("settings.url is required when adding an HTTP sink".into()),
            }
            match settings.get("headers") {
                None => Ok(()),
                Some(Value::Object(headers)) => headers
                    .iter()
                    .find(|(_, value)| !(value.is_string() || value.is_null()))
                    .map_or(Ok(()), |(name, _)| {
                        Err(format!("header '{name}' must be a string"))
                    }),
                Some(_) => {
                    Err("settings.headers must be an object of header names to values".into())
                }
            }
        }
        ("http", None) => Err("settings.url is required when adding an HTTP sink".into()),
        _ => Err("settings must be an object".into()),
    }
}

fn reject_unknown(
    fields: &Map<String, Value>,
    allowed: &[&str],
    prefix: &str,
) -> Result<(), String> {
    match fields.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(format!(
            "unknown field \"{prefix}{key}\"; expected {}",
            allowed
                .iter()
                .map(|name| format!("\"{prefix}{name}\""))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        None => Ok(()),
    }
}

fn load(client: &ApiClient, scope: &WorkflowScope, workflow_id: &str) -> Result<Workflow> {
    let value: Value = client.get(&scope.workflow_path(workflow_id, ""))?;
    serde_json::from_value(value).context("failed to parse server response")
}

/// The API replaces the whole list, so sinks that are not being changed are
/// sent back by ID. HTTP sinks omit settings, which keeps their URL and
/// write-only headers.
fn retained(sink: &Value) -> Value {
    match sink["type"].as_str() {
        Some("table") => json!({"type": "table", "id": sink["id"], "settings": {
            "database": sink["settings"]["database"],
            "table": sink["settings"]["table"],
        }}),
        _ => json!({"type": sink["type"], "id": sink["id"]}),
    }
}

fn find_sink<'a>(workflow: &'a Workflow, workflow_id: &str, sink_id: &str) -> Result<&'a Value> {
    workflow
        .sinks
        .iter()
        .find(|sink| sink["id"].as_str() == Some(sink_id))
        .ok_or_else(|| {
            output::coded_error(
                "not_found",
                format!(
                    "workflow '{}' has no sink '{sink_id}'. Run `rtree workflow sink list {workflow_id}` to see its sinks.",
                    workflow.name
                ),
                4,
            )
        })
}

fn replace_sinks(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sinks: Vec<Value>,
) -> Result<Workflow> {
    validate(&sinks)?;
    let value: Value = client.patch(
        &scope.workflow_path(workflow_id, ""),
        &json!({"sinks": sinks}),
    )?;
    serde_json::from_value(value).context("failed to parse server response")
}

pub fn list(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    json_mode: bool,
) -> Result<()> {
    let workflow = load(client, scope, workflow_id)?;
    output::print_result(&json!({"sinks": workflow.sinks}), json_mode, |_| {
        if workflow.sinks.is_empty() {
            println!(
                "Workflow '{}' has no sinks. Add one with `rtree workflow sink create {workflow_id} --table <table>`.",
                workflow.name
            );
            return;
        }
        let mut table = new_cli_table();
        table.set_header(vec!["type", "target", "headers", "id"]);
        for sink in &workflow.sinks {
            let settings = &sink["settings"];
            let (target, headers) = match sink["type"].as_str() {
                Some("table") => (
                    format!(
                        "{}.{}",
                        settings["database"].as_str().unwrap_or("?"),
                        settings["table"].as_str().unwrap_or("?")
                    ),
                    String::new(),
                ),
                Some("http") => (
                    settings["url"]
                        .as_str()
                        .unwrap_or("URL unavailable")
                        .to_string(),
                    header_names(sink).join(", "),
                ),
                _ => (settings.to_string(), String::new()),
            };
            table.add_row(vec![
                Cell::new(sink["type"].as_str().unwrap_or("?")),
                Cell::new(target),
                Cell::new(headers),
                Cell::new(sink["id"].as_str().unwrap_or("—")),
            ]);
        }
        println!("{table}");
    });
    Ok(())
}

fn header_names(sink: &Value) -> Vec<&str> {
    sink["settings"]["header_names"]
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

pub enum NewSink {
    Table(String),
    Http { url: String, headers: Vec<String> },
}

pub fn create(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    new_sink: NewSink,
    json_mode: bool,
) -> Result<()> {
    let workflow = load(client, scope, workflow_id)?;
    let new_sink = match new_sink {
        NewSink::Table(target) => table_sink(&target, &workflow.query.database),
        NewSink::Http { url, headers } => http_sink(&url, &headers)?,
    };
    let previous: BTreeSet<&str> = workflow
        .sinks
        .iter()
        .filter_map(|sink| sink["id"].as_str())
        .collect();
    let mut sinks: Vec<Value> = workflow.sinks.iter().map(retained).collect();
    sinks.push(new_sink);
    let updated = replace_sinks(client, scope, workflow_id, sinks)?;
    let created = updated
        .sinks
        .iter()
        .find(|sink| sink["id"].as_str().is_some_and(|id| !previous.contains(id)))
        .context("the server response did not include the new sink")?;
    output::print_result(created, json_mode, |sink| {
        println!("Sink added to workflow '{}':", updated.name);
        println!("  {}", describe_sink(sink));
    });
    Ok(())
}

pub struct SinkChanges {
    pub table: Option<String>,
    pub url: Option<String>,
    pub headers: Vec<String>,
    pub remove_headers: Vec<String>,
}

pub fn update(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sink_id: &str,
    changes: SinkChanges,
    json_mode: bool,
) -> Result<()> {
    let workflow = load(client, scope, workflow_id)?;
    let current = find_sink(&workflow, workflow_id, sink_id)?;
    let replacement = changed_sink(current, changes)?;
    let sinks = workflow
        .sinks
        .iter()
        .map(|sink| {
            if sink["id"].as_str() == Some(sink_id) {
                replacement.clone()
            } else {
                retained(sink)
            }
        })
        .collect();
    let updated = replace_sinks(client, scope, workflow_id, sinks)?;
    let sink = updated
        .sinks
        .iter()
        .find(|sink| sink["id"].as_str() == Some(sink_id))
        .context("the server response did not include the updated sink")?;
    output::print_result(sink, json_mode, |sink| {
        println!("Sink updated on workflow '{}':", updated.name);
        println!("  {}", describe_sink(sink));
    });
    Ok(())
}

fn changed_sink(current: &Value, changes: SinkChanges) -> Result<Value> {
    let id = current["id"].as_str().unwrap_or_default();
    let http_changes =
        changes.url.is_some() || !changes.headers.is_empty() || !changes.remove_headers.is_empty();
    match current["type"].as_str() {
        Some("table") if http_changes => {
            invalid!("sink {id} is a table sink; --url, --header, and --remove-header apply to HTTP sinks")
        }
        Some("table") => {
            let Some(target) = changes.table else {
                invalid!("nothing to update: pass --table for a table sink");
            };
            let database = current["settings"]["database"].as_str().unwrap_or_default();
            let mut sink = table_sink(&target, database);
            sink["id"] = json!(id);
            Ok(sink)
        }
        Some("http") if changes.table.is_some() => {
            invalid!("sink {id} is an HTTP sink; --table applies to table sinks")
        }
        Some("http") => {
            if !http_changes {
                invalid!("nothing to update: pass --url, --header, or --remove-header for an HTTP sink");
            }
            let mut settings = Map::new();
            if let Some(url) = changes.url {
                settings.insert("url".into(), json!(url));
            }
            if !changes.headers.is_empty() || !changes.remove_headers.is_empty() {
                settings.insert(
                    "headers".into(),
                    Value::Object(merged_headers(
                        &header_names(current),
                        &changes.headers,
                        &changes.remove_headers,
                    )?),
                );
            }
            Ok(json!({"type": "http", "id": id, "settings": settings}))
        }
        _ => invalid!("sink {id} has a type this CLI cannot edit; replace the list with `rtree workflow update --sink`"),
    }
}

/// A supplied headers map removes omitted names, so every stored header that
/// should survive is sent as null, which keeps its write-only value.
fn merged_headers(
    stored: &[&str],
    set: &[String],
    remove: &[String],
) -> Result<Map<String, Value>> {
    let stored_name = |name: &str| {
        stored
            .iter()
            .find(|stored| stored.eq_ignore_ascii_case(name))
            .map(|stored| stored.to_string())
    };
    for name in remove {
        if stored_name(name).is_none() {
            invalid!(
                "the sink has no header '{name}'; its headers are: {}",
                if stored.is_empty() {
                    "none".to_string()
                } else {
                    stored.join(", ")
                }
            );
        }
    }
    let mut parsed = BTreeMap::new();
    for raw in set {
        let (name, value) = parse_header(raw)?;
        let name = stored_name(&name).unwrap_or(name);
        if parsed
            .keys()
            .any(|existing: &String| existing.eq_ignore_ascii_case(&name))
        {
            invalid!("header '{name}' is passed more than once");
        }
        parsed.insert(name, value);
    }
    let mut headers = Map::new();
    for name in stored {
        let replaced = parsed.keys().any(|new| new.eq_ignore_ascii_case(name));
        let removed = remove.iter().any(|gone| gone.eq_ignore_ascii_case(name));
        if !replaced && !removed {
            headers.insert(name.to_string(), Value::Null);
        }
    }
    for (name, value) in parsed {
        headers.insert(name, json!(value));
    }
    Ok(headers)
}

pub fn delete(
    client: &ApiClient,
    scope: &WorkflowScope,
    workflow_id: &str,
    sink_id: &str,
    json_mode: bool,
) -> Result<()> {
    let workflow = load(client, scope, workflow_id)?;
    find_sink(&workflow, workflow_id, sink_id)?;
    let sinks = workflow
        .sinks
        .iter()
        .filter(|sink| sink["id"].as_str() != Some(sink_id))
        .map(retained)
        .collect();
    let updated = replace_sinks(client, scope, workflow_id, sinks)?;
    output::print_result(
        &json!({"workflow_id": workflow_id, "sink_id": sink_id, "deleted": true}),
        json_mode,
        |_| println!("Sink {sink_id} removed from workflow '{}'.", updated.name),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(tables: &[&str], urls: &[&str], headers: &[&str], json: &[&str]) -> SinkArgs {
        let owned = |values: &[&str]| values.iter().map(|value| value.to_string()).collect();
        SinkArgs {
            tables: owned(tables),
            urls: owned(urls),
            headers: owned(headers),
            json: owned(json),
        }
    }

    fn error(sinks: Value) -> String {
        validate(sinks.as_array().unwrap()).unwrap_err().to_string()
    }

    #[test]
    fn typed_flags_build_sinks_with_the_workflow_database() {
        let sinks = build(
            &args(
                &["alerts", "other.copy"],
                &["https://example.com/hook"],
                &["Authorization: Bearer a:b"],
                &[r#"[{"type":"table","settings":{"database":"x","table":"y"}}]"#],
            ),
            "analytics",
        )
        .unwrap();
        assert_eq!(
            sinks,
            vec![
                json!({"type": "table", "settings": {"database": "analytics", "table": "alerts"}}),
                json!({"type": "table", "settings": {"database": "other", "table": "copy"}}),
                json!({"type": "http", "settings": {"url": "https://example.com/hook",
                    "headers": {"Authorization": "Bearer a:b"}}}),
                json!({"type": "table", "settings": {"database": "x", "table": "y"}}),
            ]
        );
    }

    #[test]
    fn headers_need_exactly_one_http_sink_and_never_echo_values() {
        let err = build(
            &args(
                &[],
                &["https://a.example", "https://b.example"],
                &["X-Key: 1"],
                &[],
            ),
            "db",
        )
        .unwrap_err();
        assert!(err.to_string().contains("single --sink-http"), "{err}");

        let err = build(
            &args(&[], &["https://a.example"], &["secret-token"], &[]),
            "db",
        )
        .unwrap_err()
        .to_string();
        assert!(!err.contains("secret-token"), "{err}");
    }

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
            r#"{"type":"table","settings":{"database":"a","table":"b"}}"#.into(),
        ])
        .unwrap();
        assert_eq!(sinks.len(), 2);

        let err = read_json_args(&["[".into()]).unwrap_err().to_string();
        assert!(err.contains("not valid JSON"), "{err}");
        let err = read_json_args(&["42".into()]).unwrap_err().to_string();
        assert!(err.contains("object or an array"), "{err}");
        let err = read_json_args(&["@missing.json".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing.json"), "{err}");
    }

    #[test]
    fn validation_checks_shape_and_points_to_the_schema() {
        for (sinks, expected) in [
            (json!([{"settings": {}}]), r#"missing "type""#),
            (json!(["table"]), "expected a JSON object"),
            (
                json!([{"type": "table", "settings": {"database": "a"}}]),
                "settings.table is required",
            ),
            (
                json!([{"type": "table", "settings": {"database": "a", "table": 1}}]),
                "settings.table is required and must be a string",
            ),
            (
                json!([{"type": "table", "name": "x", "settings": {"database": "a", "table": "b"}}]),
                r#"unknown field "name""#,
            ),
            (
                json!([{"type": "http", "settings": {"uri": "https://a.example"}}]),
                r#"unknown field "settings.uri""#,
            ),
            (json!([{"type": "http"}]), "settings.url is required"),
            (
                json!([{"type": "http", "settings": {"url": "https://a.example", "headers": {"X": 1}}}]),
                "header 'X' must be a string",
            ),
            (
                json!([{"type": "http", "id": 7}]),
                r#""id" must be a string"#,
            ),
        ] {
            let err = error(sinks);
            assert!(err.contains(expected), "expected {expected:?} in {err}");
        }
        assert!(error(json!([{"type": "http"}])).contains("rtree workflow sink schema --type http"));
    }

    #[test]
    fn values_and_limits_are_left_to_the_api() {
        validate(&[
            json!({"type": "table", "settings": {"database": "1bad", "table": "a.b"}}),
            json!({"type": "table", "settings": {"database": "1bad", "table": "a.b"}}),
            json!({"type": "http", "settings": {"url": "not a url", "headers": {"X": null}}}),
            json!({"type": "http", "id": "s"}),
            json!({"type": "http", "id": "s"}),
            json!({"type": "http", "id": "t"}),
        ])
        .unwrap();
    }

    #[test]
    fn existing_sinks_may_omit_http_settings_and_unknown_types_pass_through() {
        validate(&[
            json!({"type": "http", "id": "s1"}),
            json!({"type": "http", "id": "s2", "settings": {"headers": {"Authorization": null}}}),
            json!({"type": "queue", "settings": {"anything": true}}),
        ])
        .unwrap();
    }

    #[test]
    fn retained_sinks_keep_table_targets_and_omit_http_settings() {
        assert_eq!(
            retained(
                &json!({"type": "table", "id": "t", "settings": {"database": "a", "table": "b"}})
            ),
            json!({"type": "table", "id": "t", "settings": {"database": "a", "table": "b"}})
        );
        assert_eq!(
            retained(
                &json!({"type": "http", "id": "h", "settings": {"url": "https://a.example",
                "url_configured": true, "header_names": ["Authorization"]}})
            ),
            json!({"type": "http", "id": "h"})
        );
    }

    #[test]
    fn header_changes_keep_other_stored_headers() {
        let headers = merged_headers(
            &["Authorization", "X-Team", "X-Old"],
            &["authorization: Bearer new".into(), "X-New: 1".into()],
            &["x-old".into()],
        )
        .unwrap();
        assert_eq!(
            Value::Object(headers),
            json!({"X-Team": null, "Authorization": "Bearer new", "X-New": "1"})
        );
        assert!(merged_headers(&["A"], &[], &["B".into()]).is_err());
    }

    #[test]
    fn sink_updates_reject_flags_for_the_other_type() {
        let table =
            json!({"type": "table", "id": "t", "settings": {"database": "a", "table": "b"}});
        let http = json!({"type": "http", "id": "h", "settings": {"header_names": []}});
        let changes = |table: Option<&str>, url: Option<&str>| SinkChanges {
            table: table.map(str::to_string),
            url: url.map(str::to_string),
            headers: vec![],
            remove_headers: vec![],
        };
        assert_eq!(
            changed_sink(&table, changes(Some("c"), None)).unwrap(),
            json!({"type": "table", "id": "t", "settings": {"database": "a", "table": "c"}})
        );
        assert!(changed_sink(&table, changes(None, Some("https://a.example"))).is_err());
        assert!(changed_sink(&http, changes(Some("c"), None)).is_err());
        assert!(changed_sink(&http, changes(None, None)).is_err());
        assert_eq!(
            changed_sink(&http, changes(None, Some("https://a.example"))).unwrap(),
            json!({"type": "http", "id": "h", "settings": {"url": "https://a.example"}})
        );
    }

    #[test]
    fn schema_examples_pass_validation() {
        for sink_type in &SINK_TYPES {
            validate(&[serde_json::from_str(sink_type.example).unwrap()]).unwrap();
        }
    }
}
