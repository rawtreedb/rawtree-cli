use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use comfy_table::{Cell, CellAlignment};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::logs::parse_duration;
use super::table_output::new_cli_table;
use crate::cli::WorkflowTimeRange;
use crate::client::ApiClient;
use crate::org;
use crate::output;

/// Organization and cluster are mandatory on every workflow route, even for
/// API keys that are already bound to a cluster.
pub struct WorkflowScope<'a> {
    pub organization: &'a str,
    pub cluster: &'a str,
}

impl WorkflowScope<'_> {
    fn path(&self, suffix: &str) -> String {
        org::scoped_path(
            &format!("/v1/workflows{suffix}"),
            Some(self.organization),
            Some(self.cluster),
        )
    }

    fn workflow_path(&self, id: &str, suffix: &str) -> String {
        self.path(&format!("/{}{suffix}", urlencoding::encode(id)))
    }
}

#[derive(Deserialize)]
struct WorkflowQuery {
    database: String,
    sql: String,
}

#[derive(Deserialize)]
struct Workflow {
    id: String,
    name: String,
    query: WorkflowQuery,
    enabled: bool,
    revision: i64,
    /// `None` for manual-only workflows, which have no schedule.
    interval_seconds: Option<i64>,
    next_run_at: Option<String>,
    created_at: String,
    updated_at: String,
    #[serde(default)]
    sinks: Vec<Value>,
}

impl Workflow {
    fn status(&self) -> &'static str {
        match (self.interval_seconds, self.enabled) {
            (None, _) => "manual",
            (Some(_), true) => "active",
            (Some(_), false) => "paused",
        }
    }
}

#[derive(Deserialize)]
struct ListWorkflowsResponse {
    workflows: Vec<Workflow>,
}

#[derive(Deserialize)]
struct AcceptedRun {
    id: String,
    workflow_id: String,
}

#[derive(Deserialize)]
struct WorkflowRun {
    id: String,
    source: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
}

#[derive(Deserialize)]
struct WorkflowRunsResponse {
    runs: Vec<WorkflowRun>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct WorkflowLog {
    event_type: String,
    timestamp_ms: i64,
    outcome: String,
    run_id: Option<String>,
    sink_id: Option<String>,
    sink_type: Option<String>,
    status_code: Option<u16>,
    error_code: Option<String>,
    duration_ms: Option<u64>,
    match_count: Option<u64>,
    written_rows: Option<u64>,
}

#[derive(Deserialize)]
struct WorkflowLogsResponse {
    logs: Vec<WorkflowLog>,
    next_cursor: Option<String>,
    from: i64,
    to: i64,
}

#[derive(Deserialize)]
struct MetricCounts {
    executions: u64,
    matches: u64,
    sink_events: u64,
    errors: u64,
}

#[derive(Deserialize)]
struct MetricPoint {
    bucket_ms: i64,
    #[serde(flatten)]
    counts: MetricCounts,
}

#[derive(Deserialize)]
struct WorkflowMetricsResponse {
    points: Vec<MetricPoint>,
    totals: MetricCounts,
    from: i64,
    to: i64,
}

pub struct CreateWorkflow {
    pub name: String,
    pub database: String,
    pub sql: String,
    pub interval_seconds: Option<u32>,
    pub disabled: bool,
    pub sinks: Vec<String>,
}

pub struct UpdateWorkflow {
    pub name: Option<String>,
    pub database: Option<String>,
    pub sql: Option<String>,
    pub interval_seconds: Option<Option<u32>>,
    pub enabled: Option<bool>,
    pub sinks: Option<Vec<String>>,
}

/// Prints the API response verbatim in JSON mode so fields this CLI does not
/// know about are not dropped; human output reads only the fields it renders.
fn print_response<T: DeserializeOwned>(
    value: Value,
    json_mode: bool,
    human: impl FnOnce(T),
) -> Result<()> {
    if json_mode {
        output::print_result(&value, true, |_| {});
    } else {
        human(serde_json::from_value(value).context("failed to parse server response")?);
    }
    Ok(())
}

fn parse_sinks(sinks: &[String]) -> Result<Vec<Value>> {
    sinks
        .iter()
        .map(|sink| match serde_json::from_str::<Value>(sink) {
            Ok(value @ Value::Object(_)) => Ok(value),
            _ => bail!(
                "invalid --sink '{sink}': expected a JSON object such as {{\"type\":\"table\",\"settings\":{{\"database\":\"analytics\",\"table\":\"alerts\"}}}}"
            ),
        })
        .collect()
}

fn create_body(request: CreateWorkflow) -> Result<Value> {
    Ok(json!({
        "name": request.name,
        "query": {"database": request.database, "sql": request.sql},
        "enabled": !request.disabled,
        "interval_seconds": request.interval_seconds,
        "sinks": parse_sinks(&request.sinks)?,
    }))
}

fn update_body(request: UpdateWorkflow) -> Result<Value> {
    let mut body = Map::new();
    if let Some(name) = request.name {
        body.insert("name".into(), json!(name));
    }
    let mut query = Map::new();
    if let Some(database) = request.database {
        query.insert("database".into(), json!(database));
    }
    if let Some(sql) = request.sql {
        query.insert("sql".into(), json!(sql));
    }
    if !query.is_empty() {
        body.insert("query".into(), Value::Object(query));
    }
    if let Some(enabled) = request.enabled {
        body.insert("enabled".into(), json!(enabled));
    }
    if let Some(interval) = request.interval_seconds {
        body.insert("interval_seconds".into(), json!(interval));
    }
    if let Some(sinks) = request.sinks {
        body.insert("sinks".into(), Value::Array(parse_sinks(&sinks)?));
    }
    if body.is_empty() {
        bail!("nothing to update: pass at least one of --name, --database, --sql, --interval-seconds, --enable, --disable, --sink, or --clear-sinks");
    }
    Ok(Value::Object(body))
}

fn rfc3339_to_ms(value: &str, flag: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.timestamp_millis())
        .map_err(|_| {
            anyhow::anyhow!(
                "invalid {flag} '{value}': expected RFC 3339, e.g. 2026-03-28T18:00:00Z"
            )
        })
}

/// Resolves the window to Unix milliseconds. Unset bounds are left to the
/// server, which defaults to the 24 hours before `to`.
fn resolve_window(
    range: &WorkflowTimeRange,
    now: DateTime<Utc>,
) -> Result<(Option<i64>, Option<i64>)> {
    if range.start_time.is_some() || range.end_time.is_some() {
        let from = range
            .start_time
            .as_deref()
            .map(|value| rfc3339_to_ms(value, "--start-time"))
            .transpose()?;
        let to = range
            .end_time
            .as_deref()
            .map(|value| rfc3339_to_ms(value, "--end-time"))
            .transpose()?;
        if let (Some(from), Some(to)) = (from, to) {
            if from >= to {
                bail!("invalid time range: --start-time must be before --end-time");
            }
        }
        return Ok((from, to));
    }
    let from = range
        .since
        .as_deref()
        .map(|since| parse_duration(since).map(|delta| (now - delta).timestamp_millis()))
        .transpose()?;
    let to = range
        .until
        .as_deref()
        .map(|until| parse_duration(until).map(|delta| (now - delta).timestamp_millis()))
        .transpose()?;
    if let (Some(from), Some(to)) = (from, to) {
        if from >= to {
            bail!("invalid time range: --since must be greater than --until");
        }
    }
    Ok((from, to))
}

fn window_params(range: &WorkflowTimeRange) -> Result<Vec<String>> {
    let (from, to) = resolve_window(range, Utc::now())?;
    let mut params = Vec::new();
    if let Some(from) = from {
        params.push(format!("from={from}"));
    }
    if let Some(to) = to {
        params.push(format!("to={to}"));
    }
    Ok(params)
}

fn with_params(scoped_path: String, params: &[String]) -> String {
    params
        .iter()
        .fold(scoped_path, |path, param| format!("{path}&{param}"))
}

fn format_ms(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| ms.to_string())
}

fn format_interval(seconds: Option<i64>) -> String {
    match seconds {
        None => "—".into(),
        Some(s) if s % 3600 == 0 => format!("{}h", s / 3600),
        Some(s) if s % 60 == 0 => format!("{}m", s / 60),
        Some(s) => format!("{s}s"),
    }
}

fn describe_sink(sink: &Value) -> String {
    let id = sink["id"].as_str().unwrap_or("—");
    let settings = &sink["settings"];
    match sink["type"].as_str() {
        Some("table") => format!(
            "table {}.{}  id={id}",
            settings["database"].as_str().unwrap_or("?"),
            settings["table"].as_str().unwrap_or("?")
        ),
        Some("http") => {
            let url = settings["url"].as_str().unwrap_or("URL unavailable");
            let headers = settings["header_names"]
                .as_array()
                .map(|names| {
                    names
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            if headers.is_empty() {
                format!("http {url}  id={id}")
            } else {
                format!("http {url}  id={id}  headers={headers}")
            }
        }
        _ => sink.to_string(),
    }
}

fn print_workflow(workflow: &Workflow) {
    println!("  id:       {}", workflow.id);
    println!("  name:     {}", workflow.name);
    println!("  database: {}", workflow.query.database);
    println!("  status:   {}", workflow.status());
    println!("  interval: {}", format_interval(workflow.interval_seconds));
    println!(
        "  next run: {}",
        workflow.next_run_at.as_deref().unwrap_or("—")
    );
    println!("  revision: {}", workflow.revision);
    println!("  created:  {}", workflow.created_at);
    println!("  updated:  {}", workflow.updated_at);
    if workflow.sinks.is_empty() {
        println!("  sinks:    none");
    } else {
        println!("  sinks:");
        for sink in &workflow.sinks {
            println!("    - {}", describe_sink(sink));
        }
    }
    println!("  sql:");
    for line in workflow.query.sql.lines() {
        println!("    {line}");
    }
}

pub fn list(client: &ApiClient, scope: &WorkflowScope, json_mode: bool) -> Result<()> {
    let value: Value = client.get(&scope.path(""))?;
    print_response(value, json_mode, |resp: ListWorkflowsResponse| {
        if resp.workflows.is_empty() {
            println!("No workflows yet. Create one with `rtree workflow create`.");
            return;
        }
        let mut table = new_cli_table();
        table.set_header(vec![
            "name", "database", "status", "interval", "next run", "sinks", "updated", "id",
        ]);
        for workflow in &resp.workflows {
            table.add_row(vec![
                Cell::new(&workflow.name),
                Cell::new(&workflow.query.database),
                Cell::new(workflow.status()),
                Cell::new(format_interval(workflow.interval_seconds))
                    .set_alignment(CellAlignment::Right),
                Cell::new(workflow.next_run_at.as_deref().unwrap_or("—")),
                Cell::new(workflow.sinks.len()).set_alignment(CellAlignment::Right),
                Cell::new(&workflow.updated_at),
                Cell::new(&workflow.id),
            ]);
        }
        println!("{table}");
    })
}

pub fn get(client: &ApiClient, scope: &WorkflowScope, id: &str, json_mode: bool) -> Result<()> {
    let value: Value = client.get(&scope.workflow_path(id, ""))?;
    print_response(value, json_mode, |workflow: Workflow| {
        println!("Workflow '{}':", workflow.name);
        print_workflow(&workflow);
    })
}

pub fn create(
    client: &ApiClient,
    scope: &WorkflowScope,
    request: CreateWorkflow,
    json_mode: bool,
) -> Result<()> {
    let value: Value = client.post(&scope.path(""), &create_body(request)?)?;
    print_response(value, json_mode, |workflow: Workflow| {
        println!("Workflow '{}' created:", workflow.name);
        print_workflow(&workflow);
    })
}

pub fn update(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    request: UpdateWorkflow,
    json_mode: bool,
) -> Result<()> {
    let value: Value = client.patch(&scope.workflow_path(id, ""), &update_body(request)?)?;
    print_response(value, json_mode, |workflow: Workflow| {
        println!("Workflow '{}' updated:", workflow.name);
        print_workflow(&workflow);
    })
}

pub fn delete(client: &ApiClient, scope: &WorkflowScope, id: &str, json_mode: bool) -> Result<()> {
    client.delete_no_content(&scope.workflow_path(id, ""))?;
    output::print_result(&json!({"id": id, "deleted": true}), json_mode, |_| {
        println!("Workflow '{id}' deleted.");
    });
    Ok(())
}

pub fn run(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    idempotency_key: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let value: Value =
        client.post_empty_idempotent(&scope.workflow_path(id, "/runs"), idempotency_key)?;
    print_response(value, json_mode, |run: AcceptedRun| {
        println!("Run {} started for workflow {}.", run.id, run.workflow_id);
        println!(
            "Follow it with `rtree workflow runs {}` or `rtree workflow logs {}`.",
            run.workflow_id, run.workflow_id
        );
    })
}

pub fn runs(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    limit: Option<u16>,
    cursor: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let mut params = Vec::new();
    if let Some(limit) = limit {
        params.push(format!("limit={limit}"));
    }
    if let Some(cursor) = cursor {
        params.push(format!("cursor={}", urlencoding::encode(cursor)));
    }
    let path = with_params(scope.workflow_path(id, "/runs"), &params);
    let value: Value = client.get(&path)?;
    print_response(value, json_mode, |resp: WorkflowRunsResponse| {
        if resp.runs.is_empty() {
            println!("No runs found.");
        } else {
            let mut table = new_cli_table();
            table.set_header(vec!["started", "finished", "source", "status", "id"]);
            for run in &resp.runs {
                table.add_row(vec![
                    Cell::new(&run.started_at),
                    Cell::new(run.finished_at.as_deref().unwrap_or("—")),
                    Cell::new(&run.source),
                    Cell::new(&run.status),
                    Cell::new(&run.id),
                ]);
            }
            println!("{table}");
        }
        if let Some(cursor) = resp.next_cursor {
            println!("More runs: rtree workflow runs {id} --cursor {cursor}");
        }
    })
}

pub fn cancel(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    run_id: &str,
    json_mode: bool,
) -> Result<()> {
    let path = scope.workflow_path(id, &format!("/runs/{}/cancel", urlencoding::encode(run_id)));
    client.post_no_content(&path)?;
    output::print_result(
        &json!({"workflow_id": id, "run_id": run_id, "cancel_requested": true}),
        json_mode,
        |_| println!("Cancellation requested for run {run_id}."),
    );
    Ok(())
}

fn log_detail(log: &WorkflowLog) -> String {
    let mut parts = Vec::new();
    if let Some(sink_type) = &log.sink_type {
        parts.push(format!(
            "sink={sink_type}:{}",
            log.sink_id.as_deref().unwrap_or("?")
        ));
    }
    if let Some(status) = log.status_code {
        parts.push(format!("status={status}"));
    }
    if let Some(error) = &log.error_code {
        parts.push(format!("error={error}"));
    }
    if let Some(matches) = log.match_count {
        parts.push(format!("matches={matches}"));
    }
    if let Some(rows) = log.written_rows {
        parts.push(format!("written={rows}"));
    }
    if let Some(duration) = log.duration_ms {
        parts.push(format!("{duration}ms"));
    }
    parts.join("  ")
}

pub fn logs(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    range: &WorkflowTimeRange,
    limit: Option<u16>,
    cursor: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let mut params = window_params(range)?;
    if let Some(limit) = limit {
        params.push(format!("limit={limit}"));
    }
    if let Some(cursor) = cursor {
        params.push(format!("cursor={}", urlencoding::encode(cursor)));
    }
    let path = with_params(scope.workflow_path(id, "/logs"), &params);
    let value: Value = client.get(&path)?;
    print_response(value, json_mode, |resp: WorkflowLogsResponse| {
        if resp.logs.is_empty() {
            println!(
                "No workflow logs between {} and {}.",
                format_ms(resp.from),
                format_ms(resp.to)
            );
        } else {
            let mut table = new_cli_table();
            table.set_header(vec!["time", "event", "outcome", "run", "detail"]);
            for log in &resp.logs {
                table.add_row(vec![
                    Cell::new(format_ms(log.timestamp_ms)),
                    Cell::new(&log.event_type),
                    Cell::new(&log.outcome),
                    Cell::new(log.run_id.as_deref().unwrap_or("—")),
                    Cell::new(log_detail(log)),
                ]);
            }
            println!("{table}");
        }
        if let Some(cursor) = resp.next_cursor {
            println!(
                "More logs: rtree workflow logs {id} --start-time {} --end-time {} --cursor {cursor}",
                format_ms(resp.from),
                format_ms(resp.to)
            );
        }
    })
}

pub fn metrics(
    client: &ApiClient,
    scope: &WorkflowScope,
    id: &str,
    range: &WorkflowTimeRange,
    json_mode: bool,
) -> Result<()> {
    let params = window_params(range)?;
    let path = with_params(scope.workflow_path(id, "/metrics"), &params);
    let value: Value = client.get(&path)?;
    print_response(value, json_mode, |resp: WorkflowMetricsResponse| {
        println!(
            "Workflow metrics from {} to {}:",
            format_ms(resp.from),
            format_ms(resp.to)
        );
        let mut table = new_cli_table();
        table.set_header(vec![
            "bucket",
            "executions",
            "matches",
            "sink events",
            "errors",
        ]);
        let row = |label: String, counts: &MetricCounts| {
            vec![
                Cell::new(label),
                Cell::new(counts.executions).set_alignment(CellAlignment::Right),
                Cell::new(counts.matches).set_alignment(CellAlignment::Right),
                Cell::new(counts.sink_events).set_alignment(CellAlignment::Right),
                Cell::new(counts.errors).set_alignment(CellAlignment::Right),
            ]
        };
        for point in &resp.points {
            table.add_row(row(format_ms(point.bucket_ms), &point.counts));
        }
        table.add_row(row("total".to_string(), &resp.totals));
        println!("{table}");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> WorkflowScope<'static> {
        WorkflowScope {
            organization: "team alpha",
            cluster: "prod/eu",
        }
    }

    #[test]
    fn paths_carry_required_scope_and_encode_ids() {
        assert_eq!(
            scope().path(""),
            "/v1/workflows?organization=team%20alpha&cluster=prod%2Feu"
        );
        assert_eq!(
            scope().workflow_path("a/b", "/runs"),
            "/v1/workflows/a%2Fb/runs?organization=team%20alpha&cluster=prod%2Feu"
        );
    }

    #[test]
    fn create_body_sends_defaults_explicitly_and_rejects_non_object_sinks() {
        let body = create_body(CreateWorkflow {
            name: "alerts".into(),
            database: "analytics".into(),
            sql: "SELECT 1".into(),
            interval_seconds: None,
            disabled: true,
            sinks: vec![r#"{"type":"table","settings":{"database":"a","table":"b"}}"#.into()],
        })
        .unwrap();
        assert_eq!(
            body,
            json!({
                "name": "alerts",
                "query": {"database": "analytics", "sql": "SELECT 1"},
                "enabled": false,
                "interval_seconds": null,
                "sinks": [{"type": "table", "settings": {"database": "a", "table": "b"}}],
            })
        );

        let err = create_body(CreateWorkflow {
            name: "alerts".into(),
            database: "analytics".into(),
            sql: "SELECT 1".into(),
            interval_seconds: Some(60),
            disabled: false,
            sinks: vec!["[]".into()],
        })
        .unwrap_err();
        assert!(err.to_string().contains("invalid --sink"));
    }

    #[test]
    fn update_body_sends_only_supplied_fields() {
        let empty = UpdateWorkflow {
            name: None,
            database: None,
            sql: None,
            interval_seconds: None,
            enabled: None,
            sinks: None,
        };
        assert!(update_body(empty).is_err());

        let body = update_body(UpdateWorkflow {
            name: None,
            database: None,
            sql: None,
            interval_seconds: Some(Some(30)),
            enabled: Some(false),
            sinks: Some(vec![]),
        })
        .unwrap();
        assert_eq!(
            body,
            json!({"interval_seconds": 30, "enabled": false, "sinks": []})
        );

        let body = update_body(UpdateWorkflow {
            name: None,
            database: None,
            sql: Some("SELECT 2".into()),
            interval_seconds: None,
            enabled: None,
            sinks: None,
        })
        .unwrap();
        assert_eq!(body, json!({"query": {"sql": "SELECT 2"}}));
    }

    #[test]
    fn format_interval_shows_only_scheduled_durations() {
        assert_eq!(format_interval(None), "—");
        assert_eq!(format_interval(Some(7200)), "2h");
        assert_eq!(format_interval(Some(120)), "2m");
        assert_eq!(format_interval(Some(45)), "45s");
    }

    #[test]
    fn window_uses_relative_or_absolute_bounds() {
        let now = DateTime::parse_from_rfc3339("2026-10-07T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let hour = 3_600_000;
        let relative = WorkflowTimeRange {
            since: Some("2h".into()),
            until: Some("1h".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_window(&relative, now).unwrap(),
            (
                Some(now.timestamp_millis() - 2 * hour),
                Some(now.timestamp_millis() - hour)
            )
        );

        let reversed = WorkflowTimeRange {
            since: Some("1h".into()),
            until: Some("2h".into()),
            ..Default::default()
        };
        assert!(resolve_window(&reversed, now).is_err());

        let reversed_absolute = WorkflowTimeRange {
            start_time: Some("2026-10-07T11:00:00Z".into()),
            end_time: Some("2026-10-07T10:00:00Z".into()),
            ..Default::default()
        };
        assert!(resolve_window(&reversed_absolute, now)
            .unwrap_err()
            .to_string()
            .contains("--start-time must be before --end-time"));

        let absolute = WorkflowTimeRange {
            start_time: Some("2026-10-07T10:00:00.250Z".into()),
            ..Default::default()
        };
        assert_eq!(
            resolve_window(&absolute, now).unwrap(),
            (Some(now.timestamp_millis() - 2 * hour + 250), None)
        );
        assert!(resolve_window(&WorkflowTimeRange::default(), now)
            .unwrap()
            .eq(&(None, None)));
    }

    #[test]
    fn format_ms_round_trips_through_start_time() {
        let ms = 1_791_374_400_123;
        assert_eq!(rfc3339_to_ms(&format_ms(ms), "--start-time").unwrap(), ms);
    }

    #[test]
    fn describes_known_and_unknown_sinks() {
        assert_eq!(
            describe_sink(
                &json!({"type": "table", "id": "s1", "settings": {"database": "a", "table": "b"}})
            ),
            "table a.b  id=s1"
        );
        assert_eq!(
            describe_sink(
                &json!({"type": "http", "id": "s2", "settings": {"url_configured": true, "header_names": ["Authorization"]}})
            ),
            "http URL unavailable  id=s2  headers=Authorization"
        );
        assert_eq!(
            describe_sink(&json!({"type": "queue"})),
            r#"{"type":"queue"}"#
        );
    }
}
