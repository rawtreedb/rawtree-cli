use std::collections::HashMap;

use anyhow::Result;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ApiClient;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct TablesResponse {
    tables: Vec<String>,
}

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Deserialize)]
struct DescribeTableResponse {
    table: String,
    columns: Vec<ColumnInfo>,
}

#[derive(Deserialize)]
struct QueryResponse {
    data: Vec<Value>,
}

struct TableMetadata {
    total_rows: Value,
    total_bytes: Value,
}

pub fn list(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let list_path = org::project_scoped_path(project, "/tables", organization);
    let resp: TablesResponse = client.get(&list_path)?;
    let metadata = fetch_all_table_metadata(client, project, organization)?;
    let tables = resp
        .tables
        .iter()
        .map(|name| {
            let table_metadata = metadata.get(name);
            json!({
                "name": name,
                "total_rows": table_metadata.map(|m| m.total_rows.clone()).unwrap_or(Value::Null),
                "total_bytes": table_metadata.map(|m| m.total_bytes.clone()).unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    output::print_result(&json!({"tables": tables}), json_mode, |_| {
        if resp.tables.is_empty() {
            println!("No tables yet. Insert data to auto-create a table.");
        } else {
            let mut table = new_cli_table();
            table.set_header(vec!["table", "rows", "size"]);
            for t in &resp.tables {
                match metadata.get(t) {
                    Some(table_metadata) => table.add_row(vec![
                        Cell::new(t),
                        Cell::new(format_total_rows(&table_metadata.total_rows))
                            .set_alignment(CellAlignment::Right),
                        Cell::new(format_total_bytes(&table_metadata.total_bytes))
                            .set_alignment(CellAlignment::Right),
                    ]),
                    None => table.add_row(vec![
                        Cell::new(t),
                        Cell::new("unavailable").set_alignment(CellAlignment::Right),
                        Cell::new("unavailable").set_alignment(CellAlignment::Right),
                    ]),
                };
            }
            println!("{table}");
        }
    });
    Ok(())
}

pub fn describe(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    table: &str,
    json_mode: bool,
) -> Result<()> {
    let describe_path =
        org::project_scoped_path(project, &format!("/tables/{table}"), organization);
    let resp: DescribeTableResponse = client.get(&describe_path)?;
    output::print_result(
        &json!({
            "table": resp.table,
            "columns": resp.columns.iter().map(|c| json!({
                "name": c.name,
                "type": c.col_type,
            })).collect::<Vec<_>>(),
        }),
        json_mode,
        |_| {
            let mut columns = new_cli_table();
            columns.set_header(vec!["column", "type"]);
            for col in &resp.columns {
                columns.add_row(vec![Cell::new(&col.name), Cell::new(&col.col_type)]);
            }

            println!("{columns}");
        },
    );
    Ok(())
}

fn fetch_all_table_metadata(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
) -> Result<HashMap<String, TableMetadata>> {
    let sql = "SELECT name, total_rows, total_bytes FROM system.tables WHERE database = currentDatabase() AND engine != 'View'";
    let query_path = org::project_scoped_path(project, "/query", organization);
    let resp: QueryResponse = client.post(&query_path, &json!({ "sql": sql }))?;
    Ok(resp
        .data
        .into_iter()
        .filter_map(|row| {
            let name = row.get("name")?.as_str()?.to_string();
            let total_rows = row.get("total_rows").cloned().unwrap_or(Value::Null);
            let total_bytes = row.get("total_bytes").cloned().unwrap_or(Value::Null);
            Some((
                name,
                TableMetadata {
                    total_rows,
                    total_bytes,
                },
            ))
        })
        .collect())
}

fn format_total_rows(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Null => "unavailable".to_string(),
        other => other.to_string(),
    }
}

fn format_total_bytes(value: &Value) -> String {
    match value {
        Value::Number(n) => n
            .as_u64()
            .map(format_bytes)
            .unwrap_or_else(|| n.to_string()),
        Value::String(s) => s
            .parse::<u64>()
            .map(format_bytes)
            .unwrap_or_else(|_| s.clone()),
        Value::Null => "unavailable".to_string(),
        other => other.to_string(),
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut size = bytes as f64;
    let mut unit_index = 0usize;
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{size:.1} {}", UNITS[unit_index])
}

fn new_cli_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}
