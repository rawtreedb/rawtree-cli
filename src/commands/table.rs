use std::collections::HashMap;

use anyhow::Result;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ApiClient;
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

pub fn list(client: &ApiClient, project: &str, json_mode: bool) -> Result<()> {
    let resp: TablesResponse = client.get(&format!("/v1/{}/tables", project))?;
    let total_rows = fetch_all_total_rows(client, project)?;
    let tables = resp
        .tables
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "total_rows": total_rows.get(name).cloned(),
            })
        })
        .collect::<Vec<_>>();
    output::print_result(&json!({"tables": tables}), json_mode, |_| {
        if resp.tables.is_empty() {
            println!("No tables yet. Insert data to auto-create a table.");
        } else {
            let mut table = new_cli_table();
            table.set_header(vec!["table", "rows"]);
            for t in &resp.tables {
                match total_rows.get(t) {
                    Some(total_rows) => table.add_row(vec![
                        Cell::new(t),
                        Cell::new(format_total_rows(total_rows))
                            .set_alignment(CellAlignment::Right),
                    ]),
                    None => table.add_row(vec![
                        Cell::new(t),
                        Cell::new("unavailable").set_alignment(CellAlignment::Right),
                    ]),
                };
            }
            println!("{table}");
        }
    });
    Ok(())
}

pub fn describe(client: &ApiClient, project: &str, table: &str, json_mode: bool) -> Result<()> {
    let resp: DescribeTableResponse = client.get(&format!("/v1/{}/tables/{}", project, table))?;
    let total_rows = fetch_total_rows(client, project, &resp.table)?;
    output::print_result(
        &json!({
            "table": resp.table,
            "total_rows": total_rows,
            "columns": resp.columns.iter().map(|c| json!({
                "name": c.name,
                "type": c.col_type,
            })).collect::<Vec<_>>(),
        }),
        json_mode,
        |_| {
            let mut summary = new_cli_table();
            summary.set_header(vec!["table", "rows"]);
            summary.add_row(vec![
                Cell::new(&resp.table),
                Cell::new(
                    total_rows
                        .as_ref()
                        .map(format_total_rows)
                        .unwrap_or_else(|| "unavailable".to_string()),
                )
                .set_alignment(CellAlignment::Right),
            ]);

            let mut columns = new_cli_table();
            columns.set_header(vec!["column", "type"]);
            for col in &resp.columns {
                columns.add_row(vec![Cell::new(&col.name), Cell::new(&col.col_type)]);
            }

            println!("{summary}");
            println!();
            println!("{columns}");
        },
    );
    Ok(())
}

fn fetch_total_rows(client: &ApiClient, project: &str, table: &str) -> Result<Option<Value>> {
    let sql = format!(
        "SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = '{}' AND engine != 'View' LIMIT 1",
        table
    );
    let resp: QueryResponse =
        client.post(&format!("/v1/{}/query", project), &json!({ "sql": sql }))?;
    Ok(resp
        .data
        .into_iter()
        .next()
        .and_then(|row| row.get("total_rows").cloned()))
}

fn fetch_all_total_rows(client: &ApiClient, project: &str) -> Result<HashMap<String, Value>> {
    let sql = "SELECT name, total_rows FROM system.tables WHERE database = currentDatabase() AND engine != 'View'";
    let resp: QueryResponse =
        client.post(&format!("/v1/{}/query", project), &json!({ "sql": sql }))?;
    Ok(resp
        .data
        .into_iter()
        .filter_map(|row| {
            let name = row.get("name")?.as_str()?.to_string();
            let total_rows = row.get("total_rows")?.clone();
            Some((name, total_rows))
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

fn new_cli_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}
