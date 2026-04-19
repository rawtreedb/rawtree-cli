use anyhow::Result;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table};
use serde::Deserialize;
use serde_json::json;

use crate::client::ApiClient;
use crate::org;
use crate::output;

#[derive(Deserialize)]
struct TablesResponse {
    tables: Vec<TableInfo>,
}

#[derive(Deserialize)]
struct TableInfo {
    name: String,
    created_at: String,
    rows: u64,
    size: u64,
}

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Deserialize)]
struct DescribeTableResponse {
    name: String,
    created_at: String,
    rows: u64,
    size: u64,
    columns: Vec<ColumnInfo>,
}

pub fn list(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let list_path = org::project_scoped_path(project, "/tables", organization);
    let resp: TablesResponse = client.get(&list_path)?;
    output::print_result(
        &json!({
            "tables": resp.tables.iter().map(|t| json!({
                "name": t.name,
                "created_at": t.created_at,
                "rows": t.rows,
                "size": t.size,
            })).collect::<Vec<_>>()
        }),
        json_mode,
        |_| {
        if resp.tables.is_empty() {
            println!("No tables yet. Insert data to auto-create a table.");
        } else {
            let mut table = new_cli_table();
            table.set_header(vec!["table", "rows", "size"]);
            for t in &resp.tables {
                table.add_row(vec![
                    Cell::new(&t.name),
                    Cell::new(t.rows.to_string()).set_alignment(CellAlignment::Right),
                    Cell::new(format_bytes(t.size)).set_alignment(CellAlignment::Right),
                ]);
            }
            println!("{table}");
        }
        },
    );
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
            "table": resp.name,
            "created_at": resp.created_at,
            "rows": resp.rows,
            "size": resp.size,
            "columns": resp.columns.iter().map(|c| json!({
                "name": c.name,
                "type": c.col_type,
            })).collect::<Vec<_>>(),
        }),
        json_mode,
        |_| {
            println!("Table: {}", resp.name);
            println!("Rows: {}", resp.rows);
            println!("Size: {}", format_bytes(resp.size));
            println!("Created at: {}", resp.created_at);
            println!();

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
