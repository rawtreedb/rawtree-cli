use anyhow::Result;
use serde::Deserialize;
use serde_json::json;

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

pub fn schema(
    client: &ApiClient,
    project: &str,
    table: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    match table {
        Some(table) => describe_table(client, project, table, json_mode),
        None => list_tables(client, project, json_mode),
    }
}

fn list_tables(client: &ApiClient, project: &str, json_mode: bool) -> Result<()> {
    let resp: TablesResponse = client.get(&format!("/v1/{}/tables", project))?;
    output::print_result(
        &json!({"tables": resp.tables}),
        json_mode,
        |_| {
            if resp.tables.is_empty() {
                println!("No tables yet. Insert data to auto-create a table.");
            } else {
                for t in &resp.tables {
                    println!("{}", t);
                }
            }
        },
    );
    Ok(())
}

fn describe_table(client: &ApiClient, project: &str, table: &str, json_mode: bool) -> Result<()> {
    let resp: DescribeTableResponse =
        client.get(&format!("/v1/{}/tables/{}", project, table))?;
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
            println!("Table: {}", resp.table);
            for col in &resp.columns {
                println!("  {:<30} {}", col.name, col.col_type);
            }
        },
    );
    Ok(())
}
