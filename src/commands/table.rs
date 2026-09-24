use anyhow::Result;
use comfy_table::{Cell, CellAlignment};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::table_output::new_cli_table;
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
    #[serde(alias = "rows")]
    total_rows: u64,
    #[serde(alias = "size")]
    total_bytes: u64,
}

#[derive(Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Deserialize)]
struct DescribeTableResponse {
    table: TableDetails,
}

#[derive(Deserialize)]
struct TableDetails {
    name: String,
    created_at: String,
    #[serde(alias = "rows")]
    total_rows: u64,
    #[serde(alias = "size")]
    total_bytes: u64,
    columns: Vec<ColumnInfo>,
    sorting_key: String,
}

#[derive(Deserialize, Serialize)]
struct TableMutationResponse {
    database: String,
    table: String,
    sorting_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<serde_json::Value>,
}

pub fn list(
    client: &ApiClient,
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let list_path = org::database_scoped_path(database, "/tables", organization, cluster);
    let resp: TablesResponse = client.get(&list_path)?;
    output::print_result(
        &json!({
            "tables": resp.tables.iter().map(|t| json!({
                "name": t.name,
                "total_rows": t.total_rows,
                "total_bytes": t.total_bytes,
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
                        Cell::new(t.total_rows.to_string()).set_alignment(CellAlignment::Right),
                        Cell::new(format_bytes(t.total_bytes)).set_alignment(CellAlignment::Right),
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
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    table: &str,
    json_mode: bool,
) -> Result<()> {
    let describe_path =
        org::database_scoped_path(database, &format!("/tables/{table}"), organization, cluster);
    let resp: DescribeTableResponse = client.get(&describe_path)?;
    let details = &resp.table;
    output::print_result(
        &json!({
            "table": details.name,
            "created_at": details.created_at,
            "total_rows": details.total_rows,
            "total_bytes": details.total_bytes,
            "sorting_key": details.sorting_key,
            "columns": details.columns.iter().map(|c| json!({
                "name": c.name,
                "type": c.col_type,
            })).collect::<Vec<_>>(),
        }),
        json_mode,
        |_| {
            println!("Table: {}", details.name);
            println!("Rows: {}", details.total_rows);
            println!("Size: {}", format_bytes(details.total_bytes));
            println!("Created at: {}", details.created_at);
            println!(
                "Sorting key: {}",
                if details.sorting_key.is_empty() {
                    "Automatic"
                } else {
                    &details.sorting_key
                }
            );
            println!();

            let mut columns = new_cli_table();
            columns.set_header(vec!["column", "type"]);
            for col in &details.columns {
                columns.add_row(vec![Cell::new(&col.name), Cell::new(&col.col_type)]);
            }

            println!("{columns}");
        },
    );
    Ok(())
}

pub fn create(
    client: &ApiClient,
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    table: &str,
    sorting_key: Option<&str>,
    json_mode: bool,
) -> Result<()> {
    let path = org::database_scoped_path(database, "/tables", organization, cluster);
    let body = match sorting_key {
        Some(key) => json!({ "name": table, "sorting_key": key }),
        None => json!({ "name": table }),
    };
    let response: TableMutationResponse = client.post(&path, &body)?;
    output::print_result(&response, json_mode, |_| {
        println!("Table '{}.{}' created.", response.database, response.table);
        println!(
            "Sorting key: {}",
            if response.sorting_key.is_empty() {
                "Automatic"
            } else {
                &response.sorting_key
            }
        );
    });
    Ok(())
}

pub fn update(
    client: &ApiClient,
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    table: &str,
    sorting_key: &str,
    json_mode: bool,
) -> Result<()> {
    let path =
        org::database_scoped_path(database, &format!("/tables/{table}"), organization, cluster);
    let response: TableMutationResponse =
        client.patch(&path, &json!({ "sorting_key": sorting_key }))?;
    output::print_result(&response, json_mode, |_| {
        println!(
            "Sorting key updated for '{}.{}'.",
            response.database, response.table
        );
        println!("Sorting key: {}", response.sorting_key);
        println!("Existing parts may keep the previous key until they are merged.");
    });
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

#[cfg(test)]
mod tests {
    use super::{create, update, DescribeTableResponse, TablesResponse};
    use crate::client::ApiClient;
    use serde_json::json;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    #[test]
    fn tables_response_accepts_new_field_names() {
        let payload = r#"{
            "tables": [{
                "name": "events",
                "total_rows": 1200,
                "total_bytes": 98304
            }]
        }"#;

        let resp: TablesResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.tables.len(), 1);
        assert_eq!(resp.tables[0].total_rows, 1200);
        assert_eq!(resp.tables[0].total_bytes, 98304);
    }

    #[test]
    fn describe_response_reads_nested_table() {
        let payload = r#"{
            "table": {
                "name": "events",
                "created_at": "2026-01-01 10:00:00",
                "total_rows": 1200,
                "total_bytes": 98304,
                "columns": [{"name": "event", "type": "String"}],
                "sorting_key": "event"
            }
        }"#;

        let resp: DescribeTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.table.name, "events");
        assert_eq!(resp.table.total_rows, 1200);
        assert_eq!(resp.table.total_bytes, 98304);
        assert_eq!(resp.table.columns.len(), 1);
        assert_eq!(resp.table.sorting_key, "event");
    }

    #[test]
    fn describe_response_accepts_legacy_field_names() {
        let payload = r#"{
            "table": {
                "name": "events",
                "created_at": "2026-01-01 10:00:00",
                "rows": 1200,
                "size": 98304,
                "columns": [{"name": "event", "type": "String"}],
                "sorting_key": ""
            }
        }"#;

        let resp: DescribeTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.table.total_rows, 1200);
        assert_eq!(resp.table.total_bytes, 98304);
        assert_eq!(resp.table.columns.len(), 1);
    }

    #[test]
    fn table_mutations_send_sorting_key_as_one_string() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            for (method, path, expected_body, response) in [
                (
                    "POST",
                    "/v1/tables?database=analytics&organization=acme&cluster=production",
                    json!({"name":"events","sorting_key":"region, ifNull(cityHash64(host, instanceId), 0)"}),
                    r#"{"database":"analytics","table":"events","sorting_key":"region, ifNull(cityHash64(host, instanceId), 0)","storage":{"type":"default"}}"#,
                ),
                (
                    "PATCH",
                    "/v1/tables/events?database=analytics&organization=acme&cluster=production",
                    json!({"sorting_key":"region, toStartOfHour(timestamp)"}),
                    r#"{"database":"analytics","table":"events","sorting_key":"region, toStartOfHour(timestamp)"}"#,
                ),
            ] {
                let (mut socket, _) = listener.accept().expect("request accepted");
                let mut reader = BufReader::new(socket.try_clone().expect("clone socket"));
                let mut start = String::new();
                reader.read_line(&mut start).expect("request line");
                assert!(
                    start.starts_with(&format!("{method} {path} HTTP/1.1")),
                    "{start}"
                );
                let mut content_length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).expect("header");
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length: ")
                    {
                        content_length = value.trim().parse().expect("content length");
                    }
                }
                let mut body = vec![0; content_length];
                reader.read_exact(&mut body).expect("request body");
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                    expected_body
                );
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).expect("response");
            }
        });

        let client = ApiClient::new(format!("http://{address}"), None);
        create(
            &client,
            "analytics",
            Some("acme"),
            Some("production"),
            "events",
            Some("region, ifNull(cityHash64(host, instanceId), 0)"),
            true,
        )
        .expect("create table");
        update(
            &client,
            "analytics",
            Some("acme"),
            Some("production"),
            "events",
            "region, toStartOfHour(timestamp)",
            true,
        )
        .expect("update table");
        server.join().expect("server assertions");
    }
}
