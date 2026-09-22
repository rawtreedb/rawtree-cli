use anyhow::{Context, Result};
use comfy_table::{Cell, CellAlignment};
use serde::Deserialize;
use serde_json::{json, Value};

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
    #[serde(default)]
    sorting_key: Vec<String>,
}

#[derive(Deserialize)]
struct UpdateTableResponse {
    table: String,
    sorting_key: Vec<String>,
}

#[derive(Deserialize)]
struct CreateTableResponse {
    database: String,
    table: String,
    #[serde(default)]
    sorting_key: Vec<String>,
    #[serde(default)]
    storage: Option<TableStorage>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TableStorage {
    Default,
    S3 {
        data: TableS3Destination,
        backups: TableS3Destination,
    },
    /// A storage type this CLI version predates. The table is still created, so
    /// it must not fail the response parse.
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct TableS3Destination {
    bucket: String,
    path: String,
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

pub fn create(
    client: &ApiClient,
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    table: &str,
    sorting_key: Option<&[String]>,
    json_mode: bool,
) -> Result<()> {
    let mut body = serde_json::Map::new();
    body.insert("name".to_string(), json!(table));
    if let Some(columns) = sorting_key {
        let columns = normalize_sorting_key(columns);
        if columns.is_empty() {
            anyhow::bail!("--sorting-key must list at least one column.");
        }
        body.insert("sorting_key".to_string(), json!(columns));
    }

    let create_path = org::database_scoped_path(database, "/tables", organization, cluster);
    let value: Value = client.post(&create_path, &Value::Object(body))?;
    let created: CreateTableResponse =
        serde_json::from_value(value.clone()).context("invalid table response from server")?;

    output::print_result(&value, json_mode, |_| {
        println!(
            "Table '{}' created in database '{}'.",
            created.table, created.database
        );
        println!("Sorting key: {}", format_sorting_key(&created.sorting_key));
        if let Some(storage) = format_storage(created.storage.as_ref()) {
            println!("Storage: {storage}");
        }
    });
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
            "columns": details.columns.iter().map(|c| json!({
                "name": c.name,
                "type": c.col_type,
            })).collect::<Vec<_>>(),
            "sorting_key": details.sorting_key,
        }),
        json_mode,
        |_| {
            println!("Table: {}", details.name);
            println!("Rows: {}", details.total_rows);
            println!("Size: {}", format_bytes(details.total_bytes));
            println!("Created at: {}", details.created_at);
            println!("Sorting key: {}", format_sorting_key(&details.sorting_key));
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

pub fn update(
    client: &ApiClient,
    database: &str,
    organization: Option<&str>,
    cluster: Option<&str>,
    table: &str,
    sorting_key: &[String],
    json_mode: bool,
) -> Result<()> {
    let sorting_key = normalize_sorting_key(sorting_key);
    if sorting_key.is_empty() {
        anyhow::bail!("--sorting-key must list at least one column.");
    }

    let update_path =
        org::database_scoped_path(database, &format!("/tables/{table}"), organization, cluster);
    let value: Value = client.patch(&update_path, &json!({ "sorting_key": sorting_key }))?;
    let updated: UpdateTableResponse =
        serde_json::from_value(value.clone()).context("invalid table response from server")?;

    output::print_result(&value, json_mode, |_| {
        println!(
            "Table '{}' sorting key set to: {}.",
            updated.table,
            format_sorting_key(&updated.sorting_key)
        );
        println!("New parts use it right away; existing parts are re-sorted as merges run.");
    });
    Ok(())
}

fn normalize_sorting_key(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| column.trim().to_string())
        .filter(|column| !column.is_empty())
        .collect()
}

fn format_sorting_key(sorting_key: &[String]) -> String {
    if sorting_key.is_empty() {
        "auto (chosen per part)".to_string()
    } else {
        sorting_key.join(", ")
    }
}

fn format_storage(storage: Option<&TableStorage>) -> Option<String> {
    match storage? {
        TableStorage::Default => Some("default".to_string()),
        TableStorage::S3 { data, backups } => Some(format!(
            "S3 (data s3://{}/{}, backups s3://{}/{})",
            data.bucket, data.path, backups.bucket, backups.path
        )),
        TableStorage::Unknown => None,
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

#[cfg(test)]
mod tests {
    use super::{
        format_sorting_key, format_storage, normalize_sorting_key, CreateTableResponse,
        DescribeTableResponse, TablesResponse, UpdateTableResponse,
    };

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
                "sorting_key": ["event"]
            }
        }"#;

        let resp: DescribeTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.table.name, "events");
        assert_eq!(resp.table.total_rows, 1200);
        assert_eq!(resp.table.total_bytes, 98304);
        assert_eq!(resp.table.columns.len(), 1);
        assert_eq!(resp.table.sorting_key, vec!["event".to_string()]);
    }

    #[test]
    fn describe_response_accepts_legacy_field_names() {
        let payload = r#"{
            "table": {
                "name": "events",
                "created_at": "2026-01-01 10:00:00",
                "rows": 1200,
                "size": 98304,
                "columns": [{"name": "event", "type": "String"}]
            }
        }"#;

        let resp: DescribeTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.table.total_rows, 1200);
        assert_eq!(resp.table.total_bytes, 98304);
        assert_eq!(resp.table.columns.len(), 1);
    }

    #[test]
    fn describe_response_defaults_sorting_key_when_server_omits_it() {
        let payload = r#"{
            "table": {
                "name": "events",
                "created_at": "2026-01-01 10:00:00",
                "total_rows": 1200,
                "total_bytes": 98304,
                "columns": [{"name": "event", "type": "String"}]
            }
        }"#;

        let resp: DescribeTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert!(resp.table.sorting_key.is_empty());
    }

    #[test]
    fn update_response_reads_sorting_key() {
        let payload = r#"{
            "database": "analytics",
            "table": "events",
            "sorting_key": ["region", "user.id"]
        }"#;

        let resp: UpdateTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.table, "events");
        assert_eq!(
            resp.sorting_key,
            vec!["region".to_string(), "user.id".to_string()]
        );
    }

    #[test]
    fn normalize_sorting_key_trims_and_drops_empty_columns() {
        assert_eq!(
            normalize_sorting_key(&[" region ".to_string(), String::new(), "user.id".to_string()]),
            vec!["region".to_string(), "user.id".to_string()]
        );
        assert!(normalize_sorting_key(&["  ".to_string()]).is_empty());
    }

    #[test]
    fn format_sorting_key_names_the_automatic_key() {
        assert_eq!(format_sorting_key(&[]), "auto (chosen per part)");
        assert_eq!(
            format_sorting_key(&["region".to_string(), "user.id".to_string()]),
            "region, user.id"
        );
    }

    #[test]
    fn create_response_reads_sorting_key_and_default_storage() {
        let payload = r#"{
            "database": "analytics",
            "table": "events",
            "storage": {"type": "default"},
            "sorting_key": ["region", "user.id"]
        }"#;

        let resp: CreateTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert_eq!(resp.database, "analytics");
        assert_eq!(resp.table, "events");
        assert_eq!(
            resp.sorting_key,
            vec!["region".to_string(), "user.id".to_string()]
        );
        assert_eq!(
            format_storage(resp.storage.as_ref()).as_deref(),
            Some("default")
        );
    }

    #[test]
    fn create_response_formats_s3_storage_destinations() {
        let payload = r#"{
            "database": "analytics",
            "table": "events",
            "storage": {
                "type": "s3",
                "data": {"bucket": "data-bucket", "path": "events"},
                "backups": {"bucket": "backup-bucket", "path": "events/backups"}
            },
            "sorting_key": []
        }"#;

        let resp: CreateTableResponse = serde_json::from_str(payload).expect("valid payload");
        assert!(resp.sorting_key.is_empty());
        assert_eq!(
            format_storage(resp.storage.as_ref()).as_deref(),
            Some("S3 (data s3://data-bucket/events, backups s3://backup-bucket/events/backups)")
        );
    }

    #[test]
    fn create_response_tolerates_unknown_and_missing_storage() {
        let unknown = r#"{
            "database": "analytics",
            "table": "events",
            "storage": {"type": "gcs", "data": {"bucket": "b"}},
            "sorting_key": []
        }"#;
        let resp: CreateTableResponse = serde_json::from_str(unknown).expect("valid payload");
        assert!(format_storage(resp.storage.as_ref()).is_none());

        let missing = r#"{"database": "analytics", "table": "events"}"#;
        let resp: CreateTableResponse = serde_json::from_str(missing).expect("valid payload");
        assert!(resp.sorting_key.is_empty());
        assert!(format_storage(resp.storage.as_ref()).is_none());
    }
}
