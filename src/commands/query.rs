use std::collections::HashSet;

use anyhow::Result;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use serde_json::json;
use serde_json::{Map, Value};

use crate::client::ApiClient;
use crate::org;

#[derive(Default)]
struct QuerySummary {
    rows: Option<u64>,
    elapsed_seconds: Option<f64>,
    rows_read: Option<u64>,
    bytes_read: Option<u64>,
}

pub fn query(
    client: &ApiClient,
    project: &str,
    organization: Option<&str>,
    sql: &str,
    format: Option<&str>,
    limit: Option<u64>,
    json_mode: bool,
) -> Result<()> {
    let sql = match limit {
        Some(n) => format!("{} LIMIT {}", sql.trim().trim_end_matches(';'), n),
        None => sql.to_string(),
    };

    let mut body = json!({ "sql": sql });
    if let Some(fmt) = format {
        body["format"] = json!(fmt);
    }

    let path = org::project_scoped_path(project, "/query", organization);
    let raw = client.post_raw(&path, &body)?;

    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
        if json_mode {
            println!("{}", serde_json::to_string_pretty(&value)?);
        } else if !print_json_as_table(&value) {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    } else {
        print!("{}", raw);
    }

    Ok(())
}

fn print_json_as_table(value: &Value) -> bool {
    let Some((columns, rows, summary)) = extract_rows_and_columns(value) else {
        return false;
    };
    let displayed_rows = rows.len();

    if columns.is_empty() {
        println!("No rows returned.");
        print_query_summary(&summary, displayed_rows);
        return true;
    }

    let mut table = new_cli_table();
    table.set_header(
        columns
            .iter()
            .map(|name| Cell::new(name))
            .collect::<Vec<_>>(),
    );

    for row in rows {
        let cells = columns
            .iter()
            .map(|name| Cell::new(format_cell_value(row.get(name))))
            .collect::<Vec<_>>();
        table.add_row(cells);
    }

    println!("{table}");
    print_query_summary(&summary, displayed_rows);
    true
}

fn print_query_summary(summary: &QuerySummary, displayed_rows: usize) {
    if let Some(footer) = format_query_footer(summary, displayed_rows) {
        println!();
        println!("{footer}");
    }
}

fn format_query_footer(summary: &QuerySummary, displayed_rows: usize) -> Option<String> {
    let rows_in_set = summary.rows.or(Some(displayed_rows as u64))?;
    let mut parts = vec![format!("{} rows in set", format_count(rows_in_set))];

    if let Some(elapsed) = summary.elapsed_seconds {
        parts.push(format!("Elapsed: {elapsed:.3} sec"));
    }
    if summary.rows_read.is_some() || summary.bytes_read.is_some() {
        let read_segment = match (summary.rows_read, summary.bytes_read) {
            (Some(rows_read), Some(bytes_read)) => format!(
                "Read: {} rows and {}",
                format_count(rows_read),
                format_bytes(bytes_read)
            ),
            (Some(rows_read), None) => format!("Read: {} rows", format_count(rows_read)),
            (None, Some(bytes_read)) => format!("Read: {}", format_bytes(bytes_read)),
            (None, None) => String::new(),
        };
        if !read_segment.is_empty() {
            parts.push(read_segment);
        }
    }

    Some(format!("{}.", parts.join(". ")))
}

fn extract_rows_and_columns<'a>(
    value: &'a Value,
) -> Option<(Vec<String>, Vec<&'a Map<String, Value>>, QuerySummary)> {
    let (rows, mut columns, summary) = match value {
        Value::Object(obj) => {
            let rows = obj.get("data")?.as_array()?;
            let columns = obj
                .get("meta")
                .and_then(Value::as_array)
                .map(|meta| {
                    meta.iter()
                        .filter_map(|entry| entry.get("name").and_then(Value::as_str))
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let statistics = obj.get("statistics").and_then(Value::as_object);
            let summary = QuerySummary {
                rows: parse_u64(obj.get("rows")),
                elapsed_seconds: parse_f64(statistics.and_then(|stats| stats.get("elapsed"))),
                rows_read: parse_u64(statistics.and_then(|stats| stats.get("rows_read"))),
                bytes_read: parse_u64(statistics.and_then(|stats| stats.get("bytes_read"))),
            };
            (rows, columns, summary)
        }
        Value::Array(rows) => (rows, Vec::new(), QuerySummary::default()),
        _ => return None,
    };

    let mut row_objects = Vec::with_capacity(rows.len());
    for row in rows {
        let obj = row.as_object()?;
        row_objects.push(obj);
    }

    let mut seen = columns.iter().cloned().collect::<HashSet<_>>();
    for row in &row_objects {
        for key in row.keys() {
            if seen.insert(key.clone()) {
                columns.push(key.clone());
            }
        }
    }

    Some((columns, row_objects, summary))
}

fn format_cell_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Null) | None => "null".to_string(),
        Some(Value::Bool(v)) => v.to_string(),
        Some(Value::Number(v)) => v.to_string(),
        Some(Value::String(v)) => v.clone(),
        Some(other) => other.to_string(),
    }
}

fn parse_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn format_count(value: u64) -> String {
    let chars = value.to_string().chars().rev().collect::<Vec<_>>();
    let mut out = String::new();
    for (idx, ch) in chars.iter().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(*ch);
    }
    out.chars().rev().collect()
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{extract_rows_and_columns, format_query_footer, QuerySummary};

    #[test]
    fn extract_rows_uses_meta_order_and_appends_missing_keys() {
        let value = json!({
            "meta": [{"name": "id"}, {"name": "name"}],
            "data": [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob", "extra": true}
            ]
        });

        let (columns, rows, summary) = extract_rows_and_columns(&value).expect("tabular json");
        assert_eq!(columns, vec!["id", "name", "extra"]);
        assert_eq!(rows.len(), 2);
        assert!(summary.rows.is_none());
        assert!(summary.elapsed_seconds.is_none());
        assert!(summary.rows_read.is_none());
        assert!(summary.bytes_read.is_none());
    }

    #[test]
    fn extract_rows_supports_top_level_array() {
        let value = json!([
            {"a": 1},
            {"a": 2, "b": 3}
        ]);

        let (columns, rows, summary) = extract_rows_and_columns(&value).expect("tabular json");
        assert_eq!(columns, vec!["a", "b"]);
        assert_eq!(rows.len(), 2);
        assert!(summary.rows.is_none());
        assert!(summary.elapsed_seconds.is_none());
        assert!(summary.rows_read.is_none());
        assert!(summary.bytes_read.is_none());
    }

    #[test]
    fn extract_rows_includes_rows_and_statistics_summary() {
        let value = json!({
            "rows": 1,
            "statistics": {
                "bytes_read": 25,
                "elapsed": 0.000571999,
                "rows_read": 1
            },
            "data": [{"id": 1}]
        });

        let (_, _, summary) = extract_rows_and_columns(&value).expect("tabular json");
        assert_eq!(summary.rows, Some(1));
        assert_eq!(summary.bytes_read, Some(25));
        assert_eq!(summary.rows_read, Some(1));
        assert_eq!(summary.elapsed_seconds, Some(0.000571999));
    }

    #[test]
    fn extract_rows_rejects_non_object_rows() {
        let value = json!({"data": [1, 2, 3]});
        assert!(extract_rows_and_columns(&value).is_none());
    }

    #[test]
    fn footer_includes_summary_stats_with_human_readable_values() {
        let summary = QuerySummary {
            rows: Some(5),
            elapsed_seconds: Some(0.002),
            rows_read: Some(15420),
            bytes_read: Some(15360),
        };

        let footer = format_query_footer(&summary, 5).expect("footer");
        assert_eq!(
            footer,
            "5 rows in set. Elapsed: 0.002 sec. Read: 15,420 rows and 15.0 KiB."
        );
    }

    #[test]
    fn footer_uses_displayed_row_count_when_rows_summary_missing() {
        let summary = QuerySummary::default();
        let footer = format_query_footer(&summary, 2).expect("footer");
        assert_eq!(footer, "2 rows in set.");
    }
}
