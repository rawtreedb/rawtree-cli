use std::collections::HashSet;

use anyhow::Result;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use serde_json::json;
use serde_json::{Map, Value};

use crate::client::ApiClient;
use crate::org;

struct QuerySummary<'a> {
    rows: Option<&'a Value>,
    statistics: Option<&'a Map<String, Value>>,
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

    if columns.is_empty() {
        println!("No rows returned.");
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
    print_query_summary(&summary);
    true
}

fn print_query_summary(summary: &QuerySummary<'_>) {
    let has_rows = summary.rows.is_some();
    let has_statistics = summary
        .statistics
        .map(|statistics| !statistics.is_empty())
        .unwrap_or(false);

    if !has_rows && !has_statistics {
        return;
    }

    println!();
    if let Some(rows) = summary.rows {
        println!("rows: {}", format_cell_value(Some(rows)));
    }

    if let Some(statistics) = summary.statistics {
        if statistics.is_empty() {
            return;
        }

        let mut statistics_table = new_cli_table();
        statistics_table.set_header(vec!["statistic", "value"]);
        for (name, value) in statistics {
            statistics_table.add_row(vec![
                Cell::new(name),
                Cell::new(format_cell_value(Some(value))),
            ]);
        }
        println!("{statistics_table}");
    }
}

fn extract_rows_and_columns<'a>(
    value: &'a Value,
) -> Option<(Vec<String>, Vec<&'a Map<String, Value>>, QuerySummary<'a>)> {
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
            let summary = QuerySummary {
                rows: obj.get("rows"),
                statistics: obj.get("statistics").and_then(Value::as_object),
            };
            (rows, columns, summary)
        }
        Value::Array(rows) => (
            rows,
            Vec::new(),
            QuerySummary {
                rows: None,
                statistics: None,
            },
        ),
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

    use super::extract_rows_and_columns;

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
        assert!(summary.statistics.is_none());
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
        assert!(summary.statistics.is_none());
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
        assert_eq!(summary.rows, Some(&json!(1)));
        let statistics = summary.statistics.expect("statistics");
        assert_eq!(statistics.get("bytes_read"), Some(&json!(25)));
        assert_eq!(statistics.get("rows_read"), Some(&json!(1)));
    }

    #[test]
    fn extract_rows_rejects_non_object_rows() {
        let value = json!({"data": [1, 2, 3]});
        assert!(extract_rows_and_columns(&value).is_none());
    }
}
